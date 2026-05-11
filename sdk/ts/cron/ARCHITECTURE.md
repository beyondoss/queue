# @beyond.dev/cron Architecture

Takes an array of named job definitions (spec + handler function), registers them as schedules on the beyond-queue server, subscribes the current deployment's URL to each schedule's internal topic, starts an HTTP listener that receives and dispatches fires, and blocks until the process exits.

## Data Flow

### Schedule fire (steady state)

```
beyond-queue cron worker
  │  (schedule's next_fire_at reached)
  ▼
PUT /v1/events/__cron_{name}          ← topic publish
  │
  ▼
topic fan-out
  │
  ▼
HTTP POST {BEYOND_INTERNAL_URL}/__cron/{name}   ← raw body: {}
  │
  ▼
node:http server (inside start())
  │
  ├── name not in jobMap ──► 405 / 404
  │
  ▼
job.handler(ctx)
  │
  ├── throws ──► 500  (beyond-queue retries: 10s, 30s, 60s, 300s+)
  │
  └── resolves ──► 200
```

### `start()` startup sequence

```
start(jobs)
  │
  ├─ 1. PUT /v1/schedules/{name}         per job  (upsert, target = __cron_{name} topic)
  │
  ├─ 2. GET /v1/events/__cron_{name}/subscriptions   per job
  │       ├── DELETE stale HTTP/HTTPS subs (endpoint ≠ current BEYOND_INTERNAL_URL)
  │       └── POST new sub if current endpoint not already subscribed
  │
  ├─ 3. GET /v1/schedules?target_kind=topic          (reconcile)
  │       └── for each __cron_* schedule NOT in jobs:
  │             DELETE /v1/schedules/{name}
  │             GET + DELETE /v1/events/__cron_{name}/subscriptions  (best-effort)
  │
  ├─ 4. createServer() → listen(port, "0.0.0.0")
  │
  └─ 5. block (AbortSignal | SIGTERM | SIGINT)
             └── server.close() → resolve
```

## Concepts & Terminology

| Term                  | What It Controls                                                            | NOT                                                                                  |
| --------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------ |
| `CronJob`             | A schedule spec + handler function, passed to `start()`                     | A raw `ScheduleSpec` — those go to `cron.schedules.*` directly                       |
| `CronJobSpec`         | `ScheduleSpec` without `target` — SDK owns the target                       | Anything the user sends to a queue or topic manually                                 |
| `__cron_{name}`       | Internal topic name for a job's fire events                                 | User-visible — never referenced outside the SDK                                      |
| `BEYOND_INTERNAL_URL` | The function's own reachable base URL, used to build subscription endpoints | User config — set automatically by the platform                                      |
| SDK-managed schedule  | Any schedule whose topic target starts with `__cron_`                       | Schedules with queue/workflow targets — those are never touched by reconcile         |
| `start()`             | The entire cron worker — registration + HTTP listener + blocking            | A one-shot call; calling it twice in the same process would bind the same port twice |

## Core Mechanism

### HTTP delivery model

When a schedule fires, beyond-queue publishes to the topic `__cron_{name}` with an empty message body (`{}`). Topic fan-out delivers a `POST` with raw body `{}` (no envelope) to the subscribed endpoint `{BEYOND_INTERNAL_URL}/__cron/{name}`. The `node:http` server inside `start()` matches the path, looks up the job by name in a `Map<string, CronJob>`, builds a `CronContext`, and calls the handler.

This model requires no polling. The function can sleep between fires and the platform wakes it when the HTTP POST arrives.

### Port binding

Port is extracted from `BEYOND_INTERNAL_URL` (e.g. `http://127.0.0.1:3000` → `3000`). Falls back to `PORT` env var, then protocol default (80/443). Server listens on `0.0.0.0` — all interfaces — because the platform routes traffic to the process, not a specific interface.

### Subscription lifecycle (deployment-driven cleanup)

`BEYOND_INTERNAL_URL` changes between deployments (new internal address per deploy). On each `start()`, for every job:

1. List existing HTTP/HTTPS subscriptions on `__cron_{name}`
2. Delete any whose endpoint doesn't match the current `BEYOND_INTERNAL_URL` — these are from previous deployments
3. Create a subscription to the current endpoint if not already present

No manual de-registration or deploy hooks needed. The new deployment cleans up after the old one on startup.

### Declarative reconciliation

`start(jobs)` represents the complete desired state. On startup, after registering and subscribing the provided jobs, the SDK fetches all `target_kind=topic` schedules and deletes any with a `__cron_*` topic target whose name is not in the jobs list. Their subscriptions are also deleted (best-effort — failures are silently ignored).

Only `__cron_*` topic targets are touched. Schedules targeting queues, user-defined topics, or workflows are never examined or deleted.

### Handler routing

`src/client.ts:355` — the `node:http` request handler:

- Non-`POST` → 405
- Path doesn't start with `cronPath + "/"` → 404
- Name not in `jobMap` → 404
- Handler resolves → 200
- Handler rejects → 500 (triggers beyond-queue's retry backoff)

`CronContext.scheduledFor` is always `new Date().toISOString()` — set at the moment the HTTP request arrives, not extracted from the message payload. `outOfBand` is always `false` for the same reason: the raw body is `{}` and contains no metadata. Both are limitations to address when the delivery envelope carries `_schedule` headers.

### Management client

`createCronClient()` / the lazy `cron` singleton provide direct schedule management without a handler or HTTP listener. All methods follow the same `cmd()` → `wrap()` → `camelize` pipeline as `@beyond.dev/queue` and `@beyond.dev/events`.

## State Machine

### Schedule status (server-side)

```
active ──── consecutive_failures >= failure_threshold ──► paused
  ▲                                                          │
  └─────────────── schedules.resume() ──────────────────────┘
  │
  └─ schedules.pause() ──► paused (manual)
```

| From   | Event                               | To     | What Actually Happens                                                   |
| ------ | ----------------------------------- | ------ | ----------------------------------------------------------------------- |
| active | fire succeeds                       | active | `fire_count++`, `last_fired_at` updated, `consecutive_failures` cleared |
| active | fire fails (500 from handler)       | active | `consecutive_failures++`, retry scheduled with backoff                  |
| active | `consecutive_failures >= threshold` | paused | No further fires dispatched until resumed                               |
| active | `schedules.pause()`                 | paused | Immediate; in-flight fires still complete                               |
| paused | `schedules.resume()`                | active | `next_fire_at` recalculated; `consecutive_failures` preserved           |
| any    | `schedules.delete()`                | gone   | Schedule row deleted; subscriptions must be cleaned up separately       |

### `start()` worker (process-side)

```
cold start ──► startup sequence ──► listening ──► SIGTERM/SIGINT/abort ──► closed
                     │
                   error (e.g. port in use, BEYOND_QUEUE_URL missing)
                     │
                   throws (start() rejects)
```

## Configuration

| Variable                        | Default            | What It Controls                                                                               |
| ------------------------------- | ------------------ | ---------------------------------------------------------------------------------------------- |
| `BEYOND_QUEUE_URL`              | —                  | URL of the beyond-queue server; required                                                       |
| `BEYOND_QUEUE_TOKEN`            | `"anon"`           | Bearer token on every API request                                                              |
| `BEYOND_INTERNAL_URL`           | —                  | Function's own base URL (host only); used to build subscription endpoints; set by the platform |
| `BEYOND_INTERNAL_RECEIVER_PORT` | `52000` (constant) | Dedicated port for all beyond platform event delivery; never conflicts with user app ports     |

`start()` options extend `CronClientOptions` with `path` (default `"/__cron"`) and `signal` (AbortSignal for graceful shutdown).

## Failure Modes

| Failure                                      | What Actually Happens                                                                          | Recovery                                                                                      |
| -------------------------------------------- | ---------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| Handler throws                               | HTTP 500 returned to beyond-queue topic delivery; retry with backoff (10s → 30s → 60s → 300s+) | Fix handler; `schedules.resume()` if auto-paused                                              |
| `BEYOND_INTERNAL_URL` missing                | `start()` throws synchronously before any API calls                                            | Platform must inject env var                                                                  |
| `BEYOND_QUEUE_URL` missing                   | `start()` throws synchronously                                                                 | Set env var or pass `url` option                                                              |
| Port already in use                          | `server.listen()` rejects; `start()` rejects                                                   | Choose a different port / check for duplicate workers                                         |
| Schedule PUT fails during startup            | `start()` does not throw — `await` result is discarded; schedule may not exist on server       | Silent failure; improve by checking response                                                  |
| Subscription DELETE fails (stale cleanup)    | Best-effort; old subscriptions may remain, causing duplicate fires to a dead endpoint          | beyond-queue will fail delivery to the dead URL and retry; fires still reach the new URL      |
| Orphan subscription DELETE fails (reconcile) | Silently ignored via `.then()` chain with no rejection handler                                 | Orphan subscription persists but its schedule is deleted; deliveries will 404 at the dead URL |
| Process killed mid-startup                   | Partial state: some schedules registered, some not; old subscriptions may not be cleaned       | Next `start()` run completes the reconcile                                                    |

## Files

| File                         | What It Does                                                                   |
| ---------------------------- | ------------------------------------------------------------------------------ |
| `src/client.ts`              | Everything: types, `schedule()`, `start()`, `createCronClient()`, `CronClient` |
| `src/index.ts`               | Lazy `cron` Proxy singleton; re-exports public surface                         |
| `src/errors.ts`              | `CronError` class                                                              |
| `src/utils/camelize.ts`      | Snake-to-camel key transformation applied to all API responses                 |
| `src/types.ts`               | Auto-generated from `openapi/v1.json` — do not edit                            |
| `scripts/generate-types.mjs` | Regenerates `src/types.ts` via `openapi-typescript`                            |
