# Schedules — Design Document

Time-based triggers for the Beyond platform. A small layer over the existing
queue: a schedule is a row that names _what_ to send and _when_ to send it.
A worker advances rows past their `next_fire_at`, fans into the same
`queue.send` / `queue.publish_event` / workflow-start paths everything else
already uses. One new table, zero new pgrx functions, one new SDK verb.

> **Status:** in progress. Tracked in [plans/clearly-i-want-a-soft-pascal.md](~/.claude/plans/clearly-i-want-a-soft-pascal.md).

---

## Goals

- Recurring and one-shot triggers that fire on time, survive crashes, and
  fork with the rest of the substrate.
- **Composes with the existing queue.** A schedule is just a producer with
  a clock attached. It calls `queue.send`, `queue.publish_event`, or starts a
  workflow run — same paths everything else uses.
- **Human-friendly _and_ machine-friendly.** Accept raw cron, fixed
  intervals, and natural language; canonicalize on the server; round-trip
  a description and the next N fire times so agents can verify what they
  built.
- **Forks with the rest of the platform.** Schedules live in user
  Postgres, on the user's GlideFS volume.
- **Trivial idle cost.** The scheduler worker is always on but polls a
  partial index `WHERE status = 'active'`; an empty schedule table costs
  one sub-millisecond index probe per poll cycle.
- The minimum effective surface. One table, one worker, three expression
  forms, three target kinds.

### Non-goals

- Sub-second cron. Minimum granularity is 1s; cron syntax is minute-level.
- Per-tenant rate limits, quota, or schedule-level concurrency caps.
- Backfill of arbitrary historical windows. `catchup` runs missed fires
  during an outage; it does not reach back across `created_at`.
- Holiday calendars, business-day skipping, or other domain-specific
  constructs. These belong in user code triggered by a daily fire.
- Distributed leader election. Multiple scheduler-worker replicas
  coordinate via `FOR UPDATE SKIP LOCKED`; no extra coordination service.

---

## Where this lands

Closest in **scope** to Cloudflare Cron Triggers and Vercel Cron; closest in
**ergonomics** to Inngest's `cron` triggers and EventBridge Schedules.
Postgres-native and forkable like the rest of the platform.

The wedge: a schedule is a thin row, not a separate runtime. Firing one
runs through the same `send` path a producer does, into the same queue a
consumer is already polling, with the same wakeup mechanism. The fanout
into a workflow is the same fanout a topic does.

---

## The runtime model

The queue server is platform infrastructure. It runs alongside the user's
Postgres on their GlideFS volume and does not scale to zero — same as the
database. Users don't pay for it any more than they pay for Postgres being up.

Function VMs — user code — sleep when idle. When a schedule fires, the
scheduler worker (running in the queue server) calls `queue.send` or
`queue.publish_event`. The fan-out sends HTTP to the function VM's endpoint.
The gateway sees a sleeping VM, wakes it, delivers the request. The function
processes it and goes back to sleep.

No special wakeup mechanism. No external scheduler. HTTP fan-out handles
sleeping function VMs as a natural consequence of the existing wake-on-traffic
machinery — the same path any other inbound request takes.

### Worker lifecycle

The scheduler worker starts at queue server boot and runs for the lifetime
of the process. Its idle cost is one partial-index probe per poll cycle —
sub-millisecond on an empty `schedule_due_idx` — so chasing "zero overhead"
by stopping the worker when no schedules exist would add intra-process
signaling complexity without measurable benefit. Same pattern as the HTTP
delivery worker.

On boot, the worker runs one catchup pass (firing any missed occurrences
for active schedules subject to each schedule's `catchup` and
`catchup_limit` settings) before entering the normal poll loop. This
covers fires missed while the queue server was down.

---

## Composition with the queue

Schedules are not a parallel system. They reuse queue primitives directly.

| Schedule concept                | Queue primitive                                                              |
| ------------------------------- | ---------------------------------------------------------------------------- |
| Fire a schedule into a queue    | `queue.send` with the schedule's stored payload                              |
| Fire a schedule into a topic    | `queue.publish_event` (existing fan-out, including HTTP/SNS subscribers)     |
| Fire a schedule into a workflow | The same workflow-start path topic subscriptions use                         |
| One-shot schedule               | A row whose advance step deletes it instead of computing a next fire         |
| Wake a sleeping function VM     | Existing HTTP fan-out delivers to the endpoint; gateway wakes the VM         |
| Survive crash mid-fire          | One transaction: insert the message, advance `next_fire_at`. All-or-nothing. |
| Fork with state                 | `queue.schedule` lives on the user's volume; `glide fork` carries it         |

Every schedule run is one row in `queue.schedule` advancing forward in
time. Every fire is one INSERT into a queue table (or fan-out into many).
The queue does the work. Sleeping function VMs are not a special case —
they wake the same way any other HTTP delivery wakes them.

---

## Execution model

### Three expression forms

A schedule's _when_ accepts one of:

```ts
{
  cron: "0 9 * * 1-5";
} // raw 5-field cron (or 6-field w/ seconds)
{
  every: "5m";
} // fixed interval: ms|s|m|h|d
{
  when: "every weekday at 9am";
} // humanized natural language
{
  fireAt: "2026-06-01T09:00:00Z";
} // one-shot
```

The server parses, normalizes, and stores a canonical cron string (for
recurring schedules) or a single timestamp (for one-shots). The original
input is preserved in `expression` for debugging.

### Humanization round-trip

Every schedule response carries three derived fields so callers can verify
their intent — both humans glancing at a dashboard and agents validating a
config they just wrote:

```json
{
  "name": "daily-report",
  "cron": "0 9 * * *",
  "human_readable": "At 09:00 every day, UTC",
  "next_fires": [
    "2026-05-08T09:00:00Z",
    "2026-05-09T09:00:00Z",
    "2026-05-10T09:00:00Z",
    "2026-05-11T09:00:00Z",
    "2026-05-12T09:00:00Z"
  ]
}
```

The `/preview` endpoint runs the full parse and projection without writing
anything, so callers can dry-run an expression and see exactly what would
fire before committing it. This is the single biggest agent-affordance in
the design — most schedule mistakes are misread cron expressions, and a
preview catches them at the call site.

### Targets

A schedule fires _into_ one of three things, mirroring topic subscription
target types:

```ts
{ target: { queue:    "reports",         message: {...}, headers?: {...} } }
{ target: { topic:    "billing.monthly", message: {...}, headers?: {...} } }
{ target: { workflow: "run-monthly-billing", input: {...} } }
```

The fan-out path is identical to what already exists for topic
subscriptions and workflow triggers; the schedule worker is just one more
producer that happens to be driven by a clock instead of a request.

### Catchup vs skip

When the scheduler worker is not running — queue server restart, first
startup — it may have missed fires. Two policies:

- **`catchup: false` (default)** — on resume, advance `next_fire_at` to
  the next future occurrence and skip the missed runs. Right for
  reporting, cleanup, "noisy" jobs.
- **`catchup: true`** — fire each missed occurrence in order, with the
  message body tagged `{scheduled_for: "<original ts>"}` in headers, then
  advance to the next future occurrence. Right for billing, per-period
  rollups, anything where a missed fire is a correctness bug.

Catchup is bounded by `catchup_limit` (default 100). Past that the worker
records `last_error = "catchup_limit_exceeded"` and skips to the next
future fire — so a year-long outage doesn't inject a million backlog
messages on recovery.

### Failure and auto-pause

When a fire fails (target queue missing, malformed payload, etc.), the
transaction aborts and `consecutive_failures` is incremented. Three
consecutive failures pause the schedule and surface `last_error`. On any
successful fire, `consecutive_failures` resets to zero.

`consecutive_failures` is a column on the row, not in-memory state. A
queue server restart does not reset it; a schedule stuck on a permanent
error stays paused across restarts.

### Timezones

`timezone` defaults to `UTC` and accepts any IANA name. Cron evaluation
respects DST: a `0 2 * * *` schedule in `America/New_York` fires once per
calendar day even when the clock skips or repeats. The canonical cron
stored on the row remains in local time; the worker projects each
`next_fire_at` through the named zone.

### Jitter

`jitter_secs: 30` randomizes each fire by ±N seconds. Defaults to 0. Used
to spread load when many schedules share a wall-clock minute.

### Pause and resume

`status` is `active` or `paused`. Paused schedules retain their
`next_fire_at` but are excluded from the worker's eligibility query. On
resume, the worker either catches up (if `catchup: true`) or advances to
the next future fire.

---

## Data model

One new table in the `queue` schema.

```sql
CREATE TYPE queue.schedule_status AS ENUM ('active', 'paused');

-- 'workflow' is reserved from day 1; the API rejects it with 400 until
-- the workflow runtime exists, but it is present in the enum so adding
-- workflow support later requires no schema migration.
CREATE TYPE queue.schedule_target_kind AS ENUM ('queue', 'topic', 'workflow');

CREATE TABLE queue.schedule (
    schedule_id           BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (CACHE 100),
    name                  TEXT NOT NULL UNIQUE,                  -- natural key
    expression            TEXT NOT NULL,                         -- the user's original input
    cron                  TEXT,                                  -- canonical cron (NULL for one-shot)
    fire_at               TIMESTAMP WITH TIME ZONE,              -- one-shot only
    timezone              TEXT NOT NULL DEFAULT 'UTC',
    jitter_secs           INT  NOT NULL DEFAULT 0,
    catchup               BOOLEAN NOT NULL DEFAULT false,
    catchup_limit         INT  NOT NULL DEFAULT 100,
    failure_threshold     INT  NOT NULL DEFAULT 3,

    target_kind           queue.schedule_target_kind NOT NULL,
    target_name           TEXT NOT NULL,                         -- queue name | routing key | workflow name
    payload               JSONB,                                 -- message body or workflow input
    headers               JSONB,                                 -- forwarded to queue.send / queue.publish_event

    status                queue.schedule_status NOT NULL DEFAULT 'active',
    next_fire_at          TIMESTAMP WITH TIME ZONE NOT NULL,
    last_fired_at         TIMESTAMP WITH TIME ZONE,
    last_error            TEXT,
    consecutive_failures  INT NOT NULL DEFAULT 0,
    fire_count            BIGINT NOT NULL DEFAULT 0,

    created_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at            TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),

    CHECK ((cron IS NOT NULL) <> (fire_at IS NOT NULL))    -- exactly one of cron / fire_at
);

CREATE INDEX schedule_due_idx
    ON queue.schedule (next_fire_at)
    WHERE status = 'active';
```

No fire log table and no `last_msg_id` pointer — the queue messages themselves
are the log. Every fire tags the produced message(s) with a `_schedule` header
(see [Header convention](#header-convention) below), so the audit trail is a
direct query against the target queue's archive:

```sql
SELECT msg_id, enqueued_at, message
FROM queue.a_{target_name}
WHERE headers->'_schedule'->>'name' = 'daily-report'
  AND enqueued_at > now() - interval '1 day'
ORDER BY enqueued_at DESC;
```

This works identically for queue and topic targets — and generalizes to
workflow runs once those exist. The schedule row owns _when_ it fired
(`fire_count`, `last_fired_at`); the archive owns _what landed where_.

---

## Server primitives

**Zero new pgrx functions.** The atomic fire is a sqlx transaction in the
scheduler worker; nothing requires backend-local C code.

### Scheduler worker

A Tokio task managed by the queue server. It is started when the first
schedule is created (or when the server boots with schedules already
present) and stopped when the last schedule is deleted.

On start, the worker immediately runs `poll_and_fire_due_schedules` to
catch any fires missed while it was not running — before entering the
normal poll loop.

```
on_start:
    poll_and_fire_due_schedules(now).await  // catch up missed fires first

loop:
    let now = now_utc();
    let next_wakeup = poll_and_fire_due_schedules(now).await;
    sleep_until(next_wakeup).await;
```

`poll_and_fire_due_schedules` opens one transaction, claims a batch of
due rows, fires them, advances them, commits. Concurrent replicas
coexist via `FOR UPDATE SKIP LOCKED` — exactly the pattern the HTTP
delivery worker already uses.

```sql
WITH due AS (
    SELECT schedule_id
    FROM queue.schedule
    WHERE status = 'active' AND next_fire_at <= $1   -- now()
    ORDER BY next_fire_at
    LIMIT 32
    FOR UPDATE SKIP LOCKED
)
SELECT s.*
FROM queue.schedule s
JOIN due USING (schedule_id);
```

For each claimed row, in a `SAVEPOINT` inside the outer transaction:

1. Build effective headers by merging the user-supplied `headers` with
   `{_schedule: {name, scheduled_for, out_of_band: false}}`.
2. Dispatch by `target_kind`:
   - `queue` → `SELECT queue.send($name, $payload, $effective_headers, 0)`
   - `topic` → `SELECT queue.publish_event($routing_key, $payload, $effective_headers, 0)`
   - `workflow` → unreachable; the API rejects this target kind until the
     workflow runtime exists.
3. Compute `next_fire_at`:
   - `cron IS NOT NULL`: parse cron, advance to the next occurrence after
     `now`. If `catchup` is true, fire each missed occurrence in order up to
     `catchup_limit`, tagging `scheduled_for` distinctly for each.
   - `fire_at IS NOT NULL`: this was a one-shot — `DELETE` the row.
4. `UPDATE queue.schedule SET next_fire_at = ..., last_fired_at = now(),
   fire_count = fire_count + 1, consecutive_failures = 0, last_error = NULL`.
5. `RELEASE SAVEPOINT`.

If dispatch for an individual schedule fails, the worker `ROLLBACK`s only
that schedule's savepoint, increments `consecutive_failures`, sets
`last_error`, and (when `consecutive_failures >= failure_threshold`) sets
`status = 'paused'`. Other schedules in the same claim batch are
unaffected. The whole batch commits in one outer transaction; the
XactCallback registered by each `queue.send` / `queue.publish_event` fires
on that commit and wakes any waiting receivers.

The next sleep target is `MIN(next_fire_at) WHERE status = 'active'`. If
`min` is in the past or within the poll budget, the worker loops
immediately. If the schedule table becomes empty, the worker exits.

### Why no pgrx function?

The atomic guarantee comes from the Postgres transaction. `queue.send` is
already a pgrx function and runs inside the caller's transaction; calling
it from sqlx inside our `BEGIN ... COMMIT` is the same atomic insert the
producer path uses. The `XactCallback` that wakes consumers fires when
this transaction commits. No new C code needed.

### Wakeup latency

Polling loop default: `SCHEDULE_POLL_MS = 1000`. Worst-case fire latency
is one poll interval plus dispatch time. For minute-granularity cron
this is invisible. For `every: "1s"` it is the limiting factor — the
design accepts second-level precision and does not chase finer.

---

## API surface

### Native REST (`/v1/`)

```
POST    /v1/schedules                          Strict create. 201 + Location, or 409 if name taken.
GET     /v1/schedules                          List. Query: ?status=active&target=queue. Hard limit 1000.
GET     /v1/schedules/{name}                   Get one. Includes human_readable + next_fires preview.
PUT     /v1/schedules/{name}                   Idempotent upsert. 201 if new, 200 if updated.
PATCH   /v1/schedules/{name}                   Partial update (status, payload, expression).
DELETE  /v1/schedules/{name}                   Remove. 204. Idempotent.
POST    /v1/schedules/{name}/runs              Manual run. 202 with Run resource.

POST    /v1/previews                           Dry-run any expression. 200 with cron + human_readable + next_fires.
```

**POST vs PUT.** POST and PUT mean different things and the response codes
say so:

- `POST /v1/schedules` is **strict create**. The body must contain `name`.
  Returns 201 on success or **409 Conflict** if the name is already taken.
  Use this when you believe you're creating a new schedule and want to know
  if your view of the world was stale.
- `PUT /v1/schedules/{name}` is **idempotent upsert**. Returns 201 on
  create, 200 on update. Use this when you're declaring desired state and
  don't care whether the schedule existed before — agents regenerating an
  infrastructure config `PUT` their desired set every deploy.

Each method answers a different question; conflating them throws away that
information. PATCH stays as the partial-update verb.

**Pause and resume** are `PATCH /v1/schedules/{name}` with
`{ "status": "paused" }` or `{ "status": "active" }`. No dedicated sub-routes.

**Manual runs** (`POST /v1/schedules/{name}/runs`) return a Run resource and do
not advance `next_fire_at`. They do bump `last_fired_at` and `fire_count` —
a manual run is still a fire. The produced message(s) carry
`headers._schedule.out_of_band = true` so consumers can distinguish them
from scheduled fires. The URI is plural and noun-shaped so a future
`GET /v1/schedules/{name}/runs` (list recent runs by joining
`headers._schedule.name` against the target archive) slots in cleanly.

**Previews are a top-level resource** (`POST /v1/previews`), not a
sub-route of `/schedules`. A preview is not a sub-resource of a specific
schedule — it doesn't read or write a schedule row, and `"preview"` is a
valid schedule name, so nesting would overload the URI. Top-level
placement also lets the preview endpoint extend to other time-based
expressions later without belonging to one collection.

### Schedule object

```json
{
  "name": "daily-report",
  "expression": "every weekday at 9am",
  "cron": "0 9 * * 1-5",
  "timezone": "America/New_York",
  "jitter_secs": 0,
  "catchup": false,
  "catchup_limit": 100,
  "failure_threshold": 3,
  "target": {
    "queue": "reports",
    "message": { "kind": "daily" },
    "headers": { "x-source": "schedule" }
  },
  "status": "active",
  "next_fire_at": "2026-05-08T13:00:00Z",
  "last_fired_at": "2026-05-07T13:00:00Z",
  "consecutive_failures": 0,
  "fire_count": 37,
  "human_readable": "At 09:00, Monday through Friday, America/New_York",
  "next_fires": [
    "2026-05-08T13:00:00Z",
    "2026-05-11T13:00:00Z",
    "2026-05-12T13:00:00Z",
    "2026-05-13T13:00:00Z",
    "2026-05-14T13:00:00Z"
  ],
  "created_at": "2026-05-01T17:30:00Z",
  "updated_at": "2026-05-07T13:00:00Z"
}
```

### Run object

`POST /v1/schedules/{name}/runs` returns a Run:

```json
{
  "schedule_name": "daily-report",
  "fired_at": "2026-05-10T14:23:00Z",
  "scheduled_for": "2026-05-10T14:23:00Z",
  "out_of_band": true,
  "msg_ids": [1843]
}
```

For topic targets, `msg_ids` is the list of identifiers produced by the
fan-out (one per fanned-out queue). For queue targets, it is a singleton.

### Preview

```http
POST /v1/previews
{
  "expression":    "every weekday at 9am",
  "timezone":      "America/New_York",
  "preview_count": 5
}

200 OK
{
  "cron":           "0 9 * * 1-5",
  "human_readable": "At 09:00, Monday through Friday, America/New_York",
  "next_fires":     ["2026-05-08T13:00:00Z", ...]
}
```

If parsing fails, the error response carries the parse position and a
short suggestion list (`Did you mean: "every weekday at 9:00am"?`) so an
agent can self-correct without a round trip to docs.

### Header convention

Every fire (scheduled, catchup, or out-of-band manual run) merges a
`_schedule` object into the produced message's `headers`:

```json
{
  "_schedule": {
    "name": "daily-report",
    "scheduled_for": "2026-05-10T09:00:00Z",
    "out_of_band": false
  }
}
```

- `name` — the schedule's `name`.
- `scheduled_for` — the instant the fire was scheduled to occur (for
  catchup, this is the _original_ missed instant, not now; for out-of-band,
  this is the moment the request was received).
- `out_of_band` — `true` for `POST /runs`, `false` otherwise.

A single nested object (not flat `_schedule_name` keys) so one JSONB index
path powers archive queries, user-defined headers can't collide
field-by-field, and adding fields later (e.g. `fire_attempt`,
`idempotency_key`) extends the nested object without polluting the
top-level namespace. The `_schedule` top-level key is reserved; user
headers containing it are rejected at validation.

---

## TypeScript SDK

The SDK ships as a sister package, `@beyond.dev/cron`. It talks to the same queue
server as `@beyond.dev/queue` and reads the same `BEYOND_QUEUE_URL` environment
variable. The separation keeps each package focused: queue for message send/receive,
cron for time-based firing.

```ts
import { createCronClient, cron, schedule } from "@beyond.dev/cron";

// --- Lazy singleton (reads BEYOND_QUEUE_URL automatically) ---

// 1. Define schedules declaratively (optional pattern).
export const dailyReport = schedule({
  name: "daily-report",
  when: "every weekday at 9am",
  timezone: "America/New_York",
  target: {
    queue: "reports",
    message: { kind: "daily" },
  },
});

export const monthlyBilling = schedule({
  name: "monthly-billing",
  cron: "0 0 1 * *",
  timezone: "UTC",
  catchup: true, // never miss a billing period
  target: {
    workflow: "run-monthly-billing",
    input: { tier: "all" },
  },
});

export const heartbeat = schedule({
  name: "heartbeat",
  every: "30s",
  jitter_secs: 5,
  target: { topic: "system.heartbeat", message: { source: "scheduler" } },
});

// 2. Sync them to the server (idiomatic deploy step).
await cron.schedules.sync([dailyReport, monthlyBilling, heartbeat]);
// PUTs each by name. Schedules not in the list are NOT removed —
// pass { prune: true } for a declarative reconcile.

// 3. Or call imperatively.
await cron.schedules.upsert({
  name: "weekly-cleanup",
  cron: "0 0 * * 0",
  target: { queue: "maintenance", message: { task: "cleanup" } },
});

await cron.schedules.preview({ when: "every monday at 9am" });
// → { cron: "0 9 * * 1", human_readable: "At 09:00, only on Monday, UTC", next_fires: [...] }

await cron.schedules.pause("heartbeat");
await cron.schedules.resume("heartbeat");
await cron.schedules.run("daily-report"); // fire once now, schedule unaffected

await cron.schedules.delete("weekly-cleanup");

// --- Explicit client (custom URL, token, etc.) ---
const client = createCronClient({ url: "http://localhost:9324", token: "dev" });
await client.schedules.list();
```

### Client surface

`cron.schedules.*` — all methods are on the lazy singleton or an explicit
`createCronClient()` instance.

| Method                              | Behavior                                                                      |
| ----------------------------------- | ----------------------------------------------------------------------------- |
| `cron.schedules.upsert(spec)`       | Create or update by `name`. Returns the full schedule object.                 |
| `cron.schedules.sync(specs, opts?)` | Bulk upsert. `{ prune: true }` deletes server schedules absent from input.    |
| `cron.schedules.preview(input)`     | Dry-run parse. Returns canonical cron + human_readable + next_fires.          |
| `cron.schedules.list(filter?)`      | Filter by `status`, `target_kind`, `name` prefix. Paginated.                  |
| `cron.schedules.get(name)`          | Full object including `next_fires` preview.                                   |
| `cron.schedules.pause(name)`        | `PATCH` status = paused.                                                      |
| `cron.schedules.resume(name)`       | `PATCH` status = active. Catchup behavior governed by the schedule's setting. |
| `cron.schedules.run(name)`          | Fire once out-of-band. Does not advance `next_fire_at`. Returns msg_id(s).    |
| `cron.schedules.delete(name)`       | Remove. Idempotent — 404 collapses to success.                                |

### Why a `schedule()` helper at all?

Two reasons. First, type-safety: the helper validates the spec at the
call site, so an agent producing 30 schedules from a config object gets
a compile error on the broken one rather than a runtime 400. Second, the
declarative form is the right level for reading: a glance at the
`schedule({...})` block tells you what fires when and where, and a
co-located `sync()` call makes the deploy step explicit. Fully optional
— the imperative client works for everything.

---

## Composition with workflows

A schedule whose target is a workflow is _the_ canonical way to run a
recurring durable job:

```ts
schedule({
  name: "monthly-billing",
  cron: "0 0 1 * *",
  catchup: true,
  target: { workflow: "run-monthly-billing", input: {} },
});
```

When the schedule fires, the worker calls the same workflow-start path
that the REST `POST /v1/workflows/{name}/runs` endpoint and the topic
subscription target type use. The run gets a fresh `run_id`, lands as a
FIFO message in `__wf_run-monthly-billing`, and a workflow worker picks
it up. From the workflow's perspective there is nothing scheduler-specific;
it is simply being started.

Idempotency on catchup: when `catchup: true`, the schedule worker passes
`idempotencyKey = "{schedule_name}:{scheduled_for_iso}"` to the workflow
start. A double-fire (worker crash between `INSERT` and `COMMIT` on the
schedule advance) collapses to one run, because workflow start is
idempotent on `(workflow_name, idempotency_key)`.

---

## Composition with topics

A scheduled topic publish fans out exactly like a manual one:

```ts
schedule({
  name: "system-tick",
  every: "1m",
  target: { topic: "system.tick.minute", message: { ts: "auto" } },
});
```

Every minute, every subscriber of `system.tick.minute` (queues, HTTP
endpoints, workflows) receives the tick. HTTP subscribers that are on
sleeping function VMs are woken by the fan-out delivery — no different
from any other inbound HTTP request. This is one scheduler row producing
one topic publish that may fan into many destinations.

---

## The fork story

`queue.schedule` lives in the user's Postgres on their GlideFS volume.
`glide fork` carries the table at the same logical timestamp:

- A schedule with `next_fire_at` 30 seconds in the future on production
  has the same `next_fire_at` on the fork. When wall clock reaches it,
  _both_ fire — independently, against their own data.
- `last_fired_at`, `fire_count`, and `consecutive_failures` are
  snapshotted at fork time; they diverge from there.
- Pausing a schedule on the fork does not affect production.

The queue server on each fork runs its own scheduler worker independently.
No fork-aware code path is needed.

---

## Trust boundaries

Same network model as the queue. Schedules don't add a security layer.

- Internal service. Operator's proxy is the perimeter.
- `Authorization` header presence required; contents not verified.
- Schedule names: `[a-z0-9_-]`, 1–64 chars. Stored as the natural key.
- Target queue / topic / workflow names: validated by the existing
  rules of those subsystems.
- Cron strings: parsed and validated server-side. Reject anything the
  parser cannot canonicalize.
- Payload size: 256KB cap, same as queue messages.

---

## Failure modes

| Failure                                             | What happens                                                                                                                                             | Recovery                                                   |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| Worker crashes between dispatch and advance         | Cannot — both happen in the same Postgres transaction. Either both committed or both rolled back.                                                        | n/a                                                        |
| Worker crashes after commit, before next loop iter  | Schedule already advanced. Next worker iteration sees fresh `next_fire_at`.                                                                              | Automatic.                                                 |
| Multiple replicas claim the same schedule           | Cannot — `FOR UPDATE SKIP LOCKED` enforces single-claimer.                                                                                               | n/a                                                        |
| Target queue does not exist                         | `queue.send` raises ERROR; transaction aborts; `consecutive_failures` incremented. After 3 consecutive failures the schedule is paused.                  | Operator creates the queue and resumes the schedule.       |
| Target workflow does not exist                      | Workflow-start fails; same retry-then-pause path.                                                                                                        | Operator deploys the workflow definition and resumes.      |
| Queue server restarts with active schedules         | Worker starts, immediately runs catchup for any missed fires, then enters normal poll loop.                                                              | Automatic.                                                 |
| Queue server restarts with no schedules             | Worker starts; partial-index probe returns zero rows; loop sleeps `poll_ms`. Negligible cost.                                                            | n/a                                                        |
| Function VM sleeping when schedule fires            | `queue.send` or topic fan-out delivers HTTP to the function VM's endpoint. Gateway wakes the VM. No special handling needed.                             | Automatic — standard wake-on-traffic path.                 |
| Server offline past one or more fires               | On resume: if `catchup: false`, advance to next future fire. If `catchup: true`, fire each missed occurrence in order up to `catchup_limit`.             | Automatic. `catchup_limit` exceeded → skip + record error. |
| Cron expression no longer parses (e.g. lib upgrade) | Worker sets `last_error`, pauses the schedule. Schedule rows are validated on every load to catch this immediately rather than at fire time.             | Operator updates the expression via `PATCH`.               |
| `every: "1s"` schedule with poll interval 1000ms    | Fire latency is up to one poll interval. Effective fire rate ≈ once per poll cycle, not strictly every second.                                           | Tune `SCHEDULE_POLL_MS` lower if needed; expected.         |
| One-shot schedule's fire transaction commits twice  | Cannot — the schedule row is `DELETE`d in the same tx as the dispatch. A second worker sees no row.                                                      | n/a                                                        |
| Clock jumps backward                                | A schedule's `next_fire_at` becomes "in the future" again. It fires when the clock catches back up. No double fire because the row was already advanced. | Automatic.                                                 |
| Clock jumps forward (DST / NTP slew)                | Schedules between old and new wall time fire as if catchup were on. Bound by `catchup_limit`.                                                            | Automatic.                                                 |

---

## Performance notes

- **Worker cost at idle**: one query per `SCHEDULE_POLL_MS` returning zero
  rows from the partial index `WHERE status = 'active'`. Index-only scan;
  sub-millisecond. Same whether the schedule table is empty or all
  schedules are paused.
- **Worker cost firing N due rows**: one CTE claim + N dispatches inside
  one outer transaction, each dispatch wrapped in a SAVEPOINT for
  per-schedule failure isolation. Each dispatch is one `queue.send`
  (or `queue.publish_event` fan-out) + one `UPDATE`.
- **Cron parse cost**: amortized; the canonical cron string is parsed
  once per advance into a small iterator. `next_fires` for the API
  response is N more iterator steps.
- **Hot-spot risk**: many schedules sharing a cron minute (e.g. `0 * * * *`).
  Mitigation: `jitter_secs`. With jitter 0, the worker still serializes
  fires one batch at a time, capped at 32 per claim — bounded effort,
  predictable latency.
- **Multi-replica scaling**: linear in worker count up to lock contention
  on the `schedule_due_idx`. At which point the schedule table itself
  is the bottleneck — partition by `next_fire_at` if it ever matters,
  same way the queue tables partition.

---

## Coding-agent ergonomics

The design treats agents as a primary user. Specifically:

1. **Idempotent upsert by name** (`PUT /v1/schedules/{name}`). An agent
   regenerating a config `PUT`s its desired set; rerunning is safe.
2. **Preview-before-commit** (`POST /v1/previews`). Agents
   validate an expression and see its `human_readable` + `next_fires`
   _before_ writing. Prevents the most common cron mistake — "I wrote
   `0 9 *` and it doesn't do what I think."
3. **Round-trip descriptions**. Every schedule response includes
   `human_readable` + `next_fires`. Whatever the agent wrote, the server
   tells it back in plain English.
4. **Parse errors are structured**. `{position: 12, suggestion: "...",
   examples: ["...", "..."]}` — actionable without a docs lookup.
5. **`sync()` with `prune`**. Declarative reconcile is one call. No
   diff-and-patch logic in user code.
6. **Self-describing target types**. `target: { queue | topic | workflow }`
   is a discriminated union; an agent can read the surface and know
   exactly which fields each kind takes.

---

## Project structure

Schedules join the existing `beyond-queue` crate workspace. No new
binary; the queue server gains schedule routes and one background task,
the extension gains nothing.

```
queue/
  src/
    ops/
      schedule.rs              # upsert, list, get, pause, resume, run, delete, preview
      schedule_worker.rs       # background task: poll, claim, fire, advance
    routes/
      schedules.rs             # /v1/schedules/...
    schedule/
      cron.rs                  # cron parse + advance + describe (croner wrapper)
      humanize.rs              # natural-language → cron parser
      every.rs                 # interval → cron / next-fire helper
  beyond-queue-extension/
    sql/
      schema.sql               # + schedule_status, schedule_target_kind, schedule table
  sdk/ts/
    cron/                      # @beyond.dev/cron — sister package
      src/
        index.ts               # exports cron singleton + createCronClient + schedule()
        client.ts              # CronClient, createCronClient, schedule() helper
```

Crates: `croner` for cron parsing and iteration, `chrono-tz` for IANA
zones, a small in-house humanizer (the grammar is tight enough that
pulling in a NL crate is overkill).

---

## Why this is the minimum effective abstraction

We add **one table, one background task, three expression forms**
(`cron`, `every`, `when`), three target kinds (`queue`, `topic`,
`workflow`). Nine REST routes, nine client methods.

Zero new pgrx functions. Zero new wakeup mechanisms. Zero new wire
protocols. The fire path goes through the same `queue.send` /
`queue.publish_event` / workflow-start that producers and topics already
use; the wakeup is the existing `XactCallback`. Sleeping function VMs
wake via the existing HTTP fan-out — no scheduler-specific handling.

The worker is always on but trivially cheap when idle: one query per
poll cycle returning zero rows from a partial index `WHERE status = 'active'`,
sub-millisecond. A deployment with no schedules pays effectively nothing.

Schedules are not a separate service we built next to the queue — they
are what the queue grows into when its producers start running on a
clock. Producers, topics, schedules, workflows: four things that all
end in a row landing in a `queue.q_{name}` table, then being picked up
by a consumer, then being acked. The queue does the work.
