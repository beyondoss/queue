# Schedules — Design Document

Time-based triggers for the Beyond platform. A small layer over the existing
queue: a schedule is a row that names _what_ to send and _when_ to send it.
A worker advances rows past their `next_fire_at`, fans into the same
`queue.send` / `queue.send_topic` / workflow-start paths everything else
already uses. One new table, zero new pgrx functions, one new SDK verb.

> **Status:** design proposal. Not yet built.

---

## Goals

- Recurring and one-shot triggers that fire on time, survive crashes, and
  fork with the rest of the substrate.
- **Composes with the existing queue.** A schedule is just a producer with
  a clock attached. It calls `queue.send`, `queue.send_topic`, or starts a
  workflow run — same paths everything else uses.
- **Human-friendly _and_ machine-friendly.** Accept raw cron, fixed
  intervals, and natural language; canonicalize on the server; round-trip
  a description and the next N fire times so agents can verify what they
  built.
- **Forks with the rest of the platform.** Schedules live in user
  Postgres, on the user's GlideFS volume.
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

## Composition with the queue

Schedules are not a parallel system. They reuse queue primitives directly.

| Schedule concept                | Queue primitive                                                              |
| ------------------------------- | ---------------------------------------------------------------------------- |
| Fire a schedule into a queue    | `queue.send` with the schedule's stored payload                              |
| Fire a schedule into a topic    | `queue.send_topic` (existing fan-out, including HTTP/SNS subscribers)        |
| Fire a schedule into a workflow | The same workflow-start path topic subscriptions use                         |
| One-shot schedule               | A row whose advance step deletes it instead of computing a next fire         |
| Wake-up after a fire            | Existing `XactCallback` + waiter registry (the inserted message notifies)    |
| Survive crash mid-fire          | One transaction: insert the message, advance `next_fire_at`. All-or-nothing. |
| Fork with state                 | `queue.schedule` lives on the user's volume; `glide fork` carries it         |

Every schedule run is one row in `queue.schedule` advancing forward in
time. Every fire is one INSERT into a queue table (or fan-out into many).
The queue does the work.

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
  "humanReadable": "At 09:00 every day, UTC",
  "nextFires": [
    "2026-05-08T09:00:00Z",
    "2026-05-09T09:00:00Z",
    "2026-05-10T09:00:00Z",
    "2026-05-11T09:00:00Z",
    "2026-05-12T09:00:00Z"
  ]
}
```

The `:preview` endpoint runs the full parse and projection without writing
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

When the scheduler worker is offline past one or more `next_fire_at`
deadlines, the schedule has missed fires. Two policies:

- **`catchup: false` (default)** — on resume, advance `next_fire_at` to
  the next future occurrence and skip the missed runs. Right for
  reporting, cleanup, "noisy" jobs.
- **`catchup: true`** — fire each missed occurrence in order, with the
  message body tagged `{scheduledFor: "<original ts>"}` in headers, then
  advance to the next future occurrence. Right for billing, per-period
  rollups, anything where a missed fire is a correctness bug.

Catchup is bounded by `catchupLimit` (default 100). Past that the worker
records `last_error = "catchup_limit_exceeded"` and skips to the next
future fire — so a year-long outage doesn't inject a million backlog
messages on recovery.

### Timezones

`timezone` defaults to `UTC` and accepts any IANA name. Cron evaluation
respects DST: a `0 2 * * *` schedule in `America/New_York` fires once per
calendar day even when the clock skips or repeats. The canonical cron
stored on the row remains in local time; the worker projects each
`next_fire_at` through the named zone.

### Jitter

`jitterSecs: 30` randomizes each fire by ±N seconds. Defaults to 0. Used
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

CREATE TYPE queue.schedule_target_kind AS ENUM ('queue', 'topic', 'workflow');

CREATE TABLE queue.schedule (
    schedule_id      BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (CACHE 100),
    name             TEXT NOT NULL UNIQUE,                  -- natural key
    expression       TEXT NOT NULL,                         -- the user's original input
    cron             TEXT,                                  -- canonical cron (NULL for one-shot)
    fire_at          TIMESTAMP WITH TIME ZONE,              -- one-shot only
    timezone         TEXT NOT NULL DEFAULT 'UTC',
    jitter_secs      INT  NOT NULL DEFAULT 0,
    catchup          BOOLEAN NOT NULL DEFAULT false,
    catchup_limit    INT  NOT NULL DEFAULT 100,

    target_kind      queue.schedule_target_kind NOT NULL,
    target_name      TEXT NOT NULL,                         -- queue name | routing key | workflow name
    payload          JSONB,                                 -- message body or workflow input
    headers          JSONB,                                 -- forwarded to queue.send

    status           queue.schedule_status NOT NULL DEFAULT 'active',
    next_fire_at     TIMESTAMP WITH TIME ZONE NOT NULL,
    last_fired_at    TIMESTAMP WITH TIME ZONE,
    last_msg_id      BIGINT,                                -- for observability
    last_error       TEXT,
    fire_count       BIGINT NOT NULL DEFAULT 0,

    created_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),

    CHECK ((cron IS NOT NULL) <> (fire_at IS NOT NULL))    -- exactly one of cron / fire_at
);

CREATE INDEX schedule_due_idx
    ON queue.schedule (next_fire_at)
    WHERE status = 'active';
```

That's the whole storage layer. No fire log table — the queue messages
themselves are the log, with `last_msg_id` pointing at the most recent.
For audit history, schedule fires that target a queue land in that
queue's archive table the same way any other message does.

---

## Server primitives

**Zero new pgrx functions.** The atomic fire is a sqlx transaction in the
scheduler worker; nothing requires backend-local C code.

### Scheduler worker

A long-running Tokio task on every replica, mirroring `src/ops/delivery.rs`:

```
loop {
    let now = now_utc();
    let next_wakeup = poll_and_fire_due_schedules(now).await;
    sleep_until(next_wakeup).await;
}
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

For each claimed row, in the same transaction:

1. Dispatch by `target_kind`:
   - `queue` → `SELECT queue.send($name, $payload, $headers, 0)`
   - `topic` → `SELECT queue.send_topic($routing_key, $payload, $headers, 0)`
   - `workflow` → call the workflow-start SQL the workflows runtime exposes
2. Compute `next_fire_at`:
   - `cron IS NOT NULL`: parse cron, advance to the next occurrence after
     `now` (or after the previous `next_fire_at` if `catchup`).
   - `fire_at IS NOT NULL`: this was a one-shot — `DELETE` the row.
3. `UPDATE queue.schedule SET next_fire_at = ..., last_fired_at = now(),
   last_msg_id = ..., fire_count = fire_count + 1, last_error = NULL`.

If dispatch fails (target queue missing, malformed payload, etc.), the
transaction aborts; a small wrapper retries with `last_error` set so the
schedule does not get stuck on a permanent failure. Three failures in a
row pause the schedule and surface the last error.

The next sleep target is `MIN(next_fire_at) WHERE status = 'active'`. If
`min` is in the past or within the poll budget, the worker loops
immediately.

### Why no pgrx function?

The atomic guarantee comes from the Postgres transaction. `queue.send` is
already a pgrx function and runs inside the caller's transaction; calling
it from sqlx inside our `BEGIN ... COMMIT` is the same atomic insert the
producer path uses. The `XactCallback` that wakes consumers fires when
this transaction commits. No new C code needed.

This is intentionally thinner than workflows. Workflows needed
`workflow_complete_step` because a single transaction had to write a
journal row, send a continuation, ack a receipt, _and_ update the run
record — too many constraints to express cleanly in client SQL.
Schedules need only "send, then advance," which sqlx-side is one
`queue.send` call followed by one `UPDATE`.

### Wakeup latency

Polling loop default: `SCHEDULE_POLL_MS = 1000`. Worst-case fire latency
is one poll interval plus dispatch time. For minute-granularity cron
this is invisible. For `every: "1s"` it is the limiting factor — the
design accepts second-level precision and does not chase finer.

A future refinement: have schedule mutations `NOTIFY` the worker so it
re-computes its sleep target without waiting out the poll. Not needed
for v1.

---

## API surface

### Native REST (`/v1/`)

```
POST    /v1/schedules                          Create or upsert. Body: schedule spec. 201.
GET     /v1/schedules                          List. Query: ?status=active&target=queue
GET     /v1/schedules/{name}                   Get one. Includes nextFires preview.
PUT     /v1/schedules/{name}                   Idempotent upsert by name.
PATCH   /v1/schedules/{name}                   Partial update (status, payload, expression).
DELETE  /v1/schedules/{name}                   Remove. 204.
POST    /v1/schedules/{name}/pause             Set status = paused. 200.
POST    /v1/schedules/{name}/resume            Set status = active. 200.
POST    /v1/schedules/{name}/run               Fire now (out-of-band). 200 with msg_id.
POST    /v1/schedules:preview                  Dry-run: parse expression, return cron + nextFires.
```

The collection is plural, the verbs are HTTP methods, sub-actions
(`pause`, `resume`, `run`) are nested resources rather than `?action=`
tunneling — same conventions as the existing native API.

`PUT /v1/schedules/{name}` is the agent-friendly entry point: idempotent,
keyed by a name the caller chooses, full body. An agent regenerating an
infrastructure config `PUT`s its desired set and never duplicates.

### Schedule object

```json
{
  "name": "daily-report",
  "expression": "every weekday at 9am",
  "cron": "0 9 * * 1-5",
  "timezone": "America/New_York",
  "jitterSecs": 0,
  "catchup": false,
  "catchupLimit": 100,
  "target": {
    "queue": "reports",
    "message": { "kind": "daily" },
    "headers": { "x-source": "schedule" }
  },
  "status": "active",
  "nextFireAt": "2026-05-08T13:00:00Z",
  "lastFiredAt": "2026-05-07T13:00:00Z",
  "lastMsgId": 1842,
  "fireCount": 37,
  "humanReadable": "At 09:00, Monday through Friday, America/New_York",
  "nextFires": [
    "2026-05-08T13:00:00Z",
    "2026-05-11T13:00:00Z",
    "2026-05-12T13:00:00Z",
    "2026-05-13T13:00:00Z",
    "2026-05-14T13:00:00Z"
  ],
  "createdAt": "2026-05-01T17:30:00Z",
  "updatedAt": "2026-05-07T13:00:00Z"
}
```

### Preview

```http
POST /v1/schedules:preview
{
  "expression": "every weekday at 9am",
  "timezone":   "America/New_York",
  "previewCount": 5
}

200 OK
{
  "cron":          "0 9 * * 1-5",
  "humanReadable": "At 09:00, Monday through Friday, America/New_York",
  "nextFires":     ["2026-05-08T13:00:00Z", ...]
}
```

If parsing fails, the error response carries the parse position and a
short suggestion list (`Did you mean: "every weekday at 9:00am"?`) so an
agent can self-correct without a round trip to docs.

---

## TypeScript SDK

```ts
import { createClient, schedule } from "@beyond.dev/queue";

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
  jitterSecs: 5,
  target: { topic: "system.heartbeat", message: { source: "scheduler" } },
});

// 2. Sync them to the server (idiomatic deploy step).
const client = createClient({ url: process.env.QUEUE_URL });
await client.schedules.sync([dailyReport, monthlyBilling, heartbeat]);
// PUTs each by name. Schedules not in the list are NOT removed —
// pass { prune: true } for a declarative reconcile.

// 3. Or call the client directly.
await client.schedules.upsert({
  name: "weekly-cleanup",
  cron: "0 0 * * 0",
  target: { queue: "maintenance", message: { task: "cleanup" } },
});

await client.schedules.preview({ when: "every monday at 9am" });
// → { cron: "0 9 * * 1", humanReadable: "At 09:00, only on Monday, UTC", nextFires: [...] }

await client.schedules.pause("heartbeat");
await client.schedules.resume("heartbeat");
await client.schedules.run("daily-report"); // fire once now, schedule unaffected

await client.schedules.delete("weekly-cleanup");
```

### Client surface

| Method                         | Behavior                                                                   |
| ------------------------------ | -------------------------------------------------------------------------- |
| `schedules.upsert(spec)`       | Create or update by `name`. Returns the full schedule object.              |
| `schedules.sync(specs, opts?)` | Bulk upsert. `{ prune: true }` deletes server schedules absent from input. |
| `schedules.preview(input)`     | Dry-run parse. Returns canonical cron + humanReadable + nextFires.         |
| `schedules.list(filter?)`      | Filter by `status`, `targetKind`, `name` prefix. Paginated.                |
| `schedules.get(name)`          | Full object including `nextFires` preview.                                 |
| `schedules.pause(name)`        | Set status = paused.                                                       |
| `schedules.resume(name)`       | Set status = active. Catchup behavior governed by the schedule's setting.  |
| `schedules.run(name)`          | Fire once out-of-band. Does not advance `next_fire_at`. Returns msg_id(s). |
| `schedules.delete(name)`       | Remove. Idempotent — 404 collapses to success.                             |

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
endpoints, workflows) receives the tick. This is one scheduler row
producing one topic publish that may fan into many destinations — the
existing topic system does the multiplication.

---

## The fork story

`queue.schedule` lives in the user's Postgres on their GlideFS volume.
`glide fork` carries the table at the same logical timestamp:

- A schedule with `next_fire_at` 30 seconds in the future on production
  has the same `next_fire_at` on the fork. When wall clock reaches it,
  _both_ fire — independently, against their own data.
- `last_fired_at` and `fire_count` are snapshotted at fork time; they
  diverge from there.
- Pausing a schedule on the fork does not affect production.

Side-effect isolation (forks not allowed to send real emails, hit real
payment gateways) is the substrate's job — same as workflows. The
scheduler runtime has no fork-aware code path.

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

| Failure                                             | What happens                                                                                                                                             | Recovery                                                  |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| Worker crashes between dispatch and advance         | Cannot — both happen in the same Postgres transaction. Either both committed or both rolled back.                                                        | n/a                                                       |
| Worker crashes after commit, before next loop iter  | Schedule already advanced. Next worker (or restarted worker) sees fresh `next_fire_at`.                                                                  | Automatic.                                                |
| Multiple replicas claim the same schedule           | Cannot — `FOR UPDATE SKIP LOCKED` enforces single-claimer.                                                                                               | n/a                                                       |
| Target queue does not exist                         | `queue.send` raises ERROR; transaction aborts; `last_error = "queue does not exist"`. After 3 consecutive failures the schedule is paused.               | Operator creates the queue and resumes the schedule.      |
| Target workflow does not exist                      | Workflow-start fails; same retry-then-pause path.                                                                                                        | Operator deploys the workflow definition and resumes.     |
| Server offline past one or more fires               | On resume: if `catchup: false`, advance to next future fire. If `catchup: true`, fire each missed occurrence in order up to `catchupLimit`.              | Automatic. `catchupLimit` exceeded → skip + record error. |
| Cron expression no longer parses (e.g. lib upgrade) | Worker sets `last_error`, pauses the schedule. Schedule rows are validated on every load to catch this immediately rather than at fire time.             | Operator updates the expression via `PATCH`.              |
| `every: "1s"` schedule with poll interval 1000ms    | Fire latency is up to one poll interval. Effective fire rate ≈ once per poll cycle, not strictly every second.                                           | Tune `SCHEDULE_POLL_MS` lower if needed; expected.        |
| One-shot schedule's fire transaction commits twice  | Cannot — the schedule row is `DELETE`d in the same tx as the dispatch. A second worker sees no row.                                                      | n/a                                                       |
| Clock jumps backward                                | A schedule's `next_fire_at` becomes "in the future" again. It fires when the clock catches back up. No double fire because the row was already advanced. | Automatic.                                                |
| Clock jumps forward (DST / NTP slew)                | Schedules between old and new wall time fire as if catchup were on. Bound by `catchupLimit`.                                                             | Automatic.                                                |

---

## Performance notes

- **Worker cost at idle**: one query per `SCHEDULE_POLL_MS` returning zero
  rows. With the partial index `WHERE status = 'active'` the planner does
  an index-only scan; cost is sub-millisecond.
- **Worker cost firing N due rows**: one CTE claim + N dispatches. Each
  dispatch is one `queue.send` (or fan-out) + one `UPDATE`. All in one
  transaction.
- **Cron parse cost**: amortized; the canonical cron string is parsed
  once per advance into a small iterator. `nextFires` for the API
  response is N more iterator steps.
- **Hot-spot risk**: many schedules sharing a cron minute (e.g. `0 * * * *`).
  Mitigation: `jitterSecs`. With jitter 0, the worker still serializes
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
2. **Preview-before-commit** (`POST /v1/schedules:preview`). Agents
   validate an expression and see its `humanReadable` + `nextFires`
   _before_ writing. Prevents the most common cron mistake — "I wrote
   `0 9 *` and it doesn't do what I think."
3. **Round-trip descriptions**. Every schedule response includes
   `humanReadable` + `nextFires`. Whatever the agent wrote, the server
   tells it back in plain English.
4. **Parse errors are structured**. `{position: 12, suggestion: "...",
   examples: ["...", "..."]}` — actionable without a docs lookup.
5. **`sync()` with `prune`**. Declarative reconcile is one call. No
   diff-and-patch logic in user code.
6. **Self-describing target types**. `target: { queue | topic | workflow }`
   is a discriminated union; an agent can read the surface and know
   exactly which fields each kind takes.

None of these are scheduler-specific cleverness. They are the same
patterns the rest of the queue API follows; we are surfacing them
deliberately because schedules are configuration-shaped and
configuration-shaped surfaces are where agents live.

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
    queue/
      src/
        schedules.ts           # client.schedules.* + schedule() helper
```

Crates: `croner` for cron parsing and iteration, `chrono-tz` for IANA
zones, a small in-house humanizer (the grammar is tight enough that
pulling in a NL crate is overkill).

---

## Why this is the minimum effective abstraction

We add **one table, one background task, three SDK shapes** (`cron`,
`every`, `when`), three target kinds (`queue`, `topic`, `workflow`).
Nine REST routes, nine client methods.

Zero new pgrx functions. Zero new wakeup mechanisms. Zero new wire
protocols. The fire path goes through the same `queue.send` /
`queue.send_topic` / workflow-start that producers and topics already
use; the wakeup is the existing `XactCallback`.

Schedules are not a separate service we built next to the queue — they
are what the queue grows into when its producers start running on a
clock. Producers, topics, schedules, workflows: four things that all
end in a row landing in a `queue.q_{name}` table, then being picked up
by a consumer, then being acked. The queue does the work.
