# Workflows — Design Document

Durable execution for the Beyond platform. A small layer over the existing
queue: workflow runs are FIFO message groups, steps are journal rows,
timers are delayed messages, signals are conditional sends. Two new tables,
one new pgrx function, five SDK verbs.

> **Status:** design proposal. Not yet built.

---

## Goals

- Multi-step background work that survives crashes, restarts, and deploys.
- Idiomatic TypeScript SDK — `workflow({ name, run })` + `step.run("name", fn)`.
- **Composes with the existing queue.** No new transport, no new wire
  protocol, no new wakeup mechanism. Workflows are what the queue grows
  into when its messages start carrying journals.
- **Forks with the rest of the platform.** All state lives on the user's
  GlideFS volume, so `glide fork` carries it for free.
- The minimum effective surface. Roughly Cloudflare Workflows in scope.

### Non-goals

- Deterministic-replay sandboxing (Temporal).
- Fan-out, concurrency limits, throttle, workflow-level timeouts. All
  composable later if demand shows up. Not v1.
- Pause/resume, manual replay, run lists with rich filtering. Not v1.
- HTTP-push worker model. v1 is long-poll only.
- Cross-cluster live migration of in-flight runs.

---

## Where this lands

Closest in **scope** to Cloudflare Workflows; closest in **ergonomics** to
Inngest. Postgres-native like DBOS, but the storage layer is the queue we
already have rather than a parallel runtime schema.

The wedge is what's unique to Beyond: workflow state lives on the same
volume as the database, the queue carrying its continuations, and the
boxes its steps run on. One CoW operation, the whole world forks. No other
runtime can do this — Temporal Cloud, Inngest, DBOS all sit outside the
substrate.

---

## Composition with the queue

Workflows are not a parallel system. They reuse queue primitives directly.

| Workflow concept                | Queue primitive                                                      |
| ------------------------------- | -------------------------------------------------------------------- |
| Per-run ordering, single-flight | FIFO `message_group_id = run_id`                                     |
| Resume after a step             | Self-`send` to the workflow's queue                                  |
| `step.sleep("name", duration)`  | Self-`send` with `delay_seconds = duration`                          |
| `step.waitForEvent` timeout     | Self-`send` with `delay_seconds = timeout` and a wait-marker payload |
| `step.waitForEvent` resolution  | `signal()` deletes the wait message + writes the journal in one tx   |
| At-least-once retry on crash    | `vt` expiry + `read_ct` increment                                    |
| Trigger from external event     | Topic subscription (`POST /v1/topics/...`)                           |
| Trigger from anywhere           | `send` to the workflow queue                                         |
| Wake-up after `send`            | Existing `XactCallback` + waiter registry                            |

Every workflow run is a FIFO message group on a queue named
`__wf_{workflow_name}`. Every step boundary is a queue message. Every
timer is `delay_seconds`. The queue does the work.

---

## Execution model

### Named-string steps with replay

A workflow body is an async function. Inside it, each durable boundary is a
named string call:

```ts
async run(ctx, input) {
  const user = await ctx.step.run("create-user", () => db.users.create(input));
  await ctx.step.sleep("welcome-delay", "5m");
  await ctx.step.run("send-welcome", () => email.send(user.id));
}
```

The runtime executes the body **from the top on every resume**. Each
`step.*` call:

1. Looks up its name in the run's journal (`workflow_step` table).
2. If a row exists, returns the cached output without invoking the callback.
3. Otherwise, runs the callback, atomically writes the result + sends the
   next continuation + acks the in-flight message, exits the handler.

User-visible consequence: code outside `step.run` re-runs on every step
boundary. Side effects belong inside `step.run`. Same contract as
Cloudflare Workflows, Inngest, Restate.

### Crash semantics

- **Crash inside a `step.run` callback**: vt expires, redelivery, callback
  re-runs. The callback must be idempotent.
- **Crash after callback returned, before journal write**: same — vt
  expires, callback re-runs. The journal write and the continuation send
  happen in one Postgres transaction; they commit together or not at all.
- **Crash after journal commit**: the next continuation has been enqueued.
  Recovery is automatic on next worker pickup.

### Step kinds (v1)

| Verb                            | Behavior                                                                                                                              |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `step.run(name, fn, opts?)`     | Run `fn`, journal output. Cached on replay. `opts.retry` configures backoff.                                                          |
| `step.sleep(name, duration)`    | Send a continuation with `delay_seconds = duration`. Journal `{slept_until}` so timing is replay-stable.                              |
| `step.waitForEvent(name, opts)` | Send a continuation with `delay_seconds = timeout` and a wait-marker payload. Suspends until `signal()` arrives or the timeout fires. |

Three verbs. Cover the workflow shapes Cloudflare and Vercel ship.

### Step retries

```ts
await ctx.step.run("call-stripe", () => stripe.charges.create(...), {
  retry: {
    attempts: 5,
    backoff: { kind: "exp", base: "1s", max: "60s", jitter: 0.2 },
  },
});
```

On exception, the runtime calls `change_visibility` on the in-flight
message with the next backoff delay. The message reappears at the
deadline; the handler reruns the body, hits the same step (cached up to
this one), retries the callback. `read_ct` is the attempt counter.

When `read_ct >= attempts`: `workflow_step` row is written with `error`,
the run terminates with status `failed`, the in-flight message is deleted.

Backoff kinds: `fixed`, `linear`, `exp`. Default if `retry` is omitted: 3
attempts, exponential 1s→60s, 20% jitter.

### Replay determinism

Branching on prior step outputs is fine. Branching on `Math.random()`,
`Date.now()`, or fresh `process.env` reads outside `step.run` is not.
Documented, not enforced. Inngest and CF take the same stance and it has
not been a problem.

---

## Data model

Two new tables in the `queue` schema. Both live in the user's Postgres,
on the user's volume — so they fork.

### `queue.workflow_run`

```sql
CREATE TYPE queue.workflow_status AS ENUM (
    'running', 'completed', 'failed', 'cancelled'
);

CREATE TABLE queue.workflow_run (
    run_id            BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (CACHE 100),
    workflow_name     TEXT NOT NULL,
    workflow_version  INT  NOT NULL,
    status            queue.workflow_status NOT NULL,
    input             JSONB,
    output            JSONB,
    error             JSONB,
    idempotency_key   TEXT,
    current_msg_id    BIGINT,                            -- in-flight queue msg
    retention_days    INT NOT NULL,                      -- snapshotted at start
    started_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    completed_at      TIMESTAMP WITH TIME ZONE,
    UNIQUE (workflow_name, idempotency_key)
);

-- Sweeper queries terminal runs by completed_at; partial index keeps it
-- bounded to terminal rows.
CREATE INDEX workflow_run_retention_idx
    ON queue.workflow_run (completed_at)
    WHERE status IN ('completed', 'failed', 'cancelled');

-- Listing / observability — used by GET /runs filters.
CREATE INDEX workflow_run_listing_idx
    ON queue.workflow_run (workflow_name, status, started_at DESC);
```

One row per run. `current_msg_id` lets `signal()` find and delete the
in-flight wait message atomically. The source of truth for _what to do
next_ is the queue message keyed by `message_group_id = run_id` in
`queue.q___wf_{workflow_name}`.

### `queue.workflow_step`

```sql
CREATE TABLE queue.workflow_step (
    run_id        BIGINT NOT NULL REFERENCES queue.workflow_run(run_id) ON DELETE CASCADE,
    step_name     TEXT NOT NULL,
    output        JSONB,
    started_at    TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, step_name)
);
```

The journal. A step is either absent (not yet run) or present with its
cached output. Mid-attempt state is transient and lives in `read_ct` on
the queue message. Terminal failures end the run — the cause is recorded
in `workflow_run.error`, not on the step row, so the step table holds
only successful results. `started_at` and `completed_at` enable per-step
timing in observability tools.

That's it. No waiter table — `step.waitForEvent` is just a delayed queue
message that `signal()` deletes on resolve. No concurrency, throttle,
parallel-item, or schedule tables in v1.

---

## Retention

Terminal runs (`completed`, `failed`, `cancelled`) are deleted by a
sweeper when `completed_at + retention_days < now()`. Journal rows
CASCADE.

**Retention is a property of the workflow.** The user owns the
definition; they choose. Each `workflow_run` snapshots `retention_days`
at start time so an in-flight run keeps its original retention even if
the definition changes mid-flight (same model as `workflow_version`).

```ts
export const onboardUser = workflow({
  name: "onboard-user",
  retention: "30d",       // ← user's choice; default 30 days
  run: async (ctx, input) => { ... },
});

// Override for a specific run:
await client.start("onboard-user", { email: "..." }, { retentionDays: 90 });
```

**Server-enforced ceiling.** The operator sets a hard cap via
`WORKFLOWS_MAX_RETENTION_DAYS` (default 365). The server clamps both the
definition's `retention` and per-start overrides to `min(requested, cap)`.
Bounds storage growth from a misconfigured user.

**In-flight runs are never swept.** Status `running` is excluded from
the sweep regardless of age — a workflow that runs longer than its own
retention is fine.

**Sweeper.** Runs every `WORKFLOWS_SWEEPER_INTERVAL_MS` (default 5min)
in the API process alongside the existing HTTP delivery worker. Same
`FOR UPDATE SKIP LOCKED` pattern as `ops/delivery.rs`:

```sql
DELETE FROM queue.workflow_run
 WHERE run_id IN (
   SELECT run_id FROM queue.workflow_run
    WHERE status IN ('completed', 'failed', 'cancelled')
      AND completed_at + make_interval(days => retention_days) < now()
    ORDER BY completed_at
    LIMIT 1000
    FOR UPDATE SKIP LOCKED
 );
-- workflow_step CASCADEs.
```

Per-workflow retention via a `workflow_config` table is not v1. If
operators want overrides without changing user code, they get them
through the env var ceiling.

---

## Observability

We don't ship a UI. The platform handles dashboards (`glide` CLI,
future Beyond dashboard) — building a React app inside this repo would
violate the substrate-composes-from-the-bottom principle. What we ship
is the **data surface that any UI, tracer, or metrics backend can
render against**.

Three layers:

### 1. Query API

The REST endpoints above (list + filter, per-run journal with timings,
workflow index) give a dashboard everything it needs. Cursors are
`completed_at`-based for stable pagination. Filter examples:

```
GET /v1/workflows/onboard-user/runs?status=failed&since=2026-05-01
GET /v1/workflows/onboard-user/runs/{id}/steps
GET /v1/workflows  → [{name, running, completed, failed, cancelled}, ...]
```

JSONB filtering on `input` falls out of Postgres for free
(`?input.userId=42` translates to `input @> '{"userId": 42}'`) — useful
for "find the run for this customer."

### 2. Distributed tracing

OTLP spans, naming convention:

| Span              | Attributes                                                                           |
| ----------------- | ------------------------------------------------------------------------------------ |
| `workflow.run`    | `workflow.name`, `workflow.version`, `run_id`, `status`                              |
| `workflow.step`   | `workflow.name`, `run_id`, `step.name`, `step.kind` (run/sleep/wait), `step.attempt` |
| `workflow.signal` | `workflow.name`, `run_id`, `event`                                                   |

The queue already has `OTLP_ENABLED` + `OTLP_ENDPOINT` plumbing.
Workflows hook into the same exporter — no new config. A user pointing
Honeycomb / Tempo / Jaeger at the OTLP endpoint gets a flame graph per
run for free, with each step as a span and waits showing as gaps.

### 3. Prometheus metrics

Exposed at the existing `/metrics` endpoint:

```
workflow_runs_started_total{workflow}
workflow_runs_completed_total{workflow, status}        # status: completed|failed|cancelled
workflow_runs_in_flight{workflow}                      # gauge
workflow_run_duration_seconds{workflow}                # histogram
workflow_step_duration_seconds{workflow, step}         # histogram
workflow_step_retries_total{workflow, step}
workflow_signals_total{workflow, event}
workflow_retention_swept_total                         # sweeper counter
```

Operators get Grafana dashboards and alerts (e.g.
`rate(workflow_runs_completed_total{status="failed"}[5m]) > N`)
without touching our code.

### What we deliberately don't build

- **Replay UI** — the data is in the journal; replay endpoint is cut
  from v1 and lands when demand justifies it.
- **Live log streaming per run** — runs run in the user's worker
  processes; logs go to whatever they've configured. We don't aggregate.
- **Visual workflow editor** — workflows are code, not diagrams. A
  diagram view of a static workflow is a nice-to-have but not a
  primitive.

---

## Server primitives

The runtime adds **one** new pgrx function. Everything else reuses what
the queue already exposes.

### `queue.workflow_complete_step`

The atomic transition: write the journal entry, send the continuation, ack
the in-flight message. One transaction.

```sql
queue.workflow_complete_step(
    run_id     BIGINT,
    step_name  TEXT,
    output     JSONB,           -- null if step terminally failed
    error      JSONB,           -- non-null on terminal failure
    next_msg   JSONB,            -- continuation payload; null if run terminates here
    next_delay INT,              -- delay_seconds for sleep/wait; 0 otherwise
    ack_msg_id BIGINT            -- the receipt being completed
) RETURNS void
```

Inside:

1. `INSERT INTO queue.workflow_step (run_id, step_name, output, error, ...)`
   with `ON CONFLICT DO NOTHING`. If a row already existed, the worker is a
   duplicate replay — discard `next_msg`, just delete `ack_msg_id`.
2. If `next_msg IS NOT NULL`: `INSERT INTO queue.q___wf_{name}` with
   `vt = now() + next_delay`, `message_group_id = run_id`. Capture the new
   msg_id; `UPDATE workflow_run SET current_msg_id = $new_id`.
3. If `next_msg IS NULL` (terminal): `UPDATE workflow_run SET status = ...,
   output = ..., error = ..., completed_at = now(), current_msg_id = NULL`.
4. `DELETE FROM queue.q___wf_{name} WHERE msg_id = ack_msg_id`.
5. `register_notify_after_commit('__wf_' || workflow_name)`.

Because `workflow_step`, `workflow_run`, and the queue tables live in the
same Postgres, this is one local transaction. No two-phase commit, no
compensating actions.

### Reused primitives

- `queue.send_fifo` — start a run = INSERT `workflow_run` row + send first
  message to FIFO group `run_id`. SDK helper, not a new pgrx fn.
- `queue.workflow_signal` — thin SQL wrapper that deletes
  `workflow_run.current_msg_id` from the queue table and calls
  `workflow_complete_step` with the signal payload as the journaled output.
  PL/pgSQL is fine; it's not a hot path.
- `queue.receive_fifo` — workers poll the workflow's queue. FIFO
  eligibility predicate already enforces single-flight per run.
- `queue.change_visibility` — used for retry backoff.
- `queue.delete` — cancel a run.

The queue's existing waiter registry wakes workflow handlers exactly the
way it wakes ordinary consumers. We don't add a new wakeup path.

---

## API surface

### Native REST (`/v1/`)

```
POST    /v1/workflows/{name}/runs                           Start a run. Body: {input, version?, idempotencyKey?, retentionDays?}
GET     /v1/workflows/{name}/runs                           List + filter. Query: status, since, until, cursor, limit
GET     /v1/workflows/{name}/runs/{run_id}                  Run status + output
DELETE  /v1/workflows/{name}/runs/{run_id}                  Cancel run
POST    /v1/workflows/{name}/runs/{run_id}/signals/{event}  Send signal. Body: payload
GET     /v1/workflows/{name}/runs/{run_id}/steps            Journal with timings
GET     /v1/workflows                                       List distinct workflow names + run counts (sidebar/index)
```

Five endpoints. No `POST /v1/workflows` — workflow definitions live in
user code. The server learns about a workflow the first time a run is
started for it; the queue (`__wf_{name}`) is created lazily.

Triggering from outside HTTP:

- **From a topic subscription**: subscribe a workflow to a topic via the
  existing `POST /v1/topics/{pattern}/subscriptions` with
  `{type: "workflow", name}`. New target type for the existing fan-out
  mechanism — one extra branch in `ops/topic.rs`.

---

## TypeScript SDK

```ts
import { createWorkflowClient, workflow } from "@beyond.dev/workflows";

// 1. Define a workflow.
export const onboardUser = workflow({
  name: "onboard-user",
  version: 1,
  retention: "30d",
  run: async (ctx, input: { email: string }) => {
    const user = await ctx.step.run(
      "create-user",
      () => db.users.create(input),
    );

    await ctx.step.sleep("welcome-delay", "5m");

    await ctx.step.run("send-welcome", () => email.send(user.id), {
      retry: { attempts: 5, backoff: { kind: "exp", base: "2s" } },
    });

    const verified = await ctx.step.waitForEvent("verify", {
      match: { userId: user.id },
      timeout: "7d",
    });

    if (!verified) {
      await ctx.step.run("nudge", () => email.nudge(user.id));
    }

    return user.id;
  },
});

// 2. Run a worker that polls the workflow's queue.
export const worker = onboardUser.serve({ url: process.env.QUEUE_URL });
// Inside: long-polls receive_fifo on __wf_onboard-user, replays journal,
// runs missing steps, calls workflow_complete_step.

// 3. Trigger from anywhere.
const client = createWorkflowClient({ url: process.env.QUEUE_URL });

const { runId } = await client.start("onboard-user", { email: "a@b.c" });

// Idempotent — same key returns the existing runId.
const { runId: same } = await client.start(
  "onboard-user",
  { email: "a@b.c" },
  { idempotencyKey: "user-42-onboard" },
);

await client.signal(runId, "verify", { userId: 42 });

const { status, output } = await client.runs.get("onboard-user", runId);
```

### `match` semantics

`step.waitForEvent` accepts a `match` object — a JSONB superset query.
This falls out of the queue's existing conditional-receive primitive
(`message @> $matcher`) and is strictly more expressive than a string
event-type filter:

```ts
// CF / Inngest: match by event name only
ctx.step.waitForEvent("approved", { type: "manager.approved" });

// Ours: match by any subset of the payload
ctx.step.waitForEvent("approved", {
  match: { kind: "manager.approved", managerId: 42 },
});
```

The `signal()` payload is matched against `match` via `payload @> match`.
First waiter whose superset-matches wins.

### Worker model (v1: long-poll)

`workflow.serve()` returns a long-running worker that:

1. Calls `receive_fifo` on `__wf_{name}` with a 30s wait.
2. On message: loads the run's journal, replays the body, runs the next
   missing step, calls `workflow_complete_step`.
3. On exception inside a `step.run`: calls `change_visibility` with the
   backoff delay; exits the handler.
4. Loops.

On Beyond, this is a long-running box. On serverless platforms a sidecar
worker (separate persistent process) runs the poll loop — same pattern as
self-hosted Inngest workers. There is no HTTP-push delivery model;
continuations live in the queue and pull-based workers consume them.

### Cooperative cancellation

The runtime passes `ctx.signal: AbortSignal` into every step. User code
that wants to be interruptible threads it into its async work:

```ts
await ctx.step.run("slow-call", () => fetch(url, { signal: ctx.signal }));
```

When `client.runs.cancel()` is called, the in-flight message is deleted
and the run is flagged `cancelled`. Any worker currently inside a step
callback receives `ctx.signal.aborted = true` and may abort cleanly. Code
that ignores `ctx.signal` finishes its current work — `workflow_complete_step`
becomes a no-op because the run is no longer `running`. This is the same
soft-cancel contract as every other workflow runtime.

---

## The fork story

All workflow state — the run rows, the journal, the in-flight queue
messages with their `vt` deadlines — lives in the user's Postgres on
their GlideFS volume. `glide fork` does a CoW of the volume; every byte
comes across at the same logical timestamp.

A run that was waiting on a 5-minute `step.sleep` in production is still
waiting in the fork, with the same deadline. When that wall clock is
reached, a worker on the fork picks up the continuation and runs the
next step against the fork's data. Production proceeds independently.
Same for `step.waitForEvent` — the wait message is on the volume, signals
sent to the fork resolve the fork's run, signals to production resolve
production's. Branches diverge.

Side-effect isolation (sandbox API keys, separate network egress, fork
not running workers for certain workflows) is the substrate's job. The
workflow runtime has no fork-aware code path.

---

## Versioning

`workflow_run.workflow_version` is set at start time. New runs use the
latest registered version. Old runs keep running against the version they
started with. The SDK refuses to handle a continuation for a version it
doesn't recognize:

```ts
class WorkflowVersionMismatch extends Error {
  expected: number;
  got: number;
}
```

Two binaries run side-by-side during a deploy until in-flight runs of
version N drain. On Beyond's box-per-app model, the orchestrator runs both
versions and routes by `workflow_version`.

---

## Trust boundaries

Same network model as the queue. Workflows don't add a security layer.

- Internal service. Operator's proxy is the perimeter.
- `Authorization` header presence required; contents not verified.
- Workflow names: `[a-z0-9_]`, 1–48 chars (must be a valid queue name —
  `__wf_` prefix uses 5 of the 48).
- Step names: enforced at SDK level — `[a-zA-Z0-9._-]{1,128}`. Server
  rejects > 256 bytes to bound journal size.

---

## Failure modes

| Failure                                               | What happens                                                                                                                                                 | Recovery                                                        |
| ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------- |
| Worker crashes mid-step                               | Continuation message vt expires; another worker picks it up; replays journal; reruns failed step.                                                            | Automatic. Step callback must be idempotent.                    |
| Worker crashes after journal commit                   | Cannot — journal write and continuation send are one Postgres tx.                                                                                            | n/a                                                             |
| `step.run` exhausts retries                           | Final journal row written with `error`; `workflow_run.status = 'failed'`; in-flight message deleted.                                                         | Operator inspects via `GET /runs/{id}/steps`; starts a new run. |
| Workflow throws an unhandled error outside `step.run` | Treated as a permanent body failure: run marked `failed` with the error.                                                                                     | Same as above.                                                  |
| `step.waitForEvent` times out                         | Wait message's `vt` reaches `now`; worker receives it, sees the wait marker, journals `{timeout: true}` and continues.                                       | Workflow body branches on the result.                           |
| Signal arrives before wait is registered              | Dropped. SDK does not buffer late signals in v1.                                                                                                             | Caller code orders the signal after observing the run.          |
| Two workers grab the same continuation                | Cannot — `receive_fifo` enforces single-flight per group.                                                                                                    | n/a                                                             |
| Database fails over mid-step                          | sqlx connection drops; vt expires; redelivery on a fresh connection.                                                                                         | Automatic.                                                      |
| Idempotent start collision                            | Existing `run_id` returned with current status. No duplicate row, no second continuation.                                                                    | n/a                                                             |
| Workflow definition removed but runs in flight        | Worker sees a continuation for an unknown workflow; logs error; vt expires repeatedly until `read_ct` exceeds threshold and the run is failed by the worker. | Restore the definition or `DELETE` the run.                     |

---

## Performance notes

- **Start a run**: 1 INSERT (`workflow_run`) + 1 INSERT (queue) + 1 NOTIFY.
- **Resume a run**: 1 `receive_fifo` + body re-execution + 1
  `workflow_complete_step` (1 INSERT, 1 INSERT, 1 UPDATE, 1 DELETE, 1
  NOTIFY).
- **Body re-execution cost**: linear in journal size. SDK fetches the
  whole journal once at the start of each handler invocation. For runs
  > 100 steps, the journal lookup is by `(run_id, step_name)` PK and is
  > in-memory after the first fetch.
- **Sleep accuracy**: bounded by `receive_fifo` latency. Existing waiter
  registry latency is sub-millisecond.
- **Fork cost**: O(1). Whatever GlideFS gives us.

---

## Project structure

Workflow code joins the existing `beyond-queue` crate workspace. No new
binary; the queue server gains workflow routes, the extension gains one
new pgrx function.

```
queue/
  src/
    ops/
      workflow.rs              # start, signal, complete_step, get, cancel
    routes/
      workflows.rs             # /v1/workflows/...
  beyond-queue-extension/
    src/
      workflow.rs              # pgrx: workflow_complete_step
    sql/
      schema.sql               # + workflow_status, workflow_run, workflow_step, workflow_signal()
  sdk/ts/
    workflows/
      package.json             # @beyond.dev/workflows
      src/
        client.ts              # createWorkflowClient — start/signal/runs.get/cancel
        runtime.ts             # workflow(), step.run/sleep/waitForEvent, replay engine
        worker.ts              # serve() — long-poll loop
```

The SDK ships two entry points within one package:

```ts
// trigger only — light
import { createWorkflowClient } from "@beyond.dev/workflows";

// define + serve — pulls in the replay engine
import { workflow } from "@beyond.dev/workflows/runtime";
```

---

## Composition with events

Topic subscriptions already exist (`POST /v1/topics/{pattern}/subscriptions`).
Workflows become a third subscription target type alongside `queue` and
`http`/`https`:

```ts
await events.subscriptions.create("user.signup", {
  type: "workflow",
  name: "onboard-user",
});
```

Server-side, the existing `queue.send_topic` fan-out gains a new branch:
when a subscription has `target_type = 'workflow'`, instead of `queue.send`
it calls the workflow start path. The publish from the user's perspective
is unchanged.

This is the small composition that earns the platform line. Publish a
domain event — it can hit a queue (decoupled async work), an HTTP endpoint
(webhook), or kick off a durable, multi-step, forkable workflow. Same
publish, three behaviors, one storage layer.

---

## Why this is the minimum effective abstraction

We add **one pgrx function, two tables, one sweeper task, one SDK
package**. Three SDK verbs (`run`, `sleep`, `waitForEvent`), four client
methods (`start`, `signal`, `runs.get`, `runs.cancel`), two operator env
vars (retention ceiling, sweeper interval).

Everything else is the queue. Workflows are not a separate service we
built next to the queue — they are what the queue grows into when its
messages start carrying durable journals.
