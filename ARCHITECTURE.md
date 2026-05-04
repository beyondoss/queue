# beyond-queue Architecture

beyond-queue is an HTTP service that accepts SQS-compatible and native REST requests, stores messages in PostgreSQL via the queue extension, and delivers them to consumers with visibility-timeout semantics. It is a private-network deployment: clients configure it as an SQS endpoint replacement without changing their SDK.

---

## Data Flow

### Request dispatch

```
HTTP request
     │
     ├── GET /healthz ───────────────────────────────► 200 OK  (no auth)
     │
     ▼
require_auth middleware
     │
     ├── no Authorization header ──────────────────► 403 Forbidden
     │
     ▼
Router
     ├── POST /{account_id}/{queue_name}  ──► sqs::router (path-based SQS)
     │
     ├── POST /  ──► gateway_handler
     │                    │
     │                    ├── X-Amz-Target: AmazonSNS.{Action} ──► sns::handle_service_request
     │                    │        │
     │                    │        ├── application/x-amz-json-1.0  ──► SnsProtocol::Json
     │                    │        └── application/x-www-form-urlencoded ──► SnsProtocol::Query
     │                    │
     │                    └── (anything else) ──► sqs::handle_service_request
     │                             │
     │                             ├── application/x-amz-json-1.0
     │                             │   X-Amz-Target: AmazonSQS.{Action} ──► SqsProtocol::Json
     │                             └── application/x-www-form-urlencoded
     │                                 Action= in body ──────────────────► SqsProtocol::Query
     │
     └── /v1/...  ──────────────────────────────────► routes::router (native REST)
```

### Message lifecycle (standard queue)

```
Producer                 beyond-queue                     PostgreSQL (queue extension)
   │                        │                                │
   │── POST /v1/queues ─────►│── queue.create($name) ────────►│ CREATE TABLE queue.q_{name}
   │   (or SQS CreateQueue)  │                               │ CREATE TABLE queue.a_{name}
   │                        │                               │ INSERT INTO queue.meta
   │                        │                               │
   │── POST /v1/queues/{n}/  │                               │
   │   messages ────────────►│── queue.send($name, msg, ...) ►│ INSERT INTO queue.q_{name}
   │   {message, delay}      │                               │   vt = now + delay_secs
   │◄── 201 {id} ───────────│◄── msg_id ────────────────────│
   │                        │   (XactCallback registered)   │
   │                        │                               │ [on commit] → notify_waiters
   │                        │                                     │
Consumer                    │                                     ▼
   │── GET /v1/queues/{n}/  │                          SetLatch on waiting readers
   │   messages?wait=5 ─────►│── queue.receive(                │
   │                        │    $name, vt, qty,              │
   │                        │    wait_secs, 100ms) ──────────►│ LOOP:
   │                        │                               │   ResetLatch
   │                        │                               │   UPDATE q_{name}
   │                        │                               │     SET vt = now+vt, read_ct++
   │                        │                               │   WHERE vt <= now
   │                        │                               │   FOR UPDATE SKIP LOCKED
   │                        │                               │   → if rows: return
   │                        │                               │   → else: WaitLatch(remaining)
   │◄── 200 [{id,message}] ─│◄── rows ──────────────────────│
   │                        │                               │
   │── DELETE /v1/queues/   │                               │
   │   {n}/messages/{id} ───►│── queue.delete($name, id) ────►│ DELETE FROM queue.q_{name}
   │◄── 204 ────────────────│                               │   WHERE msg_id = $id
```

### Error paths

```
Request → Auth middleware → no Authorization → 403 (no further processing)
Request → SQS dispatch → unknown Action → SqsErrorCode::InvalidAttributeName → 400 XML/JSON
Request → SQS action → deserialization fails → SqsErrorCode::InvalidMessageContents → 400
Request → ops layer → queue not found → 404 {"error": "Queue 'X' does not exist"}
Request → ops layer → sqlx error → 500 {"error": "Database error"} + tracing::error log
```

---

## Concepts & Terminology

| Term                        | What It Controls                                                                                                                            | NOT                                                                                               |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| **vt** (visibility timeout) | Timestamp before which a message is invisible to readers. Set to `now + vt_secs` on read; expires naturally.                                | A lock — expired vt makes the message visible again automatically.                                |
| **receipt handle**          | Opaque token `base64url("{queue_name}\x00{msg_id}")` encoding the queue and message ID. Used by SQS clients to delete or change visibility. | Stable across restarts; never changes once issued.                                                |
| **msg_id**                  | Auto-incrementing `BIGINT GENERATED ALWAYS AS IDENTITY (CACHE 100)` per queue. Native API uses this directly.                               | Globally unique — scoped to one queue table.                                                      |
| **read_ct**                 | Number of times a message has been delivered. Incremented atomically on each read.                                                          | Does not trigger any automatic action — consumers must check it if they need dead-letter logic.   |
| **account_id**              | Path segment in SQS URLs (`/{account_id}/{queue_name}`). Accepted but ignored.                                                              | Not authenticated or used for routing — any value works.                                          |
| **FIFO queue**              | Queue with `message_group_id` and `deduplication_id` columns. Delivers messages in per-group insertion order.                               | Not globally FIFO across groups — ordering is within a group only.                                |
| **WaiterGuard**             | RAII handle that registers/unregisters a backend's latch in the shared `WaiterRegistry`.                                                    | Does not hold a lock — registration is O(1) amortised, notification is O(waiters_for_this_queue). |

---

## Core Mechanisms

### Visibility timeout (at-least-once delivery)

`queue.receive` atomically updates `vt = now + vt_secs` and `read_ct++` in a single `UPDATE … RETURNING` statement using a `WITH … FOR UPDATE SKIP LOCKED` CTE. This means:

- A message locked by one consumer is invisible to all others until its vt expires.
- If a consumer crashes without deleting the message, vt expires and the message becomes visible again automatically — no external reaper needed.
- `FOR UPDATE SKIP LOCKED` lets concurrent readers spread across the heap without blocking each other.

### Push-based long-poll (WaitLatch)

When the extension is loaded via `shared_preload_libraries`, `receive` parks the calling PostgreSQL backend on `WaitLatch` between poll attempts. The wakeup path:

1. **Reader** (`receive`): registers latch in `WaiterRegistry`, resets its latch, attempts a read. On miss: `WaitLatch(WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, remaining_ms)`.
2. **Writer** (`send` / `send_batch`): after inserting, calls `register_notify_after_commit(queue_name)` which installs a `XactCallback`.
3. **On commit**: `XactCallback` fires `notify_waiters(queue_name)`, which hashes the name to a registry bucket and calls `SetLatch` on each matching backend's `MyLatch`.
4. **Reader wakes**: `ResetLatch` → re-attempt read → returns messages.

Race safety: the latch is reset _before_ each read attempt, so a `SetLatch` arriving during the SPI call is not missed — `WaitLatch` will return immediately on the next iteration.

**Degraded mode**: if the extension is not in `shared_preload_libraries`, `REGISTRY_READY` stays false and `WaiterGuard::new` is a no-op. `receive` falls back to `WL_TIMEOUT`-only polling — correct but higher latency.

### Why the 3-arg `queue.receive_fifo` stays PL/pgSQL

The 3-arg no-wait `receive_fifo(queue_name, vt, qty)` is implemented in PL/pgSQL, not pgrx. A pgrx `TableIterator<'static, T>` extracts every datum from each row into a Rust type then re-encodes it when PostgreSQL fetches the row — 14 datum conversions per row. PL/pgSQL `RETURN QUERY EXECUTE` copies heap tuples once. Measured delta: 6.7× latency single-threaded, ~46% slower end-to-end.

The 5-arg `receive` and `receive_fifo` overloads must be pgrx because `WaitLatch` cannot be called from PL/pgSQL. They override the PL/pgSQL fallbacks when the extension is loaded via `shared_preload_libraries`.

### SQS protocol dispatch

`detect_and_parse` in `src/sqs/mod.rs` reads the `Content-Type` header:

| Content-Type                 | Header needed                      | Protocol             | Response format                        |
| ---------------------------- | ---------------------------------- | -------------------- | -------------------------------------- |
| `application/x-amz-json-1.0` | `X-Amz-Target: AmazonSQS.{Action}` | `SqsProtocol::Json`  | JSON with `application/x-amz-json-1.0` |
| anything else                | `Action=` key in body              | `SqsProtocol::Query` | XML with `text/xml`                    |

The parsed body is normalized to `serde_json::Value` and dispatched to the same `ops/` functions regardless of protocol. `SqsContext` carries the protocol variant through the handler so `ctx.ok(body)` and `ctx.error(code)` emit the correct format.

FIFO queues are identified by `.fifo` suffix in the queue name (SQS convention). The suffix is stripped before hitting the database; the internal queue table name never contains `.fifo`.

### SNS protocol dispatch

`POST /` is shared between SQS and SNS. The `gateway_handler` in `src/lib.rs` checks the `X-Amz-Target` header prefix:

- `AmazonSNS.{Action}` → `sns::handle_service_request`
- anything else → `sqs::handle_service_request`

SNS supports the same two wire formats as SQS (JSON and Query/form-encoded). Responses are SNS-shaped XML or JSON wrapped in `{Action}Response > {Action}Result` per the SNS spec.

**Actions implemented:** `CreateTopic`, `DeleteTopic`, `ListTopics`, `Subscribe`, `Unsubscribe`, `ListSubscriptions`, `ListSubscriptionsByTopic`, `Publish`, `GetTopicAttributes`, `SetTopicAttributes` (no-op), `GetSubscriptionAttributes`, `ConfirmSubscription` (auto-confirm).

**Topics are implicit.** `CreateTopic` returns an ARN synthesized from the name (`arn:aws:sns:us-east-1:000000000000:{name}`) but stores nothing. `ListTopics` derives topic names from distinct patterns in `queue.topic_subscriptions`. `DeleteTopic` deletes all subscriptions for that pattern. This means a topic with zero subscriptions won't appear in `ListTopics` — the edge case is not worth an extra table.

**Subscribe protocol restriction:** only `Protocol=sqs` is accepted. The endpoint must be a queue URL; the queue name is extracted from the last path segment.

**Publish delivery:** the message is wrapped in a standard SNS notification envelope before being stored in the target queues. When a consumer calls SQS `ReceiveMessage`, the `Body` field is the JSON string of the envelope — the same format real SNS delivers:

```json
{
  "Type": "Notification",
  "MessageId": "uuid",
  "TopicArn": "arn:aws:sns:us-east-1:000000000000:my-topic",
  "Message": "the original Publish body",
  "Timestamp": "2024-01-01T00:00:00.000Z",
  "SignatureVersion": "1",
  "Signature": "EXAMPLE",
  ...
}
```

**Subscription ARNs** encode `(topic, queue)` as `arn:aws:sns:us-east-1:000000000000:{topic}:{queue}`. Stable across restarts; `Unsubscribe` parses them back.

**ARN region and account** are hardcoded to `us-east-1` / `000000000000`, matching the SQS layer. Clients round-trip ARNs; the values are never authenticated.

### Topic fanout

`POST /v1/topics/{routing_key}` calls `queue.send_topic(routing_key, msg, headers, delay)`, which:

1. Validates the routing key (`[a-zA-Z0-9._-]+`, no leading/trailing/consecutive dots, max 255 chars).
2. Queries `queue.topic_subscriptions` for all bindings where `routing_key ~ compiled_regex`.
3. Calls `queue.send` once per matching queue. Returns the count of matched queues.

Bindings are stored as `(pattern, queue_name)` with a stored-generated `compiled_regex` column. Pattern wildcards:

- `*` matches a single segment (no dots) → compiled to `[^.]+`
- `#` matches zero or more segments → compiled to `.*`

### Queue name validation

`validate_name` in `beyond-queue-extension/src/queue.rs` enforces: 1–48 characters, `[a-z0-9_]` only. Violations raise a PostgreSQL `ERROR` via `pgrx::error!()`. SQL wrappers in schema.sql have an additional length check of 47 (off-by-one from different code paths — the pgrx check of 48 is authoritative).

### Receipt handle encoding

`src/sqs/receipt.rs` encodes: `base64url("{queue_name}\x00{msg_id}")`. Decode splits on the null byte. The encoding is stable — changing it would break any in-flight receipt handles across a restart.

---

## FIFO Group Serialization

A FIFO read is only permitted when the entire group has no in-flight messages. The eligibility predicate:

```sql
GROUP BY message_group_id
HAVING BOOL_AND(vt <= clock_timestamp())
ORDER BY MIN(msg_id) ASC
LIMIT 1
```

`BOOL_AND(vt <= now)` is equivalent to `NOT EXISTS(vt > now)` but avoids a correlated subplan. Covered by the `_grpvt_idx` index on `(message_group_id, vt ASC, msg_id ASC)` for an index-only scan.

Within the selected group, messages are delivered in `msg_id ASC` order (FIFO).

---

## Trust Boundaries

**What the service verifies:**

- `Authorization` header is present on all non-health requests. Any value passes.
- Queue names must match `[a-z0-9_]`, 1–48 chars (enforced by pgrx, raises PostgreSQL ERROR on violation).
- Routing keys and topic patterns are validated for length and character set.

**What passes through unchecked:**

- SigV4 signature content — the header is present but not verified. This is intentional for LocalStack/ElasticMQ compatibility; the network boundary is the security layer.
- Account ID in SQS path (`/{account_id}/{queue_name}`) — any value is accepted.
- Message body content — no schema validation, no size limit enforced at the HTTP layer.

**Unauthenticated endpoints:**

- `GET /healthz` — returns 200 OK unconditionally.

---

## Configuration

| Variable                     | Default                 | What It Controls                                                                                                  |
| ---------------------------- | ----------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`               | (required)              | PostgreSQL connection string passed to sqlx `PgPoolOptions`.                                                      |
| `ADDRESS`                    | `0.0.0.0:9324`          | TCP bind address for the HTTP server.                                                                             |
| `DEFAULT_VISIBILITY_TIMEOUT` | `30`                    | Seconds applied when a `ReceiveMessage` request omits `VisibilityTimeout`.                                        |
| `MAX_CONNECTIONS`            | `10`                    | Hard cap on the sqlx connection pool. Excess operations wait for a free slot.                                     |
| `LOG_LEVEL`                  | `info`                  | `EnvFilter` directive (e.g. `beyond_queue=debug,info`). JSON-structured output.                                   |
| `OTLP_ENABLED`               | `false`                 | Enable OpenTelemetry OTLP trace export over gRPC.                                                                 |
| `OTLP_ENDPOINT`              | `http://localhost:4317` | gRPC OTLP collector. Used when `OTLP_ENABLED=true`.                                                               |
| `BASE_URL`                   | `http://{ADDRESS}`      | Base URL for SQS queue URLs returned to clients (`{BASE_URL}/000000000000/{name}`). Override when behind a proxy. |

---

## Failure Modes

| Failure                                     | What Actually Happens                                                                                 | Recovery                                                                                       |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Consumer crashes before deleting message    | Message stays in `queue.q_{name}` with vt in the future. When vt expires, next read returns it again. | None needed — automatic re-delivery. `read_ct` increments on each delivery.                    |
| PostgreSQL connection pool exhausted        | sqlx returns `PoolTimedOut`; handler returns 500 with `{"error": "Database error"}`.                  | Client retries. Pool clears as in-flight connections finish.                                   |
| PostgreSQL unavailable at startup           | `db::connect` fails; process exits non-zero.                                                          | Restart the process once PostgreSQL is available.                                              |
| PostgreSQL unavailable mid-flight           | sqlx returns an error; handler returns 500.                                                           | Client retries. Pool reconnects on next use.                                                   |
| Extension not in `shared_preload_libraries` | `WaiterRegistry` not initialized; `receive` falls back to `WL_TIMEOUT` polling at `poll_interval_ms`. | Functional but higher read latency. Fix by adding the extension to `shared_preload_libraries`. |
| Postmaster death during `WaitLatch`         | `WL_EXIT_ON_PM_DEATH` triggers; backend exits.                                                        | PostgreSQL restarts the backend on next connection.                                            |
| Queue name injection attempt                | `validate_name` in pgrx raises PostgreSQL ERROR (`pgrx::error!()`).                                   | Caught by the `match $handler(…).await` macro arm; returned as 400/InternalError to client.    |
| Mismatched headers array in `send_batch`    | pgrx raises PostgreSQL ERROR comparing array lengths before insert.                                   | Client receives 500. No partial insert.                                                        |

---

## File Map

| Path                                    | What It Does                                                                                                                                                                                                  |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/main.rs`                           | Binary entry point; delegates to `beyond_queue::run()`. Sets jemalloc as allocator.                                                                                                                           |
| `src/lib.rs`                            | Wires the axum router: `/v1/` (REST) + SNS/SQS gateway at `POST /` + SQS path handler + `/healthz`. Attaches `require_auth` to all except healthz.                                                           |
| `src/config.rs`                         | `Config` struct parsed from CLI args / env vars via clap.                                                                                                                                                     |
| `src/db.rs`                             | Creates `PgPool` with `max_connections`.                                                                                                                                                                      |
| `src/middleware/auth.rs`                | Checks for presence of `Authorization` header; rejects with 403 if absent.                                                                                                                                    |
| `src/ops/send.rs`                       | `queue.send`, `queue.send_batch`, `queue.send_fifo` — single/batch/FIFO inserts.                                                                                                                              |
| `src/ops/receive.rs`                    | `queue.receive`, `queue.receive_fifo` — long-poll reads.                                                                                                                                                      |
| `src/ops/delete.rs`                     | `queue.delete` — single and batch deletes.                                                                                                                                                                    |
| `src/ops/visibility.rs`                 | `queue.change_visibility` — change visibility timeout by msg_id.                                                                                                                                              |
| `src/ops/queue_admin.rs`                | `queue.create`, `queue.create_fifo`, `queue.delete_queue`, `queue.list_queues`, `queue.metrics`, `queue.purge_queue`.                                                                                         |
| `src/ops/topic.rs`                      | `queue.send_topic` fan-out; subscribe/unsubscribe/list ops; SNS-specific list/delete helpers.                                                                                                                 |
| `src/routes/queues.rs`                  | `GET/POST /v1/queues`, `GET/DELETE /v1/queues/{name}`, `POST /v1/queues/{name}/purge`.                                                                                                                        |
| `src/routes/messages.rs`                | `GET/POST/DELETE /v1/queues/{name}/messages`, `DELETE/PATCH /v1/queues/{name}/messages/{id}`.                                                                                                                 |
| `src/routes/topics.rs`                  | `POST /v1/topics/{routing_key}`, subscription CRUD endpoints.                                                                                                                                                 |
| `src/sns/mod.rs`                        | SNS service handler. Protocol detection (JSON/Query), action dispatch.                                                                                                                                        |
| `src/sns/context.rs`                    | `SnsContext` — per-request protocol + request ID + action. ARN helpers. Serializes responses as SNS-shaped JSON or XML.                                                                                       |
| `src/sns/types.rs`                      | Request/response structs for all SNS actions.                                                                                                                                                                 |
| `src/sns/error.rs`                      | `SnsError` + `SnsErrorCode` — serializes to JSON or XML.                                                                                                                                                      |
| `src/sns/actions/`                      | One file per SNS action. Each delegates to `ops/`.                                                                                                                                                            |
| `src/sqs/mod.rs`                        | Protocol detection, action dispatch macro. Path-based route handler + `handle_service_request` called from gateway.                                                                                           |
| `src/sqs/context.rs`                    | `SqsContext` — per-request protocol + request ID. Serializes responses as JSON or XML.                                                                                                                        |
| `src/sqs/receipt.rs`                    | `encode`/`decode` for receipt handles: `base64url("{queue_name}\x00{msg_id}")`.                                                                                                                               |
| `src/sqs/types.rs`                      | Request/response structs for all SQS actions.                                                                                                                                                                 |
| `src/sqs/error.rs`                      | `SqsError` + `SqsErrorCode` — serializes to JSON or XML depending on protocol.                                                                                                                                |
| `src/sqs/util.rs`                       | `queue_name_from_url`, `md5_of`, `message_attributes_to_headers`.                                                                                                                                             |
| `src/sqs/actions/`                      | One file per SQS action. Each delegates to `ops/`.                                                                                                                                                            |
| `beyond-queue-extension/src/lib.rs`     | pgrx module root. Installs shared-memory hooks in `_PG_init`. Loads `schema.sql`.                                                                                                                             |
| `beyond-queue-extension/src/queue.rs`   | Hot-path pgrx C functions: `send`, `send_batch` (and FIFO variants), `receive`, `receive_fifo`, `delete`, `archive`, `pop`, `change_visibility`.                                                              |
| `beyond-queue-extension/src/waiter.rs`  | `WaiterRegistry` in shared memory. FNV-1a hash, 256 buckets, 4096 slots. `WaiterGuard` RAII, `notify_waiters`, `register_notify_after_commit`.                                                                |
| `beyond-queue-extension/sql/schema.sql` | DDL for `queue.meta`, `queue.q_{name}`, `queue.a_{name}`, `queue.topic_subscriptions`, `queue.notify_insert_throttle`. PL/pgSQL functions: `receive_fifo`, FIFO grouped reads, topic routing, notification system. |

---

## API Reference

### Native REST API (`/v1/`)

| Method   | Path                              | Operation                                                                                                                     |
| -------- | --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `POST`   | `/v1/queues`                      | Create queue. Body: `{"name": "...", "fifo": false}`. Returns 201.                                                            |
| `GET`    | `/v1/queues`                      | List all queues. Returns array of `{name, is_partitioned, is_unlogged, created_at}`.                                          |
| `GET`    | `/v1/queues/{name}`               | Queue metrics: `{queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time}`.                         |
| `DELETE` | `/v1/queues/{name}`               | Delete queue. Returns 204 if deleted, 404 if not found.                                                                       |
| `POST`   | `/v1/queues/{name}/purge`         | Delete all messages. Returns `{"deleted": N}`.                                                                                |
| `POST`   | `/v1/queues/{name}/messages`      | Send single `{message, headers?, delay?}` or batch `[{message, headers?, delay?, group_id?}]`. Returns 201 `{id}` or `{ids}`. |
| `GET`    | `/v1/queues/{name}/messages`      | Receive. Query params: `max` (default 1), `vt` (default 30), `wait` (default 0), `fifo` (default false). Returns array.       |
| `DELETE` | `/v1/queues/{name}/messages`      | Batch delete. Body: `{"ids": [1,2,3]}`. Returns `{"deleted": [1,2,3]}`.                                                       |
| `DELETE` | `/v1/queues/{name}/messages/{id}` | Delete single. Returns 204 or 404.                                                                                            |
| `PATCH`  | `/v1/queues/{name}/messages/{id}` | Change visibility. Body: `{"vt": 60}`. Returns `{"id": N, "visible_at": "..."}`.                                              |
| `POST`   | `/v1/topics/{routing_key}`        | Fan-out. Body: `{message, headers?, delay?}`. Returns 201 `{"queues_matched": N}`.                                            |

### SQS-compatible API

Routes: `POST /` and `POST /{account_id}/{queue_name}`.

Supported actions: `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`, `DeleteMessageBatch`, `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch`, `CreateQueue`, `DeleteQueue`, `GetQueueUrl`, `GetQueueAttributes`, `ListQueues`, `PurgeQueue`.

Queue URL format: `{BASE_URL}/000000000000/{queue_name}`.
FIFO queues use `.fifo` suffix in queue name.
