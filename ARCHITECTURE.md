# beyond-queue Architecture

beyond-queue is an HTTP service that accepts SQS-compatible and native REST requests, stores messages in PostgreSQL via the queue extension, and delivers them to consumers with visibility-timeout semantics. It is a private-network deployment: clients configure it as an SQS endpoint replacement without changing their SDK.

---

## Data Flow

### Request dispatch

```
HTTP request
     │
     ├── GET /livez ─────────────────────────────────► 200 JSON {status,version}  (no auth)
     ├── GET /readyz ────────────────────────────────► 200 / 503 JSON  (no auth)
     ├── GET /metrics ───────────────────────────────► Prometheus text/plain  (no auth)
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
     │                    ├── X-Amz-Target: AmazonSNS.* header ─────► sns::handle_service_request
     │                    │        │
     │                    │        ├── application/x-amz-json-1.0  ──► SnsProtocol::Json
     │                    │        └── application/x-www-form-urlencoded ──► SnsProtocol::Query
     │                    │
     │                    ├── form-urlencoded + SNS Action= in body ► sns::handle_service_request
     │                    │
     │                    └── (anything else) ──────────────────────► sqs::handle_service_request
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

`POST /` is shared between SQS and SNS. The `gateway_handler` in `src/lib.rs` uses a two-step check:

1. If `X-Amz-Target` starts with `AmazonSNS.` → `sns::handle_service_request` (JSON protocol).
2. Else if `Content-Type` is **not** `application/x-amz-json-1.0`, peek at the `Action` field in the form body. If it is a known SNS action → `sns::handle_service_request` (Query/form protocol).
3. Anything else → `sqs::handle_service_request`.

This two-step check is necessary because SNS Query-protocol requests carry no `X-Amz-Target` header — the action is embedded in the form body.

SNS supports the same two wire formats as SQS (JSON and Query/form-encoded). Responses are SNS-shaped XML or JSON wrapped in `{Action}Response > {Action}Result` per the SNS spec.

**Actions implemented:** `CreateTopic`, `DeleteTopic`, `ListTopics`, `Subscribe`, `Unsubscribe`, `ListSubscriptions`, `ListSubscriptionsByTopic`, `Publish`, `GetTopicAttributes`, `SetTopicAttributes` (no-op), `GetSubscriptionAttributes`, `ConfirmSubscription` (auto-confirm).

**Topics are implicit.** `CreateTopic` returns an ARN synthesized from the name (`arn:aws:sns:us-east-1:000000000000:{name}`) but stores nothing. `ListTopics` derives topic names from distinct patterns in `queue.topic_subscriptions`. `DeleteTopic` deletes all subscriptions for that pattern. This means a topic with zero subscriptions won't appear in `ListTopics` — the edge case is not worth an extra table.

**Subscribe protocols:** `sqs`, `http`, and `https` are all accepted. For SQS subscriptions, the endpoint must be a queue URL and the queue name is extracted from the last path segment. For HTTP/HTTPS, the endpoint URL is stored directly and the delivery worker POSTs to it.

**Publish delivery:** the message is wrapped in a standard SNS notification envelope. For SQS subscriptions the envelope is stored as the message `Body`. For HTTP/HTTPS subscriptions, `raw_delivery` controls whether the raw payload or the envelope is POSTed. `RawMessageDelivery=true` (via `SetSubscriptionAttributes`) posts the raw payload.

```json
{
  "Type": "Notification",
  "MessageId": "uuid",
  "TopicArn": "arn:aws:sns:us-east-1:000000000000:my-topic",
  "Message": "the original Publish body",
  "Timestamp": "2024-01-01T00:00:00.000Z",
  "SignatureVersion": "2",
  "Signature": "<RSA-SHA256 base64>",
  "SigningCertURL": "http://{BASE_URL}/SimpleNotificationService.pem"
}
```

The signature follows the SNS v2 spec: alphabetically sorted field name/value pairs each terminated by `\n`, signed with an RSA-2048 key generated at startup. The corresponding public certificate is served at `GET /SimpleNotificationService.pem` and `GET /v1/cert`.

**Subscription ARNs** encode `(topic, id)` as `arn:aws:sns:us-east-1:000000000000:{topic}:{id}` for HTTP/HTTPS subscriptions and `(topic, queue_name)` for SQS. `Unsubscribe` parses the key: numeric → `unsubscribe_by_id`; non-numeric → treat as queue name for SQS.

**ARN region and account** are hardcoded to `us-east-1` / `000000000000`, matching the SQS layer. Clients round-trip ARNs; the values are never authenticated.

### Event fanout

`POST /v1/events/{routing_key}` fans out to both SQS queues and HTTP endpoints:

1. `queue.send_topic(routing_key, msg, headers, delay)` — fan-out to SQS subscriptions only. Validates the routing key, queries `queue.topic_subscriptions` where `routing_key ~ compiled_regex AND queue_name IS NOT NULL`, calls `queue.send` once per match.
2. `queue.queue_http_deliveries(routing_key, raw_msg, envelope_msg)` — inserts one row into `queue.http_deliveries` per matching HTTP/HTTPS subscription. `raw_delivery=true` stores `raw_msg`; `false` stores `envelope_msg`.

Both steps happen inside the same HTTP handler (`src/ops/event.rs`). The SQS fan-out is synchronous; HTTP deliveries are asynchronous (picked up by the delivery worker).

The delivery worker (`src/ops/delivery.rs`) polls `http_deliveries` in a background task. Each poll opens a transaction, fetches pending rows with `FOR UPDATE SKIP LOCKED`, POSTs to each endpoint, and deletes on success or updates `attempt` / `next_attempt_at` on failure. Backoff schedule: 10s → 30s → 60s → 300s. Exhausted rows (`attempt >= max_attempts`) stay for inspection; no automatic reaping.

**REST API subscriptions** default to `raw_delivery=true` (raw payload). Opt in to the SNS envelope with `"envelope": true` in the subscribe body. **SNS wire protocol subscriptions** default to `raw_delivery=false` (envelope); set `RawMessageDelivery=true` via `SetSubscriptionAttributes` to switch.

Bindings are stored in `queue.topic_subscriptions` with columns `protocol`, `endpoint`, `queue_name` (nullable), `raw_delivery`, and a stored-generated `compiled_regex`. Pattern wildcards:

- `*` matches a single segment (no dots) → compiled to `[^.]+`
- `#` matches zero or more segments → compiled to `.*`

### Write coalescer

When `LINGER_MS > 0`, non-FIFO sends are routed through a background coalescing task (`src/ops/coalesce.rs`) instead of writing to the database directly.

Each send submits a `PendingMessage` to an `mpsc::channel` (capacity 16 384) and awaits a `oneshot` reply with the assigned `msg_id`. The background task:

1. Blocks on `rx.recv()` until the first message arrives.
2. Collects additional messages that arrive within `linger_ms` via `timeout_at`.
3. Groups the batch by `(queue_name, delay)` — messages with different keys need separate DB calls.
4. Flushes each group: if the group has 1 message → `send_message`; if > 1 → `send_batch`.
5. Fans the resulting `msg_id`s back to the waiting callers via `oneshot`.

`sync_commit=false` (async commit opt-out) is honoured only when **every** message in the group requests it; a single `sync_commit=true` member forces synchronous commit for the whole batch.

Tradeoff: up to `LINGER_MS` added tail latency per message; messages in-flight in the channel are lost on crash (same risk as any in-flight HTTP request). Default `LINGER_MS=0` disables coalescing entirely.

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

- `GET /livez` — liveness check; returns `{"status":"ok","version":"..."}`. Use for Kubernetes `livenessProbe` to avoid restart loops during DB outages.
- `GET /readyz` — readiness check; queries the DB (`SELECT 1`). Returns `{"status":"ok"}` when healthy, `{"status":"degraded"}` + 503 when the DB is unreachable. Use for Kubernetes `readinessProbe`.
- `GET /metrics` — Prometheus text exposition (`text/plain; version=0.0.4`). Scrape with any Prometheus-compatible collector. No auth — restrict via network policy if needed.

---

## Configuration

| Variable                     | Default                 | What It Controls                                                                                                                 |
| ---------------------------- | ----------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| `DATABASE_URL`               | (required)              | PostgreSQL connection string passed to sqlx `PgPoolOptions`.                                                                     |
| `ADDRESS`                    | `0.0.0.0:9324`          | TCP bind address for the HTTP server.                                                                                            |
| `DEFAULT_VISIBILITY_TIMEOUT` | `30`                    | Seconds applied when a `ReceiveMessage` request omits `VisibilityTimeout`.                                                       |
| `MAX_CONNECTIONS`            | `10`                    | Hard cap on the sqlx connection pool. Excess operations wait for a free slot.                                                    |
| `LOG_LEVEL`                  | `info`                  | `EnvFilter` directive (e.g. `beyond_queue=debug,info`). JSON-structured output.                                                  |
| `OTLP_ENABLED`               | `false`                 | Enable OpenTelemetry OTLP trace export over gRPC.                                                                                |
| `OTLP_ENDPOINT`              | `http://localhost:4317` | gRPC OTLP collector. Used when `OTLP_ENABLED=true`.                                                                              |
| `OTLP_SAMPLE_RATE`           | `0.1`                   | Fraction of traces sampled (0.0 = never, 1.0 = always). Only effective when `OTLP_ENABLED=true`.                                 |
| `LINGER_MS`                  | `0`                     | Write coalescer window (ms). Non-FIFO sends are held up to this duration and flushed as a single batch. `0` disables coalescing. |
| `BASE_URL`                   | `http://{ADDRESS}`      | Base URL for SQS queue URLs returned to clients (`{BASE_URL}/000000000000/{name}`). Override when behind a proxy.                |
| `HTTP_DELIVERY_ENABLED`      | `true`                  | Enable the background HTTP/HTTPS delivery worker.                                                                                |
| `HTTP_DELIVERY_POLL_MS`      | `1000`                  | Delivery worker poll interval (ms). Lower values increase responsiveness at the cost of idle DB load.                            |
| `HTTP_DELIVERY_TIMEOUT_SECS` | `5`                     | Per-request timeout for outbound webhook POSTs.                                                                                  |
| `HTTP_DELIVERY_BATCH_SIZE`   | `50`                    | Maximum rows the delivery worker claims per poll cycle. Tune up under high SNS fanout load.                                      |

---

## Observability

### Prometheus metrics (`GET /metrics`)

The service exposes Prometheus-format metrics. Every request updates the HTTP counters; queue ops update per-queue counters inside the `ops/` layer; the delivery worker updates delivery counters; a background task (`start_queue_depth_scrape`, 15 s interval) sets the queue-depth gauges.

| Metric                                    | Type      | Labels                           | What It Measures                                                       |
| ----------------------------------------- | --------- | -------------------------------- | ---------------------------------------------------------------------- |
| `http_requests_total`                     | counter   | `method`, `path`, `status`       | Requests completed                                                     |
| `http_request_duration_seconds`           | histogram | `method`, `path`                 | Request latency (buckets: 5ms–2.5s)                                    |
| `http_connections_active`                 | gauge     | —                                | Requests in flight                                                     |
| `queue_messages_sent_total`               | counter   | `queue`                          | Messages enqueued                                                      |
| `queue_messages_received_total`           | counter   | `queue`                          | Messages delivered to consumers                                        |
| `queue_messages_deleted_total`            | counter   | `queue`                          | Messages acknowledged (deleted)                                        |
| `queue_messages_redelivered_total`        | counter   | `queue`                          | Messages delivered with `read_count > 1` (missed ack before vt expiry) |
| `queue_message_send_duration_seconds`     | histogram | `queue`                          | DB send latency (1ms–1s)                                               |
| `queue_message_receive_duration_seconds`  | histogram | `queue`                          | DB receive latency (1ms–1s)                                            |
| `queue_message_delete_duration_seconds`   | histogram | `queue`                          | DB delete latency (1ms–1s)                                             |
| `queue_message_age_at_receive_seconds`    | histogram | `queue`                          | Age of message at first delivery (0.1s–1h)                             |
| `queue_delivery_attempts_total`           | counter   | `outcome` (`success`\|`failure`) | HTTP webhook delivery attempts                                         |
| `queue_delivery_attempt_duration_seconds` | histogram | `outcome`                        | Webhook round-trip latency (50ms–10s)                                  |
| `queue_delivery_exhausted_total`          | counter   | —                                | Deliveries permanently abandoned after max retries                     |
| `queue_coalescer_flush_batch_size`        | histogram | —                                | Messages per coalescer flush (1–1000)                                  |
| `queue_depth`                             | gauge     | `queue`                          | Current visible messages (scrape lag ≤ 15s)                            |
| `queue_in_flight`                         | gauge     | `queue`                          | Current locked/delayed messages (scrape lag ≤ 15s)                     |
| `db_pool_size`                            | gauge     | —                                | Total DB connections (idle + active)                                   |
| `db_pool_idle`                            | gauge     | —                                | Idle DB connections                                                    |
| `db_pool_active`                          | gauge     | —                                | Active (checked-out) DB connections                                    |
| `db_pool_acquire_timeouts_total`          | counter   | —                                | Pool exhaustion errors                                                 |

### Distributed tracing (OTLP)

When `OTLP_ENABLED=true`, spans are exported to the configured gRPC collector. Incoming W3C `traceparent`/`tracestate` headers are propagated so spans become children of the caller's trace (`OtelMakeSpan` in `src/lib.rs`). `OTLP_SAMPLE_RATE` controls head-based sampling (default 10%).

---

## Failure Modes

| Failure                                          | What Actually Happens                                                                                                                                                                               | Recovery                                                                                                                   |
| ------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Consumer crashes before deleting message         | Message stays in `queue.q_{name}` with vt in the future. When vt expires, next read returns it again.                                                                                               | None needed — automatic re-delivery. `read_ct` increments on each delivery.                                                |
| PostgreSQL connection pool exhausted             | sqlx returns `PoolTimedOut`; handler returns 500 with `{"error": "Database error"}`.                                                                                                                | Client retries. Pool clears as in-flight connections finish.                                                               |
| PostgreSQL unavailable at startup                | `db::connect` fails; process exits non-zero.                                                                                                                                                        | Restart the process once PostgreSQL is available.                                                                          |
| PostgreSQL unavailable mid-flight                | sqlx returns an error; handler returns 500.                                                                                                                                                         | Client retries. Pool reconnects on next use.                                                                               |
| Extension not in `shared_preload_libraries`      | `WaiterRegistry` not initialized; `receive` falls back to `WL_TIMEOUT` polling at `poll_interval_ms`.                                                                                               | Functional but higher read latency. Fix by adding the extension to `shared_preload_libraries`.                             |
| Postmaster death during `WaitLatch`              | `WL_EXIT_ON_PM_DEATH` triggers; backend exits.                                                                                                                                                      | PostgreSQL restarts the backend on next connection.                                                                        |
| Queue name injection attempt                     | `validate_name` in pgrx raises PostgreSQL ERROR (`pgrx::error!()`).                                                                                                                                 | Caught by the `match $handler(…).await` macro arm; returned as 400/InternalError to client.                                |
| Mismatched headers array in `send_batch`         | pgrx raises PostgreSQL ERROR comparing array lengths before insert.                                                                                                                                 | Client receives 500. No partial insert.                                                                                    |
| HTTP endpoint returns non-2xx                    | Delivery worker increments `attempt`, sets `next_attempt_at = now + backoff`. Row stays in `http_deliveries`.                                                                                       | Worker retries after backoff (10s/30s/60s/300s). After `max_attempts` (5), row stays as dead-letter for inspection.        |
| HTTP endpoint unreachable / timeout              | Same as non-2xx: recorded as failure, retried with backoff.                                                                                                                                         | Same retry path. `last_error` column stores the error string.                                                              |
| Delivery worker restart mid-batch                | Transaction rolls back; rows revert to pending state (next_attempt_at unchanged).                                                                                                                   | Worker picks them up again on next poll. `FOR UPDATE SKIP LOCKED` prevents double-delivery across concurrent workers.      |
| Process killed while coalescer has pending sends | Messages in the `mpsc` channel are lost (not yet written to DB).                                                                                                                                    | Same as losing any in-flight HTTP request — client must retry. `LINGER_MS=0` eliminates this risk at the cost of batching. |
| SIGTERM received                                 | `shutdown_signal()` resolves; axum stops accepting new connections and drains in-flight requests. Coalescer drains with a 10s deadline; delivery worker aborts (abort-safe via lease-based design). | Graceful — no messages lost for in-flight DB ops.                                                                          |
| HTTP request exceeds 30s                         | `TimeoutLayer` returns 408 Request Timeout. DB query may still complete; client should retry with idempotency.                                                                                      | Client retries.                                                                                                            |

---

## File Map

| Path                                    | What It Does                                                                                                                                                                                                                                                         |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/main.rs`                           | Binary entry point; delegates to `beyond_queue::run()`. Sets jemalloc as allocator.                                                                                                                                                                                  |
| `src/lib.rs`                            | Wires the axum router: `/v1/` (REST) + SNS/SQS gateway at `POST /` + SQS path handler + `/livez` + `/readyz` + `/metrics`. Attaches `require_auth` to all authenticated routes. `record_metrics` middleware runs on every request.                                   |
| `src/config.rs`                         | `Config` struct parsed from CLI args / env vars via clap.                                                                                                                                                                                                            |
| `src/db.rs`                             | Creates `PgPool` with `max_connections`.                                                                                                                                                                                                                             |
| `src/error.rs`                          | `ApiError` enum (QueueNotFound, BadRequest, Internal, DbPoolTimeout). Implements `IntoResponse` for axum.                                                                                                                                                            |
| `src/metrics.rs`                        | `Metrics` struct — Prometheus `Registry` with counters, histograms, and gauges for HTTP, queue ops, delivery, coalescer, and DB pool. Exposed at `GET /metrics`.                                                                                                     |
| `src/telemetry.rs`                      | OpenTelemetry/OTLP setup. `init()` configures the tracing subscriber with optional OTLP export and W3C trace context propagation.                                                                                                                                    |
| `src/middleware/auth.rs`                | Checks for presence of `Authorization` header; rejects with 403 if absent.                                                                                                                                                                                           |
| `src/ops/send.rs`                       | `queue.send`, `queue.send_batch`, `queue.send_fifo` — single/batch/FIFO inserts.                                                                                                                                                                                     |
| `src/ops/receive.rs`                    | `queue.receive`, `queue.receive_fifo` — long-poll reads.                                                                                                                                                                                                             |
| `src/ops/delete.rs`                     | `queue.delete` — single and batch deletes.                                                                                                                                                                                                                           |
| `src/ops/visibility.rs`                 | `queue.change_visibility` — change visibility timeout by msg_id.                                                                                                                                                                                                     |
| `src/ops/queue_admin.rs`                | `queue.create`, `queue.create_fifo`, `queue.delete_queue`, `queue.list_queues`, `queue.metrics`, `queue.purge_queue`. `all_queue_depths()` is called by the depth-scrape background task.                                                                            |
| `src/ops/event.rs`                      | `queue.send_topic` fan-out; `queue.queue_http_deliveries`; subscribe/unsubscribe/list ops; SNS-specific list/delete helpers.                                                                                                                                         |
| `src/ops/coalesce.rs`                   | Write coalescer. `Coalescer` is a cloneable handle to an `mpsc::channel`; the background task groups pending sends by `(queue_name, delay)` and flushes them as `send_batch` calls within the `LINGER_MS` window.                                                    |
| `src/ops/delivery.rs`                   | Background HTTP delivery worker. Polls `queue.http_deliveries`, POSTs to endpoints, retries with exponential backoff.                                                                                                                                                |
| `src/signing.rs`                        | RSA-2048 keypair generated at startup. `sign_notification()` produces SNS v2 base64 signatures. Self-signed X.509 cert served at `/SimpleNotificationService.pem`.                                                                                                   |
| `src/routes/queues.rs`                  | `GET/POST /v1/queues`, `GET/DELETE /v1/queues/{name}`, `POST /v1/queues/{name}/purge`, `GET /v1/queues/{name}/subscriptions`.                                                                                                                                        |
| `src/routes/messages.rs`                | `GET/POST/DELETE /v1/queues/{name}/messages`, `DELETE/PATCH /v1/queues/{name}/messages/{id}`.                                                                                                                                                                        |
| `src/routes/events.rs`                  | `POST /v1/events/{routing_key}`, subscription CRUD at `/v1/events/{pattern}/subscriptions`.                                                                                                                                                                          |
| `src/sns/mod.rs`                        | SNS service handler. Protocol detection (JSON/Query), action dispatch.                                                                                                                                                                                               |
| `src/sns/context.rs`                    | `SnsContext` — per-request protocol + request ID + action. ARN helpers. Serializes responses as SNS-shaped JSON or XML.                                                                                                                                              |
| `src/sns/types.rs`                      | Request/response structs for all SNS actions.                                                                                                                                                                                                                        |
| `src/sns/error.rs`                      | `SnsError` + `SnsErrorCode` — serializes to JSON or XML.                                                                                                                                                                                                             |
| `src/sns/actions/`                      | One file per SNS action. Each delegates to `ops/`.                                                                                                                                                                                                                   |
| `src/sqs/mod.rs`                        | Protocol detection, action dispatch macro. Path-based route handler + `handle_service_request` called from gateway.                                                                                                                                                  |
| `src/sqs/context.rs`                    | `SqsContext` — per-request protocol + request ID. Serializes responses as JSON or XML.                                                                                                                                                                               |
| `src/sqs/receipt.rs`                    | `encode`/`decode` for receipt handles: `base64url("{queue_name}\x00{msg_id}")`.                                                                                                                                                                                      |
| `src/sqs/types.rs`                      | Request/response structs for all SQS actions.                                                                                                                                                                                                                        |
| `src/sqs/error.rs`                      | `SqsError` + `SqsErrorCode` — serializes to JSON or XML depending on protocol.                                                                                                                                                                                       |
| `src/sqs/util.rs`                       | `queue_name_from_url`, `md5_of`, `message_attributes_to_headers`.                                                                                                                                                                                                    |
| `src/sqs/actions/`                      | One file per SQS action. Each delegates to `ops/`.                                                                                                                                                                                                                   |
| `beyond-queue-extension/src/lib.rs`     | pgrx module root. Installs shared-memory hooks in `_PG_init`. Loads `schema.sql`.                                                                                                                                                                                    |
| `beyond-queue-extension/src/queue.rs`   | Hot-path pgrx C functions: `send`, `send_batch` (and FIFO variants), `receive`, `receive_fifo`, `delete`, `archive`, `pop`, `change_visibility`.                                                                                                                     |
| `beyond-queue-extension/src/waiter.rs`  | `WaiterRegistry` in shared memory. FNV-1a hash, 256 buckets, 4096 slots. `WaiterGuard` RAII, `notify_waiters`, `register_notify_after_commit`.                                                                                                                       |
| `beyond-queue-extension/sql/schema.sql` | DDL for `queue.meta`, `queue.q_{name}`, `queue.a_{name}`, `queue.topic_subscriptions`, `queue.http_deliveries`, `queue.notify_insert_throttle`. PL/pgSQL functions: `receive_fifo`, FIFO grouped reads, topic routing, `queue_http_deliveries`, notification system. |

---

## API Reference

### Native REST API (`/v1/`)

| Method   | Path                                      | Operation                                                                                                                             |
| -------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `POST`   | `/v1/queues`                              | Create queue. Body: `{"name": "...", "fifo": false}`. Returns 201.                                                                    |
| `GET`    | `/v1/queues`                              | List all queues. Returns array of `{name, is_partitioned, is_unlogged, created_at}`.                                                  |
| `GET`    | `/v1/queues/{name}`                       | Queue metrics: `{queue_length, newest_msg_age_sec, oldest_msg_age_sec, total_messages, scrape_time}`.                                 |
| `DELETE` | `/v1/queues/{name}`                       | Delete queue. Returns 204 if deleted, 404 if not found.                                                                               |
| `POST`   | `/v1/queues/{name}/purge`                 | Delete all messages. Returns `{"deleted": N}`.                                                                                        |
| `POST`   | `/v1/queues/{name}/messages`              | Send single `{message, headers?, delay?}` or batch `[{message, headers?, delay?, group_id?}]`. Returns 201 `{id}` or `{ids}`.         |
| `GET`    | `/v1/queues/{name}/messages`              | Receive. Query params: `max` (default 1), `vt` (default 30), `wait` (default 0), `fifo` (default false). Returns array.               |
| `DELETE` | `/v1/queues/{name}/messages`              | Batch delete. Body: `{"ids": [1,2,3]}`. Returns `{"deleted": [1,2,3]}`.                                                               |
| `DELETE` | `/v1/queues/{name}/messages/{id}`         | Delete single. Returns 204 or 404.                                                                                                    |
| `PATCH`  | `/v1/queues/{name}/messages/{id}`         | Change visibility. Body: `{"vt": 60}`. Returns `{"id": N, "visible_at": "..."}`.                                                      |
| `POST`   | `/v1/events/{routing_key}`                | Fan-out to SQS queues + HTTP endpoints. Body: `{message, headers?, delay?}`. Returns 201 `{"queues_matched": N, "messages": [...]}`.  |
| `POST`   | `/v1/events/{pattern}/subscriptions`      | Subscribe SQS queue (`{"queue_name":"..."}`) or HTTP endpoint (`{"protocol":"http","endpoint":"...","envelope":false}`). Returns 201. |
| `GET`    | `/v1/events/{pattern}/subscriptions`      | List subscriptions for a routing-key pattern.                                                                                         |
| `DELETE` | `/v1/events/{pattern}/subscriptions/{id}` | Unsubscribe by id. Returns 204 or 404.                                                                                                |
| `GET`    | `/v1/queues/{name}/subscriptions`         | List all event subscriptions targeting this queue.                                                                                    |
| `GET`    | `/v1/openapi.json`                        | Dynamically generated OpenAPI 3.1 spec for the native REST API.                                                                       |
| `GET`    | `/v1/cert`                                | PEM-encoded public certificate for SNS signature verification (same cert as `/SimpleNotificationService.pem`).                        |
| `GET`    | `/SimpleNotificationService.pem`          | PEM-encoded public certificate for SNS signature verification.                                                                        |
| `GET`    | `/metrics`                                | Prometheus text metrics (unauthenticated). See Observability section.                                                                 |

### SQS-compatible API

Routes: `POST /` and `POST /{account_id}/{queue_name}`.

Supported actions: `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`, `DeleteMessageBatch`, `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch`, `CreateQueue`, `DeleteQueue`, `GetQueueUrl`, `GetQueueAttributes`, `ListQueues`, `PurgeQueue`.

Queue URL format: `{BASE_URL}/000000000000/{queue_name}`.
FIFO queues use `.fifo` suffix in queue name.
