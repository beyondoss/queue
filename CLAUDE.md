## Service Model

**This is a private, internal service.** Each deployment runs inside a private network,
co-located with the application it serves. It is not a public SaaS, not a multi-tenant
platform, and not exposed to the open internet without the operator's own infrastructure
in front of it.

**Consequences — do not add to this service:**

- Rate limiting, IP blocking, DDoS mitigation — the operator's load balancer or proxy
  handles this. We see trusted traffic only.
- SQS SigV4 signature verification — we accept any `Authorization` header. The network
  boundary is the security layer. This is the LocalStack/ElasticMQ pattern and is
  intentional.
- Any feature whose justification is "what if a bad actor hammers this endpoint" — wrong
  layer.

## Architecture

**Keep docs in sync**: When changing code that affects documented behavior (data flows,
APIs, config, protocol handling), update ARCHITECTURE.md in the same commit. Stale docs
are worse than no docs.

## Database

**All sqlx queries must be type-safe.** Use `sqlx::query_as!`, `sqlx::query!`, and
related macros — never `sqlx::query` with manual `.try_get()` calls or untyped row
access. The compile-time checked macros guarantee query results match Rust types;
bypassing them removes that guarantee.

Run `mise run sqlx:prepare` after adding or changing queries to update the offline query
cache (`.sqlx/`). CI runs with `SQLX_OFFLINE=true`.

## Local Development

We use mise for running development tasks.

```sh
mise tasks          # list all tasks
mise run build      # cargo build
mise run test       # integration tests
mise run format     # dprint fmt
```

To build the pgrx extension for testing locally (requires PostgreSQL 17 dev headers):

```sh
mise run extension:build          # native build (requires local pg_config)
mise run extension:build:linux    # cross-compile for linux/arm64 in Docker
```

## System Design

**We seek the minimum effective abstraction. Elegant simplicity. Composable parts that
"just work".**

**Performance is a feature, not an optimization pass.**

- Do less work. The fastest code is code that doesn't run.
- Minimize allocations. Reuse where it matters.
- Parallelize only when the work itself is the bottleneck — not as a first instinct.
- Measure before you optimize, but design with performance in mind from the start.

## SQS Protocol

The API speaks two SQS wire protocols simultaneously. Dispatch is based on
`Content-Type`:

- `application/x-amz-json-1.0` + `X-Amz-Target: AmazonSQS.{Action}` → JSON protocol
- `application/x-www-form-urlencoded` + `Action={Action}` in body → Query protocol

Both protocols decode into the same internal action enum and delegate to the same `ops/`
functions. Responses are JSON for the JSON protocol, XML for the Query protocol.

**Never add a third protocol or a hybrid.** If a new AWS protocol version is needed,
add it as a separate dispatch branch, not by mixing into an existing one.

**Receipt handles** are `base64url("{queue_name}\x00{msg_id}")`. They are opaque to
clients and must be stable across restarts. Never change the encoding.

## Native REST API

The `/v1/` prefix hosts a clean resource-oriented API alongside the SQS layer. Follow
these rules:

- Resources are nouns, HTTP methods are the verbs.
- `GET` reads, `POST` creates, `DELETE` removes, `PATCH` partially updates.
- Collections are plural: `/v1/queues`, not `/v1/queue`.
- Sub-resources nest: `/v1/queues/{name}/messages`.
- `201 Created` with `Location` header for resource creation.
- `204 No Content` for successful deletes with no body.
- No verbs in paths, no `action=` tunneling.

## pgrx Extension

pgrx is used only where C has a unique capability unavailable in PL/pgSQL:

- `send` / `send_batch`: post-commit `XactCallback` to fire `SetLatch` on waiting
  readers; `sync_commit` parameter for async-commit opt-out.
- `receive` / `receive_fifo` (5-arg): `WaitLatch` + shared-memory waiter registry — cannot be called
  from PL/pgSQL.
- `delete`, `archive`, `pop`, `change_visibility`: scalar / tiny set returns; datum overhead
  is negligible.

**Do NOT implement set-returning hot paths in pgrx.** `queue.receive_fifo` (3-arg) is PL/pgSQL for a
reason: pgrx `TableIterator<'static, T>` extracts every datum from each row into a
Rust type then re-encodes it when PostgreSQL fetches the row — 14 datum conversions
per row vs PL/pgSQL's 1 heap-tuple copy. This adds 6.7× latency single-threaded and
~46% end-to-end. See ARCHITECTURE.md for the full measurement.

Other pgrx constraints:

- **Collect `pgrx::Array<T>` inputs before entering `Spi::connect`.** pgrx Array
  borrows PostgreSQL memory that cannot cross the SPI connection boundary. Convert to
  owned `Vec<T>` first.
- **Use a borrowed `SpiClient<'_>` parameter** when calling helper functions from
  inside a `Spi::connect` closure. Never open a nested `Spi::connect`.
- **Never panic inside a `#[pg_extern]`.** Use `pgrx::error!()` which raises a
  PostgreSQL ERROR.
- **LISTEN/NOTIFY cannot be used from a blocking `#[pg_extern]`.** `Async_Listen` is
  deferred to `PreCommit_Notify` at the listener's transaction commit; a function that
  never returns never commits, so LISTEN never registers. Use the shared-memory waiter
  registry in `waiter.rs` instead.

The schema SQL (`beyond-queue-extension/sql/schema.sql`) defines tables, types, indexes, and
non-hot functions. Hot paths are a mix: pgrx C functions override `send`, `send_batch`,
`receive`, `receive_fifo`, `delete`, `archive`, `pop`, and `change_visibility`; the 3-arg
`receive_fifo` stays PL/pgSQL.

When loading the extension in a fresh database alongside `hot_paths.sql`, use
`load_pgrx_extension.sql` — some functions change their return type from
`SETOF queue.message_record` to `TABLE(...)` and require `DROP` first.

### `queue.receive` SQL Design Rules

- **No `ORDER BY msg_id ASC` in the SKIP LOCKED CTE.** Ordering forces all concurrent
  workers to scan from the same low-msg_id index root — a hot spot. Without ordering,
  workers find any available row and spread naturally across the heap. SKIP LOCKED
  correctness does not require ordering; SQS Standard doesn't guarantee FIFO either.
- **Embed `qty` and `vt` as literals in the format string.** Parameterized LIMIT ($1)
  causes PostgreSQL to generate a generic plan where LockRows can't determine the scan
  bound at planning time, degrading SKIP LOCKED throughput under concurrency.
  Integer embedding is injection-safe (i32 parameters cannot contain SQL).
- **Two SQL strings, not one.** A separate SQL string for the empty-conditional fast
  path avoids a `CASE` expression in the CTE predicate that PostgreSQL cannot
  eliminate at planning time when the conditional is parameterized.

## Operations & State

All operations that modify state must be **idempotent and atomic**.

**Idempotent**: Running an operation multiple times produces the same result as once.

- Check before create; don't error if it exists.
- Check before destroy; don't error if it's gone.
- Safe to retry after network failures or crashes.

**Atomic**: An operation either fully succeeds, fully fails, or leaves the system in a
valid intermediate state that subsequent retries can recover from.

- Multi-step operations use transactions or compensating actions.
- If you can't make it atomic, make the intermediate states safe to observe.

## Performance Improvement

Apply the **Theory of Constraints**: a system's throughput is limited by its single
tightest bottleneck. Optimizing anything else is waste.

1. **Identify** the constraint. Profile. Trace. Measure. Don't guess.
2. **Exploit** the constraint. Squeeze maximum performance with minimal change.
3. **Subordinate** everything else. Non-bottleneck components should serve the
   constraint, not outrun it.
4. **Elevate** the constraint. If exploiting isn't enough, redesign.
5. **Repeat.** The bottleneck has shifted.

The corollary: if you can't name the current constraint, you aren't ready to optimize.

<!-- wiki-managed:start (managed by `wiki claude install`; edits inside this block will be overwritten) -->

## Wiki

This repo uses [agent-wiki](.wiki/SCHEMA.md): `.wiki/` holds synthesized entity,
concept, decision, and source pages cross-linked into a queryable knowledge graph.

**Read the wiki before grepping the codebase or reading ARCHITECTURE.md.** Pages are
pre-synthesized — searching them is faster and ~5–10× cheaper than re-deriving from raw
files.

Wiki tools — pick based on what you need:

- `wiki_query "<term>"` — first move for any specific question. BM25++ over wiki pages,
  repo docs, and code symbols; returns ranked hits with paths, scores, and inline
  snippets.
- `wiki_answer "<question>"` — returns top-ranked pages with query-relevant passage
  extracts in one round-trip. Best when you expect the answer exists and want it
  immediately.
- `wiki_read "path/to/page.md"` (optionally `section: "..."` or `paths: [...]`) — full
  page, one section, or multiple pages in one call.
- `wiki_search_code "<query>"` — search exported symbols, signatures, and doc comments
  when you need to locate a declaration or understand an API.

When shipping a feature: invoke the `wiki:reconcile_change` prompt to close the source →
code loop. When auditing the wiki itself: `Task(subagent_type="wiki-lint", ...)`.

<!-- wiki-managed:end -->
