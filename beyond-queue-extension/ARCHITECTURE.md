# beyond-queue-extension Architecture

A pgrx Rust extension that replaces pgmq's PL/pgSQL hot paths with C functions: direct heap inserts for send, `WaitLatch`-based long-poll for receive, a shared-memory waiter registry for push wakeup on commit, and a shared-memory routing cache for event fanout. Installed as a PostgreSQL `shared_preload_libraries` extension; without it, the SQL fallbacks in `schema.sql` handle all operations at higher latency.

## Data Flow

### send (single message)

```
queue.send(name, msg, headers, delay, sync_commit)
     │
     ├── validate_name ──────────────────────────────► ERROR on violation
     │
     ├── sync_commit = true ──────────────────────────► direct_heap_insert
     │       │   (bypasses SPI: no plan parsing,         │  open table by relid
     │       │    no parameter binding)                   │  nextval_internal → msg_id
     │       │                                           │  GetCurrentTimestamp → now
     │       │                                           │  heap_form_tuple → tuple
     │       │                                           │  heap_insert → WAL + heap
     │       │                                           │  index_insert for each index
     │       │                                           └► table_close (keep lock)
     │       │
     └── sync_commit = false ─────────────────────────► Spi::connect_mut
             │   (async-commit path)                     │  SET LOCAL synchronous_commit = off
             │                                           │  INSERT INTO queue.q_{name} RETURNING msg_id
             │
             └─────────────────────────────────────────► register_notify_after_commit
                                                         │  alloc queue_name in TopTransactionContext
                                                         │  RegisterXactCallback(on_xact_commit)
                                                         │
                                                         [on XACT_EVENT_COMMIT]
                                                              │
                                                              └─► notify_waiters(queue_name)
                                                                   │  LWLock SHARED
                                                                   │  bucket = FNV-1a(name) & 255
                                                                   │  walk bucket list
                                                                   └─► SetLatch(slot.latch) per match
```

### receive (long-poll)

```
queue.receive(name, vt, qty, max_poll_seconds, poll_interval_ms, conditional)
     │
     ├── validate_name
     │
     ├── WaiterGuard::new(name)
     │       │  LWLock EXCLUSIVE
     │       │  pop slot from free list
     │       │  slot.latch = MyLatch, slot.pid, slot.queue_name
     │       │  prepend to bucket[FNV-1a(name) & 255]
     │       │  LWLock release
     │       └─► idx stored in WaiterGuard (unregisters on drop)
     │
     └── LOOP until deadline:
             │
             ├── ResetLatch(MyLatch)          ← must precede read (no missed wakeups)
             │
             ├── Spi::connect_mut
             │       │  UPDATE queue.q_{name}
             │       │    SET vt = now+vt, read_ct++, last_read_at = now
             │       │    FROM (SELECT msg_id WHERE vt<=now [AND msg @> cond]
             │       │          LIMIT qty FOR UPDATE SKIP LOCKED)
             │       │    RETURNING all columns
             │       └─► rows (empty or non-empty)
             │
             ├── rows non-empty ──────────────────────────────────► return rows
             │
             ├── deadline elapsed ────────────────────────────────► return []
             │
             └── WaitLatch(WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH, wait_ms)
                     │  wakes on: SetLatch (sender committed) | timeout | postmaster death
                     └── ProcessInterrupts()  ← honours Ctrl+C / statement_timeout
```

### event fanout (publish_event)

```
queue.publish_event(routing_key, msg, headers, delay, sync_commit)
     │
     ├── validate_routing_key
     │
     └── Spi::connect_mut
             │
             ├── routing_cache::lookup(routing_key)
             │       ├── HIT  ─────────────────────────────────────► Vec<queue_name>
             │       │   (LWLock SHARED, FNV-1a slot check, gen match)
             │       │
             │       └── MISS ─────────────────────────────────────► SELECT DISTINCT queue_name
             │                                                         FROM event_subscriptions
             │                                                         WHERE routing_key ~ compiled_regex
             │                                                         ORDER BY queue_name
             │                                                         routing_cache::insert(key, names)
             │
             └── for each queue_name:
                     │  INSERT INTO queue.q_{name} (vt, message, headers) VALUES ($1,$2,$3)
                     │  RETURNING msg_id          ← msg/hdr DatumWithOid aliased, no re-serialize
                     └─► register_notify_after_commit(queue_name)
```

### routing cache invalidation

```
INSERT/UPDATE/DELETE on queue.event_subscriptions
     │
     └─► AFTER STATEMENT trigger: event_subscriptions_cache_invalidate
             │
             └─► queue._invalidate_routing_cache()  [overridden by pgrx C fn]
                     │
                     └─► routing_cache::invalidate()
                             │  LWLock EXCLUSIVE
                             └─► CACHE.generation += 1  (skip 0)
                                 ← all slots now stale on next lookup
```

## Concepts & Terminology

| Term                        | What It Controls                                                                                                                                             | NOT                                                                                                      |
| --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------- |
| `vt` (visibility timestamp) | `TIMESTAMP WITH TIME ZONE` column; a message is visible when `vt <= clock_timestamp()`. Set to `now + delay` on insert, `now + vt_secs` on receive.          | Not a TTL; messages don't expire — they stay hidden until vt passes or `change_visibility` moves it.     |
| `sync_commit`               | When false, `SET LOCAL synchronous_commit = off` skips the WAL fsync on commit. Callers opt in; the default preserves durable-commit.                        | Not about atomicity; the write still goes to WAL, just not fsynced before ack.                           |
| `WaiterRegistry`            | Shared-memory hash table (256 buckets, 4096 slots) mapping queue name → `MyLatch` pointers for waiting backends.                                             | Not a LISTEN/NOTIFY channel; no PostgreSQL interprocess messaging is involved.                           |
| `RoutingCache`              | Shared-memory direct-mapped cache (256 slots) from routing key → `Vec<queue_name>`. Invalidated wholesale on any `event_subscriptions` write.                | Not a per-queue cache; it caches the routing lookup result for a given key.                              |
| `WaiterGuard`               | RAII handle that unregisters the backend's slot on drop (normal return, panic unwind, query-cancel unwind).                                                  | Not a lock; it doesn't block any other operation.                                                        |
| `direct_heap_insert`        | Bypasses SPI entirely for single-message inserts: no plan parsing, no parameter binding, ~8µs saved per call. Uses `heap_insert` + `index_insert` directly.  | Not usable with expression indexes; only column-reference indexes on the queue table are supported.      |
| `eligible_group` (FIFO)     | The message group with the smallest `MIN(msg_id)` where `BOOL_AND(vt <= now)` — i.e., no in-flight messages. Only one group is read per `receive_fifo` call. | Not the group with the most messages; FIFO strictly picks the group whose head has been waiting longest. |
| degraded mode               | When the extension is absent from `shared_preload_libraries`, `REGISTRY_READY` and `CACHE_READY` stay false; all shared-memory operations are no-ops.        | Not a crash; the SQL fallbacks in `schema.sql` handle all operations correctly via `pg_sleep` polling.   |

## Core Mechanisms

### direct_heap_insert (`queue.rs:86`)

Used by `send_full` when `sync_commit = true`. Eliminates per-call SPI overhead (~8µs: connection setup, plan parsing, parameter binding):

1. Resolve `queue.q_{name}` → `Oid` via `get_relname_relid`.
2. Advance the `msg_id` identity sequence via `nextval_internal` (unexposed in pgrx, declared as `extern "C"`).
3. Build a 7-column values/nulls array for `(msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers)`.
4. `heap_form_tuple` + `heap_insert` — handles TOAST and WAL.
5. `RelationGetIndexList` → for each index: extract datums by `attnum`, call `index_insert`.

**Invariant**: only column-reference indexes are supported. An expression index (attnum = 0) causes a `pgrx::error!()`, not silent corruption.

**Lock protocol**: `table_open(ROW_EXCLUSIVE_LOCK)` matches a normal INSERT. Indexes opened with `ROW_EXCLUSIVE_LOCK`, closed with `NO_LOCK` to keep the parent's lock held through commit.

### WaitLatch long-poll (`queue.rs:338`, `waiter.rs`)

`receive` and `receive_fifo` use a race-free latch loop:

```
ResetLatch(MyLatch)          ← before the read, not after
SPI read attempt
if rows: return rows
if deadline: return []
WaitLatch(WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH, remaining_ms)
ProcessInterrupts()
```

`ResetLatch` before the read ensures that a `SetLatch` arriving _during_ the SPI call is not lost — `WaitLatch` returns immediately on the next iteration. The reverse order (read then reset) would create a window where a wakeup is missed.

### WaiterRegistry (`waiter.rs`)

Fixed-size shared-memory structure allocated at startup:

| Field                           | Size              | Purpose                                          |
| ------------------------------- | ----------------- | ------------------------------------------------ |
| `generation` (unused in waiter) | —                 | —                                                |
| `free_head`                     | i32               | head of free-slot linked list; -1 = full         |
| `buckets[256]`                  | i32 each          | head of per-queue linked list; -1 = empty        |
| `slots[4096]`                   | `WaiterSlot` each | latch pointer, PID, queue name (49B), next index |

- **register**: LWLock exclusive → pop free slot → init slot → prepend to bucket → release. O(1).
- **unregister**: LWLock exclusive → walk bucket list to find and unlink slot → push to free list → release. O(slots_in_bucket).
- **notify_waiters**: LWLock shared → hash name → walk bucket list → `SetLatch` per match → release. Multiple senders concurrent. O(waiters_for_this_queue).

Hash function: FNV-1a over queue name bytes, masked to 256 with bitwise AND (bucket count must be a power of 2).

### RoutingCache (`routing_cache.rs`)

256 direct-mapped slots. A single global `generation` counter provides O(1) full invalidation:

- **Lookup**: LWLock shared → `slot = slots[FNV-1a(key) & 255]` → check `slot.generation == global_gen && slot.key == key` → return queue names or None.
- **Insert**: LWLock exclusive → write slot with current generation → release. Hash collisions evict: the new key overwrites the old slot. A miss just re-runs the SQL query.
- **Invalidate**: LWLock exclusive → `generation += 1` (skip 0, which is the sentinel for uninitialised slots) → release. All existing slots become stale on the next lookup.

The AFTER STATEMENT trigger on `event_subscriptions` calls `queue._invalidate_routing_cache()`, which is a no-op stub in the schema SQL, overridden by the pgrx C function when the extension is loaded.

### FIFO group serialization (`queue.rs:520`, `schema.sql:1007`)

`receive_fifo` (5-arg, pgrx) and `receive_fifo` (3-arg, PL/pgSQL) both use the same SQL structure:

```sql
WITH eligible_group AS MATERIALIZED (
    SELECT message_group_id
    FROM queue.q_{name}
    GROUP BY message_group_id
    HAVING BOOL_AND(vt <= clock_timestamp())  -- no in-flight messages
    ORDER BY MIN(msg_id) ASC                  -- oldest-first group selection
    LIMIT 1
),
cte AS (
    SELECT m.msg_id FROM queue.q_{name} m
    WHERE m.message_group_id = (SELECT message_group_id FROM eligible_group)
      AND m.vt <= clock_timestamp()
    ORDER BY m.msg_id ASC          -- FIFO within group
    LIMIT {qty}
    FOR UPDATE SKIP LOCKED
)
UPDATE queue.q_{name} m
SET last_read_at = now, vt = now + {vt}, read_ct = read_ct + 1
FROM cte WHERE m.msg_id = cte.msg_id
RETURNING ...
```

`BOOL_AND(vt <= now)` is equivalent to `NOT EXISTS(vt > now)` but avoids the correlated subplan that would scan O(n²) on large queues. The `_grpvt_idx` index on `(message_group_id, vt ASC, msg_id ASC)` enables an index-only scan for the `eligible_group` aggregate.

### Why the 3-arg `receive_fifo` stays PL/pgSQL

The 3-arg no-wait `receive_fifo(name, vt, qty)` is implemented in PL/pgSQL (`schema.sql:1007`), not pgrx. A pgrx `TableIterator<'static, T>` must extract every datum from each row into a Rust type then re-encode it when PostgreSQL fetches the row — 14 datum conversions per row. PL/pgSQL `RETURN QUERY EXECUTE` copies heap tuples once. Measured delta: **6.7× latency single-threaded, ~46% slower end-to-end**. pgrx wins for the 5-arg `receive_fifo` only because `WaitLatch` cannot be implemented in PL/pgSQL at all.

### SQL plan stability for SKIP LOCKED

`qty` and `vt` are embedded as format-string literals in all receive SQL, not as `$N` parameters:

```rust
// Good: literal in format string
let sql = format!("... LIMIT {qty} FOR UPDATE SKIP LOCKED ...");

// Bad: generic plan degrades SKIP LOCKED throughput
let sql = "... LIMIT $1 FOR UPDATE SKIP LOCKED ...";
```

With a parameterized `LIMIT $1`, PostgreSQL generates a generic plan where `LockRows` cannot determine the scan bound at planning time. Embedding the integer as a literal lets the planner produce a custom plan with a known bound, preserving SKIP LOCKED throughput under concurrent readers. Integer embedding is injection-safe — `qty` and `vt` are typed `i32` parameters and cannot contain SQL.

Standard queue `receive` uses two SQL strings (simple and conditional) to avoid a `CASE` expression in the predicate that PostgreSQL cannot eliminate at planning time when it is parameterized.

## State Machine: message lifecycle

```
[send / send_batch / send_fifo]
        │
        ▼
  ┌──────────┐    vt = now + delay
  │ hidden   │◄──────────────────── (delay > 0 at insert, or change_visibility to future)
  └──────────┘
        │  vt <= clock_timestamp()
        ▼
  ┌──────────┐
  │ visible  │◄───────────────────── change_visibility(id, 0) or change_visibility to past
  └──────────┘
        │  receive / receive_fifo
        ▼
  ┌──────────┐    vt = now + vt_secs
  │ in-flight│◄──── read_ct++, last_read_at = now
  └──────────┘
        │
        ├── delete(id) ──────────────────────────────────────► [removed]
        │
        ├── archive(id) ─────────────────────────────────────► [in queue.a_{name}]
        │                                                          (preserves all columns)
        │
        ├── pop(name) ───────────────────────────────────────► [removed, returned to caller]
        │   (DELETE + RETURNING in one statement, no archive)
        │
        └── vt expires (no ack within vt_secs) ─────────────► [visible again, at-least-once]
```

| State     | vt predicate                           | What that means                                |
| --------- | -------------------------------------- | ---------------------------------------------- |
| hidden    | `vt > clock_timestamp()`               | Delayed insert or consumer-extended visibility |
| visible   | `vt <= clock_timestamp()`              | Available for `receive` / `receive_fifo`       |
| in-flight | `vt > clock_timestamp()` after receive | Consumer has it; vt is a future deadline       |

The schema stores no separate state column — the `vt` timestamp IS the state. Hidden and in-flight look identical in the table; the difference is whether the consumer holds the row or the delay produced it.

## Shared Memory Layout

Two independent regions allocated at startup via `_PG_init` → `shmem_request_hook` → `shmem_startup_hook`:

| Region           | Key                       | LWLock tranche          | Size                              |
| ---------------- | ------------------------- | ----------------------- | --------------------------------- |
| `WaiterRegistry` | `"queue_waiter_registry"` | `"queue_waiters"`       | `sizeof(WaiterRegistry)` ≈ 600 KB |
| `RoutingCache`   | `"queue_routing_cache"`   | `"queue_routing_cache"` | `sizeof(RoutingCache)` ≈ 4 MB     |

Both are initialized once at postmaster startup. After a crash, shared memory is reset and hooks re-run on next startup — no recovery needed.

## Schema Structure

Each queue exists as two tables and one metadata row:

| Object        | Name             | Purpose                                                      |
| ------------- | ---------------- | ------------------------------------------------------------ |
| Queue table   | `queue.q_{name}` | Live messages with identity-sequence `msg_id`, `vt` index    |
| Archive table | `queue.a_{name}` | Messages moved by `archive()`, adds `archived_at` column     |
| Registry row  | `queue.meta`     | Queue type (`standard`/`fifo`), partitioned flag, created_at |

FIFO queues add three extra indexes on `queue.q_{name}`:

| Index        | Columns                                                                   | Purpose                                                                         |
| ------------ | ------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `_grp_idx`   | `(message_group_id, msg_id ASC)`                                          | Within-group read order and cte scan                                            |
| `_grpvt_idx` | `(message_group_id, vt ASC, msg_id ASC)`                                  | Covering index for `eligible_group` aggregate (index-only scan)                 |
| `_dedup_idx` | `(message_group_id, deduplication_id) WHERE deduplication_id IS NOT NULL` | Partial unique index; NULL dedup IDs excluded (no overhead for non-dedup sends) |

Standard queues have only the `_vt_idx` on `(vt ASC)`.

## Why It Behaves This Way

### Why direct_heap_insert bypasses SPI for single sends

Each SPI connection costs ~8µs: connection setup/teardown (~2µs), plan parsing and planning (~5µs), parameter binding (~1µs). At WAL-bound throughput — where each insert round-trip is already paying for a WAL fsync — 8µs is not negligible. `direct_heap_insert` eliminates all of it by going straight to `heap_insert`. The saving is small per call but real in aggregate.

### Why XactCallback fires notify_waiters, not pg_notify

`LISTEN/NOTIFY` is deferred to `PreCommit_Notify` at the listener's transaction commit. A function that blocks inside `WaitLatch` never commits, so `LISTEN` never registers and `NOTIFY` never wakes it. `SetLatch` is a raw signal-safe operation that sets a flag in the target `PGPROC` and sends `SIGUSR1` — no transaction required, works from any callback context.

### Why the waiter registry uses a free list, not a ring buffer

The registry must support O(1) unregister from an arbitrary slot (when a reader times out or is cancelled). A ring buffer's removal is O(n). A free-list pop/push is O(1), and the bucket-list unlink is O(slots in this queue's bucket), which is short in practice (bounded by concurrent readers on one queue).

### Why routing_cache uses generation counters instead of per-slot invalidation

Any write to `event_subscriptions` potentially changes the match set for every routing key — not just the one whose pattern changed. Invalidating all 256 slots individually under an exclusive lock costs O(256) writes. Bumping a single `u64` generation is O(1) and makes every slot stale atomically. A cache miss just re-runs the SQL query, which is correct and bounded by the number of subscriptions.

### Why `receive` has no ORDER BY on msg_id

Ordering by `msg_id ASC` forces all concurrent workers to scan from the same low-msg_id index root — a hot spot under high concurrency. Without ordering, each worker finds any available row, spreading naturally across the heap. `SKIP LOCKED` correctness does not require ordering; SQS Standard does not guarantee FIFO.

## Trust Boundaries

**What the extension validates (raises PostgreSQL ERROR on violation):**

- Queue names: 1–48 chars, `[a-z0-9_]` only (`validate_name` in `queue.rs:21`)
- Routing keys: 1–255 chars, `[a-zA-Z0-9._-]`, no leading/trailing/consecutive dots (`validate_routing_key` in `queue.rs:872`)
- Array length mismatch between `msgs` and `headers` in `send_batch` — raises ERROR rather than silently misaligning headers

**What passes through unchecked:**

- Message body content — stored as opaque `JSONB`, no schema enforcement
- `deduplication_id` values — uniqueness is enforced by a partial unique index, not by the extension
- `headers` content — stored as opaque `JSONB`

**Extension vs SQL validation gap:**

`validate_name` enforces 48 chars max. `queue.validate_queue_name` in SQL enforces 48 chars. `queue.format_table_name` checks `[a-z0-9_]` but uses a length check of 47 in some paths (off-by-one vs the pgrx check of 48). The pgrx check at `queue.rs:21` is authoritative for all hot-path functions.

## Failure Modes

| Failure                                          | What Actually Happens                                                                                                                                                                                                                                                                                        | Recovery                                                                  |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------- |
| Extension absent from `shared_preload_libraries` | `REGISTRY_READY` and `CACHE_READY` stay false; `receive` falls back to `WL_TIMEOUT`-only polling; `publish_event` re-runs the SQL query on every call                                                                                                                                                        | No action needed; degraded mode is fully correct                          |
| `WaiterRegistry` full (4096 slots occupied)      | `register()` returns `None`; `WaiterGuard` holds `None`; `receive` falls back to timeout-only polling for that backend                                                                                                                                                                                       | Resolve by reducing concurrent long-poll connections                      |
| Hash collision in `RoutingCache`                 | Newer routing key overwrites the slot; the evicted key gets a cache miss on next call and re-queries                                                                                                                                                                                                         | Self-healing on next call; no stale data risk                             |
| `direct_heap_insert` on non-existent queue       | `get_relname_relid` returns `InvalidOid`; `pgrx::error!()` raises a PostgreSQL ERROR with SQLSTATE `XX000`                                                                                                                                                                                                   | Caller should call `queue.create()` first                                 |
| Expression index on a queue table                | `index_insert` detects `attnum == 0`; `pgrx::error!()` prevents silent index corruption                                                                                                                                                                                                                      | Drop the expression index or use SPI path (set `sync_commit=false`)       |
| XactCallback list growth                         | `register_notify_after_commit` registers a callback per send call. `on_xact_commit` calls `UnregisterXactCallback` as its first action (before the SetLatch work) to remove itself from the list. Without this, the list grows by one entry per send and every subsequent commit walks the accumulated list. | Self-managing: each callback unregisters itself on first invocation       |
| Postmaster death during WaitLatch                | `WL_EXIT_ON_PM_DEATH` causes `WaitLatch` to return; `ProcessInterrupts()` raises a die signal                                                                                                                                                                                                                | PostgreSQL backend exits cleanly; `WaiterGuard` drop unregisters the slot |

## File Map

| File                   | What It Does                                                                                                                                                                                                                                                                                                                     |
| ---------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `src/lib.rs`           | Module root. `_PG_init` installs shared-memory hooks for both `WaiterRegistry` and `RoutingCache`. Loads `schema.sql` via `extension_sql_file!`.                                                                                                                                                                                 |
| `src/queue.rs`         | All hot-path `#[pg_extern]` functions: `send_full`, `send_batch_internal`, `send_fifo_full`, `receive_fn`, `receive_fifo_fn`, `delete_single`, `delete_batch`, `archive_single`, `archive_batch`, `pop`, `change_visibility_*`, `publish_event_pgrx`, `publish_event_batch_pgrx`. Also `direct_heap_insert` and `validate_name`. |
| `src/waiter.rs`        | `WaiterRegistry` shared-memory struct and linked-list operations. `register`, `unregister`, `notify_waiters`, `register_notify_after_commit`, `WaiterGuard`.                                                                                                                                                                     |
| `src/routing_cache.rs` | `RoutingCache` shared-memory struct. `lookup`, `insert`, `invalidate`, `invalidate_routing_cache_fn` (exposed as `queue._invalidate_routing_cache`).                                                                                                                                                                             |
| `sql/schema.sql`       | DDL for all tables, types, indexes. PL/pgSQL implementations of all non-hot-path functions plus fallback stubs for functions that pgrx overrides. The AFTER STATEMENT trigger on `event_subscriptions` that calls `_invalidate_routing_cache`.                                                                                   |
