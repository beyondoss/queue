-- Register the pgrx-compiled hot-path functions from libbeyond_queue_extension.so.
-- Run this AFTER schema.sql and hot_paths.sql (or schema.sql alone) to replace
-- the PL/pgSQL implementations with the compiled Rust versions.
--
-- The symbol names follow pgrx's convention: {rust_fn_name}_wrapper.

-- send (canonical) — drop both the 4-arg and 5-arg PL/pgSQL overloads before installing C.
DROP FUNCTION IF EXISTS queue.send(TEXT, JSONB, JSONB, TIMESTAMP WITH TIME ZONE);
DROP FUNCTION IF EXISTS queue.send(TEXT, JSONB, JSONB, TIMESTAMP WITH TIME ZONE, BOOLEAN);
CREATE FUNCTION queue.send(
    queue_name  TEXT,
    msg         JSONB,
    headers     JSONB,
    delay       TIMESTAMP WITH TIME ZONE,
    sync_commit BOOLEAN DEFAULT TRUE
) RETURNS SETOF BIGINT
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'send_full_wrapper';

-- _send_batch (canonical batch hot path) — same: drop old 4-arg before creating 5-arg.
DROP FUNCTION IF EXISTS queue._send_batch(TEXT, JSONB[], JSONB[], TIMESTAMP WITH TIME ZONE);
DROP FUNCTION IF EXISTS queue._send_batch(TEXT, JSONB[], JSONB[], TIMESTAMP WITH TIME ZONE, BOOLEAN);
CREATE FUNCTION queue._send_batch(
    queue_name  TEXT,
    msgs        JSONB[],
    headers     JSONB[],
    delay       TIMESTAMP WITH TIME ZONE,
    sync_commit BOOLEAN DEFAULT TRUE
) RETURNS SETOF BIGINT
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'send_batch_internal_wrapper';

-- read: NOT overridden by pgrx.
-- PL/pgSQL RETURN QUERY EXECUTE copies whole heap tuples (one palloc/row) and
-- streams them directly. pgrx TABLE functions extract 7 datums per row into Rust
-- then re-encode them on return — 140+ operations per 10-row call vs 10.
-- Benchmarks: pgrx read is 6.7× slower single-threaded; PL/pgSQL version defined
-- in hot_paths.sql already uses no ORDER BY and literal embedding.

-- receive — drop first (hot_paths.sql defines this as SETOF queue.message_record)
DROP FUNCTION IF EXISTS queue.receive(TEXT, INTEGER, INTEGER, INTEGER, INTEGER, JSONB);
CREATE FUNCTION queue.receive(
    queue_name       TEXT,
    vt               INTEGER,
    qty              INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100,
    conditional      JSONB DEFAULT '{}'::jsonb
) RETURNS TABLE (
    msg_id      BIGINT,
    read_ct     INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt          TIMESTAMP WITH TIME ZONE,
    message     JSONB,
    headers     JSONB
)
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'receive_fn_wrapper';

-- delete (single)
CREATE OR REPLACE FUNCTION queue.delete(queue_name TEXT, msg_id BIGINT)
RETURNS BOOLEAN
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'delete_single_wrapper';

-- delete (batch)
CREATE OR REPLACE FUNCTION queue.delete(queue_name TEXT, msg_ids BIGINT[])
RETURNS SETOF BIGINT
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'delete_batch_wrapper';

-- archive (single)
CREATE OR REPLACE FUNCTION queue.archive(queue_name TEXT, msg_id BIGINT)
RETURNS BOOLEAN
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'archive_single_wrapper';

-- archive (batch)
CREATE OR REPLACE FUNCTION queue.archive(queue_name TEXT, msg_ids BIGINT[])
RETURNS SETOF BIGINT
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'archive_batch_wrapper';

-- pop — drop first (hot_paths.sql defines this as SETOF queue.message_record)
DROP FUNCTION IF EXISTS queue.pop(TEXT, INTEGER);
CREATE FUNCTION queue.pop(queue_name TEXT, qty INTEGER DEFAULT 1)
RETURNS TABLE (
    msg_id      BIGINT,
    read_ct     INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt          TIMESTAMP WITH TIME ZONE,
    message     JSONB,
    headers     JSONB
)
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'pop_wrapper';

-- change_visibility (timestamp) — drop first (hot_paths.sql defines this as SETOF queue.message_record)
DROP FUNCTION IF EXISTS queue.change_visibility(TEXT, BIGINT, TIMESTAMP WITH TIME ZONE);
CREATE FUNCTION queue.change_visibility(queue_name TEXT, msg_id BIGINT, vt TIMESTAMP WITH TIME ZONE)
RETURNS TABLE (
    msg_id      BIGINT,
    read_ct     INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt          TIMESTAMP WITH TIME ZONE,
    message     JSONB,
    headers     JSONB
)
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'change_visibility_ts_wrapper';

-- change_visibility (seconds) — drop first because schema.sql defines this as SETOF queue.message_record
DROP FUNCTION IF EXISTS queue.change_visibility(TEXT, BIGINT, INTEGER);
CREATE FUNCTION queue.change_visibility(queue_name TEXT, msg_id BIGINT, vt INTEGER)
RETURNS TABLE (
    msg_id      BIGINT,
    read_ct     INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt          TIMESTAMP WITH TIME ZONE,
    message     JSONB,
    headers     JSONB
)
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'change_visibility_secs_wrapper';

-- change_visibility batch (timestamp) — drop first (return type change)
DROP FUNCTION IF EXISTS queue.change_visibility(TEXT, BIGINT[], TIMESTAMP WITH TIME ZONE);
CREATE FUNCTION queue.change_visibility(queue_name TEXT, msg_ids BIGINT[], vt TIMESTAMP WITH TIME ZONE)
RETURNS TABLE (
    msg_id      BIGINT,
    read_ct     INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt          TIMESTAMP WITH TIME ZONE,
    message     JSONB,
    headers     JSONB
)
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'change_visibility_batch_ts_wrapper';

-- change_visibility batch (seconds) — drop first (return type change)
DROP FUNCTION IF EXISTS queue.change_visibility(TEXT, BIGINT[], INTEGER);
CREATE FUNCTION queue.change_visibility(queue_name TEXT, msg_ids BIGINT[], vt INTEGER)
RETURNS TABLE (
    msg_id      BIGINT,
    read_ct     INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt          TIMESTAMP WITH TIME ZONE,
    message     JSONB,
    headers     JSONB
)
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'change_visibility_batch_secs_wrapper';

-- send_fifo (canonical) — drop both overloads (6-arg PL/pgSQL and 7-arg stub added in hot_paths.sql).
DROP FUNCTION IF EXISTS queue.send_fifo(TEXT, JSONB, TEXT, TEXT, JSONB, TIMESTAMP WITH TIME ZONE);
DROP FUNCTION IF EXISTS queue.send_fifo(TEXT, JSONB, TEXT, TEXT, JSONB, TIMESTAMP WITH TIME ZONE, BOOLEAN);
CREATE FUNCTION queue.send_fifo(
    queue_name       TEXT,
    msg              JSONB,
    message_group_id TEXT,
    deduplication_id TEXT,
    headers          JSONB,
    delay            TIMESTAMP WITH TIME ZONE,
    sync_commit      BOOLEAN DEFAULT TRUE
) RETURNS SETOF BIGINT
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'send_fifo_full_wrapper';

-- receive_fifo (5-arg) — drop PL/pgSQL version, replace with WaitLatch C version.
DROP FUNCTION IF EXISTS queue.receive_fifo(TEXT, INTEGER, INTEGER, INTEGER, INTEGER);
CREATE FUNCTION queue.receive_fifo(
    queue_name       TEXT,
    vt               INTEGER,
    qty              INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100
) RETURNS TABLE (
    msg_id      BIGINT,
    read_ct     INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt          TIMESTAMP WITH TIME ZONE,
    message     JSONB,
    headers     JSONB
)
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'receive_fifo_fn_wrapper';

-- publish_event (pgrx) — replaces canonical from hot_paths.sql; return type changed to TABLE.
DROP FUNCTION IF EXISTS queue.publish_event(TEXT, JSONB, JSONB, TIMESTAMP WITH TIME ZONE);
DROP FUNCTION IF EXISTS queue.publish_event(TEXT, JSONB, JSONB, TIMESTAMP WITH TIME ZONE, BOOLEAN);
CREATE FUNCTION queue.publish_event(
    routing_key TEXT,
    msg         JSONB,
    headers     JSONB,
    delay       TIMESTAMP WITH TIME ZONE,
    sync_commit BOOLEAN DEFAULT TRUE
) RETURNS TABLE (queue_name TEXT, msg_id BIGINT)
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'publish_event_pgrx_wrapper';

-- publish_event_batch (pgrx) — replaces TIMESTAMPTZ canonical from schema.sql; adds sync_commit.
DROP FUNCTION IF EXISTS queue.publish_event_batch(TEXT, JSONB[], JSONB[], TIMESTAMP WITH TIME ZONE);
DROP FUNCTION IF EXISTS queue.publish_event_batch(TEXT, JSONB[], JSONB[], TIMESTAMP WITH TIME ZONE, BOOLEAN);
CREATE FUNCTION queue.publish_event_batch(
    routing_key TEXT,
    msgs        JSONB[],
    headers     JSONB[],
    delay       TIMESTAMP WITH TIME ZONE,
    sync_commit BOOLEAN DEFAULT TRUE
) RETURNS TABLE (queue_name TEXT, msg_id BIGINT)
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'publish_event_batch_pgrx_wrapper';

-- _invalidate_routing_cache (pgrx) — overrides the PL/pgSQL no-op from schema.sql.
-- Called by the topic_bindings_cache_invalidate trigger on every topic_bindings write.
CREATE OR REPLACE FUNCTION queue._invalidate_routing_cache()
RETURNS VOID
LANGUAGE C VOLATILE
AS '$libdir/libbeyond_queue_extension', 'invalidate_routing_cache_fn_wrapper';
