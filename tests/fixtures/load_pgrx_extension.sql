-- Register the pgrx-compiled hot-path functions from libpgmq_extension.so.
-- Run this AFTER schema.sql and hot_paths.sql (or schema.sql alone) to replace
-- the PL/pgSQL implementations with the compiled Rust versions.
--
-- The symbol names follow pgrx's convention: {rust_fn_name}_wrapper.

-- send (canonical) — drop both the 4-arg and 5-arg PL/pgSQL overloads before installing C.
DROP FUNCTION IF EXISTS pgmq.send(TEXT, JSONB, JSONB, TIMESTAMP WITH TIME ZONE);
DROP FUNCTION IF EXISTS pgmq.send(TEXT, JSONB, JSONB, TIMESTAMP WITH TIME ZONE, BOOLEAN);
CREATE FUNCTION pgmq.send(
    queue_name  TEXT,
    msg         JSONB,
    headers     JSONB,
    delay       TIMESTAMP WITH TIME ZONE,
    sync_commit BOOLEAN DEFAULT TRUE
) RETURNS SETOF BIGINT
LANGUAGE C VOLATILE
AS '$libdir/libpgmq_extension', 'send_full_wrapper';

-- _send_batch (canonical batch hot path) — same: drop old 4-arg before creating 5-arg.
DROP FUNCTION IF EXISTS pgmq._send_batch(TEXT, JSONB[], JSONB[], TIMESTAMP WITH TIME ZONE);
DROP FUNCTION IF EXISTS pgmq._send_batch(TEXT, JSONB[], JSONB[], TIMESTAMP WITH TIME ZONE, BOOLEAN);
CREATE FUNCTION pgmq._send_batch(
    queue_name  TEXT,
    msgs        JSONB[],
    headers     JSONB[],
    delay       TIMESTAMP WITH TIME ZONE,
    sync_commit BOOLEAN DEFAULT TRUE
) RETURNS SETOF BIGINT
LANGUAGE C VOLATILE
AS '$libdir/libpgmq_extension', 'send_batch_internal_wrapper';

-- read: NOT overridden by pgrx.
-- PL/pgSQL RETURN QUERY EXECUTE copies whole heap tuples (one palloc/row) and
-- streams them directly. pgrx TABLE functions extract 7 datums per row into Rust
-- then re-encode them on return — 140+ operations per 10-row call vs 10.
-- Benchmarks: pgrx read is 6.7× slower single-threaded; PL/pgSQL version defined
-- in hot_paths.sql already uses no ORDER BY and literal embedding.

-- read_with_poll — drop first (hot_paths.sql defines this as SETOF pgmq.message_record)
DROP FUNCTION IF EXISTS pgmq.read_with_poll(TEXT, INTEGER, INTEGER, INTEGER, INTEGER, JSONB);
CREATE FUNCTION pgmq.read_with_poll(
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
AS '$libdir/libpgmq_extension', 'read_with_poll_wrapper';

-- delete (single)
CREATE OR REPLACE FUNCTION pgmq.delete(queue_name TEXT, msg_id BIGINT)
RETURNS BOOLEAN
LANGUAGE C VOLATILE
AS '$libdir/libpgmq_extension', 'delete_single_wrapper';

-- delete (batch)
CREATE OR REPLACE FUNCTION pgmq.delete(queue_name TEXT, msg_ids BIGINT[])
RETURNS SETOF BIGINT
LANGUAGE C VOLATILE
AS '$libdir/libpgmq_extension', 'delete_batch_wrapper';

-- archive (single)
CREATE OR REPLACE FUNCTION pgmq.archive(queue_name TEXT, msg_id BIGINT)
RETURNS BOOLEAN
LANGUAGE C VOLATILE
AS '$libdir/libpgmq_extension', 'archive_single_wrapper';

-- archive (batch)
CREATE OR REPLACE FUNCTION pgmq.archive(queue_name TEXT, msg_ids BIGINT[])
RETURNS SETOF BIGINT
LANGUAGE C VOLATILE
AS '$libdir/libpgmq_extension', 'archive_batch_wrapper';

-- pop — drop first (hot_paths.sql defines this as SETOF pgmq.message_record)
DROP FUNCTION IF EXISTS pgmq.pop(TEXT, INTEGER);
CREATE FUNCTION pgmq.pop(queue_name TEXT, qty INTEGER DEFAULT 1)
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
AS '$libdir/libpgmq_extension', 'pop_wrapper';

-- set_vt (timestamp) — drop first (hot_paths.sql defines this as SETOF pgmq.message_record)
DROP FUNCTION IF EXISTS pgmq.set_vt(TEXT, BIGINT, TIMESTAMP WITH TIME ZONE);
CREATE FUNCTION pgmq.set_vt(queue_name TEXT, msg_id BIGINT, vt TIMESTAMP WITH TIME ZONE)
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
AS '$libdir/libpgmq_extension', 'set_vt_ts_wrapper';

-- set_vt (seconds) — drop first because schema.sql defines this as SETOF pgmq.message_record
DROP FUNCTION IF EXISTS pgmq.set_vt(TEXT, BIGINT, INTEGER);
CREATE FUNCTION pgmq.set_vt(queue_name TEXT, msg_id BIGINT, vt INTEGER)
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
AS '$libdir/libpgmq_extension', 'set_vt_secs_wrapper';

-- set_vt batch (timestamp) — drop first (return type change)
DROP FUNCTION IF EXISTS pgmq.set_vt(TEXT, BIGINT[], TIMESTAMP WITH TIME ZONE);
CREATE FUNCTION pgmq.set_vt(queue_name TEXT, msg_ids BIGINT[], vt TIMESTAMP WITH TIME ZONE)
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
AS '$libdir/libpgmq_extension', 'set_vt_batch_ts_wrapper';

-- set_vt batch (seconds) — drop first (return type change)
DROP FUNCTION IF EXISTS pgmq.set_vt(TEXT, BIGINT[], INTEGER);
CREATE FUNCTION pgmq.set_vt(queue_name TEXT, msg_ids BIGINT[], vt INTEGER)
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
AS '$libdir/libpgmq_extension', 'set_vt_batch_secs_wrapper';

-- send_fifo (canonical) — drop both overloads (6-arg PL/pgSQL and 7-arg stub added in hot_paths.sql).
DROP FUNCTION IF EXISTS pgmq.send_fifo(TEXT, JSONB, TEXT, TEXT, JSONB, TIMESTAMP WITH TIME ZONE);
DROP FUNCTION IF EXISTS pgmq.send_fifo(TEXT, JSONB, TEXT, TEXT, JSONB, TIMESTAMP WITH TIME ZONE, BOOLEAN);
CREATE FUNCTION pgmq.send_fifo(
    queue_name       TEXT,
    msg              JSONB,
    message_group_id TEXT,
    deduplication_id TEXT,
    headers          JSONB,
    delay            TIMESTAMP WITH TIME ZONE,
    sync_commit      BOOLEAN DEFAULT TRUE
) RETURNS SETOF BIGINT
LANGUAGE C VOLATILE
AS '$libdir/libpgmq_extension', 'send_fifo_full_wrapper';

-- read_fifo_with_poll — drop PL/pgSQL version, replace with WaitLatch C version.
DROP FUNCTION IF EXISTS pgmq.read_fifo_with_poll(TEXT, INTEGER, INTEGER, INTEGER, INTEGER);
CREATE FUNCTION pgmq.read_fifo_with_poll(
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
AS '$libdir/libpgmq_extension', 'read_fifo_with_poll_fn_wrapper';
