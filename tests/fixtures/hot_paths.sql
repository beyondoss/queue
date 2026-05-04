-- SQL implementations of the pgrx hot-path functions.
-- Used in integration tests and sqlx prepare, where the compiled extension is not available.
-- These match the function signatures the API calls; semantics match the pgrx implementations.

-- send (canonical): (TEXT, JSONB, JSONB, TIMESTAMPTZ) -> SETOF BIGINT
CREATE FUNCTION queue.send(
    queue_name TEXT,
    msg        JSONB,
    headers    JSONB,
    delay      TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := queue.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $Q$ INSERT INTO queue.%I (vt, message, headers) VALUES ($2, $1, $3) RETURNING msg_id; $Q$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING msg, delay, headers;
    PERFORM pg_notify('queue_' || queue_name, '');
EXCEPTION WHEN UNDEFINED_TABLE THEN
    RAISE EXCEPTION 'queue "%" does not exist', queue_name USING ERRCODE = 'Q0001';
END;
$$ LANGUAGE plpgsql;

-- send (5-arg with sync_commit): sync_commit is ignored in the PL/pgSQL stub.
-- The pgrx version issues SET LOCAL synchronous_commit = off when false.
CREATE FUNCTION queue.send(
    queue_name  TEXT,
    msg         JSONB,
    headers     JSONB,
    delay       TIMESTAMP WITH TIME ZONE,
    sync_commit BOOLEAN
) RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send(queue_name, msg, headers, delay);
$$;

-- send overloads
CREATE FUNCTION queue.send(queue_name TEXT, msg JSONB)
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send(queue_name, msg, NULL, clock_timestamp());
$$;

CREATE FUNCTION queue.send(queue_name TEXT, msg JSONB, headers JSONB)
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send(queue_name, msg, headers, clock_timestamp());
$$;

CREATE FUNCTION queue.send(queue_name TEXT, msg JSONB, delay INTEGER)
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send(queue_name, msg, NULL, clock_timestamp() + make_interval(secs => delay));
$$;

CREATE FUNCTION queue.send(queue_name TEXT, msg JSONB, headers JSONB, delay INTEGER)
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send(queue_name, msg, headers, clock_timestamp() + make_interval(secs => delay));
$$;

-- _validate_batch_params
CREATE FUNCTION queue._validate_batch_params(msgs JSONB[], headers JSONB[]) RETURNS void AS $$
BEGIN
    IF msgs IS NULL OR array_length(msgs, 1) IS NULL THEN
        RAISE EXCEPTION 'msgs cannot be NULL or empty';
    END IF;
    IF headers IS NOT NULL
        AND COALESCE(array_length(headers, 1), 0) != COALESCE(array_length(msgs, 1), 0) THEN
        RAISE EXCEPTION 'headers array length (%) must match msgs array length (%)',
            COALESCE(array_length(headers, 1), 0), COALESCE(array_length(msgs, 1), 0);
    END IF;
END;
$$ LANGUAGE plpgsql;

-- _send_batch (canonical): (TEXT, JSONB[], JSONB[], TIMESTAMPTZ) -> SETOF BIGINT
CREATE FUNCTION queue._send_batch(
    queue_name TEXT,
    msgs       JSONB[],
    headers    JSONB[],
    delay      TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := queue.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $Q$
        INSERT INTO queue.%I (vt, message, headers)
        SELECT $2, unnest($1), unnest(coalesce($3, ARRAY[]::jsonb[]))
        RETURNING msg_id;
        $Q$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING msgs, delay, headers;
    PERFORM pg_notify('queue_' || queue_name, '');
EXCEPTION WHEN UNDEFINED_TABLE THEN
    RAISE EXCEPTION 'queue "%" does not exist', queue_name USING ERRCODE = 'Q0001';
END;
$$ LANGUAGE plpgsql;

-- _send_batch (5-arg with sync_commit): sync_commit ignored in PL/pgSQL stub.
CREATE FUNCTION queue._send_batch(
    queue_name  TEXT,
    msgs        JSONB[],
    headers     JSONB[],
    delay       TIMESTAMP WITH TIME ZONE,
    sync_commit BOOLEAN
) RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue._send_batch(queue_name, msgs, headers, delay);
$$;

-- send_batch public wrappers
CREATE FUNCTION queue.send_batch(queue_name TEXT, msgs JSONB[], headers JSONB[], delay TIMESTAMP WITH TIME ZONE)
RETURNS SETOF BIGINT LANGUAGE plpgsql AS $$
BEGIN
    PERFORM queue._validate_batch_params(msgs, headers);
    RETURN QUERY SELECT * FROM queue._send_batch(queue_name, msgs, headers, delay);
END;
$$;

CREATE FUNCTION queue.send_batch(queue_name TEXT, msgs JSONB[])
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send_batch(queue_name, msgs, NULL, clock_timestamp());
$$;

CREATE FUNCTION queue.send_batch(queue_name TEXT, msgs JSONB[], headers JSONB[])
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send_batch(queue_name, msgs, headers, clock_timestamp());
$$;

CREATE FUNCTION queue.send_batch(queue_name TEXT, msgs JSONB[], delay INTEGER)
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send_batch(queue_name, msgs, NULL, clock_timestamp() + make_interval(secs => delay));
$$;

CREATE FUNCTION queue.send_batch(queue_name TEXT, msgs JSONB[], headers JSONB[], delay INTEGER)
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send_batch(queue_name, msgs, headers, clock_timestamp() + make_interval(secs => delay));
$$;

-- read: (TEXT, INTEGER, INTEGER, JSONB) -> SETOF queue.message_record
--
-- No ORDER BY: concurrent readers find any unlocked row instead of all scanning
-- from the same low-msg_id index root. Benchmarks show parity with fully-partitioned
-- tables (+78% vs ordered scan) with zero schema complexity. SKIP LOCKED correctness
-- doesn't require ordering; SQS Standard doesn't guarantee FIFO either.
--
-- Literal embedding for qty and vt: prevents PostgreSQL from using a generic plan
-- where LIMIT $1 stops the planner from bounding the LockRows node at planning time.
-- Two SQL strings (plain vs conditional) keep each plan cache slot optimal.
--
-- NOTE: pgrx has no advantage here — pgrx TABLE functions must collect all rows into
-- Vec<T> before returning (pgrx's 'static constraint), then convert each datum back.
-- PL/pgSQL RETURN QUERY EXECUTE copies whole heap tuples once. Benchmarks confirm
-- pgrx read is 6.7× slower single-threaded and ~46% slower end-to-end vs this.
-- pgrx receive (5-arg) is kept: WaitLatch cannot be implemented in PL/pgSQL.
CREATE OR REPLACE FUNCTION queue.receive(
    queue_name  TEXT,
    vt          INTEGER,
    qty         INTEGER,
    conditional JSONB DEFAULT '{}'::jsonb
) RETURNS SETOF queue.message_record AS $$
BEGIN
    IF conditional = '{}'::jsonb THEN
        RETURN QUERY EXECUTE format(
            'WITH cte AS (
                SELECT msg_id FROM queue.q_%I
                WHERE vt <= clock_timestamp()
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE queue.q_%I m
            SET last_read_at = clock_timestamp(),
                vt           = clock_timestamp() + make_interval(secs => %s),
                read_ct      = read_ct + 1
            FROM cte WHERE m.msg_id = cte.msg_id
            RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers',
            queue_name, qty, queue_name, vt
        );
    ELSE
        RETURN QUERY EXECUTE format(
            'WITH cte AS (
                SELECT msg_id FROM queue.q_%I
                WHERE vt <= clock_timestamp()
                  AND (message @> %L::jsonb)
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE queue.q_%I m
            SET last_read_at = clock_timestamp(),
                vt           = clock_timestamp() + make_interval(secs => %s),
                read_ct      = read_ct + 1
            FROM cte WHERE m.msg_id = cte.msg_id
            RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers',
            queue_name, conditional, qty, queue_name, vt
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

-- receive: (TEXT, INTEGER, INTEGER, INTEGER, INTEGER, JSONB) -> SETOF queue.message_record
-- SQL fallback used when the pgrx extension is not in shared_preload_libraries.
-- The pgrx version uses WaitLatch for true push-based wakeup (no busy polling);
-- this version falls back to pg_sleep intervals. Both share the same read SQL.
CREATE OR REPLACE FUNCTION queue.receive(
    queue_name       TEXT,
    vt               INTEGER,
    qty              INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100,
    conditional      JSONB DEFAULT '{}'::jsonb
) RETURNS SETOF queue.message_record AS $$
DECLARE
    r       queue.message_record;
    stop_at TIMESTAMP;
    sql     TEXT;
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
    -- Same SQL as queue.read: no ORDER BY, literal embedding for qty and vt.
    IF conditional = '{}'::jsonb THEN
        sql := format(
            'WITH cte AS (
                SELECT msg_id FROM queue.q_%I
                WHERE vt <= clock_timestamp()
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE queue.q_%I m
            SET last_read_at = clock_timestamp(),
                vt           = clock_timestamp() + make_interval(secs => %s),
                read_ct      = read_ct + 1
            FROM cte WHERE m.msg_id = cte.msg_id
            RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers',
            queue_name, qty, queue_name, vt
        );
    ELSE
        sql := format(
            'WITH cte AS (
                SELECT msg_id FROM queue.q_%I
                WHERE vt <= clock_timestamp()
                  AND (message @> %L::jsonb)
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE queue.q_%I m
            SET last_read_at = clock_timestamp(),
                vt           = clock_timestamp() + make_interval(secs => %s),
                read_ct      = read_ct + 1
            FROM cte WHERE m.msg_id = cte.msg_id
            RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers',
            queue_name, conditional, qty, queue_name, vt
        );
    END IF;
    -- Try reading first, then check deadline — matches pgrx semantics where
    -- wait=0 still attempts one read before returning empty.
    LOOP
        FOR r IN EXECUTE sql LOOP RETURN NEXT r; END LOOP;
        IF FOUND THEN RETURN; END IF;
        IF clock_timestamp() >= stop_at THEN RETURN; END IF;
        PERFORM pg_sleep(
            LEAST(
                poll_interval_ms::numeric / 1000,
                EXTRACT(EPOCH FROM (stop_at - clock_timestamp()))
            )
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- delete (single): (TEXT, BIGINT) -> BOOLEAN
CREATE FUNCTION queue.delete(queue_name TEXT, msg_id BIGINT) RETURNS BOOLEAN AS $$
DECLARE
    result BIGINT;
    sql    TEXT;
    qtable TEXT := queue.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT($Q$ DELETE FROM queue.%I WHERE msg_id = $1 RETURNING msg_id $Q$, qtable);
    EXECUTE sql USING msg_id INTO result;
    RETURN result IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

-- delete (batch): (TEXT, BIGINT[]) -> SETOF BIGINT
CREATE FUNCTION queue.delete(queue_name TEXT, msg_ids BIGINT[]) RETURNS SETOF BIGINT AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := queue.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT($Q$ DELETE FROM queue.%I WHERE msg_id = ANY($1) RETURNING msg_id $Q$, qtable);
    RETURN QUERY EXECUTE sql USING msg_ids;
END;
$$ LANGUAGE plpgsql;

-- archive (single): (TEXT, BIGINT) -> BOOLEAN
CREATE FUNCTION queue.archive(queue_name TEXT, msg_id BIGINT) RETURNS BOOLEAN AS $$
DECLARE
    result BIGINT;
    sql    TEXT;
    qtable TEXT := queue.format_table_name(queue_name, 'q');
    atable TEXT := queue.format_table_name(queue_name, 'a');
BEGIN
    sql := FORMAT(
        $Q$
        WITH archived AS (
            DELETE FROM queue.%I WHERE msg_id = $1
            RETURNING msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
        )
        INSERT INTO queue.%I (msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers)
        SELECT msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers FROM archived
        RETURNING msg_id;
        $Q$,
        qtable, atable
    );
    EXECUTE sql USING msg_id INTO result;
    RETURN result IS NOT NULL;
END;
$$ LANGUAGE plpgsql;

-- archive (batch): (TEXT, BIGINT[]) -> SETOF BIGINT
CREATE FUNCTION queue.archive(queue_name TEXT, msg_ids BIGINT[]) RETURNS SETOF BIGINT AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := queue.format_table_name(queue_name, 'q');
    atable TEXT := queue.format_table_name(queue_name, 'a');
BEGIN
    sql := FORMAT(
        $Q$
        WITH archived AS (
            DELETE FROM queue.%I WHERE msg_id = ANY($1)
            RETURNING msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
        )
        INSERT INTO queue.%I (msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers)
        SELECT msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers FROM archived
        RETURNING msg_id;
        $Q$,
        qtable, atable
    );
    RETURN QUERY EXECUTE sql USING msg_ids;
END;
$$ LANGUAGE plpgsql;

-- pop: (TEXT, INTEGER) -> SETOF queue.message_record
CREATE FUNCTION queue.pop(queue_name TEXT, qty INTEGER DEFAULT 1)
RETURNS SETOF queue.message_record AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := queue.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $Q$
        WITH cte AS (
            SELECT msg_id FROM queue.%I WHERE vt <= clock_timestamp()
            ORDER BY msg_id ASC LIMIT $1 FOR UPDATE SKIP LOCKED
        )
        DELETE FROM queue.%I WHERE msg_id IN (SELECT msg_id FROM cte)
        RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers;
        $Q$,
        qtable, qtable
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

-- change_visibility (timestamp): (TEXT, BIGINT, TIMESTAMPTZ) -> SETOF queue.message_record
CREATE FUNCTION queue.change_visibility(queue_name TEXT, msg_id BIGINT, vt TIMESTAMP WITH TIME ZONE)
RETURNS SETOF queue.message_record AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := queue.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $Q$
        UPDATE queue.%I SET vt = $1 WHERE msg_id = $2
        RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers;
        $Q$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING vt, msg_id;
END;
$$ LANGUAGE plpgsql;

-- change_visibility (seconds): (TEXT, BIGINT, INTEGER) -> SETOF queue.message_record
CREATE FUNCTION queue.change_visibility(queue_name TEXT, msg_id BIGINT, vt INTEGER)
RETURNS SETOF queue.message_record LANGUAGE sql AS $$
    SELECT * FROM queue.change_visibility(queue_name, msg_id, clock_timestamp() + make_interval(secs => vt));
$$;

-- change_visibility batch (timestamp): (TEXT, BIGINT[], TIMESTAMPTZ) -> SETOF queue.message_record
CREATE FUNCTION queue.change_visibility(queue_name TEXT, msg_ids BIGINT[], vt TIMESTAMP WITH TIME ZONE)
RETURNS SETOF queue.message_record AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := queue.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $Q$
        UPDATE queue.%I SET vt = $1 WHERE msg_id = ANY($2)
        RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers;
        $Q$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING vt, msg_ids;
END;
$$ LANGUAGE plpgsql;

-- change_visibility batch (seconds): (TEXT, BIGINT[], INTEGER) -> SETOF queue.message_record
CREATE FUNCTION queue.change_visibility(queue_name TEXT, msg_ids BIGINT[], vt INTEGER)
RETURNS SETOF queue.message_record LANGUAGE sql AS $$
    SELECT * FROM queue.change_visibility(queue_name, msg_ids, clock_timestamp() + make_interval(secs => vt));
$$;

-- send_fifo (7-arg with sync_commit): sync_commit ignored in PL/pgSQL stub.
-- The pgrx version issues SET LOCAL synchronous_commit = off when false.
CREATE FUNCTION queue.send_fifo(
    queue_name       TEXT,
    msg              JSONB,
    message_group_id TEXT,
    deduplication_id TEXT,
    headers          JSONB,
    delay            TIMESTAMP WITH TIME ZONE,
    sync_commit      BOOLEAN
) RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send_fifo(queue_name, msg, message_group_id, deduplication_id, headers, delay);
$$;

-- send_topic (canonical): (TEXT, JSONB, JSONB, TIMESTAMPTZ, BOOLEAN) -> TABLE(queue_name text, msg_id bigint)
-- PL/pgSQL stub matching the pgrx C function signature; used in tests without extension.
-- sync_commit is ignored in the PL/pgSQL stub.
CREATE FUNCTION queue.send_topic(
    routing_key TEXT,
    msg         JSONB,
    headers     JSONB,
    delay       TIMESTAMP WITH TIME ZONE,
    sync_commit BOOLEAN DEFAULT TRUE
) RETURNS TABLE (queue_name text, msg_id bigint) AS $$
DECLARE
    b RECORD;
BEGIN
    FOR b IN
        SELECT DISTINCT tb.queue_name FROM queue.topic_subscriptions tb
        WHERE routing_key ~ tb.compiled_regex
          AND tb.queue_name IS NOT NULL
        ORDER BY tb.queue_name
    LOOP
        RETURN QUERY
            SELECT b.queue_name, id
            FROM queue.send(b.queue_name, msg, headers, delay) AS id;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
