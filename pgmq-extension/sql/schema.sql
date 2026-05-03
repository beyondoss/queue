-- pgmq-rx schema
-- Tables, types, indexes, and non-hot-path functions.
-- Hot path functions (send, send_batch, read, read_with_poll, delete, archive,
-- pop, set_vt) are implemented in Rust via pgrx and override these declarations.

CREATE SCHEMA IF NOT EXISTS pgmq;

-- Queue registry
CREATE TABLE IF NOT EXISTS pgmq.meta (
    queue_name   VARCHAR UNIQUE NOT NULL,
    is_partitioned BOOLEAN NOT NULL,
    is_unlogged  BOOLEAN NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    queue_type   TEXT NOT NULL DEFAULT 'standard'
);

-- Notification throttle state (UNLOGGED — survives restarts only)
CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.notify_insert_throttle (
    queue_name           VARCHAR UNIQUE NOT NULL
        CONSTRAINT notify_insert_throttle_meta_fk
            REFERENCES pgmq.meta (queue_name) ON DELETE CASCADE,
    throttle_interval_ms INTEGER NOT NULL DEFAULT 0,
    last_notified_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT to_timestamp(0)
);

CREATE INDEX IF NOT EXISTS idx_notify_throttle_active
    ON pgmq.notify_insert_throttle (queue_name, last_notified_at)
    WHERE throttle_interval_ms > 0;

-- Topic binding registry (wildcard routing key → queue)
CREATE TABLE IF NOT EXISTS pgmq.topic_bindings (
    pattern        text NOT NULL,
    queue_name     text NOT NULL
        CONSTRAINT topic_bindings_meta_fk
            REFERENCES pgmq.meta (queue_name) ON DELETE CASCADE,
    bound_at       TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    compiled_regex text GENERATED ALWAYS AS (
        '^' ||
        replace(
            replace(
                regexp_replace(pattern, '([.+?{}()|\[\]\\^$])', '\\\1', 'g'),
                '*', '[^.]+'
            ),
            '#', '.*'
        ) || '$'
    ) STORED,
    CONSTRAINT topic_bindings_unique UNIQUE (pattern, queue_name)
);

CREATE INDEX IF NOT EXISTS idx_topic_bindings_covering
    ON pgmq.topic_bindings (pattern)
    INCLUDE (queue_name, compiled_regex);

DO
$$
BEGIN
    IF EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'beyond_queue_extension') THEN
        PERFORM pg_catalog.pg_extension_config_dump('pgmq.meta', '');
        PERFORM pg_catalog.pg_extension_config_dump('pgmq.notify_insert_throttle', '');
        PERFORM pg_catalog.pg_extension_config_dump('pgmq.topic_bindings', '');
    END IF;
END
$$;

-- Composite type returned by queue read/pop/archive operations
CREATE TYPE pgmq.message_record AS (
    msg_id      BIGINT,
    read_ct     INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt          TIMESTAMP WITH TIME ZONE,
    message     JSONB,
    headers     JSONB
);

CREATE TYPE pgmq.queue_record AS (
    queue_name     VARCHAR,
    is_partitioned BOOLEAN,
    is_unlogged    BOOLEAN,
    created_at     TIMESTAMP WITH TIME ZONE,
    queue_type     TEXT
);

CREATE TYPE pgmq.metrics_result AS (
    queue_name          text,
    queue_length        bigint,
    newest_msg_age_sec  int,
    oldest_msg_age_sec  int,
    total_messages      bigint,
    scrape_time         timestamp with time zone,
    queue_visible_length bigint
);

------------------------------------------------------------
-- Utility helpers
------------------------------------------------------------

CREATE FUNCTION pgmq.acquire_queue_lock(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('queue.queue_' || queue_name));
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.format_table_name(queue_name text, prefix text)
RETURNS TEXT AS $$
BEGIN
    IF queue_name ~ '\$|;|--|'''
    THEN
        RAISE EXCEPTION 'queue name contains invalid characters: $, ;, --, or ''';
    END IF;
    RETURN lower(prefix || '_' || queue_name);
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.validate_queue_name(queue_name TEXT)
RETURNS void AS $$
BEGIN
    IF length(queue_name) > 47 THEN
        RAISE EXCEPTION 'queue name is too long, maximum length is 47 characters';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq._belongs_to_pgmq(table_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    result BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1
        FROM pg_depend
        WHERE refobjid = (SELECT oid FROM pg_extension WHERE extname = 'beyond_queue_extension')
          AND objid = (SELECT oid FROM pg_class WHERE relname = table_name)
    ) INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq._get_pg_partman_schema()
RETURNS TEXT AS $$
    SELECT extnamespace::regnamespace::text
    FROM pg_extension
    WHERE extname = 'pg_partman';
$$ LANGUAGE SQL;

CREATE FUNCTION pgmq._extension_exists(extension_name TEXT)
RETURNS BOOLEAN
LANGUAGE SQL AS $$
    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = extension_name)
$$;

CREATE FUNCTION pgmq._ensure_pg_partman_installed()
RETURNS void AS $$
BEGIN
    IF NOT pgmq._extension_exists('pg_partman') THEN
        RAISE EXCEPTION 'pg_partman is required for partitioned queues';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq._get_pg_partman_major_version()
RETURNS INT
LANGUAGE SQL AS $$
    SELECT split_part(extversion, '.', 1)::INT
    FROM pg_extension
    WHERE extname = 'pg_partman'
$$;

CREATE FUNCTION pgmq._get_partition_col(partition_interval TEXT)
RETURNS TEXT AS $$
DECLARE
    num INTEGER;
BEGIN
    BEGIN
        num := partition_interval::INTEGER;
        RETURN 'msg_id';
    EXCEPTION
        WHEN others THEN
            RETURN 'enqueued_at';
    END;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- Queue lifecycle
------------------------------------------------------------

CREATE FUNCTION pgmq.create_non_partitioned(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
    PERFORM pgmq.validate_queue_name(queue_name);
    PERFORM pgmq.acquire_queue_lock(queue_name);

    EXECUTE FORMAT(
        $Q$
        CREATE TABLE IF NOT EXISTS pgmq.%I (
            msg_id      BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (CACHE 100),
            read_ct     INT DEFAULT 0 NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            last_read_at TIMESTAMP WITH TIME ZONE,
            vt          TIMESTAMP WITH TIME ZONE NOT NULL,
            message     JSONB,
            headers     JSONB
        )
        $Q$,
        qtable
    );

    EXECUTE FORMAT(
        $Q$
        CREATE TABLE IF NOT EXISTS pgmq.%I (
            msg_id       BIGINT PRIMARY KEY,
            read_ct      INT DEFAULT 0 NOT NULL,
            enqueued_at  TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            last_read_at TIMESTAMP WITH TIME ZONE,
            archived_at  TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            vt           TIMESTAMP WITH TIME ZONE NOT NULL,
            message      JSONB,
            headers      JSONB
        )
        $Q$,
        atable
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC) $Q$,
        qtable || '_vt_idx', qtable
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at) $Q$,
        'archived_at_idx_' || queue_name, atable
    );

    EXECUTE FORMAT(
        $Q$
        INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
        VALUES (%L, false, false)
        ON CONFLICT DO NOTHING
        $Q$,
        queue_name
    );
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.create_unlogged(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
    PERFORM pgmq.validate_queue_name(queue_name);
    PERFORM pgmq.acquire_queue_lock(queue_name);

    EXECUTE FORMAT(
        $Q$
        CREATE UNLOGGED TABLE IF NOT EXISTS pgmq.%I (
            msg_id      BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (CACHE 100),
            read_ct     INT DEFAULT 0 NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            last_read_at TIMESTAMP WITH TIME ZONE,
            vt          TIMESTAMP WITH TIME ZONE NOT NULL,
            message     JSONB,
            headers     JSONB
        )
        $Q$,
        qtable
    );

    EXECUTE FORMAT(
        $Q$
        CREATE TABLE IF NOT EXISTS pgmq.%I (
            msg_id       BIGINT PRIMARY KEY,
            read_ct      INT DEFAULT 0 NOT NULL,
            enqueued_at  TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            last_read_at TIMESTAMP WITH TIME ZONE,
            archived_at  TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            vt           TIMESTAMP WITH TIME ZONE NOT NULL,
            message      JSONB,
            headers      JSONB
        )
        $Q$,
        atable
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC) $Q$,
        qtable || '_vt_idx', qtable
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at) $Q$,
        'archived_at_idx_' || queue_name, atable
    );

    EXECUTE FORMAT(
        $Q$
        INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
        VALUES (%L, false, true)
        ON CONFLICT DO NOTHING
        $Q$,
        queue_name
    );
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.create_partitioned(
    queue_name          TEXT,
    partition_interval  TEXT DEFAULT '10000',
    retention_interval  TEXT DEFAULT '100000'
)
RETURNS void AS $$
DECLARE
    partition_col   TEXT;
    a_partition_col TEXT;
    qtable          TEXT := pgmq.format_table_name(queue_name, 'q');
    atable          TEXT := pgmq.format_table_name(queue_name, 'a');
    fq_qtable       TEXT := 'pgmq.' || pgmq.format_table_name(queue_name, 'q');
    fq_atable       TEXT := 'pgmq.' || pgmq.format_table_name(queue_name, 'a');
BEGIN
    PERFORM pgmq.validate_queue_name(queue_name);
    PERFORM pgmq.acquire_queue_lock(queue_name);
    PERFORM pgmq._ensure_pg_partman_installed();
    SELECT pgmq._get_partition_col(partition_interval) INTO partition_col;

    EXECUTE FORMAT(
        $Q$
        CREATE TABLE IF NOT EXISTS pgmq.%I (
            msg_id      BIGINT GENERATED ALWAYS AS IDENTITY (CACHE 100),
            read_ct     INT DEFAULT 0 NOT NULL,
            enqueued_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            last_read_at TIMESTAMP WITH TIME ZONE,
            vt          TIMESTAMP WITH TIME ZONE NOT NULL,
            message     JSONB,
            headers     JSONB
        ) PARTITION BY RANGE (%I)
        $Q$,
        qtable, partition_col
    );

    EXECUTE FORMAT(
        $Q$
        SELECT %I.create_parent(
            p_parent_table := %L,
            p_control      := %L,
            p_interval     := %L,
            p_type         := CASE
                WHEN pgmq._get_pg_partman_major_version() = 5 THEN 'range'
                ELSE 'native'
            END
        )
        $Q$,
        pgmq._get_pg_partman_schema(), fq_qtable, partition_col, partition_interval
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (%I) $Q$,
        qtable || '_part_idx', qtable, partition_col
    );

    EXECUTE FORMAT(
        $Q$
        UPDATE %I.part_config
        SET retention = %L,
            retention_keep_table = false,
            retention_keep_index = true,
            automatic_maintenance = 'on'
        WHERE parent_table = %L
        $Q$,
        pgmq._get_pg_partman_schema(), retention_interval, 'pgmq.' || qtable
    );

    EXECUTE FORMAT(
        $Q$
        INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged)
        VALUES (%L, true, false)
        ON CONFLICT DO NOTHING
        $Q$,
        queue_name
    );

    IF partition_col = 'enqueued_at' THEN
        a_partition_col := 'archived_at';
    ELSE
        a_partition_col := partition_col;
    END IF;

    EXECUTE FORMAT(
        $Q$
        CREATE TABLE IF NOT EXISTS pgmq.%I (
            msg_id       BIGINT NOT NULL,
            read_ct      INT DEFAULT 0 NOT NULL,
            enqueued_at  TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            last_read_at TIMESTAMP WITH TIME ZONE,
            archived_at  TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            vt           TIMESTAMP WITH TIME ZONE NOT NULL,
            message      JSONB,
            headers      JSONB
        ) PARTITION BY RANGE (%I)
        $Q$,
        atable, a_partition_col
    );

    EXECUTE FORMAT(
        $Q$
        SELECT %I.create_parent(
            p_parent_table := %L,
            p_control      := %L,
            p_interval     := %L,
            p_type         := CASE
                WHEN pgmq._get_pg_partman_major_version() = 5 THEN 'range'
                ELSE 'native'
            END
        )
        $Q$,
        pgmq._get_pg_partman_schema(), fq_atable, a_partition_col, partition_interval
    );

    EXECUTE FORMAT(
        $Q$
        UPDATE %I.part_config
        SET retention = %L,
            retention_keep_table = false,
            retention_keep_index = false,
            infinite_time_partitions = true
        WHERE parent_table = %L
        $Q$,
        pgmq._get_pg_partman_schema(), retention_interval, 'pgmq.' || atable
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at) $Q$,
        'archived_at_idx_' || queue_name, atable
    );
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.create(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM pgmq.create_non_partitioned(queue_name);
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.create_fifo(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
    atable TEXT := pgmq.format_table_name(queue_name, 'a');
BEGIN
    PERFORM pgmq.validate_queue_name(queue_name);
    PERFORM pgmq.acquire_queue_lock(queue_name);

    -- Queue table with FIFO-specific columns
    EXECUTE FORMAT(
        $Q$
        CREATE TABLE IF NOT EXISTS pgmq.%I (
            msg_id             BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
            read_ct            INT DEFAULT 0 NOT NULL,
            enqueued_at        TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            last_read_at       TIMESTAMP WITH TIME ZONE,
            vt                 TIMESTAMP WITH TIME ZONE NOT NULL,
            message            JSONB,
            headers            JSONB,
            message_group_id   TEXT NOT NULL,
            deduplication_id   TEXT
        )
        $Q$,
        qtable
    );

    -- Archive table: message_group_id nullable so standard archive() function remains usable
    EXECUTE FORMAT(
        $Q$
        CREATE TABLE IF NOT EXISTS pgmq.%I (
            msg_id             BIGINT PRIMARY KEY,
            read_ct            INT DEFAULT 0 NOT NULL,
            enqueued_at        TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            last_read_at       TIMESTAMP WITH TIME ZONE,
            archived_at        TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
            vt                 TIMESTAMP WITH TIME ZONE NOT NULL,
            message            JSONB,
            headers            JSONB,
            message_group_id   TEXT,
            deduplication_id   TEXT
        )
        $Q$,
        atable
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (vt ASC) $Q$,
        qtable || '_vt_idx', qtable
    );

    -- (message_group_id, msg_id): within-group read order and cte scan
    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (message_group_id, msg_id ASC) $Q$,
        qtable || '_grp_idx', qtable
    );

    -- (message_group_id, vt, msg_id): covering index for eligible_group aggregate
    -- BOOL_AND(vt <= now) GROUP BY message_group_id ORDER BY MIN(msg_id) index-only scan
    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (message_group_id, vt ASC, msg_id ASC) $Q$,
        qtable || '_grpvt_idx', qtable
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON pgmq.%I (archived_at) $Q$,
        'archived_at_idx_' || queue_name, atable
    );

    EXECUTE FORMAT(
        $Q$
        INSERT INTO pgmq.meta (queue_name, is_partitioned, is_unlogged, queue_type)
        VALUES (%L, false, false, 'fifo')
        ON CONFLICT DO NOTHING
        $Q$,
        queue_name
    );
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.drop_queue(queue_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    qtable      TEXT := pgmq.format_table_name(queue_name, 'q');
    atable      TEXT := pgmq.format_table_name(queue_name, 'a');
    partitioned BOOLEAN;
BEGIN
    PERFORM pgmq.acquire_queue_lock(queue_name);
    EXECUTE FORMAT(
        $Q$ SELECT is_partitioned FROM pgmq.meta WHERE queue_name = %L $Q$,
        queue_name
    ) INTO partitioned;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = qtable AND table_schema = 'pgmq'
    ) THEN
        RAISE NOTICE 'queue queue `%` does not exist', queue_name;
        RETURN FALSE;
    END IF;

    EXECUTE FORMAT($Q$ DROP TABLE IF EXISTS pgmq.%I $Q$, qtable);
    EXECUTE FORMAT($Q$ DROP TABLE IF EXISTS pgmq.%I $Q$, atable);

    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'meta' AND table_schema = 'pgmq'
    ) THEN
        EXECUTE FORMAT(
            $Q$ DELETE FROM pgmq.meta WHERE queue_name = %L $Q$,
            queue_name
        );
    END IF;

    IF partitioned THEN
        EXECUTE FORMAT(
            $Q$ DELETE FROM %I.part_config WHERE parent_table IN (%L, %L) $Q$,
            pgmq._get_pg_partman_schema(),
            'pgmq.' || qtable,
            'pgmq.' || atable
        );
    END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- Introspection
------------------------------------------------------------

CREATE FUNCTION pgmq.list_queues()
RETURNS SETOF pgmq.queue_record AS $$
BEGIN
    RETURN QUERY SELECT * FROM pgmq.meta;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.metrics(queue_name TEXT)
RETURNS pgmq.metrics_result AS $$
DECLARE
    result_row pgmq.metrics_result;
    qtable     TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    EXECUTE FORMAT(
        $Q$
        WITH q AS (
            SELECT
                count(*) AS queue_length,
                count(CASE WHEN vt <= NOW() THEN 1 END) AS queue_visible_length,
                EXTRACT(epoch FROM (NOW() - max(enqueued_at)))::int AS newest_msg_age_sec,
                EXTRACT(epoch FROM (NOW() - min(enqueued_at)))::int AS oldest_msg_age_sec,
                NOW() AS scrape_time
            FROM pgmq.%I
        ),
        seq AS (
            SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS total_messages
            FROM pgmq.%I
        )
        SELECT %L, q.queue_length, q.newest_msg_age_sec, q.oldest_msg_age_sec,
               seq.total_messages, q.scrape_time, q.queue_visible_length
        FROM q, seq
        $Q$,
        qtable, qtable || '_msg_id_seq', queue_name
    ) INTO result_row;
    RETURN result_row;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.metrics_all()
RETURNS SETOF pgmq.metrics_result AS $$
DECLARE
    row_name   RECORD;
    result_row pgmq.metrics_result;
BEGIN
    FOR row_name IN SELECT queue_name FROM pgmq.meta LOOP
        result_row := pgmq.metrics(row_name.queue_name);
        RETURN NEXT result_row;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.purge_queue(queue_name TEXT)
RETURNS BIGINT AS $$
DECLARE
    deleted_count INTEGER;
    qtable        TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    EXECUTE format('SELECT count(*) FROM pgmq.%I', qtable) INTO deleted_count;
    EXECUTE format('TRUNCATE TABLE pgmq.%I', qtable);
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- FIFO indexes
------------------------------------------------------------

CREATE FUNCTION pgmq._create_fifo_index_if_not_exists(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable     TEXT := pgmq.format_table_name(queue_name, 'q');
    index_name TEXT := qtable || '_fifo_idx';
BEGIN
    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON pgmq.%I USING GIN (headers) $Q$,
        index_name, qtable
    );
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.create_fifo_index(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM pgmq._create_fifo_index_if_not_exists(queue_name);
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.create_fifo_indexes_all()
RETURNS void AS $$
DECLARE
    q RECORD;
BEGIN
    FOR q IN SELECT queue_name FROM pgmq.meta LOOP
        PERFORM pgmq.create_fifo_index(q.queue_name);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- FIFO grouped reads (not hot-pathed into pgrx in v1)
------------------------------------------------------------

CREATE FUNCTION pgmq.read_grouped_rr(queue_name TEXT, vt INTEGER, qty INTEGER)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH fifo_groups AS (
            SELECT COALESCE(headers->>'x-pgmq-group', '_default_fifo_group') AS fifo_key,
                   MIN(msg_id) AS head_msg_id
            FROM pgmq.%1$I
            GROUP BY COALESCE(headers->>'x-pgmq-group', '_default_fifo_group')
        ),
        eligible_groups AS (
            SELECT g.fifo_key, g.head_msg_id,
                   ROW_NUMBER() OVER (ORDER BY g.head_msg_id) AS group_priority
            FROM fifo_groups g
            JOIN pgmq.%2$I h ON h.msg_id = g.head_msg_id
            WHERE h.vt <= clock_timestamp()
              AND pg_try_advisory_xact_lock(pg_catalog.hashtextextended(g.fifo_key, 0))
        ),
        available_messages AS (
            SELECT m.msg_id, eg.group_priority,
                   ROW_NUMBER() OVER (PARTITION BY eg.fifo_key ORDER BY m.msg_id) AS msg_rank_in_group
            FROM pgmq.%3$I m
            JOIN eligible_groups eg
              ON COALESCE(m.headers->>'x-pgmq-group', '_default_fifo_group') = eg.fifo_key
            WHERE m.vt <= clock_timestamp() AND m.msg_id >= eg.head_msg_id
        ),
        ordered_messages AS (
            SELECT msg_id, ROW_NUMBER() OVER (ORDER BY msg_rank_in_group, group_priority) AS selection_order
            FROM available_messages
        ),
        selected_messages AS (
            SELECT om.msg_id, om.selection_order
            FROM ordered_messages om
            JOIN pgmq.%4$I m ON m.msg_id = om.msg_id
            WHERE om.selection_order <= $1
            ORDER BY om.selection_order
            FOR UPDATE OF m SKIP LOCKED
        ),
        updated_messages AS (
            UPDATE pgmq.%5$I m
            SET vt = clock_timestamp() + %6$L, read_ct = read_ct + 1, last_read_at = clock_timestamp()
            FROM selected_messages sm
            WHERE m.msg_id = sm.msg_id AND m.vt <= clock_timestamp()
            RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers, sm.selection_order
        )
        SELECT msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers
        FROM updated_messages ORDER BY selection_order
        $QUERY$,
        qtable, qtable, qtable, qtable, qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.read_grouped_rr_with_poll(
    queue_name      TEXT,
    vt              INTEGER,
    qty             INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    r       pgmq.message_record;
    stop_at TIMESTAMP;
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
    LOOP
        IF (SELECT clock_timestamp() >= stop_at) THEN RETURN; END IF;
        FOR r IN SELECT * FROM pgmq.read_grouped_rr(queue_name, vt, qty) LOOP
            RETURN NEXT r;
        END LOOP;
        IF FOUND THEN RETURN;
        ELSE PERFORM pg_sleep(poll_interval_ms::numeric / 1000); END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.read_grouped_head(queue_name TEXT, vt INTEGER, qty INTEGER)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH fifo_groups AS (
            SELECT COALESCE(headers->>'x-pgmq-group', '_default_fifo_group') AS fifo_key,
                   MIN(msg_id) AS head_msg_id
            FROM pgmq.%1$I
            GROUP BY COALESCE(headers->>'x-pgmq-group', '_default_fifo_group')
        ),
        selected_messages AS (
            SELECT g.head_msg_id AS msg_id
            FROM fifo_groups g
            JOIN pgmq.%1$I q ON q.msg_id = g.head_msg_id
            WHERE q.vt <= clock_timestamp()
            ORDER BY q.msg_id
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.%1$I m
        SET vt = clock_timestamp() + %2$L, read_ct = read_ct + 1, last_read_at = clock_timestamp()
        FROM selected_messages sm
        WHERE m.msg_id = sm.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers
        $QUERY$,
        qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.read_grouped(queue_name TEXT, vt INTEGER, qty INTEGER)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH fifo_groups AS (
            SELECT COALESCE(headers->>'x-pgmq-group', '_default_fifo_group') AS fifo_key,
                   MIN(msg_id) AS min_msg_id
            FROM pgmq.%I WHERE vt <= clock_timestamp()
            GROUP BY COALESCE(headers->>'x-pgmq-group', '_default_fifo_group')
        ),
        locked_groups AS (
            SELECT m.msg_id, fg.fifo_key
            FROM pgmq.%I m
            INNER JOIN fifo_groups fg
              ON COALESCE(m.headers->>'x-pgmq-group', '_default_fifo_group') = fg.fifo_key
              AND m.msg_id = fg.min_msg_id
            WHERE m.vt <= clock_timestamp()
            ORDER BY m.msg_id ASC FOR UPDATE SKIP LOCKED
        ),
        group_priorities AS (
            SELECT fifo_key, msg_id AS min_msg_id,
                   ROW_NUMBER() OVER (ORDER BY msg_id) AS group_priority
            FROM locked_groups
        ),
        filtered_groups AS (
            SELECT * FROM group_priorities gp
            WHERE NOT EXISTS (
                SELECT 1 FROM pgmq.%I m2
                WHERE COALESCE(m2.headers->>'x-pgmq-group', '_default_fifo_group') = gp.fifo_key
                  AND m2.vt > clock_timestamp() AND m2.msg_id < gp.min_msg_id
            )
        ),
        available_messages AS (
            SELECT gp.fifo_key, t.msg_id, gp.group_priority,
                   ROW_NUMBER() OVER (PARTITION BY gp.fifo_key ORDER BY t.msg_id) AS msg_rank_in_group
            FROM filtered_groups gp
            CROSS JOIN LATERAL (
                SELECT * FROM pgmq.%I t
                WHERE COALESCE(t.headers->>'x-pgmq-group', '_default_fifo_group') = gp.fifo_key
                  AND t.vt <= clock_timestamp()
                ORDER BY msg_id LIMIT $1
            ) t ORDER BY gp.group_priority
        ),
        batch_selection AS (
            SELECT msg_id, ROW_NUMBER() OVER (ORDER BY group_priority, msg_rank_in_group) AS overall_rank
            FROM available_messages
        ),
        selected_messages AS (
            SELECT msg_id FROM batch_selection WHERE overall_rank <= $1
            ORDER BY msg_id FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.%I m
        SET vt = clock_timestamp() + %L, read_ct = read_ct + 1, last_read_at = clock_timestamp()
        FROM selected_messages sm
        WHERE m.msg_id = sm.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers
        $QUERY$,
        qtable, qtable, qtable, qtable, qtable, make_interval(secs => vt)
    );
    RETURN QUERY EXECUTE sql USING qty;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.read_grouped_with_poll(
    queue_name       TEXT,
    vt               INTEGER,
    qty              INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100
)
RETURNS SETOF pgmq.message_record AS $$
DECLARE
    r       pgmq.message_record;
    stop_at TIMESTAMP;
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
    LOOP
        IF (SELECT clock_timestamp() >= stop_at) THEN RETURN; END IF;
        FOR r IN SELECT * FROM pgmq.read_grouped(queue_name, vt, qty) LOOP
            RETURN NEXT r;
        END LOOP;
        IF FOUND THEN RETURN;
        ELSE PERFORM pg_sleep(poll_interval_ms::numeric / 1000); END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- FIFO queue type: send and read with per-group ordering
------------------------------------------------------------

-- PL/pgSQL fallback — overridden by pgrx C hot path (send_fifo_full_wrapper) when
-- load_pgrx_extension.sql is applied. The C version fires a WaitLatch XactCallback
-- after commit so read_fifo_with_poll readers wake immediately; this fallback uses
-- pg_notify (LISTEN consumers only) and does not wake WaitLatch-based readers.
CREATE FUNCTION pgmq.send_fifo(
    queue_name       TEXT,
    msg              JSONB,
    message_group_id TEXT,
    deduplication_id TEXT,
    headers          JSONB,
    delay            TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $Q$
        INSERT INTO pgmq.%I (vt, message, headers, message_group_id, deduplication_id)
        VALUES ($2, $1, $3, $4, $5)
        RETURNING msg_id;
        $Q$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING msg, delay, headers, message_group_id, deduplication_id;
    PERFORM pg_notify('queue_' || queue_name, '');
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgmq.send_fifo(queue_name TEXT, msg JSONB, message_group_id TEXT)
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM pgmq.send_fifo(queue_name, msg, message_group_id, NULL, NULL, clock_timestamp());
$$;

CREATE FUNCTION pgmq.send_fifo(queue_name TEXT, msg JSONB, message_group_id TEXT, delay INTEGER)
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM pgmq.send_fifo(queue_name, msg, message_group_id, NULL, NULL,
        clock_timestamp() + make_interval(secs => delay));
$$;

CREATE FUNCTION pgmq.send_fifo(
    queue_name TEXT, msg JSONB, message_group_id TEXT, headers JSONB
) RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM pgmq.send_fifo(queue_name, msg, message_group_id, NULL, headers, clock_timestamp());
$$;

CREATE FUNCTION pgmq.send_fifo(
    queue_name TEXT, msg JSONB, message_group_id TEXT,
    deduplication_id TEXT, headers JSONB, delay INTEGER
) RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM pgmq.send_fifo(queue_name, msg, message_group_id, deduplication_id, headers,
        clock_timestamp() + make_interval(secs => delay));
$$;

-- FIFO read: delivers up to qty messages from the eligible group with the oldest
-- pending message. A group is eligible when it has visible messages (vt <= now)
-- AND no in-flight messages (vt > now). Within the group, messages are returned
-- in msg_id ASC order (FIFO). FOR UPDATE SKIP LOCKED prevents two consumers from
-- grabbing the same group simultaneously.
--
-- qty and vt are embedded as literals (not params) to avoid generic plans that
-- degrade SKIP LOCKED throughput. ORDER BY msg_id ASC is intentional here — FIFO
-- ordering within a group is the whole point.
CREATE OR REPLACE FUNCTION pgmq.read_fifo(
    queue_name  TEXT,
    vt          INTEGER,
    qty         INTEGER
) RETURNS SETOF pgmq.message_record AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    RETURN QUERY EXECUTE format(
        $Q$
        WITH eligible_group AS MATERIALIZED (
            -- BOOL_AND(vt <= now) is equivalent to NOT EXISTS(vt > now) but avoids
            -- the correlated subplan that would run once per row (O(n²) on large queues).
            -- Uses the _grpvt_idx covering index for an index-only scan.
            SELECT message_group_id
            FROM pgmq.%1$I
            GROUP BY message_group_id
            HAVING BOOL_AND(vt <= clock_timestamp())
            ORDER BY MIN(msg_id) ASC
            LIMIT 1
        ),
        cte AS (
            SELECT m.msg_id
            FROM pgmq.%1$I m
            WHERE m.message_group_id = (SELECT message_group_id FROM eligible_group)
              AND m.vt <= clock_timestamp()
            ORDER BY m.msg_id ASC
            LIMIT %2$s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgmq.%1$I m
        SET last_read_at = clock_timestamp(),
            vt           = clock_timestamp() + make_interval(secs => %3$s),
            read_ct      = read_ct + 1
        FROM cte WHERE m.msg_id = cte.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers
        $Q$,
        qtable, qty, vt
    );
END;
$$ LANGUAGE plpgsql;

-- PL/pgSQL fallback — overridden by pgrx C hot path (read_fifo_with_poll_fn_wrapper)
-- when load_pgrx_extension.sql is applied. The C version uses WaitLatch + shared-memory
-- waiter registry for push-based wakeup; this fallback polls with pg_sleep.
CREATE OR REPLACE FUNCTION pgmq.read_fifo_with_poll(
    queue_name       TEXT,
    vt               INTEGER,
    qty              INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100
) RETURNS SETOF pgmq.message_record AS $$
DECLARE
    r       pgmq.message_record;
    stop_at TIMESTAMP;
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
    LOOP
        FOR r IN SELECT * FROM pgmq.read_fifo(queue_name, vt, qty) LOOP
            RETURN NEXT r;
        END LOOP;
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

------------------------------------------------------------
-- Notification system
------------------------------------------------------------

CREATE OR REPLACE FUNCTION pgmq.notify_queue_listeners()
RETURNS TRIGGER AS $$
DECLARE
    queue_name_extracted TEXT;
    updated_count        INTEGER;
BEGIN
    queue_name_extracted := substring(TG_TABLE_NAME FROM 3);

    UPDATE pgmq.notify_insert_throttle
    SET last_notified_at = clock_timestamp()
    WHERE queue_name = queue_name_extracted
      AND (
          throttle_interval_ms = 0
          OR clock_timestamp() - last_notified_at >= (throttle_interval_ms * INTERVAL '1 millisecond')
      );

    GET DIAGNOSTICS updated_count = ROW_COUNT;

    IF updated_count > 0 THEN
        PERFORM PG_NOTIFY('queue.' || TG_TABLE_NAME || '.' || TG_OP, NULL);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.enable_notify_insert(queue_name TEXT, throttle_interval_ms INTEGER DEFAULT 250)
RETURNS void AS $$
DECLARE
    qtable               TEXT := pgmq.format_table_name(queue_name, 'q');
    v_queue_name         TEXT := queue_name;
    v_throttle_interval  INTEGER := throttle_interval_ms;
BEGIN
    IF v_throttle_interval < 0 THEN
        RAISE EXCEPTION 'throttle_interval_ms must be non-negative';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'pgmq' AND table_name = qtable) THEN
        RAISE EXCEPTION 'Queue "%" does not exist. Create it first using pgmq.create()', v_queue_name;
    END IF;

    PERFORM pgmq.disable_notify_insert(v_queue_name);

    INSERT INTO pgmq.notify_insert_throttle (queue_name, throttle_interval_ms)
    VALUES (v_queue_name, v_throttle_interval)
    ON CONFLICT ON CONSTRAINT notify_insert_throttle_queue_name_key
        DO UPDATE SET throttle_interval_ms = EXCLUDED.throttle_interval_ms,
                      last_notified_at = to_timestamp(0);

    EXECUTE FORMAT(
        $Q$
        CREATE CONSTRAINT TRIGGER trigger_notify_queue_insert_listeners
        AFTER INSERT ON pgmq.%I
        DEFERRABLE FOR EACH ROW
        EXECUTE PROCEDURE pgmq.notify_queue_listeners()
        $Q$,
        qtable
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.disable_notify_insert(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable TEXT := pgmq.format_table_name(queue_name, 'q');
BEGIN
    EXECUTE FORMAT(
        $Q$ DROP TRIGGER IF EXISTS trigger_notify_queue_insert_listeners ON pgmq.%I $Q$,
        qtable
    );
    DELETE FROM pgmq.notify_insert_throttle nit WHERE nit.queue_name = disable_notify_insert.queue_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION pgmq.list_notify_insert_throttles()
RETURNS TABLE (queue_name text, throttle_interval_ms integer, last_notified_at TIMESTAMP WITH TIME ZONE)
LANGUAGE sql STABLE AS $$
    SELECT queue_name, throttle_interval_ms, last_notified_at
    FROM pgmq.notify_insert_throttle ORDER BY queue_name;
$$;

CREATE OR REPLACE FUNCTION pgmq.update_notify_insert(queue_name text, throttle_interval_ms integer)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF throttle_interval_ms < 0 THEN
        RAISE EXCEPTION 'throttle_interval_ms must be non-negative, got: %', throttle_interval_ms;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pgmq.meta WHERE meta.queue_name = update_notify_insert.queue_name) THEN
        RAISE EXCEPTION 'Queue "%" does not exist', queue_name;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pgmq.notify_insert_throttle WHERE notify_insert_throttle.queue_name = update_notify_insert.queue_name) THEN
        RAISE EXCEPTION 'Queue "%" does not have notify_insert enabled', queue_name;
    END IF;
    UPDATE pgmq.notify_insert_throttle
    SET throttle_interval_ms = update_notify_insert.throttle_interval_ms, last_notified_at = to_timestamp(0)
    WHERE notify_insert_throttle.queue_name = update_notify_insert.queue_name;
END;
$$;

------------------------------------------------------------
-- Topic routing
------------------------------------------------------------

CREATE OR REPLACE FUNCTION pgmq.validate_routing_key(routing_key text)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF routing_key IS NULL OR routing_key = '' THEN
        RAISE EXCEPTION 'routing_key cannot be NULL or empty';
    END IF;
    IF length(routing_key) > 255 THEN
        RAISE EXCEPTION 'routing_key length cannot exceed 255 characters';
    END IF;
    IF routing_key !~ '^[a-zA-Z0-9._-]+$' THEN
        RAISE EXCEPTION 'routing_key contains invalid characters. Got: %', routing_key;
    END IF;
    IF routing_key ~ '^\.' THEN RAISE EXCEPTION 'routing_key cannot start with a dot'; END IF;
    IF routing_key ~ '\.$' THEN RAISE EXCEPTION 'routing_key cannot end with a dot'; END IF;
    IF routing_key ~ '\.\.' THEN RAISE EXCEPTION 'routing_key cannot contain consecutive dots'; END IF;
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.validate_topic_pattern(pattern text)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF pattern IS NULL OR pattern = '' THEN
        RAISE EXCEPTION 'pattern cannot be NULL or empty';
    END IF;
    IF length(pattern) > 255 THEN
        RAISE EXCEPTION 'pattern length cannot exceed 255 characters';
    END IF;
    IF pattern !~ '^[a-zA-Z0-9._\-*#]+$' THEN
        RAISE EXCEPTION 'pattern contains invalid characters. Got: %', pattern;
    END IF;
    IF pattern ~ '^\.' THEN RAISE EXCEPTION 'pattern cannot start with a dot'; END IF;
    IF pattern ~ '\.$' THEN RAISE EXCEPTION 'pattern cannot end with a dot'; END IF;
    IF pattern ~ '\.\.' THEN RAISE EXCEPTION 'pattern cannot contain consecutive dots'; END IF;
    IF pattern ~ '\*\*' THEN RAISE EXCEPTION 'pattern cannot contain consecutive stars (**).'; END IF;
    IF pattern ~ '##' THEN RAISE EXCEPTION 'pattern cannot contain consecutive hashes (##).'; END IF;
    IF pattern ~ '\*#' OR pattern ~ '#\*' THEN
        RAISE EXCEPTION 'pattern cannot contain adjacent wildcards (*# or #*).';
    END IF;
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.bind_topic(pattern text, queue_name text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pgmq.validate_topic_pattern(pattern);
    IF queue_name IS NULL OR queue_name = '' THEN
        RAISE EXCEPTION 'queue_name cannot be NULL or empty';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pgmq.meta WHERE meta.queue_name = bind_topic.queue_name) THEN
        RAISE EXCEPTION 'Queue "%" does not exist', queue_name;
    END IF;
    INSERT INTO pgmq.topic_bindings (pattern, queue_name)
    VALUES (pattern, queue_name)
    ON CONFLICT ON CONSTRAINT topic_bindings_unique DO NOTHING;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.unbind_topic(pattern text, queue_name text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    rows_deleted integer;
BEGIN
    IF pattern IS NULL OR pattern = '' THEN RAISE EXCEPTION 'pattern cannot be NULL or empty'; END IF;
    IF queue_name IS NULL OR queue_name = '' THEN RAISE EXCEPTION 'queue_name cannot be NULL or empty'; END IF;
    DELETE FROM pgmq.topic_bindings
    WHERE topic_bindings.pattern = unbind_topic.pattern
      AND topic_bindings.queue_name = unbind_topic.queue_name;
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;
    RETURN rows_deleted > 0;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.test_routing(routing_key text)
RETURNS TABLE (pattern text, queue_name text, compiled_regex text)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    PERFORM pgmq.validate_routing_key(routing_key);
    RETURN QUERY
        SELECT b.pattern, b.queue_name, b.compiled_regex
        FROM pgmq.topic_bindings b
        WHERE routing_key ~ b.compiled_regex
        ORDER BY b.pattern;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.send_topic(routing_key text, msg jsonb, headers jsonb, delay integer)
RETURNS integer LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    b             RECORD;
    matched_count integer := 0;
BEGIN
    PERFORM pgmq.validate_routing_key(routing_key);
    IF msg IS NULL THEN RAISE EXCEPTION 'msg cannot be NULL'; END IF;
    IF delay < 0 THEN RAISE EXCEPTION 'delay cannot be negative, got: %', delay; END IF;
    FOR b IN
        SELECT DISTINCT tb.queue_name
        FROM pgmq.topic_bindings tb
        WHERE routing_key ~ tb.compiled_regex
        ORDER BY tb.queue_name
    LOOP
        PERFORM pgmq.send(b.queue_name, msg, headers, delay);
        matched_count := matched_count + 1;
    END LOOP;
    RETURN matched_count;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.send_topic(routing_key text, msg jsonb)
RETURNS integer LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    RETURN pgmq.send_topic(routing_key, msg, NULL, 0);
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.send_topic(routing_key text, msg jsonb, delay integer)
RETURNS integer LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    RETURN pgmq.send_topic(routing_key, msg, NULL, delay);
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.list_topic_bindings()
RETURNS TABLE (pattern text, queue_name text, bound_at TIMESTAMP WITH TIME ZONE, compiled_regex text)
LANGUAGE sql STABLE AS $$
    SELECT pattern, queue_name, bound_at, compiled_regex
    FROM pgmq.topic_bindings
    ORDER BY bound_at DESC, pattern, queue_name;
$$;

CREATE OR REPLACE FUNCTION pgmq.list_topic_bindings(queue_name text)
RETURNS TABLE (pattern text, queue_name text, bound_at TIMESTAMP WITH TIME ZONE, compiled_regex text)
LANGUAGE sql STABLE AS $$
    SELECT tb.pattern, tb.queue_name, tb.bound_at, tb.compiled_regex
    FROM pgmq.topic_bindings tb
    WHERE tb.queue_name = list_topic_bindings.queue_name
    ORDER BY bound_at DESC, pattern;
$$;

CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(
    routing_key text, msgs jsonb[], headers jsonb[], delay TIMESTAMP WITH TIME ZONE
)
RETURNS TABLE (queue_name text, msg_id bigint)
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    b RECORD;
BEGIN
    PERFORM pgmq.validate_routing_key(routing_key);
    PERFORM pgmq._validate_batch_params(msgs, headers);
    FOR b IN
        SELECT DISTINCT tb.queue_name FROM pgmq.topic_bindings tb
        WHERE routing_key ~ tb.compiled_regex ORDER BY tb.queue_name
    LOOP
        RETURN QUERY
        SELECT b.queue_name, batch_result.msg_id
        FROM pgmq._send_batch(b.queue_name, msgs, headers, delay) AS batch_result(msg_id);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(routing_key text, msgs jsonb[])
RETURNS TABLE (queue_name text, msg_id bigint) LANGUAGE sql VOLATILE AS $$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, NULL, clock_timestamp());
$$;

CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(routing_key text, msgs jsonb[], headers jsonb[])
RETURNS TABLE (queue_name text, msg_id bigint) LANGUAGE sql VOLATILE AS $$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, headers, clock_timestamp());
$$;

CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(routing_key text, msgs jsonb[], delay integer)
RETURNS TABLE (queue_name text, msg_id bigint) LANGUAGE sql VOLATILE AS $$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, NULL, clock_timestamp() + make_interval(secs => delay));
$$;

CREATE OR REPLACE FUNCTION pgmq.send_batch_topic(routing_key text, msgs jsonb[], headers jsonb[], delay integer)
RETURNS TABLE (queue_name text, msg_id bigint) LANGUAGE sql VOLATILE AS $$
    SELECT * FROM pgmq.send_batch_topic(routing_key, msgs, headers, clock_timestamp() + make_interval(secs => delay));
$$;

------------------------------------------------------------
-- Grant pg_monitor read access
------------------------------------------------------------

GRANT USAGE ON SCHEMA pgmq TO pg_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA pgmq TO pg_monitor;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA pgmq TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgmq GRANT SELECT ON TABLES TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA pgmq GRANT SELECT ON SEQUENCES TO pg_monitor;
