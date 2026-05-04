-- beyond-queue schema
-- Tables, types, indexes, and non-hot-path functions.
-- Hot path functions (send, send_batch, receive, receive_fifo, delete, archive,
-- pop, change_visibility) are implemented in Rust via pgrx and override these declarations.

CREATE SCHEMA IF NOT EXISTS queue;

-- Queue registry
CREATE TABLE IF NOT EXISTS queue.meta (
    queue_name   VARCHAR UNIQUE NOT NULL,
    is_partitioned BOOLEAN NOT NULL,
    is_unlogged  BOOLEAN NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    queue_type   TEXT NOT NULL DEFAULT 'standard'
);

-- Notification throttle state (UNLOGGED — survives restarts only)
CREATE UNLOGGED TABLE IF NOT EXISTS queue.notify_insert_throttle (
    queue_name           VARCHAR UNIQUE NOT NULL
        CONSTRAINT notify_insert_throttle_meta_fk
            REFERENCES queue.meta (queue_name) ON DELETE CASCADE,
    throttle_interval_ms INTEGER NOT NULL DEFAULT 0,
    last_notified_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT to_timestamp(0)
);

CREATE INDEX IF NOT EXISTS idx_notify_throttle_active
    ON queue.notify_insert_throttle (queue_name, last_notified_at)
    WHERE throttle_interval_ms > 0;

-- Topic binding registry (wildcard routing key → queue or HTTP endpoint)
CREATE TABLE IF NOT EXISTS queue.topic_subscriptions (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pattern        text NOT NULL,
    protocol       text NOT NULL DEFAULT 'sqs',  -- 'sqs' | 'http' | 'https'
    endpoint       text NOT NULL,                -- sqs://{queue_name} for sqs; full URL for http/https
    queue_name     text
        CONSTRAINT topic_subscriptions_meta_fk
            REFERENCES queue.meta (queue_name) ON DELETE CASCADE,
    bound_at       TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
    raw_delivery   boolean NOT NULL DEFAULT false, -- true = post raw payload; false = SNS envelope
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
    CONSTRAINT topic_subscriptions_unique UNIQUE (pattern, endpoint),
    CONSTRAINT topic_subscriptions_protocol_check CHECK (protocol IN ('sqs', 'http', 'https')),
    CONSTRAINT topic_subscriptions_sqs_queue_check CHECK (protocol != 'sqs' OR queue_name IS NOT NULL)
);


-- Pending HTTP/HTTPS deliveries with retry tracking
CREATE TABLE IF NOT EXISTS queue.http_deliveries (
    id              BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    subscription_id BIGINT NOT NULL
        REFERENCES queue.topic_subscriptions(id) ON DELETE CASCADE,
    endpoint        text NOT NULL,
    payload         jsonb NOT NULL,
    attempt         integer NOT NULL DEFAULT 0,
    max_attempts    integer NOT NULL DEFAULT 5,
    next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    last_error      text,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_http_deliveries_pending
    ON queue.http_deliveries (next_attempt_at ASC)
    WHERE attempt < max_attempts;

DO $$ BEGIN
    DROP INDEX IF EXISTS queue.idx_topic_subscriptions_covering;
END $$;

CREATE INDEX IF NOT EXISTS idx_topic_subscriptions_covering
    ON queue.topic_subscriptions (pattern)
    INCLUDE (id, queue_name, endpoint, protocol, raw_delivery, compiled_regex);

DO
$$
BEGIN
    IF EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'beyond_queue_extension') THEN
        PERFORM pg_catalog.pg_extension_config_dump('queue.meta', '');
        PERFORM pg_catalog.pg_extension_config_dump('queue.notify_insert_throttle', '');
        PERFORM pg_catalog.pg_extension_config_dump('queue.topic_subscriptions', '');
        PERFORM pg_catalog.pg_extension_config_dump('queue.http_deliveries', '');
    END IF;
END
$$;

-- Composite type returned by queue read/pop/archive operations
CREATE TYPE queue.message_record AS (
    msg_id      BIGINT,
    read_ct     INTEGER,
    enqueued_at TIMESTAMP WITH TIME ZONE,
    last_read_at TIMESTAMP WITH TIME ZONE,
    vt          TIMESTAMP WITH TIME ZONE,
    message     JSONB,
    headers     JSONB
);

CREATE TYPE queue.queue_record AS (
    queue_name     VARCHAR,
    is_partitioned BOOLEAN,
    is_unlogged    BOOLEAN,
    created_at     TIMESTAMP WITH TIME ZONE,
    queue_type     TEXT
);

CREATE TYPE queue.metrics_result AS (
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

CREATE FUNCTION queue.acquire_queue_lock(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('queue.queue_' || queue_name));
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.format_table_name(queue_name text, prefix text)
RETURNS TEXT AS $$
BEGIN
    IF queue_name !~ '^[a-z0-9_]+$'
    THEN
        RAISE EXCEPTION 'queue name contains invalid characters: must match [a-z0-9_]';
    END IF;
    RETURN lower(prefix || '_' || queue_name);
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.validate_queue_name(queue_name TEXT)
RETURNS void AS $$
BEGIN
    IF length(queue_name) > 48 THEN
        RAISE EXCEPTION 'queue name is too long, maximum length is 48 characters';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue._belongs_to_queue(table_name TEXT)
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

CREATE FUNCTION queue._get_pg_partman_schema()
RETURNS TEXT AS $$
    SELECT extnamespace::regnamespace::text
    FROM pg_extension
    WHERE extname = 'pg_partman';
$$ LANGUAGE SQL;

CREATE FUNCTION queue._extension_exists(extension_name TEXT)
RETURNS BOOLEAN
LANGUAGE SQL AS $$
    SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = extension_name)
$$;

CREATE FUNCTION queue._ensure_pg_partman_installed()
RETURNS void AS $$
BEGIN
    IF NOT queue._extension_exists('pg_partman') THEN
        RAISE EXCEPTION 'pg_partman is required for partitioned queues';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue._get_pg_partman_major_version()
RETURNS INT
LANGUAGE SQL AS $$
    SELECT split_part(extversion, '.', 1)::INT
    FROM pg_extension
    WHERE extname = 'pg_partman'
$$;

CREATE FUNCTION queue._get_partition_col(partition_interval TEXT)
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

CREATE FUNCTION queue.create_non_partitioned(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable TEXT := queue.format_table_name(queue_name, 'q');
    atable TEXT := queue.format_table_name(queue_name, 'a');
BEGIN
    PERFORM queue.validate_queue_name(queue_name);
    PERFORM queue.acquire_queue_lock(queue_name);

    EXECUTE FORMAT(
        $Q$
        CREATE TABLE IF NOT EXISTS queue.%I (
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
        CREATE TABLE IF NOT EXISTS queue.%I (
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
        $Q$ CREATE INDEX IF NOT EXISTS %I ON queue.%I (vt ASC) $Q$,
        qtable || '_vt_idx', qtable
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON queue.%I (archived_at) $Q$,
        'archived_at_idx_' || queue_name, atable
    );

    EXECUTE FORMAT(
        $Q$
        INSERT INTO queue.meta (queue_name, is_partitioned, is_unlogged)
        VALUES (%L, false, false)
        ON CONFLICT DO NOTHING
        $Q$,
        queue_name
    );
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.create_unlogged(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable TEXT := queue.format_table_name(queue_name, 'q');
    atable TEXT := queue.format_table_name(queue_name, 'a');
BEGIN
    PERFORM queue.validate_queue_name(queue_name);
    PERFORM queue.acquire_queue_lock(queue_name);

    EXECUTE FORMAT(
        $Q$
        CREATE UNLOGGED TABLE IF NOT EXISTS queue.%I (
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
        CREATE TABLE IF NOT EXISTS queue.%I (
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
        $Q$ CREATE INDEX IF NOT EXISTS %I ON queue.%I (vt ASC) $Q$,
        qtable || '_vt_idx', qtable
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON queue.%I (archived_at) $Q$,
        'archived_at_idx_' || queue_name, atable
    );

    EXECUTE FORMAT(
        $Q$
        INSERT INTO queue.meta (queue_name, is_partitioned, is_unlogged)
        VALUES (%L, false, true)
        ON CONFLICT DO NOTHING
        $Q$,
        queue_name
    );
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.create_partitioned(
    queue_name          TEXT,
    partition_interval  TEXT DEFAULT '10000',
    retention_interval  TEXT DEFAULT '100000'
)
RETURNS void AS $$
DECLARE
    partition_col   TEXT;
    a_partition_col TEXT;
    qtable          TEXT := queue.format_table_name(queue_name, 'q');
    atable          TEXT := queue.format_table_name(queue_name, 'a');
    fq_qtable       TEXT := 'queue.' || queue.format_table_name(queue_name, 'q');
    fq_atable       TEXT := 'queue.' || queue.format_table_name(queue_name, 'a');
BEGIN
    PERFORM queue.validate_queue_name(queue_name);
    PERFORM queue.acquire_queue_lock(queue_name);
    PERFORM queue._ensure_pg_partman_installed();
    SELECT queue._get_partition_col(partition_interval) INTO partition_col;

    EXECUTE FORMAT(
        $Q$
        CREATE TABLE IF NOT EXISTS queue.%I (
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
                WHEN queue._get_pg_partman_major_version() = 5 THEN 'range'
                ELSE 'native'
            END
        )
        $Q$,
        queue._get_pg_partman_schema(), fq_qtable, partition_col, partition_interval
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON queue.%I (%I) $Q$,
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
        queue._get_pg_partman_schema(), retention_interval, 'queue.' || qtable
    );

    EXECUTE FORMAT(
        $Q$
        INSERT INTO queue.meta (queue_name, is_partitioned, is_unlogged)
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
        CREATE TABLE IF NOT EXISTS queue.%I (
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
                WHEN queue._get_pg_partman_major_version() = 5 THEN 'range'
                ELSE 'native'
            END
        )
        $Q$,
        queue._get_pg_partman_schema(), fq_atable, a_partition_col, partition_interval
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
        queue._get_pg_partman_schema(), retention_interval, 'queue.' || atable
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON queue.%I (archived_at) $Q$,
        'archived_at_idx_' || queue_name, atable
    );
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.create(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM queue.create_non_partitioned(queue_name);
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.create_fifo(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable TEXT := queue.format_table_name(queue_name, 'q');
    atable TEXT := queue.format_table_name(queue_name, 'a');
BEGIN
    PERFORM queue.validate_queue_name(queue_name);
    PERFORM queue.acquire_queue_lock(queue_name);

    -- Queue table with FIFO-specific columns
    EXECUTE FORMAT(
        $Q$
        CREATE TABLE IF NOT EXISTS queue.%I (
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
        CREATE TABLE IF NOT EXISTS queue.%I (
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
        $Q$ CREATE INDEX IF NOT EXISTS %I ON queue.%I (vt ASC) $Q$,
        qtable || '_vt_idx', qtable
    );

    -- (message_group_id, msg_id): within-group read order and cte scan
    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON queue.%I (message_group_id, msg_id ASC) $Q$,
        qtable || '_grp_idx', qtable
    );

    -- (message_group_id, vt, msg_id): covering index for eligible_group aggregate
    -- BOOL_AND(vt <= now) GROUP BY message_group_id ORDER BY MIN(msg_id) index-only scan
    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON queue.%I (message_group_id, vt ASC, msg_id ASC) $Q$,
        qtable || '_grpvt_idx', qtable
    );

    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON queue.%I (archived_at) $Q$,
        'archived_at_idx_' || queue_name, atable
    );

    EXECUTE FORMAT(
        $Q$
        INSERT INTO queue.meta (queue_name, is_partitioned, is_unlogged, queue_type)
        VALUES (%L, false, false, 'fifo')
        ON CONFLICT DO NOTHING
        $Q$,
        queue_name
    );
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.delete_queue(queue_name TEXT)
RETURNS BOOLEAN AS $$
DECLARE
    qtable      TEXT := queue.format_table_name(queue_name, 'q');
    atable      TEXT := queue.format_table_name(queue_name, 'a');
    partitioned BOOLEAN;
BEGIN
    PERFORM queue.acquire_queue_lock(queue_name);
    EXECUTE FORMAT(
        $Q$ SELECT is_partitioned FROM queue.meta WHERE queue_name = %L $Q$,
        queue_name
    ) INTO partitioned;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = qtable AND table_schema = 'queue'
    ) THEN
        RAISE NOTICE 'queue queue `%` does not exist', queue_name;
        RETURN FALSE;
    END IF;

    EXECUTE FORMAT($Q$ DROP TABLE IF EXISTS queue.%I $Q$, qtable);
    EXECUTE FORMAT($Q$ DROP TABLE IF EXISTS queue.%I $Q$, atable);

    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'meta' AND table_schema = 'queue'
    ) THEN
        EXECUTE FORMAT(
            $Q$ DELETE FROM queue.meta WHERE queue_name = %L $Q$,
            queue_name
        );
    END IF;

    IF partitioned THEN
        EXECUTE FORMAT(
            $Q$ DELETE FROM %I.part_config WHERE parent_table IN (%L, %L) $Q$,
            queue._get_pg_partman_schema(),
            'queue.' || qtable,
            'queue.' || atable
        );
    END IF;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- Introspection
------------------------------------------------------------

CREATE FUNCTION queue.list_queues(prefix text DEFAULT NULL)
RETURNS SETOF queue.queue_record AS $$
BEGIN
    IF prefix IS NULL THEN
        RETURN QUERY SELECT * FROM queue.meta ORDER BY queue_name;
    ELSE
        RETURN QUERY SELECT * FROM queue.meta WHERE queue_name LIKE (prefix || '%') ORDER BY queue_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.metrics(queue_name TEXT)
RETURNS queue.metrics_result AS $$
DECLARE
    result_row queue.metrics_result;
    qtable     TEXT := queue.format_table_name(queue_name, 'q');
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
            FROM queue.%I
        ),
        seq AS (
            SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS total_messages
            FROM queue.%I
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

CREATE FUNCTION queue.metrics_all()
RETURNS SETOF queue.metrics_result AS $$
DECLARE
    row_name   RECORD;
    result_row queue.metrics_result;
BEGIN
    FOR row_name IN SELECT queue_name FROM queue.meta LOOP
        result_row := queue.metrics(row_name.queue_name);
        RETURN NEXT result_row;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.purge_queue(queue_name TEXT)
RETURNS BIGINT AS $$
DECLARE
    deleted_count INTEGER;
    qtable        TEXT := queue.format_table_name(queue_name, 'q');
BEGIN
    EXECUTE format('SELECT count(*) FROM queue.%I', qtable) INTO deleted_count;
    EXECUTE format('TRUNCATE TABLE queue.%I', qtable);
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- FIFO indexes
------------------------------------------------------------

CREATE FUNCTION queue._create_fifo_index_if_not_exists(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable     TEXT := queue.format_table_name(queue_name, 'q');
    index_name TEXT := qtable || '_fifo_idx';
BEGIN
    EXECUTE FORMAT(
        $Q$ CREATE INDEX IF NOT EXISTS %I ON queue.%I USING GIN (headers) $Q$,
        index_name, qtable
    );
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.create_fifo_index(queue_name TEXT)
RETURNS void AS $$
BEGIN
    PERFORM queue._create_fifo_index_if_not_exists(queue_name);
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.create_fifo_indexes_all()
RETURNS void AS $$
DECLARE
    q RECORD;
BEGIN
    FOR q IN SELECT queue_name FROM queue.meta LOOP
        PERFORM queue.create_fifo_index(q.queue_name);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------
-- FIFO grouped reads (not hot-pathed into pgrx in v1)
------------------------------------------------------------

CREATE FUNCTION queue.receive_grouped_rr(
    queue_name       TEXT,
    vt               INTEGER,
    qty              INTEGER,
    wait_secs        INTEGER DEFAULT 0,
    poll_interval_ms INTEGER DEFAULT 100
)
RETURNS SETOF queue.message_record AS $$
DECLARE
    sql     TEXT;
    qtable  TEXT := queue.format_table_name(queue_name, 'q');
    stop_at TIMESTAMP;
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH fifo_groups AS (
            SELECT COALESCE(headers->>'x-pgmq-group', '_default_fifo_group') AS fifo_key,
                   MIN(msg_id) AS head_msg_id
            FROM queue.%1$I
            GROUP BY COALESCE(headers->>'x-pgmq-group', '_default_fifo_group')
        ),
        eligible_groups AS (
            SELECT g.fifo_key, g.head_msg_id,
                   ROW_NUMBER() OVER (ORDER BY g.head_msg_id) AS group_priority
            FROM fifo_groups g
            JOIN queue.%2$I h ON h.msg_id = g.head_msg_id
            WHERE h.vt <= clock_timestamp()
              AND pg_try_advisory_xact_lock(pg_catalog.hashtextextended(g.fifo_key, 0))
        ),
        available_messages AS (
            SELECT m.msg_id, eg.group_priority,
                   ROW_NUMBER() OVER (PARTITION BY eg.fifo_key ORDER BY m.msg_id) AS msg_rank_in_group
            FROM queue.%3$I m
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
            JOIN queue.%4$I m ON m.msg_id = om.msg_id
            WHERE om.selection_order <= $1
            ORDER BY om.selection_order
            FOR UPDATE OF m SKIP LOCKED
        ),
        updated_messages AS (
            UPDATE queue.%5$I m
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
    stop_at := clock_timestamp() + make_interval(secs => wait_secs);
    LOOP
        RETURN QUERY EXECUTE sql USING qty;
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

CREATE FUNCTION queue.receive_grouped_head(
    queue_name       TEXT,
    vt               INTEGER,
    qty              INTEGER,
    wait_secs        INTEGER DEFAULT 0,
    poll_interval_ms INTEGER DEFAULT 100
)
RETURNS SETOF queue.message_record AS $$
DECLARE
    sql     TEXT;
    qtable  TEXT := queue.format_table_name(queue_name, 'q');
    stop_at TIMESTAMP;
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH fifo_groups AS (
            SELECT COALESCE(headers->>'x-pgmq-group', '_default_fifo_group') AS fifo_key,
                   MIN(msg_id) AS head_msg_id
            FROM queue.%1$I
            GROUP BY COALESCE(headers->>'x-pgmq-group', '_default_fifo_group')
        ),
        selected_messages AS (
            SELECT g.head_msg_id AS msg_id
            FROM fifo_groups g
            JOIN queue.%1$I q ON q.msg_id = g.head_msg_id
            WHERE q.vt <= clock_timestamp()
            ORDER BY q.msg_id
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        UPDATE queue.%1$I m
        SET vt = clock_timestamp() + %2$L, read_ct = read_ct + 1, last_read_at = clock_timestamp()
        FROM selected_messages sm
        WHERE m.msg_id = sm.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers
        $QUERY$,
        qtable, make_interval(secs => vt)
    );
    stop_at := clock_timestamp() + make_interval(secs => wait_secs);
    LOOP
        RETURN QUERY EXECUTE sql USING qty;
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

CREATE FUNCTION queue.receive_grouped(
    queue_name       TEXT,
    vt               INTEGER,
    qty              INTEGER,
    wait_secs        INTEGER DEFAULT 0,
    poll_interval_ms INTEGER DEFAULT 100
)
RETURNS SETOF queue.message_record AS $$
DECLARE
    sql     TEXT;
    qtable  TEXT := queue.format_table_name(queue_name, 'q');
    stop_at TIMESTAMP;
BEGIN
    sql := FORMAT(
        $QUERY$
        WITH fifo_groups AS (
            SELECT COALESCE(headers->>'x-pgmq-group', '_default_fifo_group') AS fifo_key,
                   MIN(msg_id) AS min_msg_id
            FROM queue.%I WHERE vt <= clock_timestamp()
            GROUP BY COALESCE(headers->>'x-pgmq-group', '_default_fifo_group')
        ),
        locked_groups AS (
            SELECT m.msg_id, fg.fifo_key
            FROM queue.%I m
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
                SELECT 1 FROM queue.%I m2
                WHERE COALESCE(m2.headers->>'x-pgmq-group', '_default_fifo_group') = gp.fifo_key
                  AND m2.vt > clock_timestamp() AND m2.msg_id < gp.min_msg_id
            )
        ),
        available_messages AS (
            SELECT gp.fifo_key, t.msg_id, gp.group_priority,
                   ROW_NUMBER() OVER (PARTITION BY gp.fifo_key ORDER BY t.msg_id) AS msg_rank_in_group
            FROM filtered_groups gp
            CROSS JOIN LATERAL (
                SELECT * FROM queue.%I t
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
        UPDATE queue.%I m
        SET vt = clock_timestamp() + %L, read_ct = read_ct + 1, last_read_at = clock_timestamp()
        FROM selected_messages sm
        WHERE m.msg_id = sm.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers
        $QUERY$,
        qtable, qtable, qtable, qtable, qtable, make_interval(secs => vt)
    );
    stop_at := clock_timestamp() + make_interval(secs => wait_secs);
    LOOP
        RETURN QUERY EXECUTE sql USING qty;
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
-- FIFO queue type: send and read with per-group ordering
------------------------------------------------------------

-- PL/pgSQL fallback — overridden by pgrx C hot path (send_fifo_full_wrapper) when
-- load_pgrx_extension.sql is applied. The C version fires a WaitLatch XactCallback
-- after commit so receive_fifo readers wake immediately; this fallback uses
-- pg_notify (LISTEN consumers only) and does not wake WaitLatch-based readers.
CREATE FUNCTION queue.send_fifo(
    queue_name       TEXT,
    msg              JSONB,
    message_group_id TEXT,
    deduplication_id TEXT,
    headers          JSONB,
    delay            TIMESTAMP WITH TIME ZONE
) RETURNS SETOF BIGINT AS $$
DECLARE
    sql    TEXT;
    qtable TEXT := queue.format_table_name(queue_name, 'q');
BEGIN
    sql := FORMAT(
        $Q$
        INSERT INTO queue.%I (vt, message, headers, message_group_id, deduplication_id)
        VALUES ($2, $1, $3, $4, $5)
        RETURNING msg_id;
        $Q$,
        qtable
    );
    RETURN QUERY EXECUTE sql USING msg, delay, headers, message_group_id, deduplication_id;
    PERFORM pg_notify('queue_' || queue_name, '');
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION queue.send_fifo(queue_name TEXT, msg JSONB, message_group_id TEXT)
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send_fifo(queue_name, msg, message_group_id, NULL, NULL, clock_timestamp());
$$;

CREATE FUNCTION queue.send_fifo(queue_name TEXT, msg JSONB, message_group_id TEXT, delay INTEGER)
RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send_fifo(queue_name, msg, message_group_id, NULL, NULL,
        clock_timestamp() + make_interval(secs => delay));
$$;

CREATE FUNCTION queue.send_fifo(
    queue_name TEXT, msg JSONB, message_group_id TEXT, headers JSONB
) RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send_fifo(queue_name, msg, message_group_id, NULL, headers, clock_timestamp());
$$;

CREATE FUNCTION queue.send_fifo(
    queue_name TEXT, msg JSONB, message_group_id TEXT,
    deduplication_id TEXT, headers JSONB, delay INTEGER
) RETURNS SETOF BIGINT LANGUAGE sql AS $$
    SELECT * FROM queue.send_fifo(queue_name, msg, message_group_id, deduplication_id, headers,
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
CREATE OR REPLACE FUNCTION queue.receive_fifo(
    queue_name  TEXT,
    vt          INTEGER,
    qty         INTEGER
) RETURNS SETOF queue.message_record AS $$
DECLARE
    qtable TEXT := queue.format_table_name(queue_name, 'q');
BEGIN
    RETURN QUERY EXECUTE format(
        $Q$
        WITH eligible_group AS MATERIALIZED (
            -- BOOL_AND(vt <= now) is equivalent to NOT EXISTS(vt > now) but avoids
            -- the correlated subplan that would run once per row (O(n²) on large queues).
            -- Uses the _grpvt_idx covering index for an index-only scan.
            SELECT message_group_id
            FROM queue.%1$I
            GROUP BY message_group_id
            HAVING BOOL_AND(vt <= clock_timestamp())
            ORDER BY MIN(msg_id) ASC
            LIMIT 1
        ),
        cte AS (
            SELECT m.msg_id
            FROM queue.%1$I m
            WHERE m.message_group_id = (SELECT message_group_id FROM eligible_group)
              AND m.vt <= clock_timestamp()
            ORDER BY m.msg_id ASC
            LIMIT %2$s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE queue.%1$I m
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

-- PL/pgSQL fallback — overridden by pgrx C hot path (receive_fifo_fn_wrapper)
-- when load_pgrx_extension.sql is applied. The C version uses WaitLatch + shared-memory
-- waiter registry for push-based wakeup; this fallback polls with pg_sleep.
-- SQL is inlined (not delegated to the 3-arg overload) to avoid PostgreSQL's
-- "function is not unique" error when both overloads are candidates for 3 args.
CREATE OR REPLACE FUNCTION queue.receive_fifo(
    queue_name       TEXT,
    vt               INTEGER,
    qty              INTEGER,
    max_poll_seconds INTEGER DEFAULT 5,
    poll_interval_ms INTEGER DEFAULT 100
) RETURNS SETOF queue.message_record AS $$
DECLARE
    r       queue.message_record;
    stop_at TIMESTAMP;
    qtable  TEXT := queue.format_table_name(queue_name, 'q');
    sql     TEXT;
BEGIN
    stop_at := clock_timestamp() + make_interval(secs => max_poll_seconds);
    sql := format(
        $Q$
        WITH eligible_group AS MATERIALIZED (
            SELECT message_group_id
            FROM queue.%1$I
            GROUP BY message_group_id
            HAVING BOOL_AND(vt <= clock_timestamp())
            ORDER BY MIN(msg_id) ASC
            LIMIT 1
        ),
        cte AS (
            SELECT m.msg_id
            FROM queue.%1$I m
            WHERE m.message_group_id = (SELECT message_group_id FROM eligible_group)
              AND m.vt <= clock_timestamp()
            ORDER BY m.msg_id ASC
            LIMIT %2$s
            FOR UPDATE SKIP LOCKED
        )
        UPDATE queue.%1$I m
        SET last_read_at = clock_timestamp(),
            vt           = clock_timestamp() + make_interval(secs => %3$s),
            read_ct      = read_ct + 1
        FROM cte WHERE m.msg_id = cte.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers
        $Q$,
        qtable, qty, vt
    );
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

------------------------------------------------------------
-- Notification system
------------------------------------------------------------

CREATE OR REPLACE FUNCTION queue.notify_queue_listeners()
RETURNS TRIGGER AS $$
DECLARE
    queue_name_extracted TEXT;
    updated_count        INTEGER;
BEGIN
    queue_name_extracted := substring(TG_TABLE_NAME FROM 3);

    UPDATE queue.notify_insert_throttle
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

CREATE OR REPLACE FUNCTION queue.enable_notify_insert(queue_name TEXT, throttle_interval_ms INTEGER DEFAULT 250)
RETURNS void AS $$
DECLARE
    qtable               TEXT := queue.format_table_name(queue_name, 'q');
    v_queue_name         TEXT := queue_name;
    v_throttle_interval  INTEGER := throttle_interval_ms;
BEGIN
    IF v_throttle_interval < 0 THEN
        RAISE EXCEPTION 'throttle_interval_ms must be non-negative';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'queue' AND table_name = qtable) THEN
        RAISE EXCEPTION 'Queue "%" does not exist. Create it first using queue.create()', v_queue_name;
    END IF;

    PERFORM queue.disable_notify_insert(v_queue_name);

    INSERT INTO queue.notify_insert_throttle (queue_name, throttle_interval_ms)
    VALUES (v_queue_name, v_throttle_interval)
    ON CONFLICT ON CONSTRAINT notify_insert_throttle_queue_name_key
        DO UPDATE SET throttle_interval_ms = EXCLUDED.throttle_interval_ms,
                      last_notified_at = to_timestamp(0);

    EXECUTE FORMAT(
        $Q$
        CREATE CONSTRAINT TRIGGER trigger_notify_queue_insert_listeners
        AFTER INSERT ON queue.%I
        DEFERRABLE FOR EACH ROW
        EXECUTE PROCEDURE queue.notify_queue_listeners()
        $Q$,
        qtable
    );
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION queue.disable_notify_insert(queue_name TEXT)
RETURNS void AS $$
DECLARE
    qtable TEXT := queue.format_table_name(queue_name, 'q');
BEGIN
    EXECUTE FORMAT(
        $Q$ DROP TRIGGER IF EXISTS trigger_notify_queue_insert_listeners ON queue.%I $Q$,
        qtable
    );
    DELETE FROM queue.notify_insert_throttle nit WHERE nit.queue_name = disable_notify_insert.queue_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION queue.list_notify_insert_throttles()
RETURNS TABLE (queue_name text, throttle_interval_ms integer, last_notified_at TIMESTAMP WITH TIME ZONE)
LANGUAGE sql STABLE AS $$
    SELECT queue_name, throttle_interval_ms, last_notified_at
    FROM queue.notify_insert_throttle ORDER BY queue_name;
$$;

CREATE OR REPLACE FUNCTION queue.update_notify_insert(queue_name text, throttle_interval_ms integer)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF throttle_interval_ms < 0 THEN
        RAISE EXCEPTION 'throttle_interval_ms must be non-negative, got: %', throttle_interval_ms;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM queue.meta WHERE meta.queue_name = update_notify_insert.queue_name) THEN
        RAISE EXCEPTION 'Queue "%" does not exist', queue_name;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM queue.notify_insert_throttle WHERE notify_insert_throttle.queue_name = update_notify_insert.queue_name) THEN
        RAISE EXCEPTION 'Queue "%" does not have notify_insert enabled', queue_name;
    END IF;
    UPDATE queue.notify_insert_throttle
    SET throttle_interval_ms = update_notify_insert.throttle_interval_ms, last_notified_at = to_timestamp(0)
    WHERE notify_insert_throttle.queue_name = update_notify_insert.queue_name;
END;
$$;

------------------------------------------------------------
-- Topic routing
------------------------------------------------------------

CREATE OR REPLACE FUNCTION queue.validate_routing_key(routing_key text)
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

CREATE OR REPLACE FUNCTION queue.validate_topic_pattern(pattern text)
RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF pattern IS NULL OR pattern = '' THEN
        RAISE EXCEPTION 'pattern cannot be NULL or empty' USING ERRCODE = 'Q0002';
    END IF;
    IF length(pattern) > 255 THEN
        RAISE EXCEPTION 'pattern length cannot exceed 255 characters' USING ERRCODE = 'Q0002';
    END IF;
    IF pattern !~ '^[a-zA-Z0-9._\-*#]+$' THEN
        RAISE EXCEPTION 'pattern contains invalid characters. Got: %', pattern USING ERRCODE = 'Q0002';
    END IF;
    IF pattern ~ '^\.' THEN RAISE EXCEPTION 'pattern cannot start with a dot' USING ERRCODE = 'Q0002'; END IF;
    IF pattern ~ '\.$' THEN RAISE EXCEPTION 'pattern cannot end with a dot' USING ERRCODE = 'Q0002'; END IF;
    IF pattern ~ '\.\.' THEN RAISE EXCEPTION 'pattern cannot contain consecutive dots' USING ERRCODE = 'Q0002'; END IF;
    IF pattern ~ '\*\*' THEN RAISE EXCEPTION 'pattern cannot contain consecutive stars (**).' USING ERRCODE = 'Q0002'; END IF;
    IF pattern ~ '##' THEN RAISE EXCEPTION 'pattern cannot contain consecutive hashes (##).' USING ERRCODE = 'Q0002'; END IF;
    IF pattern ~ '\*#' OR pattern ~ '#\*' THEN
        RAISE EXCEPTION 'pattern cannot contain adjacent wildcards (*# or #*).' USING ERRCODE = 'Q0002';
    END IF;
    RETURN true;
END;
$$;

CREATE OR REPLACE FUNCTION queue.subscribe(
    pattern      text,
    protocol     text,
    endpoint     text,
    queue_name   text DEFAULT NULL,
    raw_delivery boolean DEFAULT false
)
RETURNS TABLE(
    r_id bigint, r_pattern text, r_protocol text, r_endpoint text,
    r_queue_name text, r_bound_at timestamptz, r_raw_delivery boolean
) LANGUAGE plpgsql AS $$
BEGIN
    PERFORM queue.validate_topic_pattern(pattern);
    IF protocol NOT IN ('sqs', 'http', 'https') THEN
        RAISE EXCEPTION 'protocol must be sqs, http, or https' USING ERRCODE = 'Q0002';
    END IF;
    IF endpoint IS NULL OR endpoint = '' THEN
        RAISE EXCEPTION 'endpoint cannot be NULL or empty' USING ERRCODE = 'Q0002';
    END IF;
    IF protocol = 'sqs' THEN
        IF queue_name IS NULL OR queue_name = '' THEN
            RAISE EXCEPTION 'queue_name required for sqs protocol' USING ERRCODE = 'Q0002';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM queue.meta WHERE meta.queue_name = subscribe.queue_name) THEN
            RAISE EXCEPTION 'Queue "%" does not exist', queue_name USING ERRCODE = 'Q0001';
        END IF;
    END IF;
    INSERT INTO queue.topic_subscriptions (pattern, protocol, endpoint, queue_name, raw_delivery)
    VALUES (subscribe.pattern, subscribe.protocol, subscribe.endpoint, subscribe.queue_name, subscribe.raw_delivery)
    ON CONFLICT ON CONSTRAINT topic_subscriptions_unique DO NOTHING;
    RETURN QUERY
        SELECT ts.id, ts.pattern, ts.protocol, ts.endpoint, ts.queue_name, ts.bound_at, ts.raw_delivery
        FROM queue.topic_subscriptions ts
        WHERE ts.pattern = subscribe.pattern AND ts.endpoint = subscribe.endpoint;
END;
$$;

CREATE OR REPLACE FUNCTION queue.unsubscribe(pattern text, endpoint text)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    rows_deleted integer;
BEGIN
    IF pattern IS NULL OR pattern = '' THEN RAISE EXCEPTION 'pattern cannot be NULL or empty'; END IF;
    IF endpoint IS NULL OR endpoint = '' THEN RAISE EXCEPTION 'endpoint cannot be NULL or empty'; END IF;
    DELETE FROM queue.topic_subscriptions
    WHERE topic_subscriptions.pattern = unsubscribe.pattern
      AND topic_subscriptions.endpoint = unsubscribe.endpoint;
    GET DIAGNOSTICS rows_deleted = ROW_COUNT;
    RETURN rows_deleted > 0;
END;
$$;

CREATE OR REPLACE FUNCTION queue.test_routing(routing_key text)
RETURNS TABLE (pattern text, queue_name text, compiled_regex text)
LANGUAGE plpgsql STABLE AS $$
BEGIN
    PERFORM queue.validate_routing_key(routing_key);
    RETURN QUERY
        SELECT b.pattern, b.queue_name, b.compiled_regex
        FROM queue.topic_subscriptions b
        WHERE routing_key ~ b.compiled_regex
        ORDER BY b.pattern;
END;
$$;

-- Canonical send_topic(TEXT, JSONB, JSONB, TIMESTAMPTZ, BOOLEAN) is defined in hot_paths.sql
-- (PL/pgSQL stub) and replaced by the pgrx C function via load_pgrx_extension.sql.
-- plpgsql is used so the reference to the canonical is resolved at call time, not at
-- CREATE FUNCTION time (the canonical is loaded after this file).

DROP FUNCTION IF EXISTS queue.send_topic(text, jsonb, jsonb, integer);
CREATE FUNCTION queue.send_topic(routing_key text, msg jsonb, headers jsonb, delay integer)
RETURNS TABLE (queue_name text, msg_id bigint) LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    RETURN QUERY SELECT * FROM queue.send_topic(routing_key, msg, headers,
        clock_timestamp() + make_interval(secs => delay));
END;
$$;

DROP FUNCTION IF EXISTS queue.send_topic(text, jsonb);
CREATE FUNCTION queue.send_topic(routing_key text, msg jsonb)
RETURNS TABLE (queue_name text, msg_id bigint) LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    RETURN QUERY SELECT * FROM queue.send_topic(routing_key, msg, NULL::jsonb, clock_timestamp());
END;
$$;

DROP FUNCTION IF EXISTS queue.send_topic(text, jsonb, integer);
CREATE FUNCTION queue.send_topic(routing_key text, msg jsonb, delay integer)
RETURNS TABLE (queue_name text, msg_id bigint) LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    RETURN QUERY SELECT * FROM queue.send_topic(routing_key, msg, NULL::jsonb,
        clock_timestamp() + make_interval(secs => delay));
END;
$$;

CREATE OR REPLACE FUNCTION queue.list_subscriptions()
RETURNS TABLE (id bigint, pattern text, protocol text, endpoint text, queue_name text, bound_at TIMESTAMP WITH TIME ZONE, raw_delivery boolean)
LANGUAGE sql STABLE AS $$
    SELECT id, pattern, protocol, endpoint, queue_name, bound_at, raw_delivery
    FROM queue.topic_subscriptions
    ORDER BY bound_at DESC, pattern, endpoint;
$$;

CREATE OR REPLACE FUNCTION queue.list_subscriptions(queue_name text)
RETURNS TABLE (id bigint, pattern text, protocol text, endpoint text, queue_name text, bound_at TIMESTAMP WITH TIME ZONE, raw_delivery boolean)
LANGUAGE sql STABLE AS $$
    SELECT tb.id, tb.pattern, tb.protocol, tb.endpoint, tb.queue_name, tb.bound_at, tb.raw_delivery
    FROM queue.topic_subscriptions tb
    WHERE tb.queue_name = list_subscriptions.queue_name
    ORDER BY bound_at DESC, pattern;
$$;

-- Queue HTTP/HTTPS deliveries for subscriptions matching routing_key.
-- raw_msg: the original message payload.
-- envelope_msg: the SNS notification envelope (NULL = always use raw_msg).
-- Stores raw_msg or envelope_msg per subscription's raw_delivery flag.
CREATE OR REPLACE FUNCTION queue.queue_http_deliveries(
    routing_key  text,
    raw_msg      jsonb,
    envelope_msg jsonb DEFAULT NULL
) RETURNS bigint LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    n bigint;
BEGIN
    INSERT INTO queue.http_deliveries (subscription_id, endpoint, payload)
    SELECT ts.id, ts.endpoint,
           CASE WHEN ts.raw_delivery OR envelope_msg IS NULL THEN raw_msg ELSE envelope_msg END
    FROM queue.topic_subscriptions ts
    WHERE routing_key ~ ts.compiled_regex
      AND ts.protocol IN ('http', 'https');
    GET DIAGNOSTICS n = ROW_COUNT;
    RETURN n;
END;
$$;

CREATE OR REPLACE FUNCTION queue.send_batch_topic(
    routing_key text, msgs jsonb[], headers jsonb[], delay TIMESTAMP WITH TIME ZONE,
    sync_commit boolean DEFAULT TRUE
)
RETURNS TABLE (queue_name text, msg_id bigint)
LANGUAGE plpgsql VOLATILE AS $$
DECLARE
    b RECORD;
BEGIN
    PERFORM queue.validate_routing_key(routing_key);
    PERFORM queue._validate_batch_params(msgs, headers);
    FOR b IN
        SELECT DISTINCT tb.queue_name FROM queue.topic_subscriptions tb
        WHERE routing_key ~ tb.compiled_regex ORDER BY tb.queue_name
    LOOP
        RETURN QUERY
        SELECT b.queue_name, batch_result.msg_id
        FROM queue._send_batch(b.queue_name, msgs, headers, delay, sync_commit) AS batch_result(msg_id);
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION queue.send_batch_topic(routing_key text, msgs jsonb[])
RETURNS TABLE (queue_name text, msg_id bigint) LANGUAGE sql VOLATILE AS $$
    SELECT * FROM queue.send_batch_topic(routing_key, msgs, NULL, clock_timestamp());
$$;

CREATE OR REPLACE FUNCTION queue.send_batch_topic(routing_key text, msgs jsonb[], headers jsonb[])
RETURNS TABLE (queue_name text, msg_id bigint) LANGUAGE sql VOLATILE AS $$
    SELECT * FROM queue.send_batch_topic(routing_key, msgs, headers, clock_timestamp());
$$;

CREATE OR REPLACE FUNCTION queue.send_batch_topic(routing_key text, msgs jsonb[], delay integer)
RETURNS TABLE (queue_name text, msg_id bigint) LANGUAGE sql VOLATILE AS $$
    SELECT * FROM queue.send_batch_topic(routing_key, msgs, NULL, clock_timestamp() + make_interval(secs => delay));
$$;

CREATE OR REPLACE FUNCTION queue.send_batch_topic(routing_key text, msgs jsonb[], headers jsonb[], delay integer)
RETURNS TABLE (queue_name text, msg_id bigint) LANGUAGE sql VOLATILE AS $$
    SELECT * FROM queue.send_batch_topic(routing_key, msgs, headers, clock_timestamp() + make_interval(secs => delay));
$$;

------------------------------------------------------------
-- Routing cache invalidation
------------------------------------------------------------

-- No-op stub: overridden by the pgrx C function (routing_cache.rs) via
-- load_pgrx_extension.sql. Defined here so the trigger can always reference it,
-- even in environments without the pgrx extension loaded.
CREATE OR REPLACE FUNCTION queue._invalidate_routing_cache()
RETURNS VOID LANGUAGE plpgsql VOLATILE AS $$
BEGIN
END;
$$;

-- Trigger function called on every topic_bindings write.
CREATE OR REPLACE FUNCTION queue._routing_cache_invalidate_trigger()
RETURNS TRIGGER LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    PERFORM queue._invalidate_routing_cache();
    RETURN NULL;
END;
$$;

-- AFTER statement trigger: fires once per DML statement, not once per row.
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'topic_subscriptions_cache_invalidate'
          AND tgrelid = 'queue.topic_subscriptions'::regclass
    ) THEN
        CREATE TRIGGER topic_subscriptions_cache_invalidate
            AFTER INSERT OR UPDATE OR DELETE ON queue.topic_subscriptions
            FOR EACH STATEMENT EXECUTE FUNCTION queue._routing_cache_invalidate_trigger();
    END IF;
END $$;

------------------------------------------------------------
-- Grant pg_monitor read access
------------------------------------------------------------

GRANT USAGE ON SCHEMA queue TO pg_monitor;
GRANT SELECT ON ALL TABLES IN SCHEMA queue TO pg_monitor;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA queue TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA queue GRANT SELECT ON TABLES TO pg_monitor;
ALTER DEFAULT PRIVILEGES IN SCHEMA queue GRANT SELECT ON SEQUENCES TO pg_monitor;
