pgrx::pg_module_magic!();

mod queue;
mod waiter;

/// Called once when the extension is loaded.  Installs the shared-memory hooks
/// that back read_with_poll's push-based wakeup (WaiterRegistry).
///
/// The extension must be listed in shared_preload_libraries for shared memory to
/// be available.  Without it, read_with_poll falls back to timeout-only polling.
#[pgrx::pg_guard]
pub extern "C-unwind" fn _PG_init() {
    unsafe { waiter::install_hooks() }
}

// Bootstrap SQL: creates schema, tables, types, indexes, and non-hot-path functions.
// Hot path functions (send, read, delete, archive, pop, set_vt) are provided by the
// #[pg_extern] implementations in queue.rs and override any SQL versions via
// CREATE OR REPLACE semantics.
pgrx::extension_sql_file!("../sql/schema.sql", name = "queue_schema");

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    fn create(queue: &str) {
        Spi::run(&format!("SELECT queue.create('{queue}')")).expect("create failed");
    }

    fn send(queue: &str, body: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT * FROM queue.send('{queue}', '{body}'::jsonb)"
        ))
        .expect("send failed")
        .expect("send returned no id")
    }

    fn send_with_delay(queue: &str, body: &str, delay_secs: i32) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT * FROM queue.send('{queue}', '{body}'::jsonb, {delay_secs})"
        ))
        .expect("send_with_delay failed")
        .expect("send_with_delay returned no id")
    }

    fn queue_depth(queue: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM queue.q_{queue}"
        ))
        .expect("queue_depth query failed")
        .expect("count was NULL")
    }

    fn archive_depth(queue: &str) -> i64 {
        Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM queue.a_{queue}"
        ))
        .expect("archive_depth query failed")
        .expect("count was NULL")
    }

    // -------------------------------------------------------------------------
    // send / read / delete lifecycle
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_send_read_delete() {
        create("slc_queue");

        let msg_id = send("slc_queue", r#"{"hello":"world"}"#);
        assert!(msg_id > 0, "msg_id should be positive");

        // read with vt=0 (message immediately re-visible)
        let read_id: i64 = Spi::get_one::<i64>(
            "SELECT msg_id FROM queue.read('slc_queue', 0, 1)",
        )
        .expect("read failed")
        .expect("expected a message");
        assert_eq!(read_id, msg_id);

        // read_ct should now be 1
        let read_ct: i32 = Spi::get_one::<i32>(&format!(
            "SELECT read_ct FROM queue.q_slc_queue WHERE msg_id = {msg_id}"
        ))
        .expect("read_ct query failed")
        .expect("no row");
        assert_eq!(read_ct, 1);

        // delete it
        let deleted: bool = Spi::get_one::<bool>(&format!(
            "SELECT queue.delete('slc_queue', {msg_id})"
        ))
        .expect("delete failed")
        .expect("delete returned NULL");
        assert!(deleted);

        assert_eq!(queue_depth("slc_queue"), 0);
    }

    #[pg_test]
    fn test_delete_nonexistent_returns_false() {
        create("del_nex_queue");
        let deleted: bool = Spi::get_one::<bool>(
            "SELECT queue.delete('del_nex_queue', 99999)",
        )
        .expect("delete query failed")
        .expect("delete returned NULL");
        assert!(!deleted);
    }

    // -------------------------------------------------------------------------
    // send with headers
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_send_with_headers() {
        create("hdr_queue");
        let msg_id = Spi::get_one::<i64>(
            "SELECT * FROM queue.send('hdr_queue', '{\"x\":1}'::jsonb, '{\"h\":\"v\"}'::jsonb)",
        )
        .expect("send failed")
        .expect("no id");
        assert!(msg_id > 0);

        let has_header: bool = Spi::get_one::<bool>(&format!(
            "SELECT headers @> '{{\"h\":\"v\"}}' FROM queue.q_hdr_queue WHERE msg_id = {msg_id}"
        ))
        .expect("header check failed")
        .expect("no row");
        assert!(has_header);
    }

    // -------------------------------------------------------------------------
    // visibility timeout
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_visibility_timeout_hides_message() {
        create("vt_queue");
        let msg_id = send("vt_queue", r#"{"x":1}"#);

        // set vt 60 seconds into the future
        Spi::run(&format!(
            "SELECT queue.set_vt('vt_queue', {msg_id}, 60)"
        ))
        .expect("set_vt failed");

        // should not be readable
        let count: i64 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM queue.read('vt_queue', 1, 10)",
        )
        .expect("read failed")
        .expect("count was NULL");
        assert_eq!(count, 0, "message should be hidden by vt");

        // set vt to now (reveal it)
        Spi::run(&format!("SELECT queue.set_vt('vt_queue', {msg_id}, 0)"))
            .expect("set_vt to 0 failed");

        // now readable
        let read_id: i64 = Spi::get_one::<i64>(
            "SELECT msg_id FROM queue.read('vt_queue', 1, 1)",
        )
        .expect("read failed")
        .expect("expected message");
        assert_eq!(read_id, msg_id);
    }

    #[pg_test]
    fn test_set_vt_timestamp() {
        create("vt_ts_queue");
        let msg_id = send("vt_ts_queue", r#"{"x":1}"#);

        // set vt to a specific future timestamp
        let vt_future: bool = Spi::get_one::<bool>(&format!(
            "SELECT vt > clock_timestamp() + '30 seconds'::interval
             FROM queue.set_vt('vt_ts_queue', {msg_id},
                 clock_timestamp() + '60 seconds'::interval)"
        ))
        .expect("set_vt timestamp failed")
        .expect("no row returned");
        assert!(vt_future);

        // not readable
        let count: i64 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM queue.read('vt_ts_queue', 0, 5)",
        )
        .expect("read failed")
        .expect("NULL count");
        assert_eq!(count, 0);

        // set vt to past to reveal
        Spi::run(&format!(
            "SELECT queue.set_vt('vt_ts_queue', {msg_id},
                 clock_timestamp() - '1 second'::interval)"
        ))
        .expect("set_vt to past failed");

        let read_id: i64 = Spi::get_one::<i64>(
            "SELECT msg_id FROM queue.read('vt_ts_queue', 0, 1)",
        )
        .expect("read failed")
        .expect("expected message");
        assert_eq!(read_id, msg_id);
    }

    #[pg_test]
    fn test_set_vt_batch() {
        create("vt_batch_queue");
        let id1 = send("vt_batch_queue", r#"{"n":1}"#);
        let id2 = send("vt_batch_queue", r#"{"n":2}"#);

        // hide both
        let hidden_count: i64 = Spi::get_one::<i64>(&format!(
            "SELECT COUNT(*) FROM queue.set_vt('vt_batch_queue', ARRAY[{id1},{id2}]::bigint[], 60)"
        ))
        .expect("batch set_vt failed")
        .expect("NULL count");
        assert_eq!(hidden_count, 2);

        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*) FROM queue.read('vt_batch_queue', 0, 10)")
                .expect("read failed")
                .expect("NULL"),
            0
        );

        // reveal both
        Spi::run(&format!(
            "SELECT queue.set_vt('vt_batch_queue', ARRAY[{id1},{id2}]::bigint[], 0)"
        ))
        .expect("batch reveal failed");

        let visible: i64 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM queue.read('vt_batch_queue', 0, 10)",
        )
        .expect("read failed")
        .expect("NULL");
        assert_eq!(visible, 2);
    }

    // -------------------------------------------------------------------------
    // read_with_poll
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_read_with_poll_immediate() {
        create("rwp_imm_queue");
        let msg_id = send("rwp_imm_queue", r#"{"x":1}"#);

        // message is immediately available, poll should return right away
        let read_id: i64 = Spi::get_one::<i64>(
            "SELECT msg_id FROM queue.read_with_poll('rwp_imm_queue', 30, 1, 5, 100)",
        )
        .expect("read_with_poll failed")
        .expect("expected message");
        assert_eq!(read_id, msg_id);
    }

    #[pg_test]
    fn test_read_with_poll_respects_conditional() {
        create("rwp_cond_queue");
        send("rwp_cond_queue", r#"{"type":"a"}"#);
        let id_b = send("rwp_cond_queue", r#"{"type":"b"}"#);

        // vt=0 so first message gets locked for re-visibility immediately, but
        // the conditional only matches "b"
        let read_id: i64 = Spi::get_one::<i64>(
            r#"SELECT msg_id FROM queue.read_with_poll(
                'rwp_cond_queue', 0, 1, 2, 100, '{"type":"b"}'::jsonb
            )"#,
        )
        .expect("read_with_poll failed")
        .expect("expected message b");
        assert_eq!(read_id, id_b);
    }

    #[pg_test]
    fn test_read_with_poll_times_out_empty() {
        create("rwp_empty_queue");
        // Nothing sent — should return no rows after brief poll
        let count: i64 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM queue.read_with_poll('rwp_empty_queue', 30, 1, 1, 100)",
        )
        .expect("read_with_poll failed")
        .expect("NULL count");
        assert_eq!(count, 0);
    }

    // -------------------------------------------------------------------------
    // send_batch / _send_batch
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_send_batch() {
        create("batch_q");
        let ids: Vec<i64> = Spi::connect(|client| {
            client
                .select(
                    "SELECT * FROM queue.send_batch(
                        'batch_q',
                        ARRAY['{\"n\":1}','{\"n\":2}','{\"n\":3}']::jsonb[]
                    )",
                    None,
                    &[],
                )
                .expect("send_batch failed")
                .map(|row| row.get::<i64>(1).expect("get failed").expect("NULL id"))
                .collect()
        });
        assert_eq!(ids.len(), 3);
        assert!(ids.iter().all(|&id| id > 0));
        assert_eq!(queue_depth("batch_q"), 3);
    }

    #[pg_test]
    fn test_send_batch_with_headers() {
        create("batch_hdr_q");
        let ids: Vec<i64> = Spi::connect(|client| {
            client
                .select(
                    "SELECT * FROM queue.send_batch(
                        'batch_hdr_q',
                        ARRAY['{\"n\":1}','{\"n\":2}']::jsonb[],
                        ARRAY['{\"h\":1}','{\"h\":2}']::jsonb[]
                    )",
                    None,
                    &[],
                )
                .expect("send_batch with headers failed")
                .map(|row| row.get::<i64>(1).expect("get failed").expect("NULL id"))
                .collect()
        });
        assert_eq!(ids.len(), 2);
        assert_eq!(queue_depth("batch_hdr_q"), 2);
    }

    #[pg_test]
    fn test_send_batch_with_delay() {
        create("batch_delay_q");
        let ids: Vec<i64> = Spi::connect(|client| {
            client
                .select(
                    "SELECT * FROM queue.send_batch(
                        'batch_delay_q',
                        ARRAY['{\"n\":1}','{\"n\":2}']::jsonb[],
                        60
                    )",
                    None,
                    &[],
                )
                .expect("send_batch with delay failed")
                .map(|row| row.get::<i64>(1).expect("get failed").expect("NULL id"))
                .collect()
        });
        assert_eq!(ids.len(), 2);
        // messages should not be readable yet (delayed)
        let visible: i64 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM queue.read('batch_delay_q', 0, 10)",
        )
        .expect("read failed")
        .expect("NULL");
        assert_eq!(visible, 0, "delayed messages should not be visible");
    }

    // -------------------------------------------------------------------------
    // delete_batch
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_delete_batch() {
        create("del_batch_q");
        let id1 = send("del_batch_q", r#"{"n":1}"#);
        let id2 = send("del_batch_q", r#"{"n":2}"#);
        let id3 = send("del_batch_q", r#"{"n":3}"#);
        assert_eq!(queue_depth("del_batch_q"), 3);

        let deleted: Vec<i64> = Spi::connect(|client| {
            client
                .update(
                    &format!(
                        "SELECT * FROM queue.delete('del_batch_q', ARRAY[{id1},{id2}]::bigint[])"
                    ),
                    None,
                    &[],
                )
                .expect("delete_batch failed")
                .map(|row| row.get::<i64>(1).expect("get failed").expect("NULL"))
                .collect()
        });
        assert_eq!(deleted.len(), 2);
        assert!(deleted.contains(&id1));
        assert!(deleted.contains(&id2));

        assert_eq!(queue_depth("del_batch_q"), 1);

        // id3 still in queue
        let remaining: i64 = Spi::get_one::<i64>(&format!(
            "SELECT msg_id FROM queue.q_del_batch_q WHERE msg_id = {id3}"
        ))
        .expect("remaining check failed")
        .expect("id3 should still exist");
        assert_eq!(remaining, id3);
    }

    // -------------------------------------------------------------------------
    // archive
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_archive_single() {
        create("arch_q");
        let id = send("arch_q", r#"{"x":1}"#);
        assert_eq!(queue_depth("arch_q"), 1);
        assert_eq!(archive_depth("arch_q"), 0);

        let archived: bool =
            Spi::get_one::<bool>(&format!("SELECT queue.archive('arch_q', {id})"))
                .expect("archive failed")
                .expect("archive returned NULL");
        assert!(archived);

        assert_eq!(queue_depth("arch_q"), 0);
        assert_eq!(archive_depth("arch_q"), 1);

        // message and headers preserved in archive
        let preserved: bool = Spi::get_one::<bool>(&format!(
            "SELECT message @> '{{\"x\":1}}' FROM queue.a_arch_q WHERE msg_id = {id}"
        ))
        .expect("archive content check failed")
        .expect("no row in archive");
        assert!(preserved);
    }

    #[pg_test]
    fn test_archive_batch() {
        create("arch_batch_q");
        let id1 = send("arch_batch_q", r#"{"n":1}"#);
        let id2 = send("arch_batch_q", r#"{"n":2}"#);
        let _id3 = send("arch_batch_q", r#"{"n":3}"#);

        let archived: Vec<i64> = Spi::connect(|client| {
            client
                .update(
                    &format!(
                        "SELECT * FROM queue.archive('arch_batch_q', ARRAY[{id1},{id2}]::bigint[])"
                    ),
                    None,
                    &[],
                )
                .expect("archive_batch failed")
                .map(|row| row.get::<i64>(1).expect("get failed").expect("NULL"))
                .collect()
        });
        assert_eq!(archived.len(), 2);
        assert!(archived.contains(&id1));
        assert!(archived.contains(&id2));

        assert_eq!(queue_depth("arch_batch_q"), 1);
        assert_eq!(archive_depth("arch_batch_q"), 2);
    }

    #[pg_test]
    fn test_archive_nonexistent_returns_empty() {
        create("arch_nex_q");
        let archived: i64 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM queue.archive('arch_nex_q', 99999)",
        )
        .expect("archive query failed")
        .expect("NULL count");
        assert_eq!(archived, 0);
    }

    // -------------------------------------------------------------------------
    // pop
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_pop_single() {
        create("pop_q");

        // pop on empty queue returns nothing
        let empty: i64 = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM queue.pop('pop_q')",
        )
        .expect("pop empty failed")
        .expect("NULL count");
        assert_eq!(empty, 0);

        let id1 = send("pop_q", r#"{"n":1}"#);
        let _id2 = send("pop_q", r#"{"n":2}"#);

        // pop returns oldest first
        let popped_id: i64 =
            Spi::get_one::<i64>("SELECT msg_id FROM queue.pop('pop_q')")
                .expect("pop failed")
                .expect("expected a message");
        assert_eq!(popped_id, id1);

        // gone from queue
        assert_eq!(queue_depth("pop_q"), 1);

        // NOT in archive (pop doesn't archive)
        assert_eq!(archive_depth("pop_q"), 0);
    }

    #[pg_test]
    fn test_pop_batch() {
        create("pop_batch_q");
        send("pop_batch_q", r#"{"n":1}"#);
        send("pop_batch_q", r#"{"n":2}"#);
        send("pop_batch_q", r#"{"n":3}"#);

        let popped: Vec<i64> = Spi::connect(|client| {
            client
                .update("SELECT msg_id FROM queue.pop('pop_batch_q', 2)", None, &[])
                .expect("pop batch failed")
                .map(|row| row.get::<i64>(1).expect("get failed").expect("NULL"))
                .collect()
        });
        assert_eq!(popped.len(), 2);
        assert_eq!(queue_depth("pop_batch_q"), 1);
    }

    // -------------------------------------------------------------------------
    // read conditional
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_read_conditional() {
        create("cond_q");
        send("cond_q", r#"{"type":"a","val":1}"#);
        let id_b = send("cond_q", r#"{"type":"b","val":2}"#);
        send("cond_q", r#"{"type":"a","val":3}"#);

        let matched: i64 = Spi::get_one::<i64>(
            r#"SELECT msg_id FROM queue.read('cond_q', 0, 1, '{"type":"b"}'::jsonb)"#,
        )
        .expect("conditional read failed")
        .expect("expected a match");
        assert_eq!(matched, id_b);
    }

    // -------------------------------------------------------------------------
    // last_read_at tracking
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_last_read_at_set_on_read() {
        create("lra_queue");
        let msg_id = send("lra_queue", r#"{"x":1}"#);

        // before any read, last_read_at should be NULL
        let before: Option<pgrx::TimestampWithTimeZone> = Spi::get_one::<pgrx::TimestampWithTimeZone>(
            &format!("SELECT last_read_at FROM queue.q_lra_queue WHERE msg_id = {msg_id}"),
        )
        .expect("last_read_at check failed");
        assert!(before.is_none(), "last_read_at should be NULL before first read");

        // read with vt=0
        Spi::run("SELECT msg_id FROM queue.read('lra_queue', 0, 1)")
            .expect("read failed");

        // now should be set
        let after: bool = Spi::get_one::<bool>(&format!(
            "SELECT last_read_at IS NOT NULL FROM queue.q_lra_queue WHERE msg_id = {msg_id}"
        ))
        .expect("last_read_at check failed")
        .expect("no row");
        assert!(after, "last_read_at should be set after read");
    }

    #[pg_test]
    fn test_last_read_at_updates_on_reread() {
        create("lra2_queue");
        send("lra2_queue", r#"{"x":1}"#);

        // read twice with vt=0
        let t1: pgrx::TimestampWithTimeZone = Spi::get_one::<pgrx::TimestampWithTimeZone>(
            "SELECT last_read_at FROM queue.read('lra2_queue', 0, 1)",
        )
        .expect("first read failed")
        .expect("last_read_at was NULL on first read");

        let t2: pgrx::TimestampWithTimeZone = Spi::get_one::<pgrx::TimestampWithTimeZone>(
            "SELECT last_read_at FROM queue.read('lra2_queue', 0, 1)",
        )
        .expect("second read failed")
        .expect("last_read_at was NULL on second read");

        assert!(t2 >= t1, "last_read_at should advance on re-read");
    }

    // -------------------------------------------------------------------------
    // send_batch_internal validation
    // -------------------------------------------------------------------------

    #[pg_test]
    #[should_panic]
    fn test_send_batch_mismatched_headers_panics() {
        create("mismatch_q");
        // 2 messages, 3 headers — should error
        Spi::run(
            "SELECT queue.send_batch(
                'mismatch_q',
                ARRAY['{\"a\":1}','{\"b\":2}']::jsonb[],
                ARRAY['{\"h\":1}','{\"h\":2}','{\"h\":3}']::jsonb[]
            )",
        )
        .expect("should have errored");
    }

    // -------------------------------------------------------------------------
    // validate_name rejects injection attempts
    // -------------------------------------------------------------------------

    #[pg_test]
    #[should_panic]
    fn test_send_invalid_queue_name_panics() {
        // SQL injection attempt — validate_name should reject this
        Spi::run("SELECT queue.send('abc; DROP TABLE queue.meta', '{}'::jsonb)")
            .expect("should have panicked");
    }

    #[pg_test]
    #[should_panic]
    fn test_read_invalid_queue_name_panics() {
        Spi::run("SELECT queue.read('bad$name', 0, 1)")
            .expect("should have panicked");
    }

    // -------------------------------------------------------------------------
    // smoke test: extension schema loaded
    // -------------------------------------------------------------------------

    #[pg_test]
    fn test_extension_schema_loaded() {
        let exists: bool = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_tables WHERE schemaname = 'queue' AND tablename = 'meta')",
        )
        .expect("schema check failed")
        .expect("NULL");
        assert!(exists, "queue.meta table should exist");
    }

    #[pg_test]
    fn test_create_is_idempotent() {
        create("idem_q");
        create("idem_q"); // should not error
        assert_eq!(queue_depth("idem_q"), 0);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
