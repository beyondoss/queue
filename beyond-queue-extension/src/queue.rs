use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi;

// Column layout must match queue.message_record exactly:
//   msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers
//
// NOTE: type aliases cannot appear inside #[pg_extern] return types because the pgrx
// proc-macro parses the return type syntactically, before alias resolution. The full
// tuple is written inline on every #[pg_extern] that returns table rows.
type MsgRow = (
    name!(msg_id, i64),
    name!(read_ct, i32),
    name!(enqueued_at, TimestampWithTimeZone),
    name!(last_read_at, Option<TimestampWithTimeZone>),
    name!(vt, TimestampWithTimeZone),
    name!(message, Option<pgrx::JsonB>),
    name!(headers, Option<pgrx::JsonB>),
);

fn validate_name(queue_name: &str) {
    if queue_name.is_empty()
        || queue_name.len() > 48
        || !queue_name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
    {
        pgrx::error!(
            "invalid queue name: must be 1–48 chars, [a-z0-9_] only, got {:?}",
            queue_name
        );
    }
}

#[inline]
fn q(name: &str) -> String {
    format!("queue.q_{name}")
}

#[inline]
fn a(name: &str) -> String {
    format!("queue.a_{name}")
}

fn collect_msg_rows<'mcx>(
    client: &mut spi::SpiClient<'_>,
    sql: &str,
    args: &[DatumWithOid<'mcx>],
) -> Result<Vec<MsgRow>, spi::Error> {
    let mut rows = Vec::new();
    for row in client.update(sql, None, args)? {
        rows.push((
            row.get::<i64>(1)?.unwrap_or(0),
            row.get::<i32>(2)?.unwrap_or(0),
            row.get::<TimestampWithTimeZone>(3)?
                .unwrap_or_else(|| pgrx::error!("enqueued_at was NULL")),
            row.get::<TimestampWithTimeZone>(4)?,
            row.get::<TimestampWithTimeZone>(5)?
                .unwrap_or_else(|| pgrx::error!("vt was NULL")),
            row.get::<pgrx::JsonB>(6)?,
            row.get::<pgrx::JsonB>(7)?,
        ));
    }
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Direct heap insert — bypasses SPI plan parsing, binding, and framing
// ---------------------------------------------------------------------------

// PostgreSQL lock modes (from storage/lockdefs.h — #defines, not extern symbols).
const ROW_EXCLUSIVE_LOCK: pg_sys::LOCKMODE = 3;
const NO_LOCK: pg_sys::LOCKMODE = 0;

/// Insert one row directly into `queue.q_{queue_name}`, bypassing SPI.
///
/// This eliminates per-call costs: SPI connection setup/teardown (~2µs),
/// plan parsing and planning (~5µs), and parameter binding (~1µs). The
/// saving is small per call (~8µs) but real at WAL-bound throughput.
///
/// Index maintenance: iterates `RelationGetIndexList` and calls `index_insert`
/// for each index. Works for column-reference indexes only — no expression
/// indexes exist on queue tables.
///
/// Safety: must be called inside a PostgreSQL backend with a valid transaction.
unsafe fn direct_heap_insert(
    queue_name: &str,
    msg: pgrx::JsonB,
    headers: Option<pgrx::JsonB>,
    delay: TimestampWithTimeZone,
) -> i64 {
    use pgrx::datum::IntoDatum;
    use pgrx::pg_sys::*;
    use std::ffi::CString;

    let queue_ns = get_namespace_oid(c"queue".as_ptr(), false);
    let tbl_name = CString::new(format!("q_{queue_name}")).unwrap();
    let relid = get_relname_relid(tbl_name.as_ptr(), queue_ns);
    if relid == InvalidOid {
        pgrx::error!("queue table queue.q_{queue_name} does not exist");
    }

    // nextval_internal is in sequence.h but not yet exposed in pgrx's pg_sys bindings.
    unsafe extern "C" {
        fn nextval_internal(seqid: pg_sys::Oid, check_permissions: bool) -> i64;
    }

    // RowExclusiveLock matches what a regular INSERT acquires.
    let rel = table_open(relid, ROW_EXCLUSIVE_LOCK);
    let tupdesc = (*rel).rd_att;

    // Advance the identity sequence for msg_id (attnum 1, 1-based).
    // check_permissions=false: caller already holds INSERT on the table which implies USAGE.
    let seq_oid = getIdentitySequence(rel, 1, false);
    let msg_id = nextval_internal(seq_oid, false);

    // Build values array for the 7 columns:
    //   msg_id(1) read_ct(2) enqueued_at(3) last_read_at(4) vt(5) message(6) headers(7)
    let now = GetCurrentTimestamp();
    let vt_datum = delay
        .into_datum()
        .unwrap_or_else(|| Datum::from(now as usize));
    let msg_datum = msg.into_datum().unwrap();
    let hdr_datum = headers.and_then(|h| h.into_datum());

    let mut values: [Datum; 7] = [Datum::from(0usize); 7];
    let mut nulls: [bool; 7] = [false; 7];

    values[0] = Datum::from(msg_id as usize); // msg_id  (i64 → usize on 64-bit)
    values[1] = Datum::from(0u32 as usize); // read_ct = 0
    values[2] = Datum::from(now as usize); // enqueued_at = clock_timestamp()
    nulls[3] = true; // last_read_at = NULL
    values[4] = vt_datum; // vt (delay timestamp)
    values[5] = msg_datum; // message JSONB
    match hdr_datum {
        Some(d) => values[6] = d,
        None => nulls[6] = true,
    }

    // Form tuple and insert into heap. heap_insert handles TOAST and WAL.
    let tuple = heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    let cid = GetCurrentCommandId(true);
    // options=0: normal insert (no HEAP_INSERT_SKIP_FSM, no HEAP_INSERT_FROZEN)
    heap_insert(rel, tuple, cid, 0, std::ptr::null_mut());
    // After heap_insert, tuple->t_self holds the on-disk TID needed for index_insert.

    // Maintain each index on this relation.
    let index_list = RelationGetIndexList(rel);
    let n = (*index_list).length as usize;
    for i in 0..n {
        // PG13+ List stores elements in a flat array, not a linked list.
        let idx_oid = (*(*index_list).elements.add(i)).oid_value;
        let idx_rel = index_open(idx_oid, ROW_EXCLUSIVE_LOCK);
        let idx_info = BuildIndexInfo(idx_rel);

        let ncols = (*idx_info).ii_NumIndexAttrs as usize;
        let mut idx_vals: Vec<Datum> = vec![Datum::from(0usize); ncols];
        let mut idx_nulls: Vec<bool> = vec![false; ncols];

        // Extract datums by attnum for column-reference index attributes.
        // ii_IndexAttrNumbers is 1-based; 0 would mean an expression column.
        for j in 0..ncols {
            let attnum = (*idx_info).ii_IndexAttrNumbers[j]; // i16, 1-based
            if attnum > 0 && (attnum as usize) <= 7 {
                idx_vals[j] = values[(attnum - 1) as usize];
                idx_nulls[j] = nulls[(attnum - 1) as usize];
            }
        }

        let unique_check = if (*idx_info).ii_Unique {
            IndexUniqueCheck::UNIQUE_CHECK_YES
        } else {
            IndexUniqueCheck::UNIQUE_CHECK_NO
        };

        index_insert(
            idx_rel,
            idx_vals.as_mut_ptr(),
            idx_nulls.as_mut_ptr(),
            &mut (*tuple).t_self,
            rel,
            unique_check,
            false, // indexUnchanged
            idx_info,
        );

        // NoLock: we keep the parent relation's RowExclusiveLock through commit.
        index_close(idx_rel, NO_LOCK);
    }

    // Close descriptor but keep the RowExclusiveLock held until transaction commit.
    table_close(rel, NO_LOCK);

    msg_id
}

// ---------------------------------------------------------------------------
// send
// ---------------------------------------------------------------------------

// Canonical hot path: (queue_name TEXT, msg JSONB, headers JSONB, delay TIMESTAMPTZ)
// All shorter overloads in schema.sql are SQL wrappers that call this.
//
// sync_commit = false issues SET LOCAL synchronous_commit = off before the INSERT,
// making the entire containing transaction skip the WAL fsync on commit. Callers
// opt in explicitly; the default preserves PostgreSQL's durable-commit guarantee.
#[pg_extern(name = "send", schema = "queue", volatile, parallel_safe)]
fn send_full(
    queue_name: &str,
    msg: pgrx::JsonB,
    headers: Option<pgrx::JsonB>,
    delay: TimestampWithTimeZone,
    sync_commit: default!(bool, true),
) -> SetOfIterator<'static, i64> {
    validate_name(queue_name);

    let msg_id = if sync_commit {
        // Hot path: bypass SPI entirely for INSERT.
        unsafe { direct_heap_insert(queue_name, msg, headers, delay) }
    } else {
        // Async-commit path: SET LOCAL then INSERT via SPI.
        let qtable = q(queue_name);
        let insert_sql = format!(
            "INSERT INTO {qtable} (vt, message, headers) VALUES ($1, $2, $3) RETURNING msg_id"
        );
        Spi::connect_mut(|client| {
            client.update("SET LOCAL synchronous_commit = off", None, &[])?;
            let id = client
                .update(
                    &insert_sql,
                    None,
                    &[delay.into(), msg.into(), headers.into()],
                )?
                .first()
                .get::<i64>(1)?
                .unwrap_or_else(|| pgrx::error!("queue.send: no msg_id returned"));
            Ok::<_, spi::Error>(id)
        })
        .unwrap_or_else(|e| pgrx::error!("queue.send: {e}"))
    };

    // Register a XactCallback to wake WaitLatch-based readers when this
    // transaction commits and the inserted message becomes visible.
    unsafe { crate::waiter::register_notify_after_commit(queue_name) };

    SetOfIterator::new(std::iter::once(msg_id))
}

// ---------------------------------------------------------------------------
// _send_batch (canonical batch hot path)
// ---------------------------------------------------------------------------

// The public queue.send_batch wrappers in schema.sql do validation then call this.
#[pg_extern(name = "_send_batch", schema = "queue", volatile, parallel_safe)]
fn send_batch_internal(
    queue_name: &str,
    msgs: pgrx::Array<pgrx::JsonB>,
    headers: Option<pgrx::Array<pgrx::JsonB>>,
    delay: TimestampWithTimeZone,
    sync_commit: default!(bool, true),
) -> SetOfIterator<'static, i64> {
    validate_name(queue_name);
    let qtable = q(queue_name);

    // Convert arrays to DatumWithOid before entering SPI.
    //
    // Array::into_datum() returns the existing array pointer as a Datum — O(1), no element
    // iteration. Array::type_oid() calls get_array_type(JsonB::type_oid()) for the jsonb[]
    // OID. The underlying datum lives in the function call's memory context, which SPI does
    // not reset, so the pointers remain valid across Spi::connect_mut.
    let msgs_dow: DatumWithOid<'_> = msgs.into();
    let hdrs_dow: DatumWithOid<'_> = match headers {
        Some(h) => h.into(),
        None => DatumWithOid::null_oid(<pgrx::Array<pgrx::JsonB>>::type_oid()),
    };

    let insert_sql = format!(
        "INSERT INTO {qtable} (vt, message, headers)
         SELECT $2, unnest($1::jsonb[]), unnest(coalesce($3::jsonb[], ARRAY[]::jsonb[]))
         RETURNING msg_id"
    );

    let ids = Spi::connect_mut(|client| {
        if !sync_commit {
            client.update("SET LOCAL synchronous_commit = off", None, &[])?;
        }
        let mut out = Vec::new();
        for row in client.update(&insert_sql, None, &[msgs_dow, delay.into(), hdrs_dow])? {
            if let Some(id) = row.get::<i64>(1)? {
                out.push(id);
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .unwrap_or_else(|e| pgrx::error!("queue._send_batch: {e}"));

    // Wake WaitLatch-based readers when this transaction commits.
    unsafe { crate::waiter::register_notify_after_commit(queue_name) };

    SetOfIterator::new(ids.into_iter())
}

// ---------------------------------------------------------------------------
// receive
// ---------------------------------------------------------------------------

// receive: WaitLatch + shared-memory waiter registry for push-based wakeup.
//
// queue.read (the polling-free bulk read) is implemented in PL/pgSQL — not pgrx.
// PL/pgSQL RETURN QUERY EXECUTE copies whole heap tuples once and streams them
// directly; pgrx TABLE functions must extract every datum into Rust types then
// re-encode them on return (6.7× slower single-threaded, ~46% slower end-to-end).
// pgrx wins here because WaitLatch cannot be implemented in PL/pgSQL at all.
//
// Algorithm (race-free):
//   1. WaiterGuard::new — register this backend's latch in the shared registry.
//      Unregisters on drop (normal return, panic, or query-cancel unwind).
//   2. Loop:
//      a. ResetLatch(MyLatch) — must precede the read attempt so any SetLatch
//         that arrives during the SPI call is not missed: the latch is set and
//         WaitLatch returns immediately on the next iteration.
//      b. Try to read messages via SPI.
//      c. If found, break.
//      d. If deadline passed, break.
//      e. WaitLatch(WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, remaining_ms).
//         Wakes when a sender's XactCallback fires SetLatch after commit, when
//         the deadline elapses, or when the postmaster dies.
//      f. ProcessInterrupts() — honour Ctrl+C / statement_timeout.
#[pg_extern(name = "receive", schema = "queue", volatile)]
fn receive_fn(
    queue_name: &str,
    vt: i32,
    qty: i32,
    max_poll_seconds: default!(i32, 5),
    poll_interval_ms: default!(i32, 100),
    conditional: default!(pgrx::JsonB, "'{}'::jsonb"),
) -> TableIterator<
    'static,
    (
        name!(msg_id, i64),
        name!(read_ct, i32),
        name!(enqueued_at, TimestampWithTimeZone),
        name!(last_read_at, Option<TimestampWithTimeZone>),
        name!(vt, TimestampWithTimeZone),
        name!(message, Option<pgrx::JsonB>),
        name!(headers, Option<pgrx::JsonB>),
    ),
> {
    validate_name(queue_name);
    let qtable = q(queue_name);
    let cond_val: serde_json::Value = conditional.0;
    let is_empty_cond = cond_val == serde_json::Value::Object(serde_json::Map::new());

    // Two SQL variants for the same reason as read(): parameterized SPI uses generic
    // plans where the conditional parameter can't be simplified at planning time.
    // Embed qty and vt as literals — same reasoning as read(): generic plans with
    // LIMIT $1 degrade SKIP LOCKED throughput under concurrent readers.
    let read_sql_simple = format!(
        "WITH cte AS (
            SELECT msg_id FROM {qtable}
            WHERE vt <= clock_timestamp()
            LIMIT {qty}
            FOR UPDATE SKIP LOCKED
        )
        UPDATE {qtable} m
        SET last_read_at = clock_timestamp(),
            vt           = clock_timestamp() + make_interval(secs => {vt}),
            read_ct      = read_ct + 1
        FROM cte WHERE m.msg_id = cte.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers"
    );
    let read_sql_cond = format!(
        "WITH cte AS (
            SELECT msg_id FROM {qtable}
            WHERE vt <= clock_timestamp()
              AND (message @> $1::jsonb)
            LIMIT {qty}
            FOR UPDATE SKIP LOCKED
        )
        UPDATE {qtable} m
        SET last_read_at = clock_timestamp(),
            vt           = clock_timestamp() + make_interval(secs => {vt}),
            read_ct      = read_ct + 1
        FROM cte WHERE m.msg_id = cte.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers"
    );

    // Register in the waiter registry before the first read attempt.
    // If the extension is not in shared_preload_libraries, the guard is a no-op
    // and polling continues via WL_TIMEOUT only.
    let _waiter = unsafe { crate::waiter::WaiterGuard::new(queue_name) };

    let deadline_ms = max_poll_seconds as i64 * 1000;
    let start = std::time::Instant::now();

    let rows = loop {
        // ResetLatch before the read so any notify arriving during SPI will be caught:
        // the signal handler sets MyLatch, and WaitLatch returns immediately next iteration.
        unsafe { pgrx::pg_sys::ResetLatch(pgrx::pg_sys::MyLatch) };

        let rows = if is_empty_cond {
            Spi::connect_mut(|client| collect_msg_rows(client, &read_sql_simple, &[]))
                .unwrap_or_else(|e: spi::Error| pgrx::error!("queue.receive: {e}"))
        } else {
            Spi::connect_mut(|client| {
                collect_msg_rows(
                    client,
                    &read_sql_cond,
                    &[pgrx::JsonB(cond_val.clone()).into()],
                )
            })
            .unwrap_or_else(|e: spi::Error| pgrx::error!("queue.receive: {e}"))
        };

        if !rows.is_empty() {
            break rows;
        }

        let elapsed_ms = start.elapsed().as_millis() as i64;
        let remaining_ms = deadline_ms - elapsed_ms;
        if remaining_ms <= 0 {
            break Vec::new();
        }

        // Cap each wait at poll_interval_ms so we don't sleep past the deadline
        // when max_poll_seconds is large but poll_interval_ms is small.
        let wait_ms = remaining_ms.min(poll_interval_ms as i64);

        unsafe {
            pgrx::pg_sys::WaitLatch(
                pgrx::pg_sys::MyLatch,
                (pgrx::pg_sys::WL_LATCH_SET
                    | pgrx::pg_sys::WL_TIMEOUT
                    | pgrx::pg_sys::WL_EXIT_ON_PM_DEATH) as i32,
                wait_ms,
                pgrx::pg_sys::PG_WAIT_IPC,
            );
            // Honour query cancel, statement_timeout, die signals.
            pgrx::pg_sys::ProcessInterrupts();
        }
    };

    // _waiter drops here, unregistering from the waiter registry.
    TableIterator::new(rows.into_iter())
}

// ---------------------------------------------------------------------------
// send_fifo
// ---------------------------------------------------------------------------

// Canonical FIFO hot path: inserts with explicit message_group_id and
// deduplication_id. Short overloads in schema.sql call this.
//
// Fires the same WaitLatch XactCallback as send_full so receive_fifo
// wakes on commit rather than spinning on poll_interval_ms.
#[pg_extern(name = "send_fifo", schema = "queue", volatile, parallel_safe)]
fn send_fifo_full(
    queue_name: &str,
    msg: pgrx::JsonB,
    message_group_id: &str,
    deduplication_id: Option<&str>,
    headers: Option<pgrx::JsonB>,
    delay: TimestampWithTimeZone,
    sync_commit: default!(bool, true),
) -> SetOfIterator<'static, i64> {
    validate_name(queue_name);
    let qtable = q(queue_name);
    let insert_sql = format!(
        "INSERT INTO {qtable} (vt, message, headers, message_group_id, deduplication_id)
         VALUES ($1, $2, $3, $4, $5) RETURNING msg_id"
    );
    let ids = Spi::connect_mut(|client| {
        if !sync_commit {
            client.update("SET LOCAL synchronous_commit = off", None, &[])?;
        }
        let mut out = Vec::new();
        for row in client.update(
            &insert_sql,
            None,
            &[
                delay.into(),
                msg.into(),
                headers.into(),
                message_group_id.into(),
                deduplication_id.into(),
            ],
        )? {
            if let Some(id) = row.get::<i64>(1)? {
                out.push(id);
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .unwrap_or_else(|e| pgrx::error!("queue.send_fifo: {e}"));

    unsafe { crate::waiter::register_notify_after_commit(queue_name) };

    SetOfIterator::new(ids.into_iter())
}

// ---------------------------------------------------------------------------
// receive_fifo
// ---------------------------------------------------------------------------

// WaitLatch-based FIFO read.  Mirrors receive but uses the
// BOOL_AND eligible_group CTE for correct FIFO group serialization.
//
// qty and vt are embedded as format-string literals for the same reason as
// receive: generic plans with LIMIT $N degrade SKIP LOCKED throughput.
#[pg_extern(name = "receive_fifo", schema = "queue", volatile)]
fn receive_fifo_fn(
    queue_name: &str,
    vt: i32,
    qty: i32,
    max_poll_seconds: default!(i32, 5),
    poll_interval_ms: default!(i32, 100),
) -> TableIterator<
    'static,
    (
        name!(msg_id, i64),
        name!(read_ct, i32),
        name!(enqueued_at, TimestampWithTimeZone),
        name!(last_read_at, Option<TimestampWithTimeZone>),
        name!(vt, TimestampWithTimeZone),
        name!(message, Option<pgrx::JsonB>),
        name!(headers, Option<pgrx::JsonB>),
    ),
> {
    validate_name(queue_name);
    let qtable = q(queue_name);

    let read_sql = format!(
        "WITH eligible_group AS MATERIALIZED (
            SELECT message_group_id
            FROM {qtable}
            GROUP BY message_group_id
            HAVING BOOL_AND(vt <= clock_timestamp())
            ORDER BY MIN(msg_id) ASC
            LIMIT 1
        ),
        cte AS (
            SELECT m.msg_id
            FROM {qtable} m
            WHERE m.message_group_id = (SELECT message_group_id FROM eligible_group)
              AND m.vt <= clock_timestamp()
            ORDER BY m.msg_id ASC
            LIMIT {qty}
            FOR UPDATE SKIP LOCKED
        )
        UPDATE {qtable} m
        SET last_read_at = clock_timestamp(),
            vt           = clock_timestamp() + make_interval(secs => {vt}),
            read_ct      = read_ct + 1
        FROM cte WHERE m.msg_id = cte.msg_id
        RETURNING m.msg_id, m.read_ct, m.enqueued_at, m.last_read_at, m.vt, m.message, m.headers"
    );

    let _waiter = unsafe { crate::waiter::WaiterGuard::new(queue_name) };

    let deadline_ms = max_poll_seconds as i64 * 1000;
    let start = std::time::Instant::now();

    let rows = loop {
        unsafe { pgrx::pg_sys::ResetLatch(pgrx::pg_sys::MyLatch) };

        let rows = Spi::connect_mut(|client| collect_msg_rows(client, &read_sql, &[]))
            .unwrap_or_else(|e: spi::Error| pgrx::error!("queue.receive_fifo: {e}"));

        if !rows.is_empty() {
            break rows;
        }

        let elapsed_ms = start.elapsed().as_millis() as i64;
        let remaining_ms = deadline_ms - elapsed_ms;
        if remaining_ms <= 0 {
            break Vec::new();
        }

        let wait_ms = remaining_ms.min(poll_interval_ms as i64);

        unsafe {
            pgrx::pg_sys::WaitLatch(
                pgrx::pg_sys::MyLatch,
                (pgrx::pg_sys::WL_LATCH_SET
                    | pgrx::pg_sys::WL_TIMEOUT
                    | pgrx::pg_sys::WL_EXIT_ON_PM_DEATH) as i32,
                wait_ms,
                pgrx::pg_sys::PG_WAIT_IPC,
            );
            pgrx::pg_sys::ProcessInterrupts();
        }
    };

    TableIterator::new(rows.into_iter())
}

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

#[pg_extern(name = "delete", schema = "queue", volatile, parallel_safe)]
fn delete_single(queue_name: &str, msg_id: i64) -> bool {
    validate_name(queue_name);
    let qtable = q(queue_name);
    let sql = format!("DELETE FROM {qtable} WHERE msg_id = $1 RETURNING msg_id");

    Spi::connect_mut(|client| {
        let found = client
            .update(&sql, None, &[msg_id.into()])?
            .first()
            .get::<i64>(1)?
            .is_some();
        Ok::<_, spi::Error>(found)
    })
    .unwrap_or_else(|e| pgrx::error!("queue.delete: {e}"))
}

#[pg_extern(name = "delete", schema = "queue", volatile, parallel_safe)]
fn delete_batch(queue_name: &str, msg_ids: pgrx::Array<i64>) -> SetOfIterator<'static, i64> {
    validate_name(queue_name);
    let qtable = q(queue_name);

    // Collect before Spi::connect_mut — Array borrows PostgreSQL memory.
    let ids: Vec<Option<i64>> = msg_ids.iter().collect();

    let sql = format!("DELETE FROM {qtable} WHERE msg_id = ANY($1::bigint[]) RETURNING msg_id");

    let deleted = Spi::connect_mut(|client| {
        let mut out = Vec::new();
        for row in client.update(&sql, None, &[ids.into()])? {
            if let Some(id) = row.get::<i64>(1)? {
                out.push(id);
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .unwrap_or_else(|e| pgrx::error!("queue.delete: {e}"));

    SetOfIterator::new(deleted.into_iter())
}

// ---------------------------------------------------------------------------
// archive
// ---------------------------------------------------------------------------

#[pg_extern(name = "archive", schema = "queue", volatile, parallel_safe)]
fn archive_single(queue_name: &str, msg_id: i64) -> bool {
    validate_name(queue_name);
    let qtable = q(queue_name);
    let atable = a(queue_name);

    let sql = format!(
        "WITH archived AS (
            DELETE FROM {qtable} WHERE msg_id = $1
            RETURNING msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
        )
        INSERT INTO {atable} (msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers)
        SELECT msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers FROM archived
        RETURNING msg_id"
    );

    Spi::connect_mut(|client| {
        let found = client
            .update(&sql, None, &[msg_id.into()])?
            .first()
            .get::<i64>(1)?
            .is_some();
        Ok::<_, spi::Error>(found)
    })
    .unwrap_or_else(|e| pgrx::error!("queue.archive: {e}"))
}

#[pg_extern(name = "archive", schema = "queue", volatile, parallel_safe)]
fn archive_batch(queue_name: &str, msg_ids: pgrx::Array<i64>) -> SetOfIterator<'static, i64> {
    validate_name(queue_name);
    let qtable = q(queue_name);
    let atable = a(queue_name);

    let ids: Vec<Option<i64>> = msg_ids.iter().collect();

    let sql = format!(
        "WITH archived AS (
            DELETE FROM {qtable} WHERE msg_id = ANY($1::bigint[])
            RETURNING msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers
        )
        INSERT INTO {atable} (msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers)
        SELECT msg_id, vt, read_ct, enqueued_at, last_read_at, message, headers FROM archived
        RETURNING msg_id"
    );

    let archived = Spi::connect_mut(|client| {
        let mut out = Vec::new();
        for row in client.update(&sql, None, &[ids.into()])? {
            if let Some(id) = row.get::<i64>(1)? {
                out.push(id);
            }
        }
        Ok::<_, spi::Error>(out)
    })
    .unwrap_or_else(|e| pgrx::error!("queue.archive: {e}"));

    SetOfIterator::new(archived.into_iter())
}

// ---------------------------------------------------------------------------
// pop
// ---------------------------------------------------------------------------

#[pg_extern(name = "pop", schema = "queue", volatile, parallel_safe)]
fn pop(
    queue_name: &str,
    qty: default!(i32, 1),
) -> TableIterator<
    'static,
    (
        name!(msg_id, i64),
        name!(read_ct, i32),
        name!(enqueued_at, TimestampWithTimeZone),
        name!(last_read_at, Option<TimestampWithTimeZone>),
        name!(vt, TimestampWithTimeZone),
        name!(message, Option<pgrx::JsonB>),
        name!(headers, Option<pgrx::JsonB>),
    ),
> {
    validate_name(queue_name);
    let qtable = q(queue_name);

    let sql = format!(
        "WITH cte AS (
            SELECT msg_id FROM {qtable}
            WHERE vt <= clock_timestamp()
            LIMIT $1
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM {qtable}
        WHERE msg_id IN (SELECT msg_id FROM cte)
        RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers"
    );

    let rows = Spi::connect_mut(|client| collect_msg_rows(client, &sql, &[qty.into()]))
        .unwrap_or_else(|e| pgrx::error!("queue.pop: {e}"));

    TableIterator::new(rows.into_iter())
}

// ---------------------------------------------------------------------------
// change_visibility
// ---------------------------------------------------------------------------

#[pg_extern(name = "change_visibility", schema = "queue", volatile, parallel_safe)]
fn change_visibility_ts(
    queue_name: &str,
    msg_id: i64,
    vt: TimestampWithTimeZone,
) -> TableIterator<
    'static,
    (
        name!(msg_id, i64),
        name!(read_ct, i32),
        name!(enqueued_at, TimestampWithTimeZone),
        name!(last_read_at, Option<TimestampWithTimeZone>),
        name!(vt, TimestampWithTimeZone),
        name!(message, Option<pgrx::JsonB>),
        name!(headers, Option<pgrx::JsonB>),
    ),
> {
    validate_name(queue_name);
    let qtable = q(queue_name);

    let sql = format!(
        "UPDATE {qtable} SET vt = $1 WHERE msg_id = $2
         RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers"
    );

    let rows =
        Spi::connect_mut(|client| collect_msg_rows(client, &sql, &[vt.into(), msg_id.into()]))
            .unwrap_or_else(|e| pgrx::error!("queue.change_visibility: {e}"));

    TableIterator::new(rows.into_iter())
}

#[pg_extern(name = "change_visibility", schema = "queue", volatile, parallel_safe)]
fn change_visibility_secs(
    queue_name: &str,
    msg_id: i64,
    vt: i32,
) -> TableIterator<
    'static,
    (
        name!(msg_id, i64),
        name!(read_ct, i32),
        name!(enqueued_at, TimestampWithTimeZone),
        name!(last_read_at, Option<TimestampWithTimeZone>),
        name!(vt, TimestampWithTimeZone),
        name!(message, Option<pgrx::JsonB>),
        name!(headers, Option<pgrx::JsonB>),
    ),
> {
    validate_name(queue_name);
    let qtable = q(queue_name);

    let sql = format!(
        "UPDATE {qtable}
         SET vt = clock_timestamp() + make_interval(secs => $1)
         WHERE msg_id = $2
         RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers"
    );

    let rows =
        Spi::connect_mut(|client| collect_msg_rows(client, &sql, &[vt.into(), msg_id.into()]))
            .unwrap_or_else(|e| pgrx::error!("queue.change_visibility: {e}"));

    TableIterator::new(rows.into_iter())
}

#[pg_extern(name = "change_visibility", schema = "queue", volatile, parallel_safe)]
fn change_visibility_batch_ts(
    queue_name: &str,
    msg_ids: pgrx::Array<i64>,
    vt: TimestampWithTimeZone,
) -> TableIterator<
    'static,
    (
        name!(msg_id, i64),
        name!(read_ct, i32),
        name!(enqueued_at, TimestampWithTimeZone),
        name!(last_read_at, Option<TimestampWithTimeZone>),
        name!(vt, TimestampWithTimeZone),
        name!(message, Option<pgrx::JsonB>),
        name!(headers, Option<pgrx::JsonB>),
    ),
> {
    validate_name(queue_name);
    let qtable = q(queue_name);
    let ids: Vec<Option<i64>> = msg_ids.iter().collect();

    let sql = format!(
        "UPDATE {qtable} SET vt = $1 WHERE msg_id = ANY($2::bigint[])
         RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers"
    );

    let rows = Spi::connect_mut(|client| collect_msg_rows(client, &sql, &[vt.into(), ids.into()]))
        .unwrap_or_else(|e| pgrx::error!("queue.change_visibility: {e}"));

    TableIterator::new(rows.into_iter())
}

// ---------------------------------------------------------------------------
// send_topic  (pgrx hot path — replaces PL/pgSQL loop in schema.sql)
// ---------------------------------------------------------------------------

fn validate_routing_key(routing_key: &str) {
    if routing_key.is_empty() {
        pgrx::error!("routing_key cannot be NULL or empty");
    }
    if routing_key.len() > 255 {
        pgrx::error!("routing_key length cannot exceed 255 characters");
    }
    if !routing_key
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        pgrx::error!(
            "routing_key contains invalid characters. Got: {}",
            routing_key
        );
    }
    if routing_key.starts_with('.') {
        pgrx::error!("routing_key cannot start with a dot");
    }
    if routing_key.ends_with('.') {
        pgrx::error!("routing_key cannot end with a dot");
    }
    if routing_key.contains("..") {
        pgrx::error!("routing_key cannot contain consecutive dots");
    }
}

/// Fanout a single message to every queue whose binding matches `routing_key`.
/// One SPI session for routing + N inserts. msg/headers are converted to DatumWithOid
/// once before SPI, so the JSONB datum passes through without per-queue re-serialization —
/// unlike the PL/pgSQL path which calls queue.send() N times and pays a full
/// datum→serde_json→datum round-trip per queue.
#[pg_extern(name = "send_topic", schema = "queue", volatile, parallel_safe)]
fn send_topic_pgrx(
    routing_key: &str,
    msg: pgrx::JsonB,
    headers: Option<pgrx::JsonB>,
    delay: TimestampWithTimeZone,
) -> i32 {
    validate_routing_key(routing_key);

    // Convert to DatumWithOid before SPI — avoids per-queue JSON re-serialization.
    let msg_dow: DatumWithOid<'_> = msg.into();
    let hdr_dow: DatumWithOid<'_> = match headers {
        Some(h) => h.into(),
        None => DatumWithOid::null_oid(<pgrx::JsonB>::type_oid()),
    };

    let queue_names: Vec<String> = Spi::connect_mut(|client| {
        // Collect routing results first (exhausts the SPI cursor before inserting).
        let names: Vec<String> = client
            .select(
                "SELECT DISTINCT queue_name FROM queue.topic_bindings \
                 WHERE $1 ~ compiled_regex ORDER BY queue_name",
                None,
                &[routing_key.into()],
            )?
            .filter_map(|row| row.get::<&str>(1).ok().flatten().map(str::to_string))
            .collect();

        for name in &names {
            validate_name(name);
            let qtable = format!("queue.q_{name}");
            let sql = format!(
                "INSERT INTO {qtable} (vt, message, headers) VALUES ($1, $2, $3)"
            );
            client.update(&sql, None, &[
                delay.into(),
                // SAFETY: datum() copies the raw pg_sys::Datum (usize) without moving the DatumWithOid.
                unsafe { DatumWithOid::new_from_datum(msg_dow.datum(), msg_dow.oid()) },
                unsafe { DatumWithOid::new_from_datum(hdr_dow.datum(), hdr_dow.oid()) },
            ])?;
        }

        Ok::<_, spi::Error>(names)
    })
    .unwrap_or_else(|e| pgrx::error!("queue.send_topic: {e}"));

    for name in &queue_names {
        unsafe { crate::waiter::register_notify_after_commit(name) };
    }

    queue_names.len() as i32
}

// ---------------------------------------------------------------------------
// send_batch_topic  (pgrx hot path — replaces PL/pgSQL loop in schema.sql)
// ---------------------------------------------------------------------------

/// Fanout a batch of messages to every queue whose binding matches `routing_key`.
/// One SPI session: one routing scan + N inserts — no per-queue Spi::connect overhead.
#[pg_extern(name = "send_batch_topic", schema = "queue", volatile, parallel_safe)]
fn send_batch_topic_pgrx(
    routing_key: &str,
    msgs: pgrx::Array<pgrx::JsonB>,
    headers: Option<pgrx::Array<pgrx::JsonB>>,
    delay: TimestampWithTimeZone,
) -> TableIterator<
    'static,
    (
        name!(queue_name, String),
        name!(msg_id, i64),
    ),
> {
    validate_routing_key(routing_key);

    // Convert arrays to DatumWithOid before Spi::connect — arrays borrow PG memory
    // that cannot cross the SPI connection boundary.
    let msgs_dow: DatumWithOid<'_> = msgs.into();
    let hdrs_dow: DatumWithOid<'_> = match headers {
        Some(h) => h.into(),
        None => DatumWithOid::null_oid(<pgrx::Array<pgrx::JsonB>>::type_oid()),
    };

    let (queue_names, rows) = Spi::connect_mut(|client| {
        let queue_names: Vec<String> = {
            let mut names = Vec::new();
            for row in client.select(
                "SELECT DISTINCT queue_name FROM queue.topic_bindings \
                 WHERE $1 ~ compiled_regex ORDER BY queue_name",
                None,
                &[routing_key.into()],
            )? {
                if let Some(name) = row.get::<&str>(1)? {
                    names.push(name.to_string());
                }
            }
            names
        };

        let mut out: Vec<(String, i64)> = Vec::new();
        for name in &queue_names {
            validate_name(name);
            let qtable = format!("queue.q_{name}");
            let sql = format!(
                "INSERT INTO {qtable} (vt, message, headers) \
                 SELECT $1, unnest($2::jsonb[]), unnest(coalesce($3::jsonb[], ARRAY[]::jsonb[])) \
                 RETURNING msg_id"
            );
            for row in client.update(&sql, None, &[
                    delay.into(),
                    // SAFETY: datum() copies the raw pg_sys::Datum (usize) without moving msgs_dow,
                    // so we can reconstruct per iteration. new_from_datum just stores datum + oid.
                    unsafe { DatumWithOid::new_from_datum(msgs_dow.datum(), msgs_dow.oid()) },
                    unsafe { DatumWithOid::new_from_datum(hdrs_dow.datum(), hdrs_dow.oid()) },
                ])? {
                if let Some(id) = row.get::<i64>(1)? {
                    out.push((name.clone(), id));
                }
            }
        }

        Ok::<_, spi::Error>((queue_names, out))
    })
    .unwrap_or_else(|e| pgrx::error!("queue.send_batch_topic: {e}"));

    for name in &queue_names {
        unsafe { crate::waiter::register_notify_after_commit(name) };
    }

    TableIterator::new(rows.into_iter())
}

#[pg_extern(name = "change_visibility", schema = "queue", volatile, parallel_safe)]
fn change_visibility_batch_secs(
    queue_name: &str,
    msg_ids: pgrx::Array<i64>,
    vt: i32,
) -> TableIterator<
    'static,
    (
        name!(msg_id, i64),
        name!(read_ct, i32),
        name!(enqueued_at, TimestampWithTimeZone),
        name!(last_read_at, Option<TimestampWithTimeZone>),
        name!(vt, TimestampWithTimeZone),
        name!(message, Option<pgrx::JsonB>),
        name!(headers, Option<pgrx::JsonB>),
    ),
> {
    validate_name(queue_name);
    let qtable = q(queue_name);
    let ids: Vec<Option<i64>> = msg_ids.iter().collect();

    let sql = format!(
        "UPDATE {qtable}
         SET vt = clock_timestamp() + make_interval(secs => $1)
         WHERE msg_id = ANY($2::bigint[])
         RETURNING msg_id, read_ct, enqueued_at, last_read_at, vt, message, headers"
    );

    let rows = Spi::connect_mut(|client| collect_msg_rows(client, &sql, &[vt.into(), ids.into()]))
        .unwrap_or_else(|e| pgrx::error!("queue.change_visibility: {e}"));

    TableIterator::new(rows.into_iter())
}
