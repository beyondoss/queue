//! Schedule worker — polls `queue.schedule` for due rows and fires them.
//!
//! Mirrors `ops::delivery` in shape: pure polling loop, no LISTEN/NOTIFY,
//! configured via env, gracefully aborted on shutdown. Always on; an
//! empty schedule table costs one partial-index probe per poll.
//!
//! Per-row failure isolation via `SAVEPOINT`: an individual dispatch
//! failure (target queue missing, malformed payload) rolls back only
//! that row and increments `consecutive_failures` / `last_error` on the
//! outer transaction. After `failure_threshold` consecutive failures the
//! row is paused.
//!
//! See `SCHEDULES.md` § "Worker lifecycle" and § "Server primitives".

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction};
use tokio::task::JoinHandle;

use crate::metrics::Metrics;
use crate::ops::schedule::{self, ScheduleRow};
use crate::schedule::expression::{Canonical, Expression};

pub struct ScheduleWorkerConfig {
    pub poll_interval_ms: u64,
    pub batch_size: i64,
}

impl Default for ScheduleWorkerConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 1000,
            batch_size: 32,
        }
    }
}

pub fn start(pool: PgPool, config: ScheduleWorkerConfig, _metrics: Arc<Metrics>) -> JoinHandle<()> {
    tokio::spawn(run(pool, config))
}

async fn run(pool: PgPool, config: ScheduleWorkerConfig) {
    loop {
        match poll_once(&pool, &config).await {
            Ok(fired) => {
                if fired == 0 {
                    tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
                }
                // If we fired any, loop immediately — there may be more.
            }
            Err(e) => {
                tracing::error!(error = %e, "schedule worker poll failed");
                tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
            }
        }
    }
}

/// One poll cycle: claim due rows, fire each in a savepoint, commit.
/// Returns the number of rows processed.
async fn poll_once(pool: &PgPool, config: &ScheduleWorkerConfig) -> anyhow::Result<usize> {
    let mut tx = pool.begin().await?;

    let rows = sqlx::query_as!(
        ScheduleRow,
        r#"
        SELECT
            name,
            expression,
            cron,
            fire_at,
            timezone,
            jitter_secs,
            catchup,
            catchup_limit,
            failure_threshold,
            target_kind::TEXT AS "target_kind!",
            target_name,
            payload,
            headers,
            status::TEXT AS "status!",
            next_fire_at,
            last_fired_at,
            last_error,
            consecutive_failures,
            fire_count,
            created_at,
            updated_at
        FROM queue.schedule
        WHERE status = 'active' AND next_fire_at <= now()
        ORDER BY next_fire_at
        LIMIT $1
        FOR UPDATE SKIP LOCKED
        "#,
        config.batch_size,
    )
    .fetch_all(&mut *tx)
    .await?;

    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(0);
    }

    let count = rows.len();
    let now = Utc::now();

    for (idx, row) in rows.iter().enumerate() {
        let sp = format!("sp_{idx}");
        sqlx::query(&format!("SAVEPOINT {sp}"))
            .execute(&mut *tx)
            .await?;

        match fire_one(&mut tx, row, now).await {
            Ok(()) => {
                sqlx::query(&format!("RELEASE SAVEPOINT {sp}"))
                    .execute(&mut *tx)
                    .await?;
            }
            Err(e) => {
                tracing::warn!(
                    schedule = %row.name,
                    error = %e,
                    "schedule fire failed; rolling back row"
                );
                sqlx::query(&format!("ROLLBACK TO SAVEPOINT {sp}"))
                    .execute(&mut *tx)
                    .await?;
                record_failure(&mut tx, &row.name, row.failure_threshold, &e.to_string()).await?;
            }
        }
    }

    tx.commit().await?;
    Ok(count)
}

/// Fire a single schedule: emit one or more messages (catchup) and
/// advance / delete the row. Runs inside a savepoint owned by the caller.
async fn fire_one(
    tx: &mut Transaction<'_, Postgres>,
    row: &ScheduleRow,
    now: DateTime<Utc>,
) -> anyhow::Result<()> {
    let canonical = canonical_for(row)?;
    let fires = compute_fires(row, &canonical, now);

    for scheduled_for in &fires {
        let merged_headers =
            schedule::merge_schedule_headers(row.headers.clone(), &row.name, *scheduled_for, false);
        dispatch_in_tx(
            tx,
            &row.target_kind,
            &row.target_name,
            &row.payload_or_null(),
            merged_headers,
        )
        .await?;
    }

    // One-shot rows are deleted; recurring rows advance.
    let advance_to = canonical.next_after(now);
    match advance_to {
        None if row.fire_at.is_some() => {
            sqlx::query!("DELETE FROM queue.schedule WHERE name = $1", row.name)
                .execute(&mut **tx)
                .await?;
        }
        Some(next) => {
            let new_error: Option<String> = catchup_overflowed(row, &canonical, now)
                .map(|over| format!("catchup_limit_exceeded: {over} fires skipped"));
            sqlx::query!(
                r#"
                UPDATE queue.schedule
                SET next_fire_at         = $2,
                    last_fired_at        = $3,
                    fire_count           = fire_count + $4,
                    consecutive_failures = 0,
                    last_error           = $5,
                    updated_at           = now()
                WHERE name = $1
                "#,
                row.name,
                next,
                now,
                fires.len() as i64,
                new_error,
            )
            .execute(&mut **tx)
            .await?;
        }
        None => {
            // Recurring with no future occurrence (e.g. fixed-window cron whose
            // window has fully passed). Pause it so an operator can investigate.
            sqlx::query!(
                r#"
                UPDATE queue.schedule
                SET status = 'paused',
                    last_error = 'no future occurrence',
                    updated_at = now()
                WHERE name = $1
                "#,
                row.name,
            )
            .execute(&mut **tx)
            .await?;
        }
    }

    Ok(())
}

/// Determine which timestamps to fire on this poll cycle.
///
/// - Always fire the due `next_fire_at`.
/// - If `catchup` is true, additionally fire each missed occurrence
///   between `next_fire_at` and `now`, bounded by `catchup_limit`.
/// - If `catchup` is false, skip those missed occurrences entirely;
///   the next iteration will see the advanced `next_fire_at`.
fn compute_fires(
    row: &ScheduleRow,
    canonical: &Canonical,
    now: DateTime<Utc>,
) -> Vec<DateTime<Utc>> {
    let mut fires = vec![row.next_fire_at];
    if !row.catchup {
        return fires;
    }
    let mut cursor = canonical.next_after(row.next_fire_at);
    while let Some(t) = cursor {
        if t > now || fires.len() as i32 >= row.catchup_limit {
            break;
        }
        fires.push(t);
        cursor = canonical.next_after(t);
    }
    fires
}

/// Returns the number of missed fires that were skipped because catchup_limit was hit.
fn catchup_overflowed(
    row: &ScheduleRow,
    canonical: &Canonical,
    now: DateTime<Utc>,
) -> Option<usize> {
    if !row.catchup {
        return None;
    }
    // Count occurrences from `next_fire_at` (inclusive) up to `now` (inclusive)
    // and compare with what we actually fired.
    let total = {
        let mut count = 1usize; // include next_fire_at itself
        let mut cursor = canonical.next_after(row.next_fire_at);
        while let Some(t) = cursor {
            if t > now {
                break;
            }
            count += 1;
            cursor = canonical.next_after(t);
            // Cap exploration to avoid runaway when an outage spans years.
            if count > (row.catchup_limit as usize) + 1000 {
                break;
            }
        }
        count
    };
    let fired = (row.catchup_limit as usize).min(total);
    let skipped = total.saturating_sub(fired);
    (skipped > 0).then_some(skipped)
}

fn canonical_for(row: &ScheduleRow) -> anyhow::Result<Canonical> {
    let expr = if let Some(c) = &row.cron {
        Expression::Cron(c.clone())
    } else if let Some(fa) = row.fire_at {
        Expression::FireAt(fa)
    } else {
        anyhow::bail!("schedule '{}' has neither cron nor fire_at", row.name);
    };
    expr.canonicalize(&row.timezone)
        .map_err(|e| anyhow::anyhow!("canonicalize '{}': {e}", row.name))
}

/// Dispatch one fire inside the worker's outer transaction. Mirrors
/// `ops::schedule::dispatch` but takes a transaction rather than a pool.
async fn dispatch_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    target_kind: &str,
    target_name: &str,
    payload: &serde_json::Value,
    headers: serde_json::Value,
) -> anyhow::Result<Vec<i64>> {
    match target_kind {
        "queue" => {
            let row = sqlx::query!(
                r#"SELECT queue.send($1, $2::jsonb, $3::jsonb, clock_timestamp(), true) AS "msg_id!: i64""#,
                target_name,
                payload,
                Some(headers),
            )
            .fetch_one(&mut **tx)
            .await?;
            Ok(vec![row.msg_id])
        }
        "topic" => {
            let rows = sqlx::query!(
                r#"SELECT msg_id AS "msg_id!"
                   FROM queue.publish_event($1, $2::jsonb, $3::jsonb, 0::integer)"#,
                target_name,
                payload,
                Some(headers),
            )
            .fetch_all(&mut **tx)
            .await?;
            Ok(rows.into_iter().map(|r| r.msg_id).collect())
        }
        "workflow" => {
            anyhow::bail!("workflow targets are not yet supported")
        }
        other => anyhow::bail!("unknown target_kind: {other}"),
    }
}

/// Increment consecutive_failures, set last_error, and pause if threshold exceeded.
/// Runs outside the failed savepoint, on the still-open outer transaction.
async fn record_failure(
    tx: &mut Transaction<'_, Postgres>,
    name: &str,
    failure_threshold: i32,
    error_msg: &str,
) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
        UPDATE queue.schedule
        SET consecutive_failures = consecutive_failures + 1,
            last_error           = $2,
            status               = CASE
                WHEN consecutive_failures + 1 >= $3 THEN 'paused'::queue.schedule_status
                ELSE status
            END,
            updated_at           = now()
        WHERE name = $1
        "#,
        name,
        error_msg,
        failure_threshold,
    )
    .execute(&mut **tx)
    .await?;
    Ok(())
}
