use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use reqwest::Client;
use sqlx::PgPool;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::Instrument as _;

use crate::metrics::Metrics;

/// When no delivery is pending, park this long before a defensive re-probe. New
/// deliveries are signalled in-process via `notify`, so this is only a backstop;
/// it is deliberately longer than the instance light-sleep window so an idle VM
/// sleeps in the gap rather than waking itself to poll.
const IDLE_PARK: Duration = Duration::from_secs(3600);

pub struct DeliveryConfig {
    pub poll_interval_ms: u64,
    pub delivery_timeout_secs: u64,
    pub batch_size: i64,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: 1000,
            delivery_timeout_secs: 5,
            batch_size: 50,
        }
    }
}

pub fn start(
    pool: PgPool,
    config: DeliveryConfig,
    metrics: Arc<Metrics>,
    notify: Arc<Notify>,
) -> anyhow::Result<JoinHandle<()>> {
    let client = Client::builder()
        .timeout(Duration::from_secs(config.delivery_timeout_secs))
        .build()?;
    Ok(tokio::spawn(run(pool, client, config, metrics, notify)))
}

/// Event-driven delivery loop. Drains all due deliveries, then sleeps until the
/// earliest pending retry (or parks if none) — woken early by `notify` when a
/// publish inserts new deliveries. When the table is empty it generates no
/// traffic, letting the VM (and Postgres) scale to zero.
async fn run(
    pool: PgPool,
    client: Client,
    config: DeliveryConfig,
    metrics: Arc<Metrics>,
    notify: Arc<Notify>,
) {
    loop {
        match deliver_batch(&pool, &client, &config, &metrics).await {
            // A full-ish batch may mean more is due right now — loop immediately.
            Ok(n) if n > 0 => continue,
            // Nothing due — fall through to wait for the next due time / a poke.
            Ok(_) => {}
            Err(e) => {
                tracing::error!(error = %e, "http delivery batch error");
                wait_or_notified(Duration::from_millis(config.poll_interval_ms), &notify).await;
                continue;
            }
        }

        let wait = match earliest_pending_in(&pool).await {
            Ok(Some(secs)) => Duration::from_secs_f64(secs.max(0.0)),
            Ok(None) => IDLE_PARK,
            Err(e) => {
                tracing::warn!(error = %e, "delivery next-due probe failed");
                Duration::from_millis(config.poll_interval_ms)
            }
        };
        wait_or_notified(wait, &notify).await;
    }
}

/// Sleep for `dur`, returning early if `notify` is poked.
async fn wait_or_notified(dur: Duration, notify: &Notify) {
    tokio::select! {
        _ = tokio::time::sleep(dur) => {}
        _ = notify.notified() => {}
    }
}

/// Seconds until the earliest deliverable row becomes due (`None` if there are
/// no rows with attempts remaining). Negative values mean already-due.
async fn earliest_pending_in(pool: &PgPool) -> anyhow::Result<Option<f64>> {
    let row = sqlx::query!(
        r#"SELECT EXTRACT(EPOCH FROM (MIN(next_attempt_at) - now()))::float8 AS "secs"
           FROM queue.event_deliveries
           WHERE attempt < max_attempts"#
    )
    .fetch_one(pool)
    .await?;
    Ok(row.secs)
}

async fn deliver_batch(
    pool: &PgPool,
    client: &Client,
    config: &DeliveryConfig,
    metrics: &Metrics,
) -> anyhow::Result<usize> {
    // Phase 1: claim rows in a short transaction, then commit to release locks.
    // Without this split, FOR UPDATE SKIP LOCKED holds row locks across all HTTP
    // calls — up to batch_size × timeout_secs of contention.
    let rows = {
        let mut tx = pool.begin().await?;

        let rows = sqlx::query!(
            r#"SELECT
                   id              AS "id!: i64",
                   endpoint        AS "endpoint!",
                   payload         AS "payload!: serde_json::Value",
                   attempt         AS "attempt!",
                   max_attempts    AS "max_attempts!"
               FROM queue.event_deliveries
               WHERE next_attempt_at <= now() AND attempt < max_attempts
               ORDER BY next_attempt_at ASC
               LIMIT $1
               FOR UPDATE SKIP LOCKED"#,
            config.batch_size,
        )
        .fetch_all(&mut *tx)
        .await?;

        if rows.is_empty() {
            tx.rollback().await?;
            return Ok(0);
        }

        // Lease the rows by pushing next_attempt_at beyond the delivery window.
        // If this process crashes mid-delivery, rows re-surface after the lease expires.
        let timeout_secs = i64::try_from(config.delivery_timeout_secs).unwrap_or(3600);
        let lease_until = Utc::now() + chrono::Duration::seconds(timeout_secs + 30);
        let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
        sqlx::query!(
            "UPDATE queue.event_deliveries SET next_attempt_at = $1 WHERE id = ANY($2)",
            lease_until,
            &ids as &[i64],
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        rows
    };

    // Phase 2: deliver without holding any database lock.
    for row in &rows {
        let span = tracing::info_span!(
            "delivery.attempt",
            id = row.id,
            endpoint = %row.endpoint,
            attempt = row.attempt + 1,
        );
        let t = Instant::now();
        let result = async {
            client
                .post(&row.endpoint)
                .header("content-type", "application/json")
                .header("x-amz-sns-message-type", "Notification")
                .json(&row.payload)
                .send()
                .await
        }
        .instrument(span)
        .await;
        let elapsed = t.elapsed().as_secs_f64();

        let (success, error_msg) = match result {
            Ok(resp) if resp.status().is_success() => (true, None),
            Ok(resp) => (false, Some(format!("HTTP {}", resp.status()))),
            Err(e) => (false, Some(e.to_string())),
        };

        let outcome = if success { "success" } else { "failure" };
        metrics
            .delivery_attempt_duration_seconds
            .with_label_values(&[outcome])
            .observe(elapsed);

        if success {
            if let Err(e) = sqlx::query!("DELETE FROM queue.event_deliveries WHERE id = $1", row.id)
                .execute(pool)
                .await
            {
                tracing::error!(id = row.id, error = %e, "failed to delete delivered event; will retry on next poll");
                continue;
            }
            metrics
                .delivery_attempts_total
                .with_label_values(&["success"])
                .inc();
        } else {
            let next_attempt_at = Utc::now() + backoff(row.attempt + 1);
            if let Err(e) = sqlx::query!(
                r#"UPDATE queue.event_deliveries
                   SET attempt = attempt + 1, last_error = $1, next_attempt_at = $2
                   WHERE id = $3"#,
                error_msg,
                next_attempt_at,
                row.id,
            )
            .execute(pool)
            .await
            {
                tracing::error!(id = row.id, error = %e, "failed to record delivery failure; will retry on next poll");
                continue;
            }
            metrics
                .delivery_attempts_total
                .with_label_values(&["failure"])
                .inc();
            tracing::warn!(
                id = row.id,
                endpoint = %row.endpoint,
                attempt = row.attempt + 1,
                error = error_msg,
                "http delivery failed"
            );
        }
    }

    // Phase 3: sweep rows that have exhausted all attempts. These are filtered
    // out of phase 1 by the WHERE clause but never removed, causing silent
    // accumulation. Delete and log them so failures are visible.
    let exhausted = match sqlx::query!(
        r#"DELETE FROM queue.event_deliveries
           WHERE attempt >= max_attempts
           RETURNING id AS "id!: i64", endpoint AS "endpoint!""#
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "exhausted-delivery sweep failed");
            return Ok(rows.len());
        }
    };

    for row in &exhausted {
        tracing::error!(
            id = row.id,
            endpoint = %row.endpoint,
            "http delivery permanently failed: max_attempts reached, discarding"
        );
        metrics.delivery_exhausted_total.inc();
    }

    Ok(rows.len())
}

fn backoff(attempt: i32) -> chrono::Duration {
    chrono::Duration::seconds(match attempt {
        1 => 10,
        2 => 30,
        3 => 60,
        _ => 300,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_schedule() {
        assert_eq!(backoff(1).num_seconds(), 10);
        assert_eq!(backoff(2).num_seconds(), 30);
        assert_eq!(backoff(3).num_seconds(), 60);
        assert_eq!(backoff(4).num_seconds(), 300);
        assert_eq!(backoff(99).num_seconds(), 300);
    }
}
