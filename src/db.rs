use std::time::{Duration, Instant};

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;

/// How long we keep retrying the initial database connection before giving up.
const CONNECT_RETRY_BUDGET: Duration = Duration::from_secs(60);
/// First backoff after a failed connect; doubles up to [`CONNECT_BACKOFF_MAX`].
const CONNECT_BACKOFF_START: Duration = Duration::from_millis(250);
const CONNECT_BACKOFF_MAX: Duration = Duration::from_secs(3);
/// Pool acquire timeout. Sized to tolerate Postgres waking from deep sleep
/// (re-export of its volume from S3 + boot can take seconds): when the whole app
/// has been idle and Postgres has scaled to zero, the queue's first query holds
/// while Postgres wakes — via eBPF wake-on-traffic — rather than failing fast.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);

/// Connect to postgres, retrying with capped exponential backoff while it's
/// unreachable.
///
/// On a fresh deploy, postgres comes up in parallel with us and its `.internal`
/// name may not resolve for the first few seconds. Rather than exit and rely on a
/// process restart (which wastes the whole startup and re-races the same window),
/// we wait for it. Connect failures (DNS, connection refused, pool acquire
/// timeout) are transient and retried; we only give up after [`CONNECT_RETRY_BUDGET`].
pub async fn connect(database_url: &str, max_connections: u32) -> anyhow::Result<PgPool> {
    let deadline = Instant::now() + CONNECT_RETRY_BUDGET;
    let mut backoff = CONNECT_BACKOFF_START;
    loop {
        let attempt = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(ACQUIRE_TIMEOUT)
            .connect(database_url)
            .await;
        match attempt {
            Ok(pool) => return Ok(pool),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(anyhow::anyhow!(
                        "connect to database: unreachable after retrying: {e}"
                    ));
                }
                tracing::warn!(
                    error = %e,
                    backoff_ms = backoff.as_millis() as u64,
                    "database not ready; retrying"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(CONNECT_BACKOFF_MAX);
            }
        }
    }
}
