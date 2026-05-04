use std::time::Duration;

use chrono::Utc;
use reqwest::Client;
use sqlx::PgPool;

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

pub fn start(pool: PgPool, config: DeliveryConfig) {
    tokio::spawn(run(pool, config));
}

async fn run(pool: PgPool, config: DeliveryConfig) {
    let client = Client::builder()
        .timeout(Duration::from_secs(config.delivery_timeout_secs))
        .build()
        .expect("reqwest client");

    loop {
        match deliver_batch(&pool, &client, config.batch_size).await {
            Ok(0) => {
                tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
            }
            Ok(_) => {}
            Err(e) => {
                tracing::error!(error = %e, "http delivery batch error");
                tokio::time::sleep(Duration::from_millis(config.poll_interval_ms)).await;
            }
        }
    }
}

async fn deliver_batch(pool: &PgPool, client: &Client, batch_size: i64) -> anyhow::Result<usize> {
    let mut tx = pool.begin().await?;

    let rows = sqlx::query!(
        r#"SELECT
               id              AS "id!: i64",
               endpoint        AS "endpoint!",
               payload         AS "payload!: serde_json::Value",
               attempt         AS "attempt!",
               max_attempts    AS "max_attempts!"
           FROM queue.http_deliveries
           WHERE next_attempt_at <= now() AND attempt < max_attempts
           ORDER BY next_attempt_at ASC
           LIMIT $1
           FOR UPDATE SKIP LOCKED"#,
        batch_size,
    )
    .fetch_all(&mut *tx)
    .await?;

    if rows.is_empty() {
        tx.rollback().await?;
        return Ok(0);
    }

    for row in &rows {
        let result = client
            .post(&row.endpoint)
            .header("content-type", "application/json")
            .header("x-amz-sns-message-type", "Notification")
            .json(&row.payload)
            .send()
            .await;

        let (success, error_msg) = match result {
            Ok(resp) if resp.status().is_success() => (true, None),
            Ok(resp) => (false, Some(format!("HTTP {}", resp.status()))),
            Err(e) => (false, Some(e.to_string())),
        };

        if success {
            sqlx::query!("DELETE FROM queue.http_deliveries WHERE id = $1", row.id,)
                .execute(&mut *tx)
                .await?;
        } else {
            let next_attempt_at = Utc::now() + backoff(row.attempt + 1);
            sqlx::query!(
                r#"UPDATE queue.http_deliveries
                   SET attempt = attempt + 1, last_error = $1, next_attempt_at = $2
                   WHERE id = $3"#,
                error_msg,
                next_attempt_at,
                row.id,
            )
            .execute(&mut *tx)
            .await?;
            tracing::warn!(
                id = row.id,
                endpoint = %row.endpoint,
                attempt = row.attempt + 1,
                error = error_msg,
                "http delivery failed"
            );
        }
    }

    tx.commit().await?;
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
