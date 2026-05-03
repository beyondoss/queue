use sqlx::PgPool;

use crate::error::ApiError;

pub struct SendResult {
    pub msg_id: i64,
}

pub async fn send_message_fifo(
    pool: &PgPool,
    queue_name: &str,
    message: serde_json::Value,
    message_group_id: &str,
    deduplication_id: Option<&str>,
    headers: Option<serde_json::Value>,
    delay_secs: i32,
    sync_commit: bool,
) -> Result<SendResult, ApiError> {
    let row = sqlx::query!(
        r#"SELECT pgmq.send_fifo($1, $2::jsonb, $3, $4, $5::jsonb, clock_timestamp() + make_interval(secs => $6), $7) AS "msg_id!: i64""#,
        queue_name,
        message,
        message_group_id,
        deduplication_id,
        headers,
        delay_secs as f64,
        sync_commit,
    )
    .fetch_one(pool)
    .await?;

    Ok(SendResult { msg_id: row.msg_id })
}

pub async fn send_message(
    pool: &PgPool,
    queue_name: &str,
    message: serde_json::Value,
    headers: Option<serde_json::Value>,
    delay_secs: i32,
    sync_commit: bool,
) -> Result<SendResult, ApiError> {
    let row = sqlx::query!(
        r#"SELECT pgmq.send($1, $2::jsonb, $3::jsonb, clock_timestamp() + make_interval(secs => $4), $5) AS "msg_id!: i64""#,
        queue_name,
        message,
        headers,
        delay_secs as f64,
        sync_commit,
    )
    .fetch_one(pool)
    .await?;

    Ok(SendResult { msg_id: row.msg_id })
}

pub struct BatchSendResult {
    pub msg_ids: Vec<i64>,
}

pub async fn send_batch(
    pool: &PgPool,
    queue_name: &str,
    messages: Vec<serde_json::Value>,
    headers: Option<Vec<serde_json::Value>>,
    delay_secs: i32,
    sync_commit: bool,
) -> Result<BatchSendResult, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT pgmq._send_batch($1, $2::jsonb[], $3::jsonb[], clock_timestamp() + make_interval(secs => $4), $5) AS "msg_id!: i64""#,
        queue_name,
        &messages as &[serde_json::Value],
        headers.as_deref(),
        delay_secs as f64,
        sync_commit,
    )
    .fetch_all(pool)
    .await?;

    Ok(BatchSendResult {
        msg_ids: rows.into_iter().map(|r| r.msg_id).collect(),
    })
}
