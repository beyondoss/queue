use sqlx::PgPool;

use crate::error::ApiError;

pub struct TopicSendResult {
    pub queues_matched: i64,
}

pub async fn send_topic(
    pool: &PgPool,
    routing_key: &str,
    message: serde_json::Value,
    headers: Option<serde_json::Value>,
    delay_secs: i32,
) -> Result<TopicSendResult, ApiError> {
    let row = sqlx::query!(
        r#"SELECT queue.send_topic($1, $2::jsonb, $3::jsonb, $4) AS "queues_matched!: i64""#,
        routing_key,
        message,
        headers,
        delay_secs,
    )
    .fetch_one(pool)
    .await?;

    Ok(TopicSendResult {
        queues_matched: row.queues_matched,
    })
}
