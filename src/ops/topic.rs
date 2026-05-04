use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;

use crate::error::{ApiError, topic_bind_error};

// ---------------------------------------------------------------------------
// send_topic
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct TopicMessage {
    pub queue_name: String,
    pub msg_id: i64,
}

pub struct TopicSendResult {
    pub messages: Vec<TopicMessage>,
}

impl TopicSendResult {
    pub fn queues_matched(&self) -> usize {
        self.messages.len()
    }
}

pub async fn send_topic(
    pool: &PgPool,
    routing_key: &str,
    message: serde_json::Value,
    headers: Option<serde_json::Value>,
    delay_secs: i32,
) -> Result<TopicSendResult, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT queue_name AS "queue_name!", msg_id AS "msg_id!"
           FROM queue.send_topic($1, $2::jsonb, $3::jsonb, $4::integer)"#,
        routing_key,
        message,
        headers,
        delay_secs,
    )
    .fetch_all(pool)
    .await?;

    Ok(TopicSendResult {
        messages: rows
            .into_iter()
            .map(|r| TopicMessage {
                queue_name: r.queue_name,
                msg_id: r.msg_id,
            })
            .collect(),
    })
}

// ---------------------------------------------------------------------------
// bindings
// ---------------------------------------------------------------------------

#[derive(Serialize)]
pub struct TopicSubscription {
    pub pattern: String,
    pub queue_name: String,
    pub bound_at: DateTime<Utc>,
}

/// Bind `queue_name` to `pattern`. Silently succeeds if already bound.
pub async fn subscribe(pool: &PgPool, pattern: &str, queue_name: &str) -> Result<TopicSubscription, ApiError> {
    // subscribe validates pattern and checks queue existence; raises RAISE EXCEPTION on error.
    sqlx::query!(
        "SELECT queue.subscribe($1, $2)",
        pattern,
        queue_name,
    )
    .execute(pool)
    .await
    .map_err(topic_bind_error)?;

    let row = sqlx::query!(
        r#"SELECT pattern, queue_name, bound_at FROM queue.topic_subscriptions
           WHERE pattern = $1 AND queue_name = $2"#,
        pattern,
        queue_name,
    )
    .fetch_one(pool)
    .await?;

    Ok(TopicSubscription {
        pattern: row.pattern,
        queue_name: row.queue_name,
        bound_at: row.bound_at,
    })
}

/// Remove the binding. Returns `BindingNotFound` if it did not exist.
pub async fn unsubscribe(pool: &PgPool, pattern: &str, queue_name: &str) -> Result<(), ApiError> {
    let row = sqlx::query!(
        r#"SELECT queue.unsubscribe($1, $2) AS "removed!: bool""#,
        pattern,
        queue_name,
    )
    .fetch_one(pool)
    .await?;

    if row.removed {
        Ok(())
    } else {
        Err(ApiError::BindingNotFound)
    }
}

/// All queues bound to `pattern`, ordered by queue name.
pub async fn list_by_pattern(pool: &PgPool, pattern: &str) -> Result<Vec<TopicSubscription>, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT pattern, queue_name, bound_at
           FROM queue.topic_subscriptions
           WHERE pattern = $1
           ORDER BY queue_name"#,
        pattern,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| TopicSubscription {
            pattern: r.pattern,
            queue_name: r.queue_name,
            bound_at: r.bound_at,
        })
        .collect())
}

/// All topic subscriptions across all patterns, ordered by pattern then queue.
pub async fn list_all_subscriptions(pool: &PgPool) -> Result<Vec<TopicSubscription>, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT pattern, queue_name, bound_at
           FROM queue.topic_subscriptions
           ORDER BY pattern, queue_name"#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| TopicSubscription {
            pattern: r.pattern,
            queue_name: r.queue_name,
            bound_at: r.bound_at,
        })
        .collect())
}

/// Distinct topic names derived from active subscriptions, for SNS ListTopics.
pub async fn list_sns_topics(pool: &PgPool) -> Result<Vec<String>, ApiError> {
    let rows = sqlx::query!(
        "SELECT DISTINCT pattern FROM queue.topic_subscriptions ORDER BY pattern",
    )
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|r| r.pattern).collect())
}

/// Delete all subscriptions for a topic pattern, for SNS DeleteTopic.
pub async fn delete_sns_topic(pool: &PgPool, pattern: &str) -> Result<(), ApiError> {
    sqlx::query!(
        "DELETE FROM queue.topic_subscriptions WHERE pattern = $1",
        pattern,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// All patterns `queue_name` is bound to, ordered by pattern.
pub async fn list_by_queue(pool: &PgPool, queue_name: &str) -> Result<Vec<TopicSubscription>, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT pattern, queue_name, bound_at
           FROM queue.topic_subscriptions
           WHERE queue_name = $1
           ORDER BY pattern"#,
        queue_name,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| TopicSubscription {
            pattern: r.pattern,
            queue_name: r.queue_name,
            bound_at: r.bound_at,
        })
        .collect())
}
