use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::PgPool;

use crate::error::{ApiError, topic_bind_error};

// ---------------------------------------------------------------------------
// publish_event
// ---------------------------------------------------------------------------

/// A message enqueued in a single matched queue as a result of a topic publish.
#[derive(Serialize, utoipa::ToSchema)]
pub struct TopicMessage {
    /// Name of the queue that received the message.
    pub queue_name: String,
    /// Assigned message ID within that queue.
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

pub async fn publish_event(
    pool: &PgPool,
    routing_key: &str,
    message: serde_json::Value,
    headers: Option<serde_json::Value>,
    delay_secs: i32,
) -> Result<TopicSendResult, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT queue_name AS "queue_name!", msg_id AS "msg_id!"
           FROM queue.publish_event($1, $2::jsonb, $3::jsonb, $4::integer)"#,
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

/// Queue HTTP/HTTPS deliveries for all subscriptions matching `routing_key`.
/// `raw_message` is the original payload; `envelope` is the SNS notification JSON (None for REST API calls).
/// Returns the number of deliveries queued.
pub async fn queue_event_deliveries(
    pool: &PgPool,
    routing_key: &str,
    raw_message: &serde_json::Value,
    envelope: Option<&serde_json::Value>,
) -> Result<i64, ApiError> {
    let n = sqlx::query_scalar!(
        r#"SELECT queue.queue_event_deliveries($1, $2::jsonb, $3::jsonb) AS "n!""#,
        routing_key,
        raw_message,
        envelope,
    )
    .fetch_one(pool)
    .await?;
    Ok(n)
}

// ---------------------------------------------------------------------------
// bindings
// ---------------------------------------------------------------------------

/// A topic subscription record.
#[derive(Serialize, utoipa::ToSchema)]
pub struct TopicSubscription {
    /// Subscription ID. Use this to unsubscribe via `DELETE /v1/events/{pattern}/subscriptions/{id}`.
    pub id: i64,
    /// Glob pattern matched against routing keys at publish time.
    pub pattern: String,
    /// Delivery protocol: `"sqs"` for internal queue delivery, `"http"` or `"https"` for
    /// webhook delivery.
    pub protocol: String,
    /// Delivery endpoint. `sqs://<queue_name>` for queue subscriptions; the HTTP/HTTPS URL
    /// for webhook subscriptions.
    pub endpoint: String,
    /// Name of the target queue. Present only for `sqs` protocol subscriptions; `null` for
    /// HTTP/HTTPS webhook subscriptions.
    #[schema(nullable)]
    pub queue_name: Option<String>,
    /// Timestamp when the subscription was created.
    pub bound_at: DateTime<Utc>,
    /// `true` when the message payload is delivered as-is (raw); `false` when wrapped in an
    /// SNS-compatible notification envelope.
    pub raw_delivery: bool,
}

/// Bind an endpoint to a pattern. Idempotent — silently succeeds if already bound.
pub async fn subscribe(
    pool: &PgPool,
    pattern: &str,
    protocol: &str,
    endpoint: &str,
    queue_name: Option<&str>,
    raw_delivery: bool,
) -> Result<TopicSubscription, ApiError> {
    let row = sqlx::query!(
        r#"SELECT
               r_id           AS "id!: i64",
               r_pattern      AS "pattern!",
               r_protocol     AS "protocol!",
               r_endpoint     AS "endpoint!",
               r_queue_name   AS "queue_name",
               r_bound_at     AS "bound_at!: DateTime<Utc>",
               r_raw_delivery AS "raw_delivery!"
           FROM queue.subscribe($1, $2, $3, $4, $5)"#,
        pattern,
        protocol,
        endpoint,
        queue_name,
        raw_delivery,
    )
    .fetch_one(pool)
    .await
    .map_err(topic_bind_error)?;

    Ok(TopicSubscription {
        id: row.id,
        pattern: row.pattern,
        protocol: row.protocol,
        endpoint: row.endpoint,
        queue_name: row.queue_name,
        bound_at: row.bound_at,
        raw_delivery: row.raw_delivery,
    })
}

/// Remove the binding by endpoint. Returns `BindingNotFound` if it did not exist.
pub async fn unsubscribe(pool: &PgPool, pattern: &str, endpoint: &str) -> Result<(), ApiError> {
    let row = sqlx::query!(
        r#"SELECT queue.unsubscribe($1, $2) AS "removed!: bool""#,
        pattern,
        endpoint,
    )
    .fetch_one(pool)
    .await?;

    if row.removed {
        Ok(())
    } else {
        Err(ApiError::BindingNotFound)
    }
}

/// Remove the binding by subscription id. Returns `BindingNotFound` if it did not exist.
pub async fn unsubscribe_by_id(pool: &PgPool, id: i64) -> Result<(), ApiError> {
    let result = sqlx::query!(
        r#"DELETE FROM queue.event_subscriptions WHERE id = $1 RETURNING id AS "id!: i64""#,
        id,
    )
    .fetch_optional(pool)
    .await?;

    if result.is_some() {
        Ok(())
    } else {
        Err(ApiError::BindingNotFound)
    }
}

/// Look up a subscription by (pattern, queue_name) — for SQS ARN lookups.
pub async fn get_by_queue(
    pool: &PgPool,
    pattern: &str,
    queue_name: &str,
) -> Result<Option<TopicSubscription>, ApiError> {
    let row = sqlx::query!(
        r#"SELECT
               id           AS "id!: i64",
               pattern      AS "pattern!",
               protocol     AS "protocol!",
               endpoint     AS "endpoint!",
               queue_name,
               bound_at     AS "bound_at!: DateTime<Utc>",
               raw_delivery AS "raw_delivery!"
           FROM queue.event_subscriptions WHERE pattern = $1 AND queue_name = $2"#,
        pattern,
        queue_name,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| TopicSubscription {
        id: r.id,
        pattern: r.pattern,
        protocol: r.protocol,
        endpoint: r.endpoint,
        queue_name: r.queue_name,
        bound_at: r.bound_at,
        raw_delivery: r.raw_delivery,
    }))
}

/// Look up a subscription by id.
pub async fn get_by_id(pool: &PgPool, id: i64) -> Result<Option<TopicSubscription>, ApiError> {
    let row = sqlx::query!(
        r#"SELECT
               id           AS "id!: i64",
               pattern      AS "pattern!",
               protocol     AS "protocol!",
               endpoint     AS "endpoint!",
               queue_name,
               bound_at     AS "bound_at!: DateTime<Utc>",
               raw_delivery AS "raw_delivery!"
           FROM queue.event_subscriptions WHERE id = $1"#,
        id,
    )
    .fetch_optional(pool)
    .await?;

    Ok(row.map(|r| TopicSubscription {
        id: r.id,
        pattern: r.pattern,
        protocol: r.protocol,
        endpoint: r.endpoint,
        queue_name: r.queue_name,
        bound_at: r.bound_at,
        raw_delivery: r.raw_delivery,
    }))
}

/// All subscriptions bound to `pattern`, ordered by endpoint.
pub async fn list_by_pattern(
    pool: &PgPool,
    pattern: &str,
) -> Result<Vec<TopicSubscription>, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT
               id           AS "id!: i64",
               pattern      AS "pattern!",
               protocol     AS "protocol!",
               endpoint     AS "endpoint!",
               queue_name,
               bound_at     AS "bound_at!: DateTime<Utc>",
               raw_delivery AS "raw_delivery!"
           FROM queue.event_subscriptions
           WHERE pattern = $1
           ORDER BY endpoint"#,
        pattern,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| TopicSubscription {
            id: r.id,
            pattern: r.pattern,
            protocol: r.protocol,
            endpoint: r.endpoint,
            queue_name: r.queue_name,
            bound_at: r.bound_at,
            raw_delivery: r.raw_delivery,
        })
        .collect())
}

/// All topic subscriptions across all patterns.
pub async fn list_all_subscriptions(pool: &PgPool) -> Result<Vec<TopicSubscription>, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT
               id           AS "id!: i64",
               pattern      AS "pattern!",
               protocol     AS "protocol!",
               endpoint     AS "endpoint!",
               queue_name,
               bound_at     AS "bound_at!: DateTime<Utc>",
               raw_delivery AS "raw_delivery!"
           FROM queue.event_subscriptions
           ORDER BY pattern, endpoint"#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| TopicSubscription {
            id: r.id,
            pattern: r.pattern,
            protocol: r.protocol,
            endpoint: r.endpoint,
            queue_name: r.queue_name,
            bound_at: r.bound_at,
            raw_delivery: r.raw_delivery,
        })
        .collect())
}

/// Distinct topic names derived from active subscriptions, for SNS ListTopics.
pub async fn list_sns_topics(pool: &PgPool) -> Result<Vec<String>, ApiError> {
    let rows =
        sqlx::query!("SELECT DISTINCT pattern FROM queue.event_subscriptions ORDER BY pattern",)
            .fetch_all(pool)
            .await?;
    Ok(rows.into_iter().map(|r| r.pattern).collect())
}

/// Delete all subscriptions for a topic pattern, for SNS DeleteTopic.
pub async fn delete_sns_topic(pool: &PgPool, pattern: &str) -> Result<(), ApiError> {
    sqlx::query!(
        "DELETE FROM queue.event_subscriptions WHERE pattern = $1",
        pattern,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// All patterns a queue is bound to, ordered by pattern.
pub async fn list_by_queue(
    pool: &PgPool,
    queue_name: &str,
) -> Result<Vec<TopicSubscription>, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT
               id           AS "id!: i64",
               pattern      AS "pattern!",
               protocol     AS "protocol!",
               endpoint     AS "endpoint!",
               queue_name,
               bound_at     AS "bound_at!: DateTime<Utc>",
               raw_delivery AS "raw_delivery!"
           FROM queue.event_subscriptions
           WHERE queue_name = $1
           ORDER BY pattern"#,
        queue_name,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| TopicSubscription {
            id: r.id,
            pattern: r.pattern,
            protocol: r.protocol,
            endpoint: r.endpoint,
            queue_name: r.queue_name,
            bound_at: r.bound_at,
            raw_delivery: r.raw_delivery,
        })
        .collect())
}
