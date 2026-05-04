use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::error::{ApiError, queue_error};

pub struct QueueInfo {
    pub queue_name: String,
    pub is_partitioned: bool,
    pub is_unlogged: bool,
    pub created_at: DateTime<Utc>,
}

pub struct QueueMetrics {
    pub queue_name: String,
    pub queue_length: i64,
    pub newest_msg_age_sec: Option<i64>,
    pub oldest_msg_age_sec: Option<i64>,
    pub total_messages: i64,
    pub scrape_time: DateTime<Utc>,
}

pub async fn create_queue(pool: &PgPool, queue_name: &str) -> Result<(), ApiError> {
    sqlx::query!("SELECT queue.create($1)", queue_name)
        .execute(pool)
        .await
        .map_err(queue_error)?;

    Ok(())
}

pub async fn create_fifo_queue(pool: &PgPool, queue_name: &str) -> Result<(), ApiError> {
    sqlx::query!("SELECT queue.create_fifo($1)", queue_name)
        .execute(pool)
        .await
        .map_err(queue_error)?;

    Ok(())
}

pub async fn delete_queue(pool: &PgPool, queue_name: &str) -> Result<bool, ApiError> {
    let row = sqlx::query!(
        r#"SELECT queue.delete_queue($1) AS "dropped!: bool""#,
        queue_name,
    )
    .fetch_one(pool)
    .await?;

    Ok(row.dropped)
}

pub async fn list_queues(pool: &PgPool, prefix: Option<&str>) -> Result<Vec<QueueInfo>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT
            queue_name      AS "queue_name!: String",
            is_partitioned  AS "is_partitioned!: bool",
            is_unlogged     AS "is_unlogged!: bool",
            created_at      AS "created_at!: DateTime<Utc>"
        FROM queue.list_queues($1)
        "#,
        prefix,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|r| QueueInfo {
            queue_name: r.queue_name,
            is_partitioned: r.is_partitioned,
            is_unlogged: r.is_unlogged,
            created_at: r.created_at,
        })
        .collect())
}

pub async fn get_queue_metrics(pool: &PgPool, queue_name: &str) -> Result<QueueMetrics, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT
            queue_name          AS "queue_name!: String",
            queue_length        AS "queue_length!: i64",
            newest_msg_age_sec  AS "newest_msg_age_sec?: i64",
            oldest_msg_age_sec  AS "oldest_msg_age_sec?: i64",
            total_messages      AS "total_messages!: i64",
            scrape_time         AS "scrape_time!: DateTime<Utc>"
        FROM queue.metrics($1)
        "#,
        queue_name,
    )
    .fetch_one(pool)
    .await
    .map_err(|e| match &e {
        sqlx::Error::RowNotFound => ApiError::QueueNotFound(queue_name.to_string()),
        _ => ApiError::Database(e),
    })?;

    Ok(QueueMetrics {
        queue_name: row.queue_name,
        queue_length: row.queue_length,
        newest_msg_age_sec: row.newest_msg_age_sec,
        oldest_msg_age_sec: row.oldest_msg_age_sec,
        total_messages: row.total_messages,
        scrape_time: row.scrape_time,
    })
}

pub async fn purge_queue(pool: &PgPool, queue_name: &str) -> Result<i64, ApiError> {
    let row = sqlx::query!(
        r#"SELECT queue.purge_queue($1) AS "count!: i64""#,
        queue_name,
    )
    .fetch_one(pool)
    .await?;

    Ok(row.count)
}
