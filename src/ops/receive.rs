use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::error::{ApiError, queue_error};

#[tracing::instrument(skip(pool))]
pub async fn receive_messages_fifo(
    pool: &PgPool,
    queue_name: &str,
    qty: i32,
    vt_secs: i32,
    wait_secs: i32,
) -> Result<Vec<Message>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT
            msg_id          AS "msg_id!: i64",
            read_ct         AS "read_count!: i32",
            enqueued_at     AS "enqueued_at!: DateTime<Utc>",
            vt              AS "visible_at!: DateTime<Utc>",
            message         AS "message!: serde_json::Value",
            headers         AS "headers?: serde_json::Value"
        FROM queue.receive_fifo($1, $2, $3, $4, 100)
        "#,
        queue_name,
        vt_secs,
        qty,
        wait_secs,
    )
    .fetch_all(pool)
    .await
    .map_err(queue_error)?;

    Ok(rows
        .into_iter()
        .map(|r| Message {
            msg_id: r.msg_id,
            read_count: r.read_count,
            enqueued_at: r.enqueued_at,
            visible_at: r.visible_at,
            message: r.message,
            headers: r.headers,
        })
        .collect())
}

pub struct Message {
    pub msg_id: i64,
    pub read_count: i32,
    pub enqueued_at: DateTime<Utc>,
    pub visible_at: DateTime<Utc>,
    pub message: serde_json::Value,
    pub headers: Option<serde_json::Value>,
}

#[tracing::instrument(skip(pool))]
pub async fn receive_messages(
    pool: &PgPool,
    queue_name: &str,
    qty: i32,
    vt_secs: i32,
    wait_secs: i32,
) -> Result<Vec<Message>, ApiError> {
    let rows = sqlx::query!(
        r#"
        SELECT
            msg_id          AS "msg_id!: i64",
            read_ct         AS "read_count!: i32",
            enqueued_at     AS "enqueued_at!: DateTime<Utc>",
            vt              AS "visible_at!: DateTime<Utc>",
            message         AS "message!: serde_json::Value",
            headers         AS "headers?: serde_json::Value"
        FROM queue.receive($1, $2, $3, $4, 100)
        "#,
        queue_name,
        vt_secs,
        qty,
        wait_secs,
    )
    .fetch_all(pool)
    .await
    .map_err(queue_error)?;

    Ok(rows
        .into_iter()
        .map(|r| Message {
            msg_id: r.msg_id,
            read_count: r.read_count,
            enqueued_at: r.enqueued_at,
            visible_at: r.visible_at,
            message: r.message,
            headers: r.headers,
        })
        .collect())
}
