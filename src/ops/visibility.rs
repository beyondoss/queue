use chrono::{DateTime, Utc};
use sqlx::PgPool;

use crate::error::ApiError;

pub struct VisibilityResult {
    pub msg_id: i64,
    pub visible_at: DateTime<Utc>,
}

pub async fn change_visibility(
    pool: &PgPool,
    queue_name: &str,
    msg_id: i64,
    vt_secs: i32,
) -> Result<VisibilityResult, ApiError> {
    let row = sqlx::query!(
        r#"
        SELECT
            msg_id  AS "msg_id!: i64",
            vt      AS "visible_at!: DateTime<Utc>"
        FROM queue.set_vt($1::text, $2::bigint, $3::int)
        "#,
        queue_name,
        msg_id,
        vt_secs,
    )
    .fetch_one(pool)
    .await?;

    Ok(VisibilityResult {
        msg_id: row.msg_id,
        visible_at: row.visible_at,
    })
}

pub struct BatchVisibilityEntry {
    pub msg_id: i64,
    pub vt_secs: i32,
}

pub async fn change_visibility_batch(
    pool: &PgPool,
    queue_name: &str,
    entries: Vec<BatchVisibilityEntry>,
) -> Result<Vec<VisibilityResult>, ApiError> {
    let mut results = Vec::with_capacity(entries.len());
    for entry in entries {
        let result = change_visibility(pool, queue_name, entry.msg_id, entry.vt_secs).await?;
        results.push(result);
    }
    Ok(results)
}
