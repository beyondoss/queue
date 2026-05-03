use sqlx::PgPool;

use crate::error::ApiError;

pub async fn delete_message(
    pool: &PgPool,
    queue_name: &str,
    msg_id: i64,
) -> Result<bool, ApiError> {
    let row = sqlx::query!(
        r#"SELECT queue.delete($1::text, $2::bigint) AS "deleted!: bool""#,
        queue_name,
        msg_id,
    )
    .fetch_one(pool)
    .await?;

    Ok(row.deleted)
}

pub async fn delete_batch(
    pool: &PgPool,
    queue_name: &str,
    msg_ids: &[i64],
) -> Result<Vec<i64>, ApiError> {
    let rows = sqlx::query!(
        r#"SELECT queue.delete($1::text, $2::bigint[]) AS "msg_id!: i64""#,
        queue_name,
        msg_ids,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|r| r.msg_id).collect())
}
