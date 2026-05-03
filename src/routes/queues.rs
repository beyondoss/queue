use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::ApiError;
use crate::ops::queue_admin;

#[derive(Deserialize)]
pub struct CreateQueueRequest {
    pub name: String,
    #[serde(default)]
    pub fifo: bool,
}

#[derive(Serialize)]
pub struct QueueResponse {
    pub name: String,
    pub is_partitioned: bool,
    pub is_unlogged: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
pub struct QueueMetricsResponse {
    pub name: String,
    pub queue_length: i64,
    pub newest_msg_age_sec: Option<i64>,
    pub oldest_msg_age_sec: Option<i64>,
    pub total_messages: i64,
    pub scrape_time: chrono::DateTime<chrono::Utc>,
}

pub async fn create_queue(
    State(state): State<AppState>,
    Json(body): Json<CreateQueueRequest>,
) -> Result<impl IntoResponse, ApiError> {
    if body.fifo {
        queue_admin::create_fifo_queue(&state.pool, &body.name).await?;
    } else {
        queue_admin::create_queue(&state.pool, &body.name).await?;
    }
    Ok(StatusCode::CREATED)
}

pub async fn list_queues(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let queues = queue_admin::list_queues(&state.pool).await?;
    let response: Vec<QueueResponse> = queues
        .into_iter()
        .map(|q| QueueResponse {
            name: q.queue_name,
            is_partitioned: q.is_partitioned,
            is_unlogged: q.is_unlogged,
            created_at: q.created_at,
        })
        .collect();
    Ok(Json(response))
}

pub async fn get_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let metrics = queue_admin::get_queue_metrics(&state.pool, &name).await?;
    Ok(Json(QueueMetricsResponse {
        name: metrics.queue_name,
        queue_length: metrics.queue_length,
        newest_msg_age_sec: metrics.newest_msg_age_sec,
        oldest_msg_age_sec: metrics.oldest_msg_age_sec,
        total_messages: metrics.total_messages,
        scrape_time: metrics.scrape_time,
    }))
}

pub async fn delete_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let dropped = queue_admin::delete_queue(&state.pool, &name).await?;
    if dropped {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::QueueNotFound(name))
    }
}

pub async fn purge_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let count = queue_admin::purge_queue(&state.pool, &name).await?;
    Ok(Json(serde_json::json!({ "deleted": count })))
}
