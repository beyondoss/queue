use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::{ApiError, ErrorResponse};
use crate::ops::topic::TopicSubscription;
use crate::ops::{queue_admin, topic};

#[derive(Deserialize, utoipa::ToSchema)]
pub struct CreateQueueRequest {
    pub name: String,
    #[serde(default)]
    pub fifo: bool,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct QueueResponse {
    pub name: String,
    pub is_partitioned: bool,
    pub is_unlogged: bool,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct QueueMetricsResponse {
    pub name: String,
    pub queue_length: i64,
    pub newest_msg_age_sec: Option<i64>,
    pub oldest_msg_age_sec: Option<i64>,
    pub total_messages: i64,
    pub scrape_time: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub struct PurgeResponse {
    pub deleted: i64,
}

#[utoipa::path(
    post,
    path = "/v1/queues",
    tag = "queues",
    request_body = CreateQueueRequest,
    responses(
        (status = 201, description = "Queue created"),
        (status = 400, body = ErrorResponse),
    )
)]
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

#[utoipa::path(
    get,
    path = "/v1/queues",
    tag = "queues",
    responses(
        (status = 200, body = [QueueResponse]),
    )
)]
pub async fn list_queues(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let queues = queue_admin::list_queues(&state.pool, None).await?;
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

#[utoipa::path(
    get,
    path = "/v1/queues/{name}",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name"),
    ),
    responses(
        (status = 200, body = QueueMetricsResponse),
        (status = 404, body = ErrorResponse),
    )
)]
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

#[utoipa::path(
    delete,
    path = "/v1/queues/{name}",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name"),
    ),
    responses(
        (status = 204, description = "Queue deleted"),
        (status = 404, body = ErrorResponse),
    )
)]
pub async fn delete_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    queue_admin::delete_queue(&state.pool, &name).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    post,
    path = "/v1/queues/{name}/purge",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name"),
    ),
    responses(
        (status = 200, body = PurgeResponse),
        (status = 404, body = ErrorResponse),
    )
)]
pub async fn purge_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let count = queue_admin::purge_queue(&state.pool, &name).await?;
    Ok(Json(PurgeResponse { deleted: count }))
}

#[utoipa::path(
    get,
    path = "/v1/queues/{name}/subscriptions",
    operation_id = "list_queue_subscriptions",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name"),
    ),
    responses(
        (status = 200, body = [TopicSubscription]),
        (status = 404, body = ErrorResponse),
    )
)]
pub async fn list_subscriptions(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let bindings = topic::list_by_queue(&state.pool, &name).await?;
    Ok(Json(bindings))
}
