use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::{ApiError, ErrorResponse};
use crate::ops::event::TopicSubscription;
use crate::ops::{event, queue_admin};

/// Request body for queue creation.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct CreateQueueRequest {
    /// Queue name. Must be unique within the instance. Letters, digits, hyphens, and
    /// underscores only.
    pub name: String,
    /// When `true`, creates a FIFO queue: messages with the same `group_id` are delivered
    /// in strict enqueue order. Default: `false` (standard queue, at-least-once unordered).
    #[serde(default)]
    pub fifo: bool,
}

/// Queue metadata returned by list operations.
#[derive(Serialize, utoipa::ToSchema)]
pub struct QueueResponse {
    /// Queue name.
    pub name: String,
    /// `true` when the queue is backed by a partitioned table for high-throughput workloads.
    pub is_partitioned: bool,
    /// `true` when the queue uses an unlogged table — writes are faster but messages are
    /// not crash-safe.
    pub is_unlogged: bool,
    /// Timestamp when the queue was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Real-time queue metrics.
#[derive(Serialize, utoipa::ToSchema)]
pub struct QueueMetricsResponse {
    /// Queue name.
    pub name: String,
    /// Number of messages currently visible (not hidden by a visibility timeout).
    pub queue_length: i64,
    /// Age in seconds of the newest visible message. `null` when the queue is empty.
    #[schema(nullable)]
    pub newest_msg_age_sec: Option<i64>,
    /// Age in seconds of the oldest visible message. `null` when the queue is empty.
    #[schema(nullable)]
    pub oldest_msg_age_sec: Option<i64>,
    /// Total messages ever enqueued, including already-deleted ones.
    pub total_messages: i64,
    /// Timestamp when these metrics were sampled.
    pub scrape_time: chrono::DateTime<chrono::Utc>,
}

/// Result of a purge operation.
#[derive(Serialize, utoipa::ToSchema)]
pub struct PurgeResponse {
    /// Number of messages deleted from the queue.
    pub deleted: i64,
}

/// Create a new queue. Standard queues offer at-least-once, unordered delivery. FIFO
/// queues (`fifo: true`) guarantee per-group ordering at lower throughput. Returns 201
/// on success; the queue name is reserved immediately.
#[utoipa::path(
    post,
    path = "/v1/queues",
    operation_id = "create_queue",
    tag = "queues",
    request_body = CreateQueueRequest,
    responses(
        (status = 201, description = "Queue created."),
        (status = 400, body = ErrorResponse, description = "Invalid queue name or parameters."),
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

/// List all queues. Returns metadata for every queue in the system, ordered by name.
#[utoipa::path(
    get,
    path = "/v1/queues",
    operation_id = "list_queues",
    tag = "queues",
    responses(
        (status = 200, body = [QueueResponse], description = "All queues, ordered by name."),
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

/// Fetch real-time metrics for a single queue: depth, message age, and total throughput.
#[utoipa::path(
    get,
    path = "/v1/queues/{name}",
    operation_id = "get_queue",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name."),
    ),
    responses(
        (status = 200, body = QueueMetricsResponse, description = "Current queue metrics."),
        (status = 404, body = ErrorResponse, description = "Queue does not exist."),
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

/// Delete a queue and all of its messages permanently. Idempotent: safe to call even
/// if the queue is already gone.
#[utoipa::path(
    delete,
    path = "/v1/queues/{name}",
    operation_id = "delete_queue",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name."),
    ),
    responses(
        (status = 204, description = "Queue deleted."),
        (status = 404, body = ErrorResponse, description = "Queue does not exist."),
    )
)]
pub async fn delete_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    queue_admin::delete_queue(&state.pool, &name).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Delete all messages in a queue without removing the queue itself. Returns the count
/// of messages removed.
#[utoipa::path(
    post,
    path = "/v1/queues/{name}/purge",
    operation_id = "purge_queue",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name."),
    ),
    responses(
        (status = 200, body = PurgeResponse, description = "Number of messages deleted."),
        (status = 404, body = ErrorResponse, description = "Queue does not exist."),
    )
)]
pub async fn purge_queue(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let count = queue_admin::purge_queue(&state.pool, &name).await?;
    Ok(Json(PurgeResponse { deleted: count }))
}

/// List all topic subscriptions targeting this queue.
#[utoipa::path(
    get,
    path = "/v1/queues/{name}/subscriptions",
    operation_id = "list_queue_subscriptions",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name."),
    ),
    responses(
        (status = 200, body = [TopicSubscription], description = "Active topic subscriptions for the queue."),
        (status = 404, body = ErrorResponse, description = "Queue does not exist."),
    )
)]
pub async fn list_subscriptions(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let bindings = event::list_by_queue(&state.pool, &name).await?;
    Ok(Json(bindings))
}
