use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::Deserialize;

use crate::AppState;
use crate::error::ApiError;
use crate::ops::topic;

// ---------------------------------------------------------------------------
// send
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct TopicSendRequest {
    pub message: serde_json::Value,
    pub headers: Option<serde_json::Value>,
    #[serde(default)]
    pub delay: i32,
}

pub async fn send_topic(
    State(state): State<AppState>,
    Path(routing_key): Path<String>,
    Json(body): Json<TopicSendRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let result = topic::send_topic(
        &state.pool,
        &routing_key,
        body.message,
        body.headers,
        body.delay,
    )
    .await?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "queues_matched": result.queues_matched(),
            "messages": result.messages,
        })),
    ))
}

// ---------------------------------------------------------------------------
// bindings
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SubscribeRequest {
    pub queue_name: String,
}

/// POST /v1/topics/{pattern}/subscriptions
/// Body: { "queue_name": "..." }
/// Binds a queue to a topic pattern. Idempotent — silently succeeds if already bound.
pub async fn subscribe_queue(
    State(state): State<AppState>,
    Path(pattern): Path<String>,
    Json(body): Json<SubscribeRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let binding = topic::subscribe(&state.pool, &pattern, &body.queue_name).await?;
    Ok((StatusCode::CREATED, Json(binding)))
}

/// DELETE /v1/topics/{pattern}/subscriptions/{queue_name}
/// Removes a queue binding. Returns 404 if the binding does not exist.
pub async fn unsubscribe_queue(
    State(state): State<AppState>,
    Path((pattern, queue_name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    topic::unsubscribe(&state.pool, &pattern, &queue_name).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// GET /v1/topics/{pattern}/subscriptions
/// Lists all queues bound to this pattern.
pub async fn list_subscriptions(
    State(state): State<AppState>,
    Path(pattern): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let bindings = topic::list_by_pattern(&state.pool, &pattern).await?;
    Ok(Json(bindings))
}
