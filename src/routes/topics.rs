use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;

use crate::error::ApiError;
use crate::ops::topic;
use crate::AppState;

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
    let result =
        topic::send_topic(&state.pool, &routing_key, body.message, body.headers, body.delay)
            .await?;
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "queues_matched": result.queues_matched })),
    ))
}
