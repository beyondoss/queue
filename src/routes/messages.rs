use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::ApiError;
use crate::ops::{delete, receive, send, visibility};

#[derive(Deserialize)]
pub struct SendRequest {
    pub message: serde_json::Value,
    pub headers: Option<serde_json::Value>,
    #[serde(default)]
    pub delay: i32,
    pub group_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
pub enum SendBody {
    Single(SendRequest),
    Batch(Vec<SendRequest>),
}

#[derive(Deserialize)]
pub struct SendQuery {
    /// Skip WAL fsync on commit. Improves throughput at the cost of durability:
    /// messages can be lost on a PostgreSQL crash. Default: false (durable).
    #[serde(default)]
    pub async_commit: bool,
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum SendResponse {
    Single { id: i64 },
    Batch { ids: Vec<i64> },
}

#[derive(Deserialize)]
pub struct ReceiveQuery {
    #[serde(default = "default_max")]
    pub max: i32,
    #[serde(default)]
    pub wait: i32,
    #[serde(default = "default_vt")]
    pub vt: i32,
    #[serde(default)]
    pub fifo: bool,
}

fn default_max() -> i32 {
    1
}

fn default_vt() -> i32 {
    30
}

#[derive(Serialize)]
pub struct MessageResponse {
    pub id: i64,
    pub read_count: i32,
    pub enqueued_at: chrono::DateTime<chrono::Utc>,
    pub visible_at: chrono::DateTime<chrono::Utc>,
    pub message: serde_json::Value,
    pub headers: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub struct ChangeVisibilityRequest {
    pub vt: i32,
}

#[derive(Deserialize)]
pub struct DeleteBatchRequest {
    pub ids: Vec<i64>,
}

pub async fn send_messages(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(SendQuery { async_commit }): Query<SendQuery>,
    Json(body): Json<SendBody>,
) -> Result<impl IntoResponse, ApiError> {
    let sync_commit = !async_commit;
    match body {
        SendBody::Single(req) => {
            let msg_id = if let Some(ref group_id) = req.group_id {
                // FIFO sends bypass the coalescer — no batch API for FIFO.
                send::send_message_fifo(
                    &state.pool,
                    &name,
                    req.message,
                    group_id,
                    None,
                    req.headers,
                    req.delay,
                    sync_commit,
                )
                .await?
                .msg_id
            } else if let Some(ref coalescer) = state.coalescer {
                coalescer
                    .send(
                        name.clone(),
                        req.message,
                        req.headers,
                        req.delay,
                        sync_commit,
                    )
                    .await?
            } else {
                send::send_message(
                    &state.pool,
                    &name,
                    req.message,
                    req.headers,
                    req.delay,
                    sync_commit,
                )
                .await?
                .msg_id
            };
            Ok((
                StatusCode::CREATED,
                Json(SendResponse::Single { id: msg_id }),
            ))
        }
        SendBody::Batch(reqs) => {
            let has_group_ids = reqs.iter().any(|r| r.group_id.is_some());
            if has_group_ids {
                let mut ids = Vec::with_capacity(reqs.len());
                for req in &reqs {
                    let group_id = req.group_id.as_deref().ok_or_else(|| {
                        ApiError::BadRequest(
                            "group_id required for all messages in a FIFO batch".into(),
                        )
                    })?;
                    let r = send::send_message_fifo(
                        &state.pool,
                        &name,
                        req.message.clone(),
                        group_id,
                        None,
                        req.headers.clone(),
                        req.delay,
                        sync_commit,
                    )
                    .await?;
                    ids.push(r.msg_id);
                }
                Ok((StatusCode::CREATED, Json(SendResponse::Batch { ids })))
            } else if let Some(ref coalescer) = state.coalescer {
                // Route each message through the coalescer; they'll likely land
                // in the same linger window and be flushed as a single batch.
                let mut ids = Vec::with_capacity(reqs.len());
                for req in reqs {
                    let id = coalescer
                        .send(
                            name.clone(),
                            req.message,
                            req.headers,
                            req.delay,
                            sync_commit,
                        )
                        .await?;
                    ids.push(id);
                }
                Ok((StatusCode::CREATED, Json(SendResponse::Batch { ids })))
            } else {
                let has_headers = reqs.iter().any(|r| r.headers.is_some());
                let delay = reqs.first().map(|r| r.delay).unwrap_or(0);
                let messages: Vec<serde_json::Value> =
                    reqs.iter().map(|r| r.message.clone()).collect();
                let headers: Option<Vec<serde_json::Value>> = if has_headers {
                    Some(
                        reqs.iter()
                            .map(|r| r.headers.clone().unwrap_or(serde_json::Value::Null))
                            .collect(),
                    )
                } else {
                    None
                };
                let result =
                    send::send_batch(&state.pool, &name, messages, headers, delay, sync_commit)
                        .await?;
                Ok((
                    StatusCode::CREATED,
                    Json(SendResponse::Batch {
                        ids: result.msg_ids,
                    }),
                ))
            }
        }
    }
}

pub async fn receive_messages(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<ReceiveQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let messages = if params.fifo {
        receive::receive_messages_fifo(&state.pool, &name, params.max, params.vt, params.wait)
            .await?
    } else {
        receive::receive_messages(&state.pool, &name, params.max, params.vt, params.wait).await?
    };
    let response: Vec<MessageResponse> = messages
        .into_iter()
        .map(|m| MessageResponse {
            id: m.msg_id,
            read_count: m.read_count,
            enqueued_at: m.enqueued_at,
            visible_at: m.visible_at,
            message: m.message,
            headers: m.headers,
        })
        .collect();
    Ok(Json(response))
}

pub async fn delete_message(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, i64)>,
) -> Result<impl IntoResponse, ApiError> {
    let deleted = delete::delete_message(&state.pool, &name, id).await?;
    if deleted {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::MessageNotFound)
    }
}

pub async fn delete_batch(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<DeleteBatchRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let deleted = delete::delete_batch(&state.pool, &name, &body.ids).await?;
    Ok(Json(serde_json::json!({ "deleted": deleted })))
}

pub async fn change_visibility(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, i64)>,
    Json(body): Json<ChangeVisibilityRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let result = visibility::change_visibility(&state.pool, &name, id, body.vt).await?;
    Ok(Json(serde_json::json!({
        "id": result.msg_id,
        "visible_at": result.visible_at,
    })))
}
