use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::error::{ApiError, ErrorResponse};
use crate::ops::{delete, receive, send, visibility};

/// A single message to enqueue.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct SendRequest {
    /// Message body. Any JSON value: object, array, string, number, or boolean.
    #[schema(value_type = Object)]
    pub message: serde_json::Value,
    /// Optional key-value metadata to attach to the message. Any JSON object. Delivered
    /// alongside the body on receive.
    #[schema(nullable, value_type = Object)]
    pub headers: Option<serde_json::Value>,
    /// Delivery delay in seconds. The message becomes visible to receivers after this
    /// many seconds. Default: `0` (immediately visible).
    #[serde(default)]
    pub delay: i32,
    /// FIFO group identifier. Messages with the same `group_id` are delivered in strict
    /// enqueue order. Requires a FIFO queue. All messages in a batch must include a
    /// `group_id` if any do.
    #[schema(nullable)]
    pub group_id: Option<String>,
}

/// Request body for send — either a single message object or a JSON array for batch
/// sends. The shape is detected automatically: a JSON object is a single send, a JSON
/// array is a batch.
#[derive(Deserialize, utoipa::ToSchema)]
#[serde(untagged)]
pub enum SendBody {
    /// Send a single message.
    Single(SendRequest),
    /// Send multiple messages in one request.
    Batch(Vec<SendRequest>),
}

#[derive(Deserialize, utoipa::IntoParams)]
pub struct SendQuery {
    /// Skip WAL fsync on commit. Improves throughput at the cost of durability:
    /// messages can be lost on a PostgreSQL crash. Default: false (durable).
    #[serde(default)]
    pub async_commit: bool,
}

/// Send response — mirrors the request shape.
/// Single sends return `{ "id": <i64> }`;
/// batch sends return `{ "ids": [<i64>, ...] }`.
#[derive(Serialize, utoipa::ToSchema)]
#[serde(untagged)]
pub enum SendResponse {
    /// Result of a single-message send.
    Single {
        /// Assigned message ID.
        id: i64,
    },
    /// Result of a batch send.
    Batch {
        /// Assigned message IDs, in the same order as the request array.
        ids: Vec<i64>,
    },
}

#[derive(Deserialize, utoipa::IntoParams)]
pub struct ReceiveQuery {
    /// Maximum number of messages to return. Default: 1.
    #[serde(default = "default_max")]
    pub max: i32,
    /// Long-poll wait time in seconds. The call blocks until a message arrives or the
    /// timeout elapses. Default: 0 (no wait — returns immediately).
    #[serde(default)]
    pub wait: i32,
    /// Visibility timeout in seconds. Received messages are hidden from other consumers
    /// for this duration. Delete the message before it elapses to prevent re-delivery.
    /// Default: 30.
    #[serde(default = "default_vt")]
    pub vt: i32,
    /// Return messages in FIFO order within each `group_id`. Only applies to FIFO
    /// queues. Default: false.
    #[serde(default)]
    pub fifo: bool,
}

fn default_max() -> i32 {
    1
}

fn default_vt() -> i32 {
    30
}

/// A received message.
#[derive(Serialize, utoipa::ToSchema)]
pub struct MessageResponse {
    /// Message ID. Use this to delete or extend the visibility of the message.
    pub id: i64,
    /// How many times this message has been delivered. Starts at `1` on first delivery.
    /// A value greater than `1` indicates a prior consumer did not delete the message
    /// before its visibility timeout expired.
    pub read_count: i32,
    /// Timestamp when the message was added to the queue.
    pub enqueued_at: chrono::DateTime<chrono::Utc>,
    /// Timestamp after which the message becomes visible again if not deleted. Extends
    /// on each `PATCH /v1/queues/{name}/messages/{id}` call.
    pub visible_at: chrono::DateTime<chrono::Utc>,
    /// Message body as originally sent.
    #[schema(value_type = Object)]
    pub message: serde_json::Value,
    /// Metadata attached at send time. `null` when none was provided.
    #[schema(nullable, value_type = Object)]
    pub headers: Option<serde_json::Value>,
}

/// Request body to extend or reduce a message's visibility timeout.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct ChangeVisibilityRequest {
    /// New visibility timeout in seconds from now. The message will not be returned by
    /// receive calls until this duration elapses. Set to `0` to make the message
    /// immediately visible.
    pub vt: i32,
}

/// Confirmation of a visibility timeout change.
#[derive(Serialize, utoipa::ToSchema)]
pub struct ChangeVisibilityResponse {
    /// Message ID.
    pub id: i64,
    /// Updated timestamp after which the message becomes visible again.
    pub visible_at: chrono::DateTime<chrono::Utc>,
}

/// Request body for batch message deletion.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct DeleteBatchRequest {
    /// IDs of messages to delete. Non-existent IDs are silently skipped.
    pub ids: Vec<i64>,
}

/// Result of a batch delete operation.
#[derive(Serialize, utoipa::ToSchema)]
pub struct DeletedResponse {
    /// IDs of the messages that were actually deleted.
    pub deleted: Vec<i64>,
}

/// Send one or more messages to a queue. The body is either a single message object or
/// a JSON array of message objects — the shape is detected automatically.
///
/// Batch sends with a uniform `delay` value are committed in a single statement for
/// maximum throughput. Set `async_commit=true` to skip WAL fsync for best-effort,
/// high-speed sends at the cost of crash durability.
#[utoipa::path(
    post,
    path = "/v1/queues/{name}/messages",
    operation_id = "send_messages",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name."),
        SendQuery,
    ),
    request_body = SendBody,
    responses(
        (status = 201, body = SendResponse, description = "Message(s) enqueued. Single send returns `{\"id\": <n>}`; batch send returns `{\"ids\": [...]}`." ),
        (status = 400, body = ErrorResponse, description = "Invalid request body or parameters."),
        (status = 404, body = ErrorResponse, description = "Queue does not exist."),
    )
)]
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
            if reqs.is_empty() {
                return Err(ApiError::BadRequest("batch cannot be empty".into()));
            }
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
                let delay = reqs.first().map(|r| r.delay).unwrap_or(0);
                let all_same_delay = reqs.iter().all(|r| r.delay == delay);
                if all_same_delay {
                    let has_headers = reqs.iter().any(|r| r.headers.is_some());
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
                } else {
                    // Mixed delays — send individually to honour per-message delay values.
                    let mut ids = Vec::with_capacity(reqs.len());
                    for req in reqs {
                        let r = send::send_message(
                            &state.pool,
                            &name,
                            req.message,
                            req.headers,
                            req.delay,
                            sync_commit,
                        )
                        .await?;
                        ids.push(r.msg_id);
                    }
                    Ok((StatusCode::CREATED, Json(SendResponse::Batch { ids })))
                }
            }
        }
    }
}

/// Receive up to `max` messages from a queue. Received messages are hidden from other
/// consumers for `vt` seconds (visibility timeout). Delete them after processing to
/// prevent re-delivery. If `wait` > 0 and the queue is empty, the call long-polls for
/// up to that many seconds before returning an empty array. Set `fifo=true` to receive
/// messages in strict enqueue order within each `group_id`.
#[utoipa::path(
    get,
    path = "/v1/queues/{name}/messages",
    operation_id = "receive_messages",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name."),
        ReceiveQuery,
    ),
    responses(
        (status = 200, body = [MessageResponse], description = "Up to `max` messages. Empty array when the queue is empty or the long-poll wait expires."),
        (status = 404, body = ErrorResponse, description = "Queue does not exist."),
    )
)]
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

/// Acknowledge and permanently delete a single message. Safe to call multiple times —
/// returns 404 if the message has already been deleted or never existed.
#[utoipa::path(
    delete,
    path = "/v1/queues/{name}/messages/{id}",
    operation_id = "delete_message",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name."),
        ("id" = i64, Path, description = "Message ID returned by send or receive."),
    ),
    responses(
        (status = 204, description = "Message deleted."),
        (status = 404, body = ErrorResponse, description = "Message does not exist."),
    )
)]
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

/// Acknowledge and delete multiple messages in one request. Non-existent IDs are
/// silently ignored. The response lists only the IDs that were actually deleted.
#[utoipa::path(
    delete,
    path = "/v1/queues/{name}/messages",
    operation_id = "delete_batch",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name."),
    ),
    request_body = DeleteBatchRequest,
    responses(
        (status = 200, body = DeletedResponse, description = "IDs of messages that were deleted."),
        (status = 404, body = ErrorResponse, description = "Queue does not exist."),
    )
)]
pub async fn delete_batch(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<DeleteBatchRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let deleted = delete::delete_batch(&state.pool, &name, &body.ids).await?;
    Ok(Json(DeletedResponse { deleted }))
}

/// Extend or reduce the visibility timeout of an in-flight message. Use this to
/// prevent re-delivery when processing takes longer than the original `vt`, or to
/// make a message immediately visible again by setting `vt=0`.
#[utoipa::path(
    patch,
    path = "/v1/queues/{name}/messages/{id}",
    operation_id = "change_visibility",
    tag = "messages",
    params(
        ("name" = String, Path, description = "Queue name."),
        ("id" = i64, Path, description = "Message ID returned by receive."),
    ),
    request_body = ChangeVisibilityRequest,
    responses(
        (status = 200, body = ChangeVisibilityResponse, description = "Updated visibility window."),
        (status = 404, body = ErrorResponse, description = "Message does not exist."),
    )
)]
pub async fn change_visibility(
    State(state): State<AppState>,
    Path((name, id)): Path<(String, i64)>,
    Json(body): Json<ChangeVisibilityRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let result = visibility::change_visibility(&state.pool, &name, id, body.vt).await?;
    Ok(Json(ChangeVisibilityResponse {
        id: result.msg_id,
        visible_at: result.visible_at,
    }))
}
