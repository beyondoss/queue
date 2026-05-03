pub mod actions;
pub mod context;
pub mod error;
pub mod receipt;
pub mod types;
pub mod util;

use std::collections::HashMap;

use crate::AppState;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use context::SqsContext;
use error::{SqsError, SqsErrorCode, SqsProtocol};
use types::*;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/{account_id}/{queue_name}", post(queue_handler))
        .route("/", post(service_handler))
}

async fn queue_handler(
    State(state): State<AppState>,
    Path((_account_id, queue_name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (protocol, action, mut parsed) = match detect_and_parse(&headers, &body) {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };

    let base_url = state.config.base_url();
    let ctx = SqsContext::new(protocol, base_url);
    let queue_url = ctx.queue_url(&queue_name);

    // Inject QueueUrl from path if not present in body
    if let serde_json::Value::Object(ref mut map) = parsed {
        map.entry("QueueUrl")
            .or_insert_with(|| serde_json::Value::String(queue_url));
    }

    dispatch_action(&state, ctx, &action, parsed, protocol).await
}

async fn service_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (protocol, action, parsed) = match detect_and_parse(&headers, &body) {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };

    let base_url = state.config.base_url();
    let ctx = SqsContext::new(protocol, base_url);

    dispatch_action(&state, ctx, &action, parsed, protocol).await
}

fn detect_and_parse(
    headers: &HeaderMap,
    body: &Bytes,
) -> Result<(SqsProtocol, String, serde_json::Value), SqsError> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.contains("application/x-amz-json-1.0") {
        let target = headers
            .get("x-amz-target")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let action = target
            .strip_prefix("AmazonSQS.")
            .unwrap_or(target)
            .to_string();
        let value: serde_json::Value =
            serde_json::from_slice(body).unwrap_or(serde_json::json!({}));
        Ok((SqsProtocol::Json, action, value))
    } else {
        // Query (form-encoded) or unknown — treat as Query
        let map: HashMap<String, String> = form_urlencoded::parse(body).into_owned().collect();
        let action = map.get("Action").cloned().unwrap_or_default();
        let value = serde_json::to_value(&map).unwrap_or(serde_json::json!({}));
        Ok((SqsProtocol::Query, action, value))
    }
}

async fn dispatch_action(
    state: &AppState,
    ctx: SqsContext,
    action: &str,
    body: serde_json::Value,
    _protocol: SqsProtocol,
) -> Response {
    macro_rules! dispatch {
        ($req_type:ty, $handler:path) => {{
            let req: $req_type = match serde_json::from_value(body) {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!(action, error = %e, "failed to deserialize SQS request");
                    return ctx.error(SqsErrorCode::InvalidMessageContents).into_response();
                }
            };
            match $handler(axum::extract::State(state.clone()), ctx, req).await {
                Ok(r) => r.into_response(),
                Err(e) => e.into_response(),
            }
        }};
    }

    match action {
        "SendMessage" => dispatch!(SendMessageRequest, actions::send_message::handle),
        "SendMessageBatch" => {
            dispatch!(SendMessageBatchRequest, actions::send_message_batch::handle)
        }
        "ReceiveMessage" => dispatch!(ReceiveMessageRequest, actions::receive_message::handle),
        "DeleteMessage" => dispatch!(DeleteMessageRequest, actions::delete_message::handle),
        "DeleteMessageBatch" => dispatch!(
            DeleteMessageBatchRequest,
            actions::delete_message_batch::handle
        ),
        "ChangeMessageVisibility" => dispatch!(
            ChangeMessageVisibilityRequest,
            actions::change_message_visibility::handle
        ),
        "ChangeMessageVisibilityBatch" => dispatch!(
            ChangeMessageVisibilityBatchRequest,
            actions::change_message_visibility_batch::handle
        ),
        "CreateQueue" => dispatch!(CreateQueueRequest, actions::create_queue::handle),
        "DeleteQueue" => dispatch!(DeleteQueueRequest, actions::delete_queue::handle),
        "GetQueueUrl" => dispatch!(GetQueueUrlRequest, actions::get_queue_url::handle),
        "GetQueueAttributes" => dispatch!(
            GetQueueAttributesRequest,
            actions::get_queue_attributes::handle
        ),
        "ListQueues" => {
            let req: ListQueuesRequest = serde_json::from_value(body).unwrap_or_default();
            match actions::list_queues::handle(axum::extract::State(state.clone()), ctx, req).await
            {
                Ok(r) => r.into_response(),
                Err(e) => e.into_response(),
            }
        }
        "PurgeQueue" => dispatch!(PurgeQueueRequest, actions::purge_queue::handle),
        _ => {
            tracing::warn!(action, "unknown SQS action");
            ctx.error(SqsErrorCode::InvalidAttributeName)
                .into_response()
        }
    }
}
