pub mod actions;
pub mod context;
pub mod error;
pub mod receipt;
pub mod types;
pub mod util;

use crate::AppState;
use crate::parse_service_body;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use context::SqsContext;
use error::{SqsErrorCode, SqsProtocol};
use types::*;

pub fn router() -> Router<AppState> {
    Router::new().route("/{account_id}/{queue_name}", post(queue_handler))
    // POST / is handled by the gateway in lib.rs to allow SNS/SQS co-dispatch
}

pub async fn handle_service_request(state: AppState, headers: HeaderMap, body: Bytes) -> Response {
    let (is_json, action, parsed) = parse_service_body(&headers, &body, "AmazonSQS.");
    let protocol = if is_json {
        SqsProtocol::Json
    } else {
        SqsProtocol::Query
    };
    let ctx = SqsContext::new(protocol, state.base_url.clone(), &action);
    dispatch_action(&state, ctx, &action, parsed).await
}

async fn queue_handler(
    State(state): State<AppState>,
    Path((_account_id, queue_name)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let (is_json, action, mut parsed) = parse_service_body(&headers, &body, "AmazonSQS.");
    let protocol = if is_json {
        SqsProtocol::Json
    } else {
        SqsProtocol::Query
    };
    let ctx = SqsContext::new(protocol, state.base_url.clone(), &action);
    let queue_url = ctx.queue_url(&queue_name);

    // Inject QueueUrl from path if not present in body
    if let serde_json::Value::Object(ref mut map) = parsed {
        map.entry("QueueUrl")
            .or_insert_with(|| serde_json::Value::String(queue_url));
    }

    dispatch_action(&state, ctx, &action, parsed).await
}

async fn dispatch_action(
    state: &AppState,
    ctx: SqsContext,
    action: &str,
    body: serde_json::Value,
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
