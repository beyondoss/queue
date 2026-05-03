use std::collections::HashMap;

use axum::extract::State;
use axum::response::IntoResponse;

use crate::ops::receive;
use crate::sqs::context::SqsContext;
use crate::sqs::error::SqsError;
use crate::sqs::receipt;
use crate::sqs::types::{ReceiveMessageRequest, ReceiveMessageResponse, SqsMessage};
use crate::sqs::util::{md5_of, queue_name_from_url};
use crate::AppState;

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: ReceiveMessageRequest,
) -> Result<impl IntoResponse, SqsError> {
    let raw_name = queue_name_from_url(req.queue_url.as_deref(), &ctx)?;
    let is_fifo = raw_name.ends_with(".fifo");
    let queue_name = if is_fifo {
        raw_name.strip_suffix(".fifo").unwrap().to_string()
    } else {
        raw_name
    };

    let vt = req
        .visibility_timeout
        .unwrap_or(state.config.default_visibility_timeout);

    let messages = if is_fifo {
        receive::receive_messages_fifo(
            &state.pool,
            &queue_name,
            req.max_number_of_messages,
            vt,
            req.wait_time_seconds,
        )
        .await
        .map_err(|e| ctx.internal_error(e))?
    } else {
        receive::receive_messages(
            &state.pool,
            &queue_name,
            req.max_number_of_messages,
            vt,
            req.wait_time_seconds,
        )
        .await
        .map_err(|e| ctx.internal_error(e))?
    };

    let sqs_messages: Vec<SqsMessage> = messages
        .into_iter()
        .map(|m| {
            let body = extract_body(&m.message);
            let receipt_handle = receipt::encode(&queue_name, m.msg_id);

            let mut attributes = HashMap::new();
            attributes.insert("ApproximateReceiveCount".to_string(), m.read_count.to_string());
            attributes.insert(
                "SentTimestamp".to_string(),
                m.enqueued_at.timestamp_millis().to_string(),
            );
            attributes.insert(
                "ApproximateFirstReceiveTimestamp".to_string(),
                m.enqueued_at.timestamp_millis().to_string(),
            );

            SqsMessage {
                message_id: m.msg_id.to_string(),
                receipt_handle,
                md5_of_body: md5_of(&body),
                body,
                attributes,
                message_attributes: HashMap::new(),
            }
        })
        .collect();

    Ok(ctx.ok(ReceiveMessageResponse {
        messages: sqs_messages,
    }))
}

fn extract_body(message: &serde_json::Value) -> String {
    message
        .get("Body")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| message.to_string())
}
