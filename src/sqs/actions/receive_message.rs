use std::collections::HashMap;

use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::receive;
use crate::sqs::context::SqsContext;
use crate::sqs::error::SqsError;
use crate::sqs::receipt;
use crate::sqs::types::{ReceiveMessageRequest, ReceiveMessageResponse, SqsMessage};
use crate::sqs::util::{md5_of, queue_name_from_url, strip_fifo_suffix};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: ReceiveMessageRequest,
) -> Result<impl IntoResponse, SqsError> {
    let raw_name = queue_name_from_url(req.queue_url.as_deref(), &ctx)?;
    let (queue_name, is_fifo) = strip_fifo_suffix(raw_name);

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
            attributes.insert(
                "ApproximateReceiveCount".to_string(),
                m.read_count.to_string(),
            );
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

#[derive(serde::Deserialize)]
struct StoredMessage {
    #[serde(rename = "Body")]
    body: String,
}

fn extract_body(message: &serde_json::Value) -> String {
    serde_json::from_value::<StoredMessage>(message.clone())
        .map(|m| m.body)
        .unwrap_or_else(|_| {
            tracing::warn!("stored message missing Body field; serializing raw value");
            message.to_string()
        })
}
