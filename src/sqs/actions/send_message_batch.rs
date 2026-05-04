use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::send;
use crate::sqs::context::SqsContext;
use crate::sqs::error::{SqsError, SqsErrorCode};
use crate::sqs::types::{
    SendMessageBatchRequest, SendMessageBatchResponse, SendMessageBatchResultEntry,
};
use crate::sqs::util::{
    md5_of, message_attributes_to_headers, queue_name_from_url, strip_fifo_suffix,
};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: SendMessageBatchRequest,
) -> Result<impl IntoResponse, SqsError> {
    if req.entries.is_empty() {
        return Err(ctx.error(SqsErrorCode::EmptyBatchRequest));
    }
    if req.entries.len() > 10 {
        return Err(ctx.error(SqsErrorCode::TooManyEntriesInBatchRequest));
    }

    let raw_name = queue_name_from_url(req.queue_url.as_deref(), &ctx)?;
    let (queue_name, is_fifo) = strip_fifo_suffix(raw_name);

    let msg_ids: Vec<i64> = if is_fifo {
        let mut ids = Vec::with_capacity(req.entries.len());
        for entry in &req.entries {
            let group_id = entry
                .message_group_id
                .as_deref()
                .ok_or_else(|| ctx.error(SqsErrorCode::InvalidMessageContents))?;
            let headers = message_attributes_to_headers(
                &entry.message_attributes,
                &entry.message_group_id,
                &entry.message_deduplication_id,
            );
            let body_json = serde_json::json!({ "Body": entry.message_body });
            let r = send::send_message_fifo(
                &state.pool,
                &queue_name,
                body_json,
                group_id,
                entry.message_deduplication_id.as_deref(),
                headers,
                entry.delay_seconds,
                true,
            )
            .await
            .map_err(|e| ctx.internal_error(e))?;
            ids.push(r.msg_id);
        }
        ids
    } else {
        let delay = req.entries.first().map(|e| e.delay_seconds).unwrap_or(0);
        let messages: Vec<serde_json::Value> = req
            .entries
            .iter()
            .map(|e| serde_json::json!({ "Body": e.message_body }))
            .collect();
        let headers: Vec<serde_json::Value> = req
            .entries
            .iter()
            .map(|e| {
                message_attributes_to_headers(
                    &e.message_attributes,
                    &e.message_group_id,
                    &e.message_deduplication_id,
                )
                .unwrap_or(serde_json::Value::Null)
            })
            .collect();
        send::send_batch(
            &state.pool,
            &queue_name,
            messages,
            Some(headers),
            delay,
            true,
        )
        .await
        .map_err(|e| ctx.internal_error(e))?
        .msg_ids
    };

    let successful: Vec<SendMessageBatchResultEntry> = req
        .entries
        .iter()
        .zip(msg_ids.iter())
        .map(|(entry, &msg_id)| SendMessageBatchResultEntry {
            id: entry.id.clone(),
            message_id: msg_id.to_string(),
            md5_of_message_body: md5_of(&entry.message_body),
        })
        .collect();

    Ok(ctx.ok(SendMessageBatchResponse {
        successful,
        failed: vec![],
    }))
}
