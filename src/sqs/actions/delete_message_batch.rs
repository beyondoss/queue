use std::collections::HashMap;

use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::delete;
use crate::sqs::context::SqsContext;
use crate::sqs::error::{SqsError, SqsErrorCode};
use crate::sqs::receipt;
use crate::sqs::types::{
    DeleteMessageBatchRequest, DeleteMessageBatchResponse, DeleteMessageBatchResultEntry,
};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: DeleteMessageBatchRequest,
) -> Result<impl IntoResponse, SqsError> {
    if req.entries.is_empty() {
        return Err(ctx.error(SqsErrorCode::EmptyBatchRequest));
    }
    if req.entries.len() > 10 {
        return Err(ctx.error(SqsErrorCode::TooManyEntriesInBatchRequest));
    }

    // Group entries by queue (receipt handles encode the queue name).
    let mut by_queue: HashMap<String, Vec<(String, i64)>> = HashMap::new();
    let mut failed = Vec::new();

    for entry in &req.entries {
        match receipt::decode(&entry.receipt_handle) {
            Ok((queue_name, msg_id)) => {
                by_queue
                    .entry(queue_name)
                    .or_default()
                    .push((entry.id.clone(), msg_id));
            }
            Err(_) => {
                failed.push(serde_json::json!({
                    "Id": entry.id,
                    "SenderFault": true,
                    "Code": "ReceiptHandleIsInvalid",
                    "Message": "The input receipt handle is not a valid receipt handle.",
                }));
            }
        }
    }

    let mut successful = Vec::new();

    for (queue_name, entries) in by_queue {
        let (ids, msg_ids): (Vec<String>, Vec<i64>) = entries.into_iter().unzip();
        match delete::delete_batch(&state.pool, &queue_name, &msg_ids).await {
            Ok(_) => {
                // delete is idempotent — all valid receipt handles succeed
                for id in ids {
                    successful.push(DeleteMessageBatchResultEntry { id });
                }
            }
            Err(e) => {
                for id in ids {
                    failed.push(serde_json::json!({
                        "Id": id,
                        "SenderFault": false,
                        "Code": "InternalFailure",
                        "Message": e.to_string(),
                    }));
                }
            }
        }
    }

    Ok(ctx.ok(DeleteMessageBatchResponse { successful, failed }))
}
