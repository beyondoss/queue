use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::visibility::{self, BatchVisibilityEntry};
use crate::sqs::context::SqsContext;
use crate::sqs::error::{SqsError, SqsErrorCode};
use crate::sqs::receipt;
use crate::sqs::types::{
    ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResponse,
    ChangeMessageVisibilityBatchResultEntry,
};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: ChangeMessageVisibilityBatchRequest,
) -> Result<impl IntoResponse, SqsError> {
    if req.entries.is_empty() {
        return Err(ctx.error(SqsErrorCode::EmptyBatchRequest));
    }
    if req.entries.len() > 10 {
        return Err(ctx.error(SqsErrorCode::TooManyEntriesInBatchRequest));
    }

    let mut successful = Vec::new();
    let mut failed = Vec::new();

    // Group by queue name (receipt handles may reference different queues, though
    // the SQS spec requires all entries in a batch to be from the same queue)
    let mut by_queue: std::collections::HashMap<String, Vec<(String, BatchVisibilityEntry)>> =
        std::collections::HashMap::new();

    for entry in &req.entries {
        match receipt::decode(&entry.receipt_handle) {
            Ok((queue_name, msg_id)) => {
                by_queue.entry(queue_name).or_default().push((
                    entry.id.clone(),
                    BatchVisibilityEntry {
                        msg_id,
                        vt_secs: entry.visibility_timeout,
                    },
                ));
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

    for (queue_name, entries) in by_queue {
        let ids: Vec<String> = entries.iter().map(|(id, _)| id.clone()).collect();
        let vis_entries: Vec<BatchVisibilityEntry> = entries.into_iter().map(|(_, e)| e).collect();

        match visibility::change_visibility_batch(&state.pool, &queue_name, vis_entries).await {
            Ok(_) => {
                for id in ids {
                    successful.push(ChangeMessageVisibilityBatchResultEntry { id });
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

    Ok(ctx.ok(ChangeMessageVisibilityBatchResponse { successful, failed }))
}
