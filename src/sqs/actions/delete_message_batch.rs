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

    let mut successful = Vec::new();
    let mut failed = Vec::new();

    for entry in &req.entries {
        match receipt::decode(&entry.receipt_handle) {
            Ok((queue_name, msg_id)) => {
                match delete::delete_message(&state.pool, &queue_name, msg_id).await {
                    Ok(_) => successful.push(DeleteMessageBatchResultEntry {
                        id: entry.id.clone(),
                    }),
                    Err(e) => failed.push(serde_json::json!({
                        "Id": entry.id,
                        "SenderFault": false,
                        "Code": "InternalFailure",
                        "Message": e.to_string(),
                    })),
                }
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

    Ok(ctx.ok(DeleteMessageBatchResponse { successful, failed }))
}
