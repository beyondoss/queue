use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::delete;
use crate::sqs::context::SqsContext;
use crate::sqs::error::{SqsError, SqsErrorCode};
use crate::sqs::receipt;
use crate::sqs::types::DeleteMessageRequest;

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: DeleteMessageRequest,
) -> Result<impl IntoResponse, SqsError> {
    let (queue_name, msg_id) = receipt::decode(&req.receipt_handle)
        .map_err(|_| ctx.error(SqsErrorCode::ReceiptHandleIsInvalid))?;

    delete::delete_message(&state.pool, &queue_name, msg_id)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.empty_ok())
}
