use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::visibility;
use crate::sqs::context::SqsContext;
use crate::sqs::error::{SqsError, SqsErrorCode};
use crate::sqs::receipt;
use crate::sqs::types::ChangeMessageVisibilityRequest;

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: ChangeMessageVisibilityRequest,
) -> Result<impl IntoResponse, SqsError> {
    let (queue_name, msg_id) = receipt::decode(&req.receipt_handle)
        .map_err(|_| ctx.error(SqsErrorCode::ReceiptHandleIsInvalid))?;

    visibility::change_visibility(&state.pool, &queue_name, msg_id, req.visibility_timeout)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.empty_ok())
}
