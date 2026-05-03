use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::queue_admin;
use crate::sqs::context::SqsContext;
use crate::sqs::error::{SqsError, SqsErrorCode};
use crate::sqs::types::DeleteQueueRequest;
use crate::sqs::util::queue_name_from_url;

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: DeleteQueueRequest,
) -> Result<impl IntoResponse, SqsError> {
    let queue_name = queue_name_from_url(req.queue_url.as_deref(), &ctx)?;

    let dropped = queue_admin::delete_queue(&state.pool, &queue_name)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    if !dropped {
        return Err(ctx.error(SqsErrorCode::NonExistentQueue));
    }

    Ok(ctx.empty_ok())
}
