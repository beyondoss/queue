use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::queue_admin;
use crate::sqs::context::SqsContext;
use crate::sqs::error::{SqsError, SqsErrorCode};
use crate::sqs::types::{GetQueueUrlRequest, GetQueueUrlResponse};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: GetQueueUrlRequest,
) -> Result<impl IntoResponse, SqsError> {
    let queues = queue_admin::list_queues(&state.pool, Some(&req.queue_name))
        .await
        .map_err(|e| ctx.internal_error(e))?;

    let exists = queues.iter().any(|q| q.queue_name == req.queue_name);
    if !exists {
        return Err(ctx.error(SqsErrorCode::NonExistentQueue));
    }

    Ok(ctx.ok(GetQueueUrlResponse {
        queue_url: ctx.queue_url(&req.queue_name),
    }))
}
