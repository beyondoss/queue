use axum::extract::State;
use axum::response::IntoResponse;

use crate::ops::queue_admin;
use crate::sqs::context::SqsContext;
use crate::sqs::error::SqsError;
use crate::sqs::types::PurgeQueueRequest;
use crate::sqs::util::queue_name_from_url;
use crate::AppState;

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: PurgeQueueRequest,
) -> Result<impl IntoResponse, SqsError> {
    let queue_name = queue_name_from_url(req.queue_url.as_deref(), &ctx)?;

    queue_admin::purge_queue(&state.pool, &queue_name)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.empty_ok())
}
