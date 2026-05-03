use axum::extract::State;
use axum::response::IntoResponse;

use crate::ops::queue_admin;
use crate::sqs::context::SqsContext;
use crate::sqs::error::SqsError;
use crate::sqs::types::{CreateQueueRequest, CreateQueueResponse};
use crate::AppState;

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: CreateQueueRequest,
) -> Result<impl IntoResponse, SqsError> {
    let is_fifo = req.attributes.get("FifoQueue").map(|v| v == "true").unwrap_or(false);

    let internal_name = if is_fifo {
        req.queue_name
            .strip_suffix(".fifo")
            .unwrap_or(&req.queue_name)
            .to_string()
    } else {
        req.queue_name.clone()
    };

    if is_fifo {
        queue_admin::create_fifo_queue(&state.pool, &internal_name)
            .await
            .map_err(|e| ctx.internal_error(e))?;
    } else {
        queue_admin::create_queue(&state.pool, &internal_name)
            .await
            .map_err(|e| ctx.internal_error(e))?;
    }

    let queue_url = ctx.queue_url(&req.queue_name);
    Ok(ctx.ok(CreateQueueResponse { queue_url }))
}
