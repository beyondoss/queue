use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::queue_admin;
use crate::sqs::context::SqsContext;
use crate::sqs::error::SqsError;
use crate::sqs::types::{ListQueuesRequest, ListQueuesResponse};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: ListQueuesRequest,
) -> Result<impl IntoResponse, SqsError> {
    let queues = queue_admin::list_queues(&state.pool)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    let queue_urls: Vec<String> = queues
        .into_iter()
        .filter(|q| {
            req.queue_name_prefix
                .as_deref()
                .map(|prefix| q.queue_name.starts_with(prefix))
                .unwrap_or(true)
        })
        .map(|q| ctx.queue_url(&q.queue_name))
        .collect();

    Ok(ctx.ok(ListQueuesResponse { queue_urls }))
}
