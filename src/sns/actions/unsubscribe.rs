use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::topic;
use crate::sns::context::SnsContext;
use crate::sns::error::{SnsError, SnsErrorCode};
use crate::sns::types::UnsubscribeRequest;

pub async fn handle(
    State(state): State<AppState>,
    ctx: SnsContext,
    req: UnsubscribeRequest,
) -> Result<impl IntoResponse, SnsError> {
    let (topic_name, queue_name) = ctx
        .parse_subscription_arn(&req.subscription_arn)
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?;

    topic::unsubscribe(&state.pool, &topic_name, &queue_name)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.empty_ok())
}
