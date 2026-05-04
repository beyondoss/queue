use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::topic;
use crate::sns::context::SnsContext;
use crate::sns::error::{SnsError, SnsErrorCode};
use crate::sns::types::{SubscribeRequest, SubscribeResponse};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SnsContext,
    req: SubscribeRequest,
) -> Result<impl IntoResponse, SnsError> {
    if req.protocol != "sqs" {
        return Err(ctx.error(SnsErrorCode::InvalidParameter));
    }

    let topic_name = ctx
        .topic_name_from_arn(&req.topic_arn)
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?
        .to_string();

    // Extract queue name from endpoint URL (last path segment)
    let queue_name = req
        .endpoint
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?
        .to_string();

    topic::subscribe(&state.pool, &topic_name, &queue_name)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.ok(SubscribeResponse {
        subscription_arn: ctx.subscription_arn(&topic_name, &queue_name),
    }))
}
