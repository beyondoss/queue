use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::topic;
use crate::sns::actions::list_subscriptions::subscription_entry;
use crate::sns::context::SnsContext;
use crate::sns::error::{SnsError, SnsErrorCode};
use crate::sns::types::{ListSubscriptionsByTopicRequest, ListSubscriptionsResponse};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SnsContext,
    req: ListSubscriptionsByTopicRequest,
) -> Result<impl IntoResponse, SnsError> {
    let topic_name = ctx
        .topic_name_from_arn(&req.topic_arn)
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?
        .to_string();

    let subs = topic::list_by_pattern(&state.pool, &topic_name)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.ok(ListSubscriptionsResponse {
        subscriptions: subs
            .into_iter()
            .map(|s| subscription_entry(&ctx, &s.pattern, &s.queue_name))
            .collect(),
    }))
}
