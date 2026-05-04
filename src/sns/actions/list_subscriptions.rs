use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::topic;
use crate::sns::context::SnsContext;
use crate::sns::error::SnsError;
use crate::sns::types::{ListSubscriptionsResponse, SubscriptionEntry};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SnsContext,
) -> Result<impl IntoResponse, SnsError> {
    let subs = topic::list_all_subscriptions(&state.pool)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.ok(ListSubscriptionsResponse {
        subscriptions: subs
            .into_iter()
            .map(|s| subscription_entry(&ctx, &s.pattern, &s.queue_name))
            .collect(),
    }))
}

pub fn subscription_entry(ctx: &SnsContext, topic: &str, queue: &str) -> SubscriptionEntry {
    SubscriptionEntry {
        subscription_arn: ctx.subscription_arn(topic, queue),
        owner: "000000000000".to_string(),
        protocol: "sqs".to_string(),
        endpoint: ctx.queue_endpoint(queue),
        topic_arn: ctx.topic_arn(topic),
    }
}
