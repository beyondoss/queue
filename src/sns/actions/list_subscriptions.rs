use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::topic::{self, TopicSubscription};
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
        subscriptions: subs.iter().map(|s| subscription_entry(&ctx, s)).collect(),
    }))
}

pub fn subscription_entry(ctx: &SnsContext, sub: &TopicSubscription) -> SubscriptionEntry {
    let endpoint = match sub.protocol.as_str() {
        "sqs" => ctx.queue_endpoint(sub.queue_name.as_deref().unwrap_or("")),
        _ => sub.endpoint.clone(),
    };
    SubscriptionEntry {
        subscription_arn: ctx.subscription_arn_for(sub),
        owner: "000000000000".to_string(),
        protocol: sub.protocol.clone(),
        endpoint,
        topic_arn: ctx.topic_arn(&sub.pattern),
    }
}
