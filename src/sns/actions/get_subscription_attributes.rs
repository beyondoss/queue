use std::collections::HashMap;

use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::sns::context::SnsContext;
use crate::sns::error::{SnsError, SnsErrorCode};
use crate::sns::types::{GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse};

pub async fn handle(
    State(_state): State<AppState>,
    ctx: SnsContext,
    req: GetSubscriptionAttributesRequest,
) -> Result<impl IntoResponse, SnsError> {
    let (topic_name, queue_name) = ctx
        .parse_subscription_arn(&req.subscription_arn)
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?;

    let mut attrs = HashMap::new();
    attrs.insert("SubscriptionArn".to_string(), req.subscription_arn.clone());
    attrs.insert("TopicArn".to_string(), ctx.topic_arn(&topic_name));
    attrs.insert("Owner".to_string(), "000000000000".to_string());
    attrs.insert("Protocol".to_string(), "sqs".to_string());
    attrs.insert("Endpoint".to_string(), ctx.queue_endpoint(&queue_name));
    attrs.insert("PendingConfirmation".to_string(), "false".to_string());
    attrs.insert("ConfirmationWasAuthenticated".to_string(), "true".to_string());
    attrs.insert("RawMessageDelivery".to_string(), "false".to_string());

    Ok(ctx.ok(GetSubscriptionAttributesResponse { attributes: attrs }))
}
