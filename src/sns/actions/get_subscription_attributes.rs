use std::collections::HashMap;

use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::topic;
use crate::sns::context::SnsContext;
use crate::sns::error::{SnsError, SnsErrorCode};
use crate::sns::types::{GetSubscriptionAttributesRequest, GetSubscriptionAttributesResponse};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SnsContext,
    req: GetSubscriptionAttributesRequest,
) -> Result<impl IntoResponse, SnsError> {
    let (topic_name, key) = ctx
        .parse_subscription_arn(&req.subscription_arn)
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?;

    let sub = if let Ok(id) = key.parse::<i64>() {
        topic::get_by_id(&state.pool, id)
            .await
            .map_err(|e| ctx.internal_error(e))?
    } else {
        topic::get_by_queue(&state.pool, &topic_name, &key)
            .await
            .map_err(|e| ctx.internal_error(e))?
    }
    .ok_or_else(|| ctx.error(SnsErrorCode::NotFound))?;

    let endpoint = match sub.protocol.as_str() {
        "sqs" => ctx.queue_endpoint(sub.queue_name.as_deref().unwrap_or("")),
        _ => sub.endpoint.clone(),
    };

    let mut attrs = HashMap::new();
    attrs.insert("SubscriptionArn".to_string(), req.subscription_arn.clone());
    attrs.insert("TopicArn".to_string(), ctx.topic_arn(&sub.pattern));
    attrs.insert("Owner".to_string(), "000000000000".to_string());
    attrs.insert("Protocol".to_string(), sub.protocol.clone());
    attrs.insert("Endpoint".to_string(), endpoint);
    attrs.insert("PendingConfirmation".to_string(), "false".to_string());
    attrs.insert(
        "ConfirmationWasAuthenticated".to_string(),
        "true".to_string(),
    );
    attrs.insert(
        "RawMessageDelivery".to_string(),
        sub.raw_delivery.to_string(),
    );

    Ok(ctx.ok(GetSubscriptionAttributesResponse { attributes: attrs }))
}
