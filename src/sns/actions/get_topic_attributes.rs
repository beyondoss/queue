use std::collections::HashMap;

use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::sns::context::SnsContext;
use crate::sns::error::{SnsError, SnsErrorCode};
use crate::sns::types::{GetTopicAttributesRequest, GetTopicAttributesResponse};

pub async fn handle(
    State(_state): State<AppState>,
    ctx: SnsContext,
    req: GetTopicAttributesRequest,
) -> Result<impl IntoResponse, SnsError> {
    let topic_name = ctx
        .topic_name_from_arn(&req.topic_arn)
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?;

    let mut attrs = HashMap::new();
    attrs.insert("TopicArn".to_string(), ctx.topic_arn(topic_name));
    attrs.insert("Owner".to_string(), "000000000000".to_string());
    attrs.insert("SubscriptionsConfirmed".to_string(), "0".to_string());
    attrs.insert("SubscriptionsPending".to_string(), "0".to_string());
    attrs.insert("SubscriptionsDeleted".to_string(), "0".to_string());
    attrs.insert("DisplayName".to_string(), String::new());

    Ok(ctx.ok(GetTopicAttributesResponse { attributes: attrs }))
}
