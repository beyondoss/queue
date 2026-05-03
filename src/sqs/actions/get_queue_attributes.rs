use std::collections::HashMap;

use axum::extract::State;
use axum::response::IntoResponse;

use crate::ops::queue_admin;
use crate::sqs::context::SqsContext;
use crate::sqs::error::{SqsError, SqsErrorCode};
use crate::sqs::types::{GetQueueAttributesRequest, GetQueueAttributesResponse};
use crate::sqs::util::queue_name_from_url;
use crate::AppState;

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: GetQueueAttributesRequest,
) -> Result<impl IntoResponse, SqsError> {
    let queue_name = queue_name_from_url(req.queue_url.as_deref(), &ctx)?;

    let metrics = queue_admin::get_queue_metrics(&state.pool, &queue_name)
        .await
        .map_err(|e| match e {
            crate::error::ApiError::QueueNotFound(_) => ctx.error(SqsErrorCode::NonExistentQueue),
            other => ctx.internal_error(other),
        })?;

    let want_all = req.attribute_names.is_empty()
        || req.attribute_names.iter().any(|n| n == "All");

    let mut attributes: HashMap<String, String> = HashMap::new();

    let include = |name: &str| want_all || req.attribute_names.iter().any(|n| n == name);

    if include("ApproximateNumberOfMessages") {
        attributes.insert(
            "ApproximateNumberOfMessages".to_string(),
            metrics.queue_length.to_string(),
        );
    }
    if include("ApproximateNumberOfMessagesNotVisible") {
        attributes.insert(
            "ApproximateNumberOfMessagesNotVisible".to_string(),
            "0".to_string(),
        );
    }
    if include("CreatedTimestamp") {
        attributes.insert(
            "CreatedTimestamp".to_string(),
            metrics.scrape_time.timestamp().to_string(),
        );
    }
    if include("LastModifiedTimestamp") {
        attributes.insert(
            "LastModifiedTimestamp".to_string(),
            metrics.scrape_time.timestamp().to_string(),
        );
    }
    if include("VisibilityTimeout") {
        attributes.insert(
            "VisibilityTimeout".to_string(),
            state.config.default_visibility_timeout.to_string(),
        );
    }
    if include("QueueArn") {
        attributes.insert(
            "QueueArn".to_string(),
            format!("arn:aws:sqs:us-east-1:000000000000:{}", queue_name),
        );
    }

    Ok(ctx.ok(GetQueueAttributesResponse { attributes }))
}
