use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::event;
use crate::sns::context::SnsContext;
use crate::sns::error::{SnsError, SnsErrorCode};
use crate::sns::types::UnsubscribeRequest;

pub async fn handle(
    State(state): State<AppState>,
    ctx: SnsContext,
    req: UnsubscribeRequest,
) -> Result<impl IntoResponse, SnsError> {
    let (topic_name, key) = ctx
        .parse_subscription_arn(&req.subscription_arn)
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?;

    // HTTP/HTTPS subs use a numeric id as the ARN key; SQS subs use queue_name.
    if let Ok(id) = key.parse::<i64>() {
        event::unsubscribe_by_id(&state.pool, id)
            .await
            .map_err(|e| ctx.internal_error(e))?;
    } else {
        let endpoint = format!("sqs://{key}");
        event::unsubscribe(&state.pool, &topic_name, &endpoint)
            .await
            .map_err(|e| ctx.internal_error(e))?;
    }

    Ok(ctx.empty_ok())
}
