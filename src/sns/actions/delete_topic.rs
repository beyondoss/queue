use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::topic;
use crate::sns::context::SnsContext;
use crate::sns::error::{SnsError, SnsErrorCode};
use crate::sns::types::DeleteTopicRequest;

pub async fn handle(
    State(state): State<AppState>,
    ctx: SnsContext,
    req: DeleteTopicRequest,
) -> Result<impl IntoResponse, SnsError> {
    let name = ctx
        .topic_name_from_arn(&req.topic_arn)
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?
        .to_string();

    topic::delete_sns_topic(&state.pool, &name)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.empty_ok())
}
