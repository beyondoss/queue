use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::sns::context::SnsContext;
use crate::sns::error::SnsError;
use crate::sns::types::{CreateTopicRequest, CreateTopicResponse};

pub async fn handle(
    State(_state): State<AppState>,
    ctx: SnsContext,
    req: CreateTopicRequest,
) -> Result<impl IntoResponse, SnsError> {
    Ok(ctx.ok(CreateTopicResponse {
        topic_arn: ctx.topic_arn(&req.name),
    }))
}
