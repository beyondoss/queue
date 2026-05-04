use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::topic;
use crate::sns::context::SnsContext;
use crate::sns::error::SnsError;
use crate::sns::types::{ListTopicsResponse, TopicEntry};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SnsContext,
) -> Result<impl IntoResponse, SnsError> {
    let names = topic::list_sns_topics(&state.pool)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.ok(ListTopicsResponse {
        topics: names
            .into_iter()
            .map(|n| TopicEntry {
                topic_arn: ctx.topic_arn(&n),
            })
            .collect(),
    }))
}
