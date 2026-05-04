use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::topic;
use crate::sns::context::SnsContext;
use crate::sns::error::{SnsError, SnsErrorCode};
use crate::sns::types::{SubscribeRequest, SubscribeResponse};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SnsContext,
    req: SubscribeRequest,
) -> Result<impl IntoResponse, SnsError> {
    let protocol = req.protocol.as_str();
    if !matches!(protocol, "sqs" | "http" | "https") {
        return Err(ctx.error(SnsErrorCode::InvalidParameter));
    }

    let topic_name = ctx
        .topic_name_from_arn(&req.topic_arn)
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?
        .to_string();

    let (endpoint, queue_name) = match protocol {
        "sqs" => {
            let qname = req
                .endpoint
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .filter(|s| !s.is_empty())
                .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?
                .to_string();
            let ep = format!("sqs://{qname}");
            (ep, Some(qname))
        }
        _ => (req.endpoint.clone(), None),
    };

    let sub = topic::subscribe(
        &state.pool,
        &topic_name,
        protocol,
        &endpoint,
        queue_name.as_deref(),
        false, // SNS wire protocol defaults to envelope delivery
    )
    .await
    .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.ok(SubscribeResponse {
        subscription_arn: ctx.subscription_arn_for(&sub),
    }))
}
