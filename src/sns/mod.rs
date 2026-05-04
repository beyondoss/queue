pub mod actions;
pub mod context;
pub mod error;
pub mod types;

use axum::body::Bytes;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use context::SnsContext;
use error::{SnsErrorCode, SnsProtocol};
use types::*;

use crate::AppState;
use crate::parse_service_body;

pub async fn handle_service_request(state: AppState, headers: HeaderMap, body: Bytes) -> Response {
    let (is_json, action, parsed) = parse_service_body(&headers, &body, "AmazonSNS.");
    let protocol = if is_json {
        SnsProtocol::Json
    } else {
        SnsProtocol::Query
    };
    let ctx = SnsContext::new(protocol, state.base_url.clone(), &action);
    dispatch(&state, ctx, &action, parsed).await
}

async fn dispatch(
    state: &AppState,
    ctx: SnsContext,
    action: &str,
    body: serde_json::Value,
) -> Response {
    macro_rules! dispatch {
        ($req_type:ty, $handler:path) => {{
            let req: $req_type = match serde_json::from_value(body) {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!(action, error = %e, "failed to deserialize SNS request");
                    return ctx.error(SnsErrorCode::InvalidParameter).into_response();
                }
            };
            match $handler(axum::extract::State(state.clone()), ctx, req).await {
                Ok(r) => r.into_response(),
                Err(e) => e.into_response(),
            }
        }};
    }

    match action {
        "CreateTopic" => dispatch!(CreateTopicRequest, actions::create_topic::handle),
        "DeleteTopic" => dispatch!(DeleteTopicRequest, actions::delete_topic::handle),
        "ListTopics" => {
            match actions::list_topics::handle(axum::extract::State(state.clone()), ctx).await {
                Ok(r) => r.into_response(),
                Err(e) => e.into_response(),
            }
        }
        "Subscribe" => dispatch!(SubscribeRequest, actions::subscribe::handle),
        "Unsubscribe" => dispatch!(UnsubscribeRequest, actions::unsubscribe::handle),
        "ListSubscriptions" => {
            match actions::list_subscriptions::handle(axum::extract::State(state.clone()), ctx)
                .await
            {
                Ok(r) => r.into_response(),
                Err(e) => e.into_response(),
            }
        }
        "ListSubscriptionsByTopic" => dispatch!(
            ListSubscriptionsByTopicRequest,
            actions::list_subscriptions_by_topic::handle
        ),
        "Publish" => dispatch!(PublishRequest, actions::publish::handle),
        "GetTopicAttributes" => {
            dispatch!(
                GetTopicAttributesRequest,
                actions::get_topic_attributes::handle
            )
        }
        "SetTopicAttributes" => {
            // No-op: we don't support delivery policies or filters, but return success
            // so SDK setup flows don't break.
            let _: SetTopicAttributesRequest = match serde_json::from_value(body) {
                Ok(r) => r,
                Err(_) => return ctx.error(SnsErrorCode::InvalidParameter).into_response(),
            };
            ctx.empty_ok()
        }
        "GetSubscriptionAttributes" => dispatch!(
            GetSubscriptionAttributesRequest,
            actions::get_subscription_attributes::handle
        ),
        "ConfirmSubscription" => {
            // SQS subscriptions are auto-confirmed; return the subscription ARN from the token
            // field (we encode it there in Subscribe). Real SNS sends an HTTP POST to confirm;
            // we skip that since we're the delivery target ourselves.
            ctx.empty_ok()
        }
        _ => {
            tracing::warn!(action, "unknown SNS action");
            ctx.error(SnsErrorCode::InvalidParameter).into_response()
        }
    }
}
