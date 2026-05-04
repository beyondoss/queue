use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use crate::AppState;
use crate::error::ApiError;
use crate::ops::topic;

// ---------------------------------------------------------------------------
// send
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct TopicSendRequest {
    pub message: serde_json::Value,
    pub headers: Option<serde_json::Value>,
    #[serde(default)]
    pub delay: i32,
}

pub async fn send_topic(
    State(state): State<AppState>,
    Path(routing_key): Path<String>,
    Json(body): Json<TopicSendRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let result = topic::send_topic(
        &state.pool,
        &routing_key,
        body.message.clone(),
        body.headers,
        body.delay,
    )
    .await?;

    // Build SNS-style envelope for envelope=true HTTP subscribers
    let message_id = Uuid::new_v4().to_string();
    let timestamp = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let topic_arn = format!("arn:aws:sns:us-east-1:000000000000:{routing_key}");
    let message_str = body.message.to_string();
    let envelope = serde_json::json!({
        "Type": "Notification",
        "MessageId": message_id,
        "TopicArn": topic_arn,
        "Message": message_str,
        "Timestamp": timestamp,
        "SignatureVersion": "2",
        "Signature": state.signer.sign_notification(&topic_arn, &message_id, &message_str, &timestamp),
        "SigningCertURL": format!("{}/SimpleNotificationService.pem", state.base_url.trim_end_matches('/')),
    });

    // raw_delivery=true → post body.message; raw_delivery=false → post envelope
    topic::queue_http_deliveries(&state.pool, &routing_key, &body.message, Some(&envelope)).await?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "queues_matched": result.queues_matched(),
            "messages": result.messages,
        })),
    ))
}

// ---------------------------------------------------------------------------
// bindings
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct SubscribeRequest {
    // SQS form
    pub queue_name: Option<String>,
    // HTTP/HTTPS form
    pub protocol: Option<String>,
    pub endpoint: Option<String>,
    // Opt-in to SNS notification envelope; default false means raw payload delivery
    #[serde(default)]
    pub envelope: bool,
}

/// POST /v1/topics/{pattern}/subscriptions
/// Binds a queue or HTTP endpoint to a topic pattern. Idempotent.
pub async fn subscribe_queue(
    State(state): State<AppState>,
    Path(pattern): Path<String>,
    Json(body): Json<SubscribeRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let (protocol, endpoint, queue_name) = if let Some(qname) = body.queue_name {
        let ep = format!("sqs://{qname}");
        ("sqs".to_string(), ep, Some(qname))
    } else if let (Some(proto), Some(ep)) = (body.protocol, body.endpoint) {
        if !matches!(proto.as_str(), "http" | "https") {
            return Err(ApiError::BadRequest(
                "protocol must be 'sqs', 'http', or 'https'".into(),
            ));
        }
        (proto, ep, None)
    } else {
        return Err(ApiError::BadRequest(
            "provide either queue_name or protocol+endpoint".into(),
        ));
    };

    let raw_delivery = !body.envelope;
    let binding = topic::subscribe(
        &state.pool,
        &pattern,
        &protocol,
        &endpoint,
        queue_name.as_deref(),
        raw_delivery,
    )
    .await?;
    Ok((StatusCode::CREATED, Json(binding)))
}

/// DELETE /v1/topics/{pattern}/subscriptions/{queue_name}
/// Removes an SQS queue subscription by name. Returns 404 if not found.
pub async fn unsubscribe_queue(
    State(state): State<AppState>,
    Path((pattern, queue_name)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    // REST API subscriptions store endpoint as "sqs://{queue_name}".
    let endpoint = format!("sqs://{queue_name}");
    topic::unsubscribe(&state.pool, &pattern, &endpoint).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// GET /v1/topics/{pattern}/subscriptions
/// Lists all subscriptions bound to this pattern.
pub async fn list_subscriptions(
    State(state): State<AppState>,
    Path(pattern): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let bindings = topic::list_by_pattern(&state.pool, &pattern).await?;
    Ok(Json(bindings))
}
