use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::AppState;
use crate::error::{ApiError, ErrorResponse};
use crate::ops::topic;
use crate::ops::topic::{TopicMessage, TopicSubscription};

// ---------------------------------------------------------------------------
// send
// ---------------------------------------------------------------------------

/// Request body for publishing a message to a topic.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct TopicSendRequest {
    /// Message body to fan out to all matching queues and HTTP endpoints. Any JSON value.
    #[schema(value_type = Object)]
    pub message: serde_json::Value,
    /// Optional metadata to attach to each enqueued copy. Any JSON object.
    #[schema(nullable, value_type = Object)]
    pub headers: Option<serde_json::Value>,
    /// Delivery delay in seconds applied to each enqueued message. Default: `0`.
    #[serde(default)]
    pub delay: i32,
}

/// Result of a topic publish operation.
#[derive(Serialize, utoipa::ToSchema)]
pub struct TopicSendResponse {
    /// Number of queues that matched the routing key and received a copy of the message.
    pub queues_matched: i64,
    /// One entry per matched queue, containing the queue name and assigned message ID.
    pub messages: Vec<TopicMessage>,
}

/// Publish a message to a topic. The routing key is matched against all subscription
/// patterns (glob syntax). One copy of the message is enqueued in each matching queue
/// and an HTTP POST is dispatched to each matching HTTP/HTTPS endpoint.
///
/// HTTP subscribers receive the raw message body or an SNS-style notification envelope
/// depending on how the subscription was created (`envelope` flag at subscribe time).
#[utoipa::path(
    post,
    path = "/v1/topics/{routing_key}",
    operation_id = "publish_topic",
    tag = "topics",
    params(
        ("routing_key" = String, Path, description = "Routing key matched against subscription patterns using glob syntax (e.g. `payments.created`, `orders.*`)."),
    ),
    request_body = TopicSendRequest,
    responses(
        (status = 201, body = TopicSendResponse, description = "Message published. Returns the matched queue count and per-queue message IDs."),
        (status = 404, body = ErrorResponse, description = "No subscriptions match the routing key."),
    )
)]
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
        Json(TopicSendResponse {
            queues_matched: result.queues_matched() as i64,
            messages: result.messages,
        }),
    ))
}

// ---------------------------------------------------------------------------
// bindings
// ---------------------------------------------------------------------------

/// Request body to create a topic subscription. Provide either `queue_name` (to route
/// into an internal queue) or `protocol` + `endpoint` (for HTTP/HTTPS webhook delivery).
/// The two forms are mutually exclusive.
#[derive(Deserialize, utoipa::ToSchema)]
pub struct SubscribeRequest {
    /// Name of an existing queue to deliver matching messages to. Mutually exclusive
    /// with `protocol` and `endpoint`.
    #[schema(nullable)]
    pub queue_name: Option<String>,
    /// Delivery protocol for webhook subscriptions. One of `"http"` or `"https"`.
    /// Required when `queue_name` is absent. Mutually exclusive with `queue_name`.
    #[schema(nullable)]
    pub protocol: Option<String>,
    /// HTTP or HTTPS URL to POST the message to. Required when `protocol` is set.
    #[schema(nullable)]
    pub endpoint: Option<String>,
    /// When `true`, HTTP/HTTPS subscribers receive an SNS-compatible notification
    /// envelope instead of the raw payload. Default: `false` (raw delivery).
    #[serde(default)]
    pub envelope: bool,
}

/// Subscribe a queue or HTTP endpoint to a topic pattern. Messages published to any
/// routing key that matches `pattern` (glob syntax) will be delivered to this subscriber.
///
/// Queue subscription: provide `queue_name` — messages are enqueued directly.
/// Webhook subscription: provide `protocol` (`"http"` or `"https"`) and `endpoint` URL.
#[utoipa::path(
    post,
    path = "/v1/topics/{pattern}/subscriptions",
    operation_id = "subscribe",
    tag = "topics",
    params(
        ("pattern" = String, Path, description = "Glob pattern matched against routing keys at publish time (e.g. `payments.*`, `**` to match all topics)."),
    ),
    request_body = SubscribeRequest,
    responses(
        (status = 201, body = TopicSubscription, description = "Subscription created."),
        (status = 400, body = ErrorResponse, description = "Invalid parameters or conflicting options."),
    )
)]
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

/// Remove a topic subscription by ID. Idempotent: returns 404 if the subscription
/// no longer exists.
#[utoipa::path(
    delete,
    path = "/v1/topics/{pattern}/subscriptions/{id}",
    operation_id = "unsubscribe",
    tag = "topics",
    params(
        ("pattern" = String, Path, description = "Topic pattern (for URL structure; the subscription ID alone uniquely identifies the record)."),
        ("id" = i64, Path, description = "Subscription ID returned by subscribe."),
    ),
    responses(
        (status = 204, description = "Subscription removed."),
        (status = 404, body = ErrorResponse, description = "Subscription does not exist."),
    )
)]
pub async fn unsubscribe_queue(
    State(state): State<AppState>,
    Path((pattern, id)): Path<(String, i64)>,
) -> Result<impl IntoResponse, ApiError> {
    let _ = pattern; // id is globally unique; pattern is for URL structure
    topic::unsubscribe_by_id(&state.pool, id).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// List all subscriptions for a topic pattern. Returns subscriptions whose pattern
/// exactly matches the given value.
#[utoipa::path(
    get,
    path = "/v1/topics/{pattern}/subscriptions",
    operation_id = "list_topic_subscriptions",
    tag = "topics",
    params(
        ("pattern" = String, Path, description = "Exact topic pattern to look up."),
    ),
    responses(
        (status = 200, body = [TopicSubscription], description = "Subscriptions for this pattern."),
    )
)]
pub async fn list_subscriptions(
    State(state): State<AppState>,
    Path(pattern): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let bindings = topic::list_by_pattern(&state.pool, &pattern).await?;
    Ok(Json(bindings))
}
