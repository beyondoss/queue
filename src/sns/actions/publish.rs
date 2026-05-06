use axum::extract::State;
use axum::response::IntoResponse;
use chrono::Utc;
use uuid::Uuid;

use crate::AppState;
use crate::ops::event;
use crate::sns::context::SnsContext;
use crate::sns::error::{SnsError, SnsErrorCode};
use crate::sns::types::{PublishRequest, PublishResponse};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SnsContext,
    req: PublishRequest,
) -> Result<impl IntoResponse, SnsError> {
    let arn = req
        .topic_arn
        .as_deref()
        .or(req.target_arn.as_deref())
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?;

    let topic_name = ctx
        .topic_name_from_arn(arn)
        .ok_or_else(|| ctx.error(SnsErrorCode::InvalidParameter))?
        .to_string();

    let message_id = Uuid::new_v4().to_string();
    let topic_arn = ctx.topic_arn(&topic_name);
    let timestamp = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

    // SNS notification envelope — delivered to sqs/http(s) subscribers with raw_delivery=false
    let envelope = serde_json::json!({
        "Type": "Notification",
        "MessageId": message_id,
        "TopicArn": topic_arn,
        "Subject": req.subject,
        "Message": req.message,
        "Timestamp": timestamp,
        "SignatureVersion": "2",
        "Signature": state.signer.sign_notification(&topic_arn, &message_id, &req.message, &timestamp),
        "SigningCertURL": format!("{}/SimpleNotificationService.pem", ctx.base_url.trim_end_matches('/')),
        "UnsubscribeURL": format!("{}/", ctx.base_url.trim_end_matches('/')),
    });

    // Raw message for subscribers with raw_delivery=true: just the message content.
    let raw_message = serde_json::json!({ "Message": req.message, "Subject": req.subject });

    // Stored as {"Body": "<envelope-json-string>"} so SQS ReceiveMessage surfaces
    // the envelope string in the Body field, matching real SNS→SQS delivery.
    let stored = serde_json::json!({ "Body": envelope.to_string() });

    // Fan out to SQS queues
    event::publish_event(&state.pool, &topic_name, stored, None, 0)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    // Queue HTTP/HTTPS deliveries
    event::queue_event_deliveries(&state.pool, &topic_name, &raw_message, Some(&envelope))
        .await
        .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.ok(PublishResponse { message_id }))
}
