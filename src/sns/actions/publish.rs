use axum::extract::State;
use axum::response::IntoResponse;
use chrono::Utc;
use uuid::Uuid;

use crate::AppState;
use crate::ops::topic;
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

    // SNS notification envelope — this is the SQS message body consumers receive
    let envelope = serde_json::json!({
        "Type": "Notification",
        "MessageId": message_id,
        "TopicArn": topic_arn,
        "Subject": req.subject,
        "Message": req.message,
        "Timestamp": Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        "SignatureVersion": "1",
        "Signature": "EXAMPLE",
        "SigningCertURL": format!("https://sns.{}.amazonaws.com/SimpleNotificationService.pem", "us-east-1"),
        "UnsubscribeURL": format!("{}/", ctx.base_url.trim_end_matches('/')),
    });

    // Stored as {"Body": "<envelope-json-string>"} so SQS ReceiveMessage surfaces
    // the envelope string in the Body field, matching real SNS→SQS delivery.
    let stored = serde_json::json!({ "Body": envelope.to_string() });

    topic::send_topic(&state.pool, &topic_name, stored, None, 0)
        .await
        .map_err(|e| ctx.internal_error(e))?;

    Ok(ctx.ok(PublishResponse { message_id }))
}
