use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::ops::send;
use crate::sqs::context::SqsContext;
use crate::sqs::error::{SqsError, SqsErrorCode};
use crate::sqs::types::{SendMessageRequest, SendMessageResponse};
use crate::sqs::util::{md5_of, message_attributes_to_headers, queue_name_from_url};

pub async fn handle(
    State(state): State<AppState>,
    ctx: SqsContext,
    req: SendMessageRequest,
) -> Result<impl IntoResponse, SqsError> {
    let raw_name = queue_name_from_url(req.queue_url.as_deref(), &ctx)?;
    let is_fifo = raw_name.ends_with(".fifo");
    let queue_name = if is_fifo {
        raw_name.strip_suffix(".fifo").unwrap().to_string()
    } else {
        raw_name
    };

    let headers = message_attributes_to_headers(
        &req.message_attributes,
        &req.message_group_id,
        &req.message_deduplication_id,
    );
    let body_json = serde_json::json!({ "Body": req.message_body });

    let result = if is_fifo {
        let group_id = req
            .message_group_id
            .as_deref()
            .ok_or_else(|| ctx.error(SqsErrorCode::InvalidMessageContents))?;
        send::send_message_fifo(
            &state.pool,
            &queue_name,
            body_json,
            group_id,
            req.message_deduplication_id.as_deref(),
            headers,
            req.delay_seconds,
            true,
        )
        .await
        .map_err(|e| ctx.internal_error(e))?
    } else {
        send::send_message(
            &state.pool,
            &queue_name,
            body_json,
            headers,
            req.delay_seconds,
            true,
        )
        .await
        .map_err(|e| ctx.internal_error(e))?
    };

    Ok(ctx.ok(SendMessageResponse {
        message_id: result.msg_id.to_string(),
        md5_of_message_body: md5_of(&req.message_body),
    }))
}
