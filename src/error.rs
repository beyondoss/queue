use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("queue not found: {0}")]
    QueueNotFound(String),

    #[error("message not found")]
    MessageNotFound,

    #[error("invalid receipt handle")]
    InvalidReceiptHandle,

    #[error("bad request: {0}")]
    BadRequest(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            ApiError::QueueNotFound(name) => (
                StatusCode::NOT_FOUND,
                format!("Queue '{name}' does not exist"),
            ),
            ApiError::MessageNotFound => (StatusCode::NOT_FOUND, "Message not found".into()),
            ApiError::InvalidReceiptHandle => {
                (StatusCode::BAD_REQUEST, "Invalid receipt handle".into())
            }
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            ApiError::Database(e) => {
                tracing::error!("database error: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, "Database error".into())
            }
            ApiError::Internal(e) => {
                tracing::error!("internal error: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, "Internal error".into())
            }
        };

        let body = json!({ "error": message });
        (status, axum::Json(body)).into_response()
    }
}
