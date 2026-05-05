use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde_json::json;

#[derive(Serialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("queue not found: {0}")]
    QueueNotFound(String),

    #[error("message not found")]
    MessageNotFound,

    #[error("binding not found")]
    BindingNotFound,

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
        let (status, code, message) = match &self {
            ApiError::QueueNotFound(name) => (
                StatusCode::NOT_FOUND,
                "queue_not_found",
                format!("Queue '{name}' does not exist"),
            ),
            ApiError::MessageNotFound => (
                StatusCode::NOT_FOUND,
                "message_not_found",
                "Message not found".into(),
            ),
            ApiError::BindingNotFound => (
                StatusCode::NOT_FOUND,
                "binding_not_found",
                "Binding not found".into(),
            ),
            ApiError::InvalidReceiptHandle => (
                StatusCode::BAD_REQUEST,
                "invalid_receipt_handle",
                "Invalid receipt handle".into(),
            ),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "bad_request", msg.clone()),
            ApiError::Database(e) => {
                tracing::error!("database error: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "Database error".into(),
                )
            }
            ApiError::Internal(e) => {
                tracing::error!("internal error: {e}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "Internal error".into(),
                )
            }
        };

        let body = json!({ "code": code, "message": message });
        (status, axum::Json(body)).into_response()
    }
}

/// Translate a PostgreSQL error carrying a Q-prefixed SQLSTATE into a typed ApiError.
///   Q0001 → QueueNotFound, Q0002 → BadRequest (invalid name/parameter).
/// Used by queue admin, send, and topic operations.
pub fn queue_error(e: sqlx::Error) -> ApiError {
    if let sqlx::Error::Database(ref db_err) = e {
        match db_err.code().as_deref() {
            Some("Q0001") => return ApiError::QueueNotFound(db_err.message().to_string()),
            Some("Q0002") => return ApiError::BadRequest(db_err.message().to_string()),
            _ => {}
        }
    }
    ApiError::Database(e)
}

/// Alias kept for call sites that predate the unified queue_error.
pub use queue_error as topic_bind_error;
