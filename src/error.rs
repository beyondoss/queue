use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

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
        let (status, message) = match &self {
            ApiError::QueueNotFound(name) => (
                StatusCode::NOT_FOUND,
                format!("Queue '{name}' does not exist"),
            ),
            ApiError::MessageNotFound => (StatusCode::NOT_FOUND, "Message not found".into()),
            ApiError::BindingNotFound => (StatusCode::NOT_FOUND, "Binding not found".into()),
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

/// Translate a PostgreSQL error from a topic binding operation into a typed ApiError.
/// subscribe raises RAISE EXCEPTION for queue-not-found and invalid-pattern cases.
pub fn topic_bind_error(e: sqlx::Error) -> ApiError {
    if let sqlx::Error::Database(ref db_err) = e {
        let msg = db_err.message();
        if msg.contains("does not exist") {
            // "Queue "foo" does not exist"
            return ApiError::QueueNotFound(msg.to_string());
        }
        if msg.contains("invalid") || msg.contains("pattern") || msg.contains("NULL") || msg.contains("empty") {
            return ApiError::BadRequest(msg.to_string());
        }
    }
    ApiError::Database(e)
}
