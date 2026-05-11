use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// Response extension inserted when a request fails due to database pool exhaustion.
/// Consumed by the `record_metrics` middleware to increment `db_pool_acquire_timeouts_total`.
#[derive(Clone)]
pub struct DbPoolTimeout;

/// Inner error payload for all non-2xx responses.
#[derive(Serialize, utoipa::ToSchema)]
pub struct ErrorBody {
    /// Machine-readable error code, e.g. `"queue_not_found"`, `"bad_request"`.
    pub code: String,
    /// Human-readable description of the error.
    pub message: String,
    /// Optional actionable guidance present on configuration-gate errors.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

/// Wire-format error envelope returned on all non-2xx responses.
#[derive(Serialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub error: ErrorBody,
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

    #[error("schedule not found: {0}")]
    ScheduleNotFound(String),

    #[error("schedule conflict: {0}")]
    ScheduleConflict(String),

    #[error("invalid schedule: {0}")]
    ScheduleInvalid(String),

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("internal error: {0}")]
    Internal(#[from] anyhow::Error),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // Pool exhaustion gets a distinct 503 and a response extension so the
        // record_metrics middleware can increment db_pool_acquire_timeouts_total.
        if matches!(self, ApiError::Database(sqlx::Error::PoolTimedOut)) {
            tracing::error!("database pool exhausted: acquire timeout");
            let body = ErrorResponse {
                error: ErrorBody {
                    code: "service_unavailable".into(),
                    message: "Service temporarily unavailable".into(),
                    hint: None,
                },
            };
            let mut resp = (StatusCode::SERVICE_UNAVAILABLE, axum::Json(body)).into_response();
            resp.extensions_mut().insert(DbPoolTimeout);
            return resp;
        }

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
            ApiError::ScheduleNotFound(name) => (
                StatusCode::NOT_FOUND,
                "schedule_not_found",
                format!("Schedule '{name}' does not exist"),
            ),
            ApiError::ScheduleConflict(name) => (
                StatusCode::CONFLICT,
                "schedule_conflict",
                format!("Schedule '{name}' already exists"),
            ),
            ApiError::ScheduleInvalid(msg) => {
                (StatusCode::BAD_REQUEST, "schedule_invalid", msg.clone())
            }
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

        let body = ErrorResponse {
            error: ErrorBody {
                code: code.to_string(),
                message,
                hint: None,
            },
        };
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
