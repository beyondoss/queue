//! `/v1/previews` — dry-run a schedule expression.
//!
//! Preview is a top-level transient resource, not a sub-route of
//! `/schedules`. POST a spec, get back the canonical cron + human
//! description + projection of next fires. No persistence, no DB I/O.

use axum::Json;
use axum::extract::State;
use axum::response::IntoResponse;

use crate::AppState;
use crate::error::{ApiError, ErrorResponse};
use crate::ops::schedule::{self, Preview, PreviewSpec};

/// Compute a preview of a schedule expression without saving anything.
#[utoipa::path(
    post,
    path = "/v1/previews",
    operation_id = "preview_schedule",
    tag = "previews",
    summary = "Preview schedule expression (dry-run, no persistence)",
    description = "Parses and validates a schedule expression and returns the canonical cron string, \
        a human-readable description, and a projection of upcoming fire times. No schedule is created \
        or modified. Use this to validate user input before calling `POST /v1/schedules` or to \
        display a human-friendly preview in a UI.",
    request_body = PreviewSpec,
    responses(
        (status = 200, description = "Preview computed successfully.", body = Preview),
        (status = 400, body = ErrorResponse, description = "Invalid expression — bad cron pattern, unknown timezone, invalid interval, or unparseable natural language."),
    )
)]
pub async fn create_preview(
    State(state): State<AppState>,
    Json(spec): Json<PreviewSpec>,
) -> Result<impl IntoResponse, ApiError> {
    let preview = schedule::preview(spec, state.config.schedule_preview_count)?;
    Ok(Json(preview))
}
