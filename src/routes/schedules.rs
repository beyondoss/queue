//! `/v1/schedules` — REST handlers for the schedule resource.
//!
//! POST is strict-create (409 on duplicate); PUT is idempotent upsert
//! (200 update / 201 create); PATCH is partial. Manual runs nest as
//! `/{name}/runs` and return a Run resource — see `SCHEDULES.md`.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::AppState;
use crate::error::{ApiError, ErrorResponse};
use crate::ops::schedule::{
    self, ListFilter, RunResult, Schedule, SchedulePatch, ScheduleSpec, ScheduleStatusSql,
};

/// Strict create. Body must include `name`. 201 on success, 409 on conflict.
#[utoipa::path(
    post,
    path = "/v1/schedules",
    operation_id = "create_schedule",
    tag = "schedules",
    summary = "Create schedule (strict — 409 if name already exists)",
    description = "Creates a new schedule. Returns `201 Created` with a `Location` header on success. \
        Returns `409 Conflict` if a schedule with the same `name` already exists — use \
        `PUT /v1/schedules/{name}` for idempotent upsert.",
    request_body = ScheduleSpec,
    responses(
        (status = 201, description = "Schedule created.", body = Schedule),
        (status = 400, body = ErrorResponse, description = "Invalid spec — bad expression, unknown timezone, unsupported target kind."),
        (status = 409, body = ErrorResponse, description = "Schedule name already exists. Use PUT to upsert."),
    )
)]
pub async fn create_schedule(
    State(state): State<AppState>,
    Json(spec): Json<ScheduleSpec>,
) -> Result<impl IntoResponse, ApiError> {
    let preview_count = state.config.schedule_preview_count;
    let name = spec.name.clone();
    let sched = schedule::create(&state.pool, spec, preview_count).await?;
    state.schedule_notify.notify_one();
    Ok((StatusCode::CREATED, location_header(&name), Json(sched)))
}

/// List schedules. Optional filters: status, target_kind, name_prefix.
#[utoipa::path(
    get,
    path = "/v1/schedules",
    operation_id = "list_schedules",
    tag = "schedules",
    summary = "List schedules",
    description = "Returns all schedules up to `QUEUE_SCHEDULE_LIST_MAX` (default 1000), ordered by name. \
        Use query parameters to filter by `status`, `target_kind`, or `name_prefix`.",
    params(ListFilter),
    responses(
        (status = 200, description = "Schedules.", body = Vec<Schedule>),
    )
)]
pub async fn list_schedules(
    State(state): State<AppState>,
    Query(filter): Query<ListFilter>,
) -> Result<impl IntoResponse, ApiError> {
    let schedules = schedule::list(
        &state.pool,
        filter,
        state.config.schedule_preview_count,
        state.config.schedule_list_max as i64,
    )
    .await?;
    Ok(Json(schedules))
}

/// Get a schedule by name.
#[utoipa::path(
    get,
    path = "/v1/schedules/{name}",
    operation_id = "get_schedule",
    tag = "schedules",
    summary = "Get schedule",
    description = "Fetches a single schedule by its name. The response includes \
        `human_readable` and `next_fires` computed at request time.",
    responses(
        (status = 200, description = "Schedule.", body = Schedule),
        (status = 404, body = ErrorResponse, description = "Schedule does not exist."),
    )
)]
pub async fn get_schedule(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let sched = schedule::get(&state.pool, &name, state.config.schedule_preview_count).await?;
    Ok(Json(sched))
}

/// Idempotent upsert. 201 if created, 200 if updated.
#[utoipa::path(
    put,
    path = "/v1/schedules/{name}",
    operation_id = "upsert_schedule",
    tag = "schedules",
    summary = "Upsert schedule (idempotent — safe to call repeatedly)",
    description = "Creates or replaces the schedule at `{name}`. Returns `201 Created` on first write, \
        `200 OK` on subsequent calls with the same or different spec. On update, `fire_count` and \
        operational state are preserved; `consecutive_failures` is cleared. Use this for \
        config-sync workflows where the caller wants \"make it so\" semantics. For \
        strict create-or-error, use `POST /v1/schedules`.",
    request_body = ScheduleSpec,
    responses(
        (status = 200, description = "Schedule updated.", body = Schedule),
        (status = 201, description = "Schedule created.", body = Schedule),
        (status = 400, body = ErrorResponse, description = "Invalid spec — bad expression, unknown timezone, unsupported target kind."),
    )
)]
pub async fn upsert_schedule(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(spec): Json<ScheduleSpec>,
) -> Result<Response, ApiError> {
    let (sched, created) = schedule::upsert(
        &state.pool,
        &name,
        spec,
        state.config.schedule_preview_count,
    )
    .await?;
    state.schedule_notify.notify_one();
    let status = if created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, location_header(&name), Json(sched)).into_response())
}

/// Partial update. Pass status="paused"/"active" here to pause/resume.
#[utoipa::path(
    patch,
    path = "/v1/schedules/{name}",
    operation_id = "patch_schedule",
    tag = "schedules",
    summary = "Patch schedule (partial update or pause/resume)",
    description = "Applies a partial update to the schedule. Only fields present in the body are changed; \
        omitted fields keep their current values. \
        To pause: `{\"status\": \"paused\"}`. To resume: `{\"status\": \"active\"}`. \
        When `status` is the only field in the request, a fast path is used that skips \
        expression re-parsing and `next_fire_at` recomputation. \
        Updating any expression field (`cron`, `every`, `when`, `fire_at`, or `timezone`) \
        recomputes `next_fire_at` and resets `consecutive_failures`.",
    request_body = SchedulePatch,
    responses(
        (status = 200, description = "Schedule updated.", body = Schedule),
        (status = 400, body = ErrorResponse, description = "Invalid patch — bad expression, unknown timezone, unsupported target kind."),
        (status = 404, body = ErrorResponse, description = "Schedule does not exist."),
    )
)]
pub async fn patch_schedule(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(patch): Json<SchedulePatch>,
) -> Result<impl IntoResponse, ApiError> {
    // status is the one field where PATCH is the right verb but no
    // expression/target change is needed; route it through set_status.
    if let Some(s) = patch.status.as_deref()
        && patch.cron.is_none()
        && patch.every.is_none()
        && patch.when.is_none()
        && patch.fire_at.is_none()
        && patch.timezone.is_none()
        && patch.jitter_secs.is_none()
        && patch.catchup.is_none()
        && patch.catchup_limit.is_none()
        && patch.failure_threshold.is_none()
        && patch.target.is_none()
    {
        let status = parse_status(s)?;
        let sched = schedule::set_status(
            &state.pool,
            &name,
            status,
            state.config.schedule_preview_count,
        )
        .await?;
        state.schedule_notify.notify_one();
        return Ok(Json(sched));
    }

    let sched = schedule::patch(
        &state.pool,
        &name,
        patch,
        state.config.schedule_preview_count,
    )
    .await?;
    state.schedule_notify.notify_one();
    Ok(Json(sched))
}

/// Idempotent delete. 204 whether or not the schedule existed.
#[utoipa::path(
    delete,
    path = "/v1/schedules/{name}",
    operation_id = "delete_schedule",
    tag = "schedules",
    summary = "Delete schedule (idempotent)",
    description = "Deletes the schedule. Returns `204 No Content` whether or not the schedule existed. \
        Safe to call multiple times or after the schedule has already been deleted.",
    responses(
        (status = 204, description = "Deleted (or was already absent)."),
    )
)]
pub async fn delete_schedule(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    schedule::delete(&state.pool, &name).await?;
    state.schedule_notify.notify_one();
    Ok(StatusCode::NO_CONTENT)
}

/// Manual run. 202 with a Run resource (msg_ids of produced messages).
#[utoipa::path(
    post,
    path = "/v1/schedules/{name}/runs",
    operation_id = "run_schedule",
    tag = "schedules",
    summary = "Trigger manual run (out-of-band fire)",
    description = "Fires the schedule immediately, out of band. The `next_fire_at` is not changed — \
        the normal schedule continues unaffected. `fire_count` is incremented and `last_fired_at` is \
        updated. The produced message(s) carry `headers._schedule.out_of_band = true`. \
        For topic targets, `msg_ids` contains one id per fanned-out subscriber queue.",
    responses(
        (status = 202, description = "Run accepted. Message(s) dispatched.", body = RunResult),
        (status = 404, body = ErrorResponse, description = "Schedule does not exist."),
    )
)]
pub async fn run_schedule(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let run = schedule::run_now(&state.pool, &name).await?;
    // A topic target may have created deliveries; wake the delivery worker.
    state.delivery_notify.notify_one();
    Ok((StatusCode::ACCEPTED, Json(run)))
}

// ---------- helpers ----------

fn location_header(name: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    if let Ok(v) = HeaderValue::from_str(&format!("/v1/schedules/{name}")) {
        h.insert(axum::http::header::LOCATION, v);
    }
    h
}

fn parse_status(s: &str) -> Result<ScheduleStatusSql, ApiError> {
    match s {
        "active" => Ok(ScheduleStatusSql::Active),
        "paused" => Ok(ScheduleStatusSql::Paused),
        other => Err(ApiError::ScheduleInvalid(format!(
            "status must be 'active' or 'paused', got '{other}'"
        ))),
    }
}
