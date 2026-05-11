//! Schedule operations — CRUD, listing, manual runs, and previews.
//!
//! All functions take `&PgPool` and return the strongly-typed
//! `ScheduleRecord` / `Schedule` / `Preview` / `RunResult` shapes the
//! REST and worker layers consume. Validation (timezone, expression
//! parsing, reserved header keys, target kind support) runs entirely in
//! Rust before any SQL executes, so the DB row is always well-formed.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

use crate::error::ApiError;
use crate::schedule::expression::{Canonical, Expression, ExpressionError};

// ---------- Wire types (request/response) ----------

/// Full schedule spec for create / upsert.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "name": "daily-report",
    "when": "every weekday at 9am",
    "timezone": "America/New_York",
    "target": {
        "queue": "reports",
        "message": {"type": "daily_summary"}
    }
}))]
pub struct ScheduleSpec {
    /// Schedule name — 1–64 characters, `[a-z0-9_-]`. Used as the natural key for GET/PUT/PATCH/DELETE.
    pub name: String,

    /// Raw 5- or 6-field cron pattern, e.g. `"0 9 * * 1-5"`. Mutually exclusive with `every`, `when`, `fire_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,

    /// Fixed-interval shorthand: `"5m"`, `"30s"`, `"2h"`. Must evenly divide the next-larger unit. Mutually exclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every: Option<String>,

    /// Natural-language expression, e.g. `"every weekday at 9am"`. Converted to cron internally. Mutually exclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,

    /// ISO-8601 one-shot fire timestamp. Must be in the future. The schedule row is deleted after firing. Mutually exclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fire_at: Option<DateTime<Utc>>,

    /// IANA timezone name for cron evaluation, e.g. `"America/New_York"`. Defaults to `"UTC"`. Ignored for `fire_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,

    /// Random jitter in seconds added to each computed fire time to spread bursts. Defaults to `0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter_secs: Option<i32>,

    /// If `true`, fire missed occurrences when the worker restarts after downtime (bounded by `catchup_limit`). Defaults to `false`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catchup: Option<bool>,

    /// Maximum number of missed fires to backfill in a single catchup pass. Defaults to `100`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catchup_limit: Option<i32>,

    /// Number of consecutive dispatch failures before the schedule is auto-paused. Defaults to `3`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_threshold: Option<i32>,

    /// Dispatch target. Provide exactly one of the `queue`, `topic`, or `workflow` shapes.
    pub target: TargetSpec,
}

/// Dispatch target — provide exactly one of the three shapes. The presence of
/// `queue`, `topic`, or `workflow` determines which kind is used.
///
/// **Queue** — sends a single message to a named queue:
/// ```json
/// { "queue": "my-queue", "message": { "key": "value" } }
/// ```
///
/// **Topic** — fans the message out to every subscribed queue via a routing key:
/// ```json
/// { "topic": "events.order.created", "message": { "order_id": 42 } }
/// ```
///
/// **Workflow** — reserved; currently rejected with `400`. Will dispatch to a
/// named workflow when the workflow runtime ships.
#[derive(Debug, Clone, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(untagged)]
pub enum TargetSpec {
    /// Send a message to a named queue.
    #[schema(title = "QueueTarget", example = json!({ "queue": "my-queue", "message": {"key": "value"} }))]
    Queue {
        /// Name of the target queue.
        queue: String,
        /// Message payload (any JSON value).
        message: serde_json::Value,
        /// User-defined headers merged into the message. Must not contain the reserved key `_schedule`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        headers: Option<serde_json::Value>,
    },
    /// Fan a message out to all queues subscribed to this routing key.
    #[schema(title = "TopicTarget", example = json!({ "topic": "events.order.created", "message": {"order_id": 42} }))]
    Topic {
        /// Routing key used for event fan-out (topic name).
        topic: String,
        /// Message payload (any JSON value).
        message: serde_json::Value,
        /// User-defined headers merged into each fanned-out message. Must not contain `_schedule`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        headers: Option<serde_json::Value>,
    },
    /// Dispatch to a named workflow (reserved — currently rejected with `400`).
    #[schema(title = "WorkflowTarget", example = json!({ "workflow": "order-fulfillment", "input": {"order_id": 42} }))]
    Workflow {
        /// Workflow identifier. Not yet supported.
        workflow: String,
        /// Input payload passed to the workflow on start.
        input: serde_json::Value,
    },
}

impl TargetSpec {
    fn kind(&self) -> &'static str {
        match self {
            TargetSpec::Queue { .. } => "queue",
            TargetSpec::Topic { .. } => "topic",
            TargetSpec::Workflow { .. } => "workflow",
        }
    }

    fn target_name(&self) -> &str {
        match self {
            TargetSpec::Queue { queue, .. } => queue,
            TargetSpec::Topic { topic, .. } => topic,
            TargetSpec::Workflow { workflow, .. } => workflow,
        }
    }

    fn payload(&self) -> &serde_json::Value {
        match self {
            TargetSpec::Queue { message, .. } | TargetSpec::Topic { message, .. } => message,
            TargetSpec::Workflow { input, .. } => input,
        }
    }

    fn headers(&self) -> Option<&serde_json::Value> {
        match self {
            TargetSpec::Queue { headers, .. } | TargetSpec::Topic { headers, .. } => {
                headers.as_ref()
            }
            TargetSpec::Workflow { .. } => None,
        }
    }
}

/// Partial spec for PATCH. All fields are optional; present fields replace the
/// existing value. Omitted fields are preserved. If any of `cron`, `every`,
/// `when`, or `fire_at` is provided, all four are replaced (they are mutually
/// exclusive).
///
/// To pause or resume a schedule, send `{"status": "paused"}` or
/// `{"status": "active"}` as the only field.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[schema(example = json!({"status": "paused"}))]
pub struct SchedulePatch {
    /// Replacement cron pattern. Mutually exclusive with `every`, `when`, `fire_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// Replacement interval shorthand, e.g. `"10m"`. Mutually exclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every: Option<String>,
    /// Replacement natural-language expression. Mutually exclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    /// Replacement one-shot timestamp (must be in the future). Mutually exclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fire_at: Option<DateTime<Utc>>,
    /// Replacement IANA timezone name for cron evaluation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// Replacement jitter in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jitter_secs: Option<i32>,
    /// Enable or disable catchup for missed fires.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catchup: Option<bool>,
    /// Replacement catchup backfill limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catchup_limit: Option<i32>,
    /// Replacement consecutive-failure threshold before auto-pause.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_threshold: Option<i32>,
    /// Replacement dispatch target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<TargetSpec>,
    /// Set to `"active"` to resume or `"paused"` to pause. When this is the
    /// only field in the request, it takes an optimized fast path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// A stored schedule, including derived `human_readable` description and
/// `next_fires` projection computed at response time.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "name": "daily-report",
    "expression": "every weekday at 9am",
    "cron": "0 9 * * 1-5",
    "timezone": "America/New_York",
    "jitter_secs": 0,
    "catchup": false,
    "catchup_limit": 100,
    "failure_threshold": 3,
    "target": { "queue": "reports", "message": { "type": "daily_summary" } },
    "status": "active",
    "next_fire_at": "2026-05-12T13:00:00Z",
    "consecutive_failures": 0,
    "fire_count": 42,
    "human_readable": "At 09:00, Monday through Friday, America/New_York",
    "next_fires": ["2026-05-12T13:00:00Z", "2026-05-13T13:00:00Z"],
    "created_at": "2026-05-01T00:00:00Z",
    "updated_at": "2026-05-01T00:00:00Z"
}))]
pub struct Schedule {
    /// Schedule name — the natural key.
    pub name: String,
    /// Original user-supplied expression string (cron pattern, interval, natural language, or ISO timestamp).
    pub expression: String,
    /// Canonical cron pattern stored in the database. Present for recurring schedules; absent for one-shots.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// One-shot fire timestamp. Present only for `fire_at` schedules; absent for recurring ones.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fire_at: Option<DateTime<Utc>>,
    /// IANA timezone used for cron evaluation.
    pub timezone: String,
    /// Random jitter in seconds applied to each fire time.
    pub jitter_secs: i32,
    /// Whether missed fires are backfilled on worker restart.
    pub catchup: bool,
    /// Maximum backfill count per catchup pass.
    pub catchup_limit: i32,
    /// Consecutive-failure threshold before auto-pause.
    pub failure_threshold: i32,
    /// Dispatch target.
    pub target: TargetSpec,
    /// Current status: `"active"` or `"paused"`.
    pub status: String,
    /// Next scheduled fire time.
    pub next_fire_at: DateTime<Utc>,
    /// Timestamp of the most recent fire (scheduled or manual). Absent if never fired.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_fired_at: Option<DateTime<Utc>>,
    /// Error message from the most recent failed dispatch. Cleared on success.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Number of consecutive failures since the last successful fire.
    pub consecutive_failures: i32,
    /// Total number of times this schedule has fired (scheduled + manual).
    pub fire_count: i64,
    /// Human-readable summary of the schedule expression, e.g. `"At 09:00, Monday through Friday"`.
    pub human_readable: String,
    /// Projected next N fire times (UTC). Count is controlled by `SCHEDULE_PREVIEW_COUNT`.
    pub next_fires: Vec<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Result of a manual `POST /v1/schedules/{name}/runs`.
/// For queue targets `msg_ids` is a singleton; for topic targets it contains
/// one id per fanned-out subscriber queue.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "schedule_name": "daily-report",
    "fired_at": "2026-05-10T14:23:00Z",
    "scheduled_for": "2026-05-10T14:23:00Z",
    "out_of_band": true,
    "msg_ids": [42]
}))]
pub struct RunResult {
    /// Name of the schedule that was run.
    pub schedule_name: String,
    /// Timestamp when the run was dispatched.
    pub fired_at: DateTime<Utc>,
    /// `fired_at` for manual runs; the due `next_fire_at` for scheduled runs.
    pub scheduled_for: DateTime<Utc>,
    /// `true` for manual runs; `false` for worker-scheduled fires.
    pub out_of_band: bool,
    /// Message IDs produced. Singleton for queue targets; one per subscriber for topic targets.
    pub msg_ids: Vec<i64>,
}

/// Preview returned by `POST /v1/previews` — the canonical form of a schedule
/// expression plus a projection of upcoming fire times. No schedule is created.
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
#[schema(example = json!({
    "cron": "0 9 * * 1-5",
    "timezone": "America/New_York",
    "human_readable": "At 09:00, Monday through Friday, America/New_York",
    "next_fires": [
        "2026-05-11T13:00:00Z",
        "2026-05-12T13:00:00Z",
        "2026-05-13T13:00:00Z"
    ]
}))]
pub struct Preview {
    /// Canonical cron pattern derived from the expression. Absent for one-shot `fire_at` inputs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// One-shot fire timestamp. Present only when the input was `fire_at`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fire_at: Option<DateTime<Utc>>,
    /// IANA timezone used for projection (defaults to `"UTC"`).
    pub timezone: String,
    /// Human-readable description of the schedule expression.
    pub human_readable: String,
    /// Projected next N fire times in UTC.
    pub next_fires: Vec<DateTime<Utc>>,
}

/// Query parameters for listing schedules.
#[derive(Debug, Default, Clone, Deserialize, utoipa::IntoParams)]
pub struct ListFilter {
    /// Filter by status. `active` returns only firing schedules; `paused` returns only paused ones.
    #[serde(default)]
    #[param(example = "active")]
    pub status: Option<String>,
    /// Filter by dispatch target kind: `queue`, `topic`, or `workflow`.
    #[serde(default)]
    #[param(example = "queue")]
    pub target_kind: Option<String>,
    /// Return only schedules whose name starts with this prefix.
    #[serde(default)]
    #[param(example = "daily-")]
    pub name_prefix: Option<String>,
}

// ---------- Public API ----------

/// Strict create. Returns 409 if a schedule with this name already exists.
pub async fn create(
    pool: &PgPool,
    spec: ScheduleSpec,
    preview_count: usize,
) -> Result<Schedule, ApiError> {
    let prepared = prepare_for_write(&spec)?;
    let row = sqlx::query!(
        r#"
        INSERT INTO queue.schedule (
            name, expression, cron, fire_at, timezone, jitter_secs,
            catchup, catchup_limit, failure_threshold,
            target_kind, target_name, payload, headers, next_fire_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10::queue.schedule_target_kind, $11, $12::jsonb, $13::jsonb, $14
        )
        ON CONFLICT (name) DO NOTHING
        RETURNING schedule_id
        "#,
        spec.name,
        prepared.expression,
        prepared.cron,
        prepared.fire_at,
        prepared.timezone,
        prepared.jitter_secs,
        prepared.catchup,
        prepared.catchup_limit,
        prepared.failure_threshold,
        prepared.target_kind as _,
        prepared.target_name,
        prepared.payload,
        prepared.headers,
        prepared.next_fire_at,
    )
    .fetch_optional(pool)
    .await?;

    if row.is_none() {
        return Err(ApiError::ScheduleConflict(spec.name));
    }

    get(pool, &spec.name, preview_count).await
}

/// Idempotent upsert. Returns the schedule and `true` if newly created.
pub async fn upsert(
    pool: &PgPool,
    name: &str,
    mut spec: ScheduleSpec,
    preview_count: usize,
) -> Result<(Schedule, bool), ApiError> {
    spec.name = name.to_string();
    let prepared = prepare_for_write(&spec)?;

    let row = sqlx::query!(
        r#"
        INSERT INTO queue.schedule (
            name, expression, cron, fire_at, timezone, jitter_secs,
            catchup, catchup_limit, failure_threshold,
            target_kind, target_name, payload, headers, next_fire_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9,
            $10::queue.schedule_target_kind, $11, $12::jsonb, $13::jsonb, $14
        )
        ON CONFLICT (name) DO UPDATE SET
            expression           = EXCLUDED.expression,
            cron                 = EXCLUDED.cron,
            fire_at              = EXCLUDED.fire_at,
            timezone             = EXCLUDED.timezone,
            jitter_secs          = EXCLUDED.jitter_secs,
            catchup              = EXCLUDED.catchup,
            catchup_limit        = EXCLUDED.catchup_limit,
            failure_threshold    = EXCLUDED.failure_threshold,
            target_kind          = EXCLUDED.target_kind,
            target_name          = EXCLUDED.target_name,
            payload              = EXCLUDED.payload,
            headers              = EXCLUDED.headers,
            -- Only recompute next_fire_at when the schedule shape actually changed.
            next_fire_at         = CASE
                WHEN queue.schedule.cron IS DISTINCT FROM EXCLUDED.cron
                  OR queue.schedule.fire_at IS DISTINCT FROM EXCLUDED.fire_at
                  OR queue.schedule.timezone IS DISTINCT FROM EXCLUDED.timezone
                THEN EXCLUDED.next_fire_at
                ELSE queue.schedule.next_fire_at
            END,
            -- A PUT is an operator-acknowledged change; clear failure state.
            consecutive_failures = 0,
            last_error           = NULL,
            updated_at           = now()
        RETURNING (xmax = 0) AS "is_insert!: bool"
        "#,
        spec.name,
        prepared.expression,
        prepared.cron,
        prepared.fire_at,
        prepared.timezone,
        prepared.jitter_secs,
        prepared.catchup,
        prepared.catchup_limit,
        prepared.failure_threshold,
        prepared.target_kind as _,
        prepared.target_name,
        prepared.payload,
        prepared.headers,
        prepared.next_fire_at,
    )
    .fetch_one(pool)
    .await?;

    let schedule = get(pool, &spec.name, preview_count).await?;
    Ok((schedule, row.is_insert))
}

/// PATCH: partial update.
pub async fn patch(
    pool: &PgPool,
    name: &str,
    p: SchedulePatch,
    preview_count: usize,
) -> Result<Schedule, ApiError> {
    // Load current row, merge, validate, then write back.
    let current = load_record(pool, name).await?;
    let merged = merge_patch(current, p)?;
    let prepared = prepare_for_write(&merged)?;

    sqlx::query!(
        r#"
        UPDATE queue.schedule SET
            expression           = $2,
            cron                 = $3,
            fire_at              = $4,
            timezone             = $5,
            jitter_secs          = $6,
            catchup              = $7,
            catchup_limit        = $8,
            failure_threshold    = $9,
            target_kind          = $10::queue.schedule_target_kind,
            target_name          = $11,
            payload              = $12::jsonb,
            headers              = $13::jsonb,
            status               = $14::queue.schedule_status,
            next_fire_at         = $15,
            consecutive_failures = 0,
            last_error           = NULL,
            updated_at           = now()
        WHERE name = $1
        "#,
        name,
        prepared.expression,
        prepared.cron,
        prepared.fire_at,
        prepared.timezone,
        prepared.jitter_secs,
        prepared.catchup,
        prepared.catchup_limit,
        prepared.failure_threshold,
        prepared.target_kind as _,
        prepared.target_name,
        prepared.payload,
        prepared.headers,
        prepared.status as _,
        prepared.next_fire_at,
    )
    .execute(pool)
    .await?;

    get(pool, name, preview_count).await
}

/// Get one schedule by name, including derived fields.
pub async fn get(pool: &PgPool, name: &str, preview_count: usize) -> Result<Schedule, ApiError> {
    let r = load_record(pool, name).await?;
    record_to_schedule(r, preview_count)
}

/// List schedules with optional filters. Hard cap at `max_returned`.
pub async fn list(
    pool: &PgPool,
    filter: ListFilter,
    preview_count: usize,
    max_returned: i64,
) -> Result<Vec<Schedule>, ApiError> {
    let status = filter.status.as_deref();
    let target_kind = filter.target_kind.as_deref();
    let name_prefix = filter.name_prefix.as_deref();

    let rows = sqlx::query_as!(
        ScheduleRow,
        r#"
        SELECT
            name,
            expression,
            cron,
            fire_at,
            timezone,
            jitter_secs,
            catchup,
            catchup_limit,
            failure_threshold,
            target_kind::TEXT AS "target_kind!",
            target_name,
            payload,
            headers,
            status::TEXT AS "status!",
            next_fire_at,
            last_fired_at,
            last_error,
            consecutive_failures,
            fire_count,
            created_at,
            updated_at
        FROM queue.schedule
        WHERE ($1::TEXT IS NULL OR status::TEXT = $1)
          AND ($2::TEXT IS NULL OR target_kind::TEXT = $2)
          AND ($3::TEXT IS NULL OR name LIKE $3 || '%')
        ORDER BY name
        LIMIT $4
        "#,
        status,
        target_kind,
        name_prefix,
        max_returned,
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|r| record_to_schedule(r, preview_count))
        .collect()
}

/// Idempotent delete. 204; absent rows collapse to success.
pub async fn delete(pool: &PgPool, name: &str) -> Result<(), ApiError> {
    sqlx::query!("DELETE FROM queue.schedule WHERE name = $1", name)
        .execute(pool)
        .await?;
    Ok(())
}

/// Fire a schedule out-of-band. Does NOT advance next_fire_at; does bump
/// fire_count and last_fired_at. The dispatch path is identical to a
/// scheduled fire except `out_of_band = true` in the message headers.
pub async fn run_now(pool: &PgPool, name: &str) -> Result<RunResult, ApiError> {
    let rec = load_record(pool, name).await?;
    let fired_at = Utc::now();
    let scheduled_for = fired_at;

    let merged_headers =
        merge_schedule_headers(rec.headers.clone(), &rec.name, scheduled_for, true);

    let msg_ids = dispatch(
        pool,
        &rec.target_kind,
        &rec.target_name,
        &rec.payload_or_null(),
        merged_headers,
    )
    .await?;

    sqlx::query!(
        r#"
        UPDATE queue.schedule
        SET last_fired_at = $2,
            fire_count    = fire_count + 1,
            updated_at    = now()
        WHERE name = $1
        "#,
        name,
        fired_at,
    )
    .execute(pool)
    .await?;

    Ok(RunResult {
        schedule_name: rec.name,
        fired_at,
        scheduled_for,
        out_of_band: true,
        msg_ids,
    })
}

/// Pure dry-run: parse an expression, return canonical + projection.
/// No DB I/O. Returns `Preview` on success, `ScheduleInvalid` on parse failure.
pub fn preview(spec: PreviewSpec, count: usize) -> Result<Preview, ApiError> {
    let expr = Expression::from_inputs(
        spec.cron.as_deref(),
        spec.every.as_deref(),
        spec.when.as_deref(),
        spec.fire_at,
    )
    .map_err(expression_to_api_error)?;

    let timezone = spec.timezone.unwrap_or_else(|| "UTC".to_string());
    let canon = expr
        .canonicalize(&timezone)
        .map_err(expression_to_api_error)?;
    let (cron, fire_at) = canon.for_storage();
    let now = Utc::now();
    Ok(Preview {
        cron,
        fire_at,
        timezone,
        human_readable: canon.human_readable(),
        next_fires: canon.next_n_after(now, count),
    })
}

/// Input for a dry-run preview. Provide exactly one of `cron`, `every`,
/// `when`, or `fire_at`. No schedule is created or modified.
#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
#[schema(example = json!({
    "when": "every weekday at 9am",
    "timezone": "America/New_York"
}))]
pub struct PreviewSpec {
    /// Raw 5- or 6-field cron pattern. Mutually exclusive with `every`, `when`, `fire_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// Fixed-interval shorthand, e.g. `"5m"`. Mutually exclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub every: Option<String>,
    /// Natural-language expression, e.g. `"every weekday at 9am"`. Mutually exclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<String>,
    /// One-shot ISO-8601 timestamp. Mutually exclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fire_at: Option<DateTime<Utc>>,
    /// IANA timezone for cron evaluation. Defaults to `"UTC"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
}

// ---------- Internals shared with the worker ----------

/// Merge `{_schedule: {name, scheduled_for, out_of_band}}` into the user-supplied
/// headers. Used by `run_now` and the schedule worker so every fire is queryable
/// from the archive by `headers->'_schedule'->>'name'`.
pub fn merge_schedule_headers(
    user_headers: Option<serde_json::Value>,
    schedule_name: &str,
    scheduled_for: DateTime<Utc>,
    out_of_band: bool,
) -> serde_json::Value {
    let mut base = match user_headers {
        Some(serde_json::Value::Object(m)) => serde_json::Value::Object(m),
        _ => serde_json::json!({}),
    };
    if let serde_json::Value::Object(ref mut map) = base {
        map.insert(
            "_schedule".into(),
            serde_json::json!({
                "name": schedule_name,
                "scheduled_for": scheduled_for.to_rfc3339(),
                "out_of_band": out_of_band,
            }),
        );
    }
    base
}

/// Dispatch a single fire by calling queue.send or queue.publish_event.
/// Returns the list of produced msg_ids.
pub async fn dispatch(
    pool: &PgPool,
    target_kind: &str,
    target_name: &str,
    payload: &serde_json::Value,
    headers: serde_json::Value,
) -> Result<Vec<i64>, ApiError> {
    match target_kind {
        "queue" => {
            let row = sqlx::query!(
                r#"SELECT queue.send($1, $2::jsonb, $3::jsonb, clock_timestamp(), true) AS "msg_id!: i64""#,
                target_name,
                payload,
                Some(headers),
            )
            .fetch_one(pool)
            .await
            .map_err(crate::error::queue_error)?;
            Ok(vec![row.msg_id])
        }
        "topic" => {
            let rows = sqlx::query!(
                r#"SELECT msg_id AS "msg_id!"
                   FROM queue.publish_event($1, $2::jsonb, $3::jsonb, 0::integer)"#,
                target_name,
                payload,
                Some(headers),
            )
            .fetch_all(pool)
            .await?;
            Ok(rows.into_iter().map(|r| r.msg_id).collect())
        }
        "workflow" => Err(ApiError::ScheduleInvalid(
            "workflow targets are not yet supported".into(),
        )),
        other => Err(ApiError::Internal(anyhow::anyhow!(
            "unknown target_kind: {other}"
        ))),
    }
}

// ---------- Helpers ----------

/// Row shape pulled out of `queue.schedule`. Mirrors the table.
#[derive(Debug, Clone)]
pub struct ScheduleRow {
    pub name: String,
    pub expression: String,
    pub cron: Option<String>,
    pub fire_at: Option<DateTime<Utc>>,
    pub timezone: String,
    pub jitter_secs: i32,
    pub catchup: bool,
    pub catchup_limit: i32,
    pub failure_threshold: i32,
    pub target_kind: String,
    pub target_name: String,
    pub payload: Option<serde_json::Value>,
    pub headers: Option<serde_json::Value>,
    pub status: String,
    pub next_fire_at: DateTime<Utc>,
    pub last_fired_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub consecutive_failures: i32,
    pub fire_count: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ScheduleRow {
    pub fn payload_or_null(&self) -> serde_json::Value {
        self.payload.clone().unwrap_or(serde_json::Value::Null)
    }

    pub fn target_spec(&self) -> TargetSpec {
        match self.target_kind.as_str() {
            "queue" => TargetSpec::Queue {
                queue: self.target_name.clone(),
                message: self.payload_or_null(),
                headers: self.headers.clone(),
            },
            "topic" => TargetSpec::Topic {
                topic: self.target_name.clone(),
                message: self.payload_or_null(),
                headers: self.headers.clone(),
            },
            _ => TargetSpec::Workflow {
                workflow: self.target_name.clone(),
                input: self.payload_or_null(),
            },
        }
    }
}

async fn load_record(pool: &PgPool, name: &str) -> Result<ScheduleRow, ApiError> {
    sqlx::query_as!(
        ScheduleRow,
        r#"
        SELECT
            name,
            expression,
            cron,
            fire_at,
            timezone,
            jitter_secs,
            catchup,
            catchup_limit,
            failure_threshold,
            target_kind::TEXT AS "target_kind!",
            target_name,
            payload,
            headers,
            status::TEXT AS "status!",
            next_fire_at,
            last_fired_at,
            last_error,
            consecutive_failures,
            fire_count,
            created_at,
            updated_at
        FROM queue.schedule
        WHERE name = $1
        "#,
        name,
    )
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| ApiError::ScheduleNotFound(name.to_string()))
}

fn record_to_schedule(r: ScheduleRow, preview_count: usize) -> Result<Schedule, ApiError> {
    let canon = recompute_canonical(&r)?;
    let now = Utc::now();
    Ok(Schedule {
        name: r.name.clone(),
        expression: r.expression.clone(),
        cron: r.cron.clone(),
        fire_at: r.fire_at,
        timezone: r.timezone.clone(),
        jitter_secs: r.jitter_secs,
        catchup: r.catchup,
        catchup_limit: r.catchup_limit,
        failure_threshold: r.failure_threshold,
        target: r.target_spec(),
        status: r.status.clone(),
        next_fire_at: r.next_fire_at,
        last_fired_at: r.last_fired_at,
        last_error: r.last_error.clone(),
        consecutive_failures: r.consecutive_failures,
        fire_count: r.fire_count,
        human_readable: canon.human_readable(),
        next_fires: canon.next_n_after(now, preview_count),
        created_at: r.created_at,
        updated_at: r.updated_at,
    })
}

fn recompute_canonical(r: &ScheduleRow) -> Result<Canonical, ApiError> {
    let expr = if let Some(c) = &r.cron {
        Expression::Cron(c.clone())
    } else if let Some(fa) = r.fire_at {
        Expression::FireAt(fa)
    } else {
        return Err(ApiError::Internal(anyhow::anyhow!(
            "schedule row has neither cron nor fire_at: {}",
            r.name
        )));
    };
    expr.canonicalize(&r.timezone)
        .map_err(expression_to_api_error)
}

struct Prepared {
    expression: String,
    cron: Option<String>,
    fire_at: Option<DateTime<Utc>>,
    timezone: String,
    jitter_secs: i32,
    catchup: bool,
    catchup_limit: i32,
    failure_threshold: i32,
    target_kind: TargetKindSql,
    target_name: String,
    payload: Option<serde_json::Value>,
    headers: Option<serde_json::Value>,
    next_fire_at: DateTime<Utc>,
    status: ScheduleStatusSql,
}

#[derive(sqlx::Type, Debug, Clone, Copy)]
#[sqlx(type_name = "queue.schedule_target_kind", rename_all = "lowercase")]
pub enum TargetKindSql {
    Queue,
    Topic,
    Workflow,
}

#[derive(sqlx::Type, Debug, Clone, Copy)]
#[sqlx(type_name = "queue.schedule_status", rename_all = "lowercase")]
pub enum ScheduleStatusSql {
    Active,
    Paused,
}

fn prepare_for_write(spec: &ScheduleSpec) -> Result<Prepared, ApiError> {
    validate_name(&spec.name)?;
    validate_headers(spec.target.headers())?;

    if matches!(spec.target, TargetSpec::Workflow { .. }) {
        return Err(ApiError::ScheduleInvalid(
            "workflow targets are not yet supported".into(),
        ));
    }

    // Explicit "fire_at must be in the future" check at the spec-validation layer.
    // Expression::canonicalize is pure and does not enforce this.
    if let Some(fa) = spec.fire_at
        && fa <= Utc::now()
    {
        return Err(ApiError::ScheduleInvalid(
            "fire_at must be in the future".into(),
        ));
    }

    let expr = Expression::from_inputs(
        spec.cron.as_deref(),
        spec.every.as_deref(),
        spec.when.as_deref(),
        spec.fire_at,
    )
    .map_err(expression_to_api_error)?;

    let timezone = spec.timezone.clone().unwrap_or_else(|| "UTC".to_string());
    let canon = expr
        .canonicalize(&timezone)
        .map_err(expression_to_api_error)?;
    let (cron_storage, fire_at_storage) = canon.for_storage();

    let now = Utc::now();
    let next_fire_at = canon
        .next_after(now)
        .ok_or_else(|| ApiError::ScheduleInvalid("no future fire time exists".into()))?;

    let target_kind = match spec.target.kind() {
        "queue" => TargetKindSql::Queue,
        "topic" => TargetKindSql::Topic,
        "workflow" => TargetKindSql::Workflow,
        _ => unreachable!(),
    };

    Ok(Prepared {
        expression: expr.as_user_input(),
        cron: cron_storage,
        fire_at: fire_at_storage,
        timezone,
        jitter_secs: spec.jitter_secs.unwrap_or(0),
        catchup: spec.catchup.unwrap_or(false),
        catchup_limit: spec.catchup_limit.unwrap_or(100),
        failure_threshold: spec.failure_threshold.unwrap_or(3),
        target_kind,
        target_name: spec.target.target_name().to_string(),
        payload: Some(spec.target.payload().clone()),
        headers: spec.target.headers().cloned(),
        next_fire_at,
        status: ScheduleStatusSql::Active,
    })
}

fn validate_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty() || name.len() > 64 {
        return Err(ApiError::ScheduleInvalid(
            "name must be 1-64 characters".into(),
        ));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
    {
        return Err(ApiError::ScheduleInvalid(
            "name must contain only [a-z0-9_-]".into(),
        ));
    }
    Ok(())
}

fn validate_headers(headers: Option<&serde_json::Value>) -> Result<(), ApiError> {
    let Some(serde_json::Value::Object(map)) = headers else {
        return Ok(());
    };
    if map.contains_key("_schedule") {
        return Err(ApiError::ScheduleInvalid(
            "header key '_schedule' is reserved for system metadata".into(),
        ));
    }
    Ok(())
}

fn merge_patch(existing: ScheduleRow, p: SchedulePatch) -> Result<ScheduleSpec, ApiError> {
    // If any of cron/every/when/fire_at is in the patch, that replaces ALL of them
    // (the four are mutually exclusive). If none are present, keep the existing expression.
    let any_when = p.cron.is_some() || p.every.is_some() || p.when.is_some() || p.fire_at.is_some();

    let (cron, every, when, fire_at) = if any_when {
        (p.cron, p.every, p.when, p.fire_at)
    } else {
        // Reconstruct from existing row.
        (existing.cron.clone(), None, None, existing.fire_at)
    };

    // Target: replace if provided; otherwise reconstruct.
    let target = p.target.unwrap_or_else(|| existing.target_spec());

    Ok(ScheduleSpec {
        name: existing.name.clone(),
        cron,
        every,
        when,
        fire_at,
        timezone: p.timezone.or(Some(existing.timezone.clone())),
        jitter_secs: p.jitter_secs.or(Some(existing.jitter_secs)),
        catchup: p.catchup.or(Some(existing.catchup)),
        catchup_limit: p.catchup_limit.or(Some(existing.catchup_limit)),
        failure_threshold: p.failure_threshold.or(Some(existing.failure_threshold)),
        target,
    })
}

fn expression_to_api_error(e: ExpressionError) -> ApiError {
    ApiError::ScheduleInvalid(e.to_string())
}

/// Set status to `paused` or `active`. Used by pause/resume routes.
pub async fn set_status(
    pool: &PgPool,
    name: &str,
    status: ScheduleStatusSql,
    preview_count: usize,
) -> Result<Schedule, ApiError> {
    let res = sqlx::query!(
        r#"UPDATE queue.schedule SET status = $2, updated_at = now() WHERE name = $1"#,
        name,
        status as _,
    )
    .execute(pool)
    .await?;
    if res.rows_affected() == 0 {
        return Err(ApiError::ScheduleNotFound(name.to_string()));
    }
    get(pool, name, preview_count).await
}
