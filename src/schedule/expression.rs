//! Expression — the parsed form of a schedule's "when".
//!
//! A user submits one of four shapes (`cron`, `every`, `when`, `fireAt`).
//! `Expression::parse` normalizes that into the `Expression` enum;
//! `Expression::canonicalize` reduces it further into the `Canonical`
//! form actually stored in the database (canonical cron string + timezone,
//! or a single fire timestamp).
//!
//! Canonicalization is where natural language and interval shorthand get
//! converted to cron — by the time something hits the database, it is
//! either a parsed cron pattern in a named zone, or a single UTC instant.

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use thiserror::Error;

use super::{cron::CronSchedule, every, humanize};

/// User-supplied expression shape, before canonicalization.
#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    /// Raw 5- or 6-field cron pattern.
    Cron(String),
    /// Fixed interval shorthand: "5m", "30s", "2h".
    Every(String),
    /// Natural language: "every weekday at 9am".
    When(String),
    /// One-shot ISO-8601 timestamp.
    FireAt(DateTime<Utc>),
}

/// Canonical, storage-ready form. Recurring schedules carry a parsed
/// cron pattern bound to a timezone; one-shots carry only a UTC instant.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)] // Canonical is transient — never held in arrays.
pub enum Canonical {
    Recurring {
        /// Canonical cron string (the original user input may differ; this
        /// is what the parser accepts back).
        cron: String,
        tz: Tz,
        schedule: CronSchedule,
    },
    OneShot {
        fire_at: DateTime<Utc>,
    },
}

#[derive(Debug, Error)]
pub enum ExpressionError {
    #[error("expression is empty")]
    Empty,
    #[error("expression must specify exactly one of cron, every, when, or fire_at")]
    Ambiguous,
    #[error("invalid cron pattern: {0}")]
    InvalidCron(String),
    #[error("invalid timezone: {0}")]
    InvalidTimezone(String),
    #[error("invalid interval: {0}")]
    InvalidInterval(String),
    #[error("could not parse natural-language expression: {input}")]
    Unhumanizable {
        input: String,
        suggestion: Option<String>,
    },
}

impl Expression {
    /// Build an Expression from the four mutually-exclusive optional inputs.
    /// Exactly one must be `Some`.
    pub fn from_inputs(
        cron: Option<&str>,
        every: Option<&str>,
        when: Option<&str>,
        fire_at: Option<DateTime<Utc>>,
    ) -> Result<Self, ExpressionError> {
        let count = [
            cron.is_some(),
            every.is_some(),
            when.is_some(),
            fire_at.is_some(),
        ]
        .iter()
        .filter(|x| **x)
        .count();
        match count {
            0 => Err(ExpressionError::Empty),
            1 => {
                if let Some(s) = cron {
                    Ok(Expression::Cron(s.to_string()))
                } else if let Some(s) = every {
                    Ok(Expression::Every(s.to_string()))
                } else if let Some(s) = when {
                    Ok(Expression::When(s.to_string()))
                } else if let Some(ts) = fire_at {
                    Ok(Expression::FireAt(ts))
                } else {
                    unreachable!("count == 1 but no arm matched")
                }
            }
            _ => Err(ExpressionError::Ambiguous),
        }
    }

    /// Reduce to canonical form bound to a timezone (UTC for one-shots).
    ///
    /// This is a pure transformation — it does not enforce time-relative
    /// invariants like "fire_at must be in the future." That check belongs
    /// at the spec-validation layer; the worker and the read paths both
    /// need to canonicalize past timestamps (a one-shot picked up by the
    /// worker has `fire_at <= now` by definition, and a row reloaded by
    /// `GET /v1/schedules/{name}` after its fire may also be in the past
    /// until the next worker poll deletes it).
    pub fn canonicalize(&self, timezone: &str) -> Result<Canonical, ExpressionError> {
        match self {
            Expression::Cron(s) => canonical_recurring(s, timezone),
            Expression::Every(s) => {
                let cron = every::to_cron(s)?;
                canonical_recurring(&cron, timezone)
            }
            Expression::When(s) => {
                let cron = humanize::to_cron(s)?;
                canonical_recurring(&cron, timezone)
            }
            Expression::FireAt(ts) => Ok(Canonical::OneShot { fire_at: *ts }),
        }
    }

    /// The original input string, for storage in `schedule.expression`.
    pub fn as_user_input(&self) -> String {
        match self {
            Expression::Cron(s) | Expression::Every(s) | Expression::When(s) => s.clone(),
            Expression::FireAt(ts) => ts.to_rfc3339(),
        }
    }
}

fn canonical_recurring(cron: &str, timezone: &str) -> Result<Canonical, ExpressionError> {
    let tz: Tz = timezone
        .parse()
        .map_err(|_| ExpressionError::InvalidTimezone(timezone.to_string()))?;
    let schedule =
        CronSchedule::parse(cron).map_err(|e| ExpressionError::InvalidCron(e.to_string()))?;
    Ok(Canonical::Recurring {
        cron: schedule.canonical(),
        tz,
        schedule,
    })
}

impl Canonical {
    /// Next occurrence strictly after `now`.
    pub fn next_after(&self, now: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Canonical::Recurring { tz, schedule, .. } => schedule.next_after(now, *tz),
            Canonical::OneShot { fire_at } => (*fire_at > now).then_some(*fire_at),
        }
    }

    /// Next `n` occurrences strictly after `now`.
    pub fn next_n_after(&self, now: DateTime<Utc>, n: usize) -> Vec<DateTime<Utc>> {
        match self {
            Canonical::Recurring { tz, schedule, .. } => schedule.next_n_after(now, *tz, n),
            Canonical::OneShot { fire_at } => {
                if *fire_at > now && n > 0 {
                    vec![*fire_at]
                } else {
                    vec![]
                }
            }
        }
    }

    /// Human-readable summary suitable for display alongside the raw cron.
    pub fn human_readable(&self) -> String {
        match self {
            Canonical::Recurring { cron, tz, .. } => {
                let body = humanize::describe_cron(cron);
                if matches!(*tz, chrono_tz::UTC) {
                    body
                } else {
                    format!("{body}, {tz}")
                }
            }
            Canonical::OneShot { fire_at } => format!("Once at {}", fire_at.to_rfc3339()),
        }
    }

    /// Returns `(cron_opt, fire_at_opt)` for storing into the schedule row.
    pub fn for_storage(&self) -> (Option<String>, Option<DateTime<Utc>>) {
        match self {
            Canonical::Recurring { cron, .. } => (Some(cron.clone()), None),
            Canonical::OneShot { fire_at } => (None, Some(*fire_at)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn empty_inputs_error() {
        assert!(matches!(
            Expression::from_inputs(None, None, None, None),
            Err(ExpressionError::Empty)
        ));
    }

    #[test]
    fn multiple_inputs_error() {
        assert!(matches!(
            Expression::from_inputs(Some("* * * * *"), Some("5m"), None, None),
            Err(ExpressionError::Ambiguous)
        ));
    }

    #[test]
    fn cron_canonicalizes() {
        let expr = Expression::Cron("0 9 * * 1-5".into());
        let canon = expr.canonicalize("UTC").unwrap();
        assert!(matches!(canon, Canonical::Recurring { .. }));
    }

    #[test]
    fn every_canonicalizes() {
        let expr = Expression::Every("5m".into());
        let canon = expr.canonicalize("UTC").unwrap();
        assert!(matches!(canon, Canonical::Recurring { .. }));
    }

    #[test]
    fn fire_at_in_past_canonicalizes_to_one_shot() {
        // canonicalize is pure; the "in past" check is at the spec-validation layer.
        let past = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
        let expr = Expression::FireAt(past);
        assert!(matches!(
            expr.canonicalize("UTC"),
            Ok(Canonical::OneShot { .. })
        ));
    }

    #[test]
    fn fire_at_in_future_is_one_shot() {
        let future = Utc::now() + chrono::Duration::hours(1);
        let expr = Expression::FireAt(future);
        let canon = expr.canonicalize("UTC").unwrap();
        assert!(matches!(canon, Canonical::OneShot { .. }));
    }

    #[test]
    fn invalid_timezone_rejected() {
        let expr = Expression::Cron("* * * * *".into());
        assert!(matches!(
            expr.canonicalize("Not/A_Zone"),
            Err(ExpressionError::InvalidTimezone(_))
        ));
    }
}
