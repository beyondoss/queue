//! Schedules — time-based triggers that fan into queues or topics.
//!
//! The schedule layer parses user input (cron / every / when / fireAt),
//! canonicalizes it into either a cron string (recurring) or a single
//! timestamp (one-shot), and projects future fire times for previews,
//! advancement, and humanization.
//!
//! See `SCHEDULES.md` for the full design.

pub mod cron;
pub mod every;
pub mod expression;
pub mod humanize;

pub use cron::CronSchedule;
pub use expression::{Canonical, Expression, ExpressionError};
