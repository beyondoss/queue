//! Thin wrapper over the `croner` crate.
//!
//! croner parses and iterates cron patterns; we layer on timezone-aware
//! "next N occurrences" projection in UTC (the timezone the queue
//! database stores) and a canonical-form helper used to deduplicate
//! equivalent patterns at write time.

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use croner::Cron;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CronParseError {
    #[error("{0}")]
    Parse(String),
}

/// A parsed cron pattern. Carries no timezone of its own; the timezone
/// is supplied at projection time so the same pattern can be re-projected
/// after a TZ change without re-parsing.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    cron: Cron,
}

impl CronSchedule {
    /// Parse a 5- or 6-field cron pattern. Seconds (the 6th field) are
    /// optional; minute-level granularity is the common case.
    pub fn parse(pattern: &str) -> Result<Self, CronParseError> {
        let mut builder = Cron::new(pattern);
        builder.with_seconds_optional();
        let cron = builder
            .parse()
            .map_err(|e| CronParseError::Parse(e.to_string()))?;
        Ok(Self { cron })
    }

    /// Canonical cron string (what croner accepts back; may differ
    /// slightly from the user's input in whitespace or case).
    pub fn canonical(&self) -> String {
        self.cron.pattern.to_string()
    }

    /// Next occurrence strictly after `now`, projected through `tz` and
    /// returned in UTC for storage.
    pub fn next_after(&self, now: DateTime<Utc>, tz: Tz) -> Option<DateTime<Utc>> {
        let local = now.with_timezone(&tz);
        self.cron
            .find_next_occurrence(&local, false)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    }

    /// Next `n` occurrences strictly after `now`.
    pub fn next_n_after(&self, now: DateTime<Utc>, tz: Tz, n: usize) -> Vec<DateTime<Utc>> {
        if n == 0 {
            return vec![];
        }
        let local = now.with_timezone(&tz);
        self.cron
            .iter_after(local)
            .take(n)
            .map(|dt| dt.with_timezone(&Utc))
            .collect()
    }

    /// Iterate all missed occurrences in (from, up_to], inclusive of `up_to`.
    /// Used by catchup to enumerate which fires to backfill.
    pub fn missed_between(
        &self,
        from_exclusive: DateTime<Utc>,
        up_to_inclusive: DateTime<Utc>,
        tz: Tz,
        limit: usize,
    ) -> Vec<DateTime<Utc>> {
        if from_exclusive >= up_to_inclusive || limit == 0 {
            return vec![];
        }
        let local_from = from_exclusive.with_timezone(&tz);
        self.cron
            .iter_after(local_from)
            .take(limit)
            .map(|dt| dt.with_timezone(&Utc))
            .take_while(|dt| *dt <= up_to_inclusive)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn parse_five_field() {
        let s = CronSchedule::parse("0 9 * * 1-5").expect("parse");
        let now = Utc.with_ymd_and_hms(2026, 5, 10, 0, 0, 0).unwrap(); // Sunday
        let next = s.next_after(now, chrono_tz::UTC).expect("next");
        // 2026-05-11 is a Monday at 09:00 UTC
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 5, 11, 9, 0, 0).unwrap());
    }

    #[test]
    fn parse_six_field_with_seconds() {
        let s = CronSchedule::parse("*/30 * * * * *").expect("parse");
        let now = Utc.with_ymd_and_hms(2026, 5, 10, 12, 0, 1).unwrap();
        let next = s.next_after(now, chrono_tz::UTC).expect("next");
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 5, 10, 12, 0, 30).unwrap());
    }

    #[test]
    fn rejects_invalid() {
        assert!(CronSchedule::parse("not a cron").is_err());
    }

    #[test]
    fn next_n_returns_n() {
        let s = CronSchedule::parse("0 * * * *").expect("parse");
        let now = Utc.with_ymd_and_hms(2026, 5, 10, 12, 30, 0).unwrap();
        let next = s.next_n_after(now, chrono_tz::UTC, 3);
        assert_eq!(next.len(), 3);
        assert_eq!(
            next[0],
            Utc.with_ymd_and_hms(2026, 5, 10, 13, 0, 0).unwrap()
        );
        assert_eq!(
            next[1],
            Utc.with_ymd_and_hms(2026, 5, 10, 14, 0, 0).unwrap()
        );
        assert_eq!(
            next[2],
            Utc.with_ymd_and_hms(2026, 5, 10, 15, 0, 0).unwrap()
        );
    }

    #[test]
    fn missed_between_bounded() {
        let s = CronSchedule::parse("0 * * * *").expect("parse");
        let from = Utc.with_ymd_and_hms(2026, 5, 10, 12, 0, 0).unwrap();
        let up_to = Utc.with_ymd_and_hms(2026, 5, 10, 15, 30, 0).unwrap();
        let missed = s.missed_between(from, up_to, chrono_tz::UTC, 100);
        assert_eq!(missed.len(), 3);
        assert_eq!(
            missed[0],
            Utc.with_ymd_and_hms(2026, 5, 10, 13, 0, 0).unwrap()
        );
        assert_eq!(
            missed[2],
            Utc.with_ymd_and_hms(2026, 5, 10, 15, 0, 0).unwrap()
        );
    }

    #[test]
    fn missed_between_respects_limit() {
        let s = CronSchedule::parse("* * * * *").expect("parse");
        let from = Utc.with_ymd_and_hms(2026, 5, 10, 12, 0, 0).unwrap();
        let up_to = Utc.with_ymd_and_hms(2026, 5, 10, 13, 0, 0).unwrap();
        let missed = s.missed_between(from, up_to, chrono_tz::UTC, 5);
        assert_eq!(missed.len(), 5);
    }
}
