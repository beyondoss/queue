//! Natural-language ⇄ cron, narrow and tight.
//!
//! `to_cron` accepts a small grammar of common phrasings agents and
//! humans actually type and returns the canonical cron string.
//! `describe_cron` does the inverse for common patterns and falls back
//! to a literal "At cron: …" rendering for everything else.
//!
//! The grammar is deliberately narrow — easier to grow with real demand
//! than to start broad and rot. When `to_cron` fails it returns a
//! suggestion when there's a near miss, so agents can self-correct.

use super::expression::ExpressionError;

/// Convert a natural-language schedule expression to a canonical cron string.
pub fn to_cron(input: &str) -> Result<String, ExpressionError> {
    let normalized = input.trim().to_lowercase();
    parse_phrase(&normalized).ok_or_else(|| ExpressionError::Unhumanizable {
        input: input.to_string(),
        suggestion: suggest(&normalized),
    })
}

fn parse_phrase(s: &str) -> Option<String> {
    if let Some(t) = strip_prefix_any(s, &["every minute"]) {
        return if t.is_empty() {
            Some("* * * * *".into())
        } else {
            None
        };
    }
    if let Some(rest) = s.strip_prefix("every ") {
        if let Some(cron) = parse_every_n_units(rest) {
            return Some(cron);
        }
        if let Some(cron) = parse_every_day_phrase(rest) {
            return Some(cron);
        }
    }
    if let Some(rest) = s.strip_prefix("daily at ") {
        let (h, m) = parse_time(rest)?;
        return Some(format!("{m} {h} * * *"));
    }
    if let Some(rest) = s.strip_prefix("hourly") {
        let rest = rest.trim();
        if rest.is_empty() {
            return Some("0 * * * *".into());
        }
        if let Some(after_at) = rest.strip_prefix("at :") {
            let m: u8 = after_at.trim().parse().ok()?;
            if m > 59 {
                return None;
            }
            return Some(format!("{m} * * * *"));
        }
    }
    None
}

fn parse_every_n_units(rest: &str) -> Option<String> {
    let (value_word, unit_word) = rest.split_once(' ')?;
    let n: u32 = value_word.parse().ok()?;
    let unit = unit_word.trim_end_matches('s');
    match unit {
        "second" => super::every::to_cron(&format!("{n}s")).ok(),
        "minute" => super::every::to_cron(&format!("{n}m")).ok(),
        "hour" => super::every::to_cron(&format!("{n}h")).ok(),
        "day" if n == 1 => Some("0 0 * * *".into()),
        _ => None,
    }
}

fn parse_every_day_phrase(rest: &str) -> Option<String> {
    if let Some(time) = rest.strip_prefix("day at ") {
        let (h, m) = parse_time(time)?;
        return Some(format!("{m} {h} * * *"));
    }
    if let Some(time) = rest.strip_prefix("weekday at ") {
        let (h, m) = parse_time(time)?;
        return Some(format!("{m} {h} * * 1-5"));
    }
    if let Some(time) = rest.strip_prefix("weekend at ") {
        let (h, m) = parse_time(time)?;
        return Some(format!("{m} {h} * * 0,6"));
    }
    for (name, n) in WEEKDAYS {
        let prefix = format!("{name} at ");
        if let Some(time) = rest.strip_prefix(&prefix) {
            let (h, m) = parse_time(time)?;
            return Some(format!("{m} {h} * * {n}"));
        }
    }
    if rest == "hour" {
        return Some("0 * * * *".into());
    }
    if rest == "day" {
        return Some("0 0 * * *".into());
    }
    None
}

const WEEKDAYS: &[(&str, u8)] = &[
    ("sunday", 0),
    ("monday", 1),
    ("tuesday", 2),
    ("wednesday", 3),
    ("thursday", 4),
    ("friday", 5),
    ("saturday", 6),
];

/// Parse a time-of-day expression: "9am", "9:30am", "21:00", "9", "noon", "midnight".
fn parse_time(s: &str) -> Option<(u8, u8)> {
    let s = s.trim();
    match s {
        "noon" => return Some((12, 0)),
        "midnight" => return Some((0, 0)),
        _ => {}
    }
    let (body, suffix) = if let Some(rest) = s.strip_suffix("am") {
        (rest.trim(), Some(false))
    } else if let Some(rest) = s.strip_suffix("pm") {
        (rest.trim(), Some(true))
    } else {
        (s, None)
    };
    let (h, m) = if let Some((h_str, m_str)) = body.split_once(':') {
        let h: u8 = h_str.trim().parse().ok()?;
        let m: u8 = m_str.trim().parse().ok()?;
        (h, m)
    } else {
        let h: u8 = body.trim().parse().ok()?;
        (h, 0)
    };
    let h = match suffix {
        Some(false) => {
            // "am"
            if h == 12 {
                0
            } else if h > 12 {
                return None;
            } else {
                h
            }
        }
        Some(true) => {
            // "pm"
            if h == 12 {
                12
            } else if h > 12 {
                return None;
            } else {
                h + 12
            }
        }
        None => {
            if h > 23 {
                return None;
            } else {
                h
            }
        }
    };
    if m > 59 {
        return None;
    }
    Some((h, m))
}

fn strip_prefix_any<'a>(s: &'a str, prefixes: &[&str]) -> Option<&'a str> {
    for p in prefixes {
        if let Some(rest) = s.strip_prefix(p) {
            return Some(rest.trim());
        }
    }
    None
}

fn suggest(input: &str) -> Option<String> {
    if input.contains("weekdays") {
        return Some(input.replace("weekdays", "weekday"));
    }
    if input.contains("9 am") {
        return Some(input.replace("9 am", "9am"));
    }
    if input.starts_with("at ") {
        return Some(format!("every day {input}"));
    }
    None
}

/// Best-effort human-readable description of a cron string. Used for the
/// `human_readable` field returned alongside any schedule.
pub fn describe_cron(cron: &str) -> String {
    match cron {
        "* * * * *" => "Every minute".into(),
        "0 * * * *" => "Every hour".into(),
        "0 0 * * *" => "Every day at 00:00".into(),
        _ => {
            if let Some(desc) = describe_every_n(cron) {
                return desc;
            }
            if let Some(desc) = describe_daily_at(cron) {
                return desc;
            }
            format!("At cron: {cron}")
        }
    }
}

fn describe_every_n(cron: &str) -> Option<String> {
    let parts: Vec<&str> = cron.split_whitespace().collect();
    match parts.as_slice() {
        [m, "*", "*", "*", "*"] if m.starts_with("*/") => {
            Some(format!("Every {} minutes", m.trim_start_matches("*/")))
        }
        [_s, _m, "*", "*", "*", "*"] if parts[0].starts_with("*/") => Some(format!(
            "Every {} seconds",
            parts[0].trim_start_matches("*/")
        )),
        ["0", h, "*", "*", "*"] if h.starts_with("*/") => {
            Some(format!("Every {} hours", h.trim_start_matches("*/")))
        }
        _ => None,
    }
}

fn describe_daily_at(cron: &str) -> Option<String> {
    let parts: Vec<&str> = cron.split_whitespace().collect();
    if parts.len() != 5 {
        return None;
    }
    let m: u8 = parts[0].parse().ok()?;
    let h: u8 = parts[1].parse().ok()?;
    let dom = parts[2];
    let mon = parts[3];
    let dow = parts[4];

    let time = format!("{h:02}:{m:02}");
    match (dom, mon, dow) {
        ("*", "*", "*") => Some(format!("At {time} every day")),
        ("*", "*", "1-5") => Some(format!("At {time} on weekdays")),
        ("*", "*", "0,6") | ("*", "*", "6,0") => Some(format!("At {time} on weekends")),
        ("*", "*", dow_only) => weekday_name(dow_only).map(|n| format!("At {time} on {n}")),
        _ => None,
    }
}

fn weekday_name(dow: &str) -> Option<&'static str> {
    match dow {
        "0" | "7" => Some("Sunday"),
        "1" => Some("Monday"),
        "2" => Some("Tuesday"),
        "3" => Some("Wednesday"),
        "4" => Some("Thursday"),
        "5" => Some("Friday"),
        "6" => Some("Saturday"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_weekday_at_9am() {
        assert_eq!(to_cron("every weekday at 9am").unwrap(), "0 9 * * 1-5");
    }

    #[test]
    fn every_day_at_midnight() {
        assert_eq!(to_cron("every day at midnight").unwrap(), "0 0 * * *");
    }

    #[test]
    fn every_monday_at_9_30am() {
        assert_eq!(to_cron("every monday at 9:30am").unwrap(), "30 9 * * 1");
    }

    #[test]
    fn every_5_minutes() {
        assert_eq!(to_cron("every 5 minutes").unwrap(), "*/5 * * * *");
    }

    #[test]
    fn every_hour() {
        assert_eq!(to_cron("every hour").unwrap(), "0 * * * *");
    }

    #[test]
    fn hourly_at_30() {
        assert_eq!(to_cron("hourly at :30").unwrap(), "30 * * * *");
    }

    #[test]
    fn daily_at_noon() {
        assert_eq!(to_cron("daily at noon").unwrap(), "0 12 * * *");
    }

    #[test]
    fn pm_times() {
        assert_eq!(to_cron("every day at 9pm").unwrap(), "0 21 * * *");
        assert_eq!(to_cron("every day at 12pm").unwrap(), "0 12 * * *");
        assert_eq!(to_cron("every day at 12am").unwrap(), "0 0 * * *");
    }

    #[test]
    fn unparseable_returns_suggestion() {
        let err = to_cron("every weekdays at 9am").unwrap_err();
        match err {
            ExpressionError::Unhumanizable { suggestion, .. } => {
                assert_eq!(suggestion, Some("every weekday at 9am".to_string()));
            }
            _ => panic!("wrong error"),
        }
    }

    #[test]
    fn describe_common() {
        assert_eq!(describe_cron("* * * * *"), "Every minute");
        assert_eq!(describe_cron("0 9 * * 1-5"), "At 09:00 on weekdays");
        assert_eq!(describe_cron("*/5 * * * *"), "Every 5 minutes");
        assert_eq!(describe_cron("0 0 1 * *"), "At cron: 0 0 1 * *");
    }
}
