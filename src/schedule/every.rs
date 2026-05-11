//! Convert a fixed-interval shorthand ("5m", "30s", "2h", "1d") to a
//! canonical cron pattern.
//!
//! The interval must evenly divide the next-larger unit (seconds into 60,
//! minutes into 60, hours into 24); otherwise it can't be expressed in
//! cron and is rejected. This matches every other cron-based scheduler:
//! "every 5 minutes" fires at :00, :05, :10... not at arbitrary offsets
//! from when the schedule was created.

use super::expression::ExpressionError;

/// Parse a shorthand interval into its canonical cron pattern.
pub fn to_cron(input: &str) -> Result<String, ExpressionError> {
    let (value, unit) = split_value_unit(input)?;
    match unit {
        "s" => {
            if !(1..=59).contains(&value) || 60 % value != 0 {
                return Err(ExpressionError::InvalidInterval(format!(
                    "{input}: seconds must divide 60 (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30)"
                )));
            }
            if value == 1 {
                Ok("* * * * * *".into())
            } else {
                Ok(format!("*/{value} * * * * *"))
            }
        }
        "m" => {
            if !(1..=59).contains(&value) || 60 % value != 0 {
                return Err(ExpressionError::InvalidInterval(format!(
                    "{input}: minutes must divide 60 (1, 2, 3, 4, 5, 6, 10, 12, 15, 20, 30)"
                )));
            }
            if value == 1 {
                Ok("* * * * *".into())
            } else {
                Ok(format!("*/{value} * * * *"))
            }
        }
        "h" => {
            if !(1..=23).contains(&value) || 24 % value != 0 {
                return Err(ExpressionError::InvalidInterval(format!(
                    "{input}: hours must divide 24 (1, 2, 3, 4, 6, 8, 12)"
                )));
            }
            if value == 1 {
                Ok("0 * * * *".into())
            } else {
                Ok(format!("0 */{value} * * *"))
            }
        }
        "d" => {
            if value != 1 {
                return Err(ExpressionError::InvalidInterval(format!(
                    "{input}: only \"1d\" is supported (use cron for multi-day intervals)"
                )));
            }
            Ok("0 0 * * *".into())
        }
        other => Err(ExpressionError::InvalidInterval(format!(
            "unknown unit '{other}'; expected one of s, m, h, d"
        ))),
    }
}

fn split_value_unit(input: &str) -> Result<(u32, &str), ExpressionError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ExpressionError::InvalidInterval("empty interval".into()));
    }
    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| ExpressionError::InvalidInterval(format!("{input}: missing unit")))?;
    if split_at == 0 {
        return Err(ExpressionError::InvalidInterval(format!(
            "{input}: must start with a number"
        )));
    }
    let value: u32 = trimmed[..split_at]
        .parse()
        .map_err(|_| ExpressionError::InvalidInterval(format!("{input}: bad number")))?;
    let unit = trimmed[split_at..].trim();
    Ok((value, unit))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seconds() {
        assert_eq!(to_cron("30s").unwrap(), "*/30 * * * * *");
        assert_eq!(to_cron("1s").unwrap(), "* * * * * *");
    }

    #[test]
    fn minutes() {
        assert_eq!(to_cron("5m").unwrap(), "*/5 * * * *");
        assert_eq!(to_cron("1m").unwrap(), "* * * * *");
        assert_eq!(to_cron("30m").unwrap(), "*/30 * * * *");
    }

    #[test]
    fn hours() {
        assert_eq!(to_cron("1h").unwrap(), "0 * * * *");
        assert_eq!(to_cron("6h").unwrap(), "0 */6 * * *");
        assert_eq!(to_cron("12h").unwrap(), "0 */12 * * *");
    }

    #[test]
    fn one_day() {
        assert_eq!(to_cron("1d").unwrap(), "0 0 * * *");
    }

    #[test]
    fn non_divisible_rejected() {
        assert!(to_cron("7m").is_err());
        assert!(to_cron("5h").is_err());
        assert!(to_cron("2d").is_err());
    }

    #[test]
    fn bad_inputs_rejected() {
        assert!(to_cron("").is_err());
        assert!(to_cron("5").is_err());
        assert!(to_cron("m5").is_err());
        assert!(to_cron("abc").is_err());
        assert!(to_cron("0s").is_err());
    }
}
