use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

use crate::error::ApiError;

pub fn encode(queue_name: &str, msg_id: i64) -> String {
    let raw = format!("{}\x00{}", queue_name, msg_id);
    URL_SAFE_NO_PAD.encode(raw.as_bytes())
}

pub fn decode(handle: &str) -> Result<(String, i64), ApiError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(handle)
        .map_err(|_| ApiError::InvalidReceiptHandle)?;

    let s = String::from_utf8(bytes).map_err(|_| ApiError::InvalidReceiptHandle)?;

    let null_pos = s.find('\x00').ok_or(ApiError::InvalidReceiptHandle)?;
    let queue_name = s[..null_pos].to_string();
    let msg_id_str = &s[null_pos + 1..];
    let msg_id = msg_id_str
        .parse::<i64>()
        .map_err(|_| ApiError::InvalidReceiptHandle)?;

    Ok((queue_name, msg_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let handle = encode("my-queue", 42);
        let (name, id) = decode(&handle).unwrap();
        assert_eq!(name, "my-queue");
        assert_eq!(id, 42);
    }

    #[test]
    fn queue_name_with_special_chars() {
        let handle = encode("my.fifo-queue_test", 999);
        let (name, id) = decode(&handle).unwrap();
        assert_eq!(name, "my.fifo-queue_test");
        assert_eq!(id, 999);
    }

    #[test]
    fn invalid_handle_rejected() {
        assert!(decode("not-valid-base64!!!").is_err());
        assert!(decode("bm9udWxsYnl0ZQ").is_err()); // valid b64 but no null byte
    }
}
