use std::sync::Arc;

use axum::Json;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use uuid::Uuid;

use crate::sns::error::{SnsError, SnsErrorCode, SnsProtocol};

const REGION: &str = "us-east-1";
const ACCOUNT: &str = "000000000000";

#[derive(Clone)]
pub struct SnsContext {
    pub protocol: SnsProtocol,
    pub request_id: String,
    pub base_url: Arc<str>,
    pub action: String,
}

impl SnsContext {
    pub fn new(protocol: SnsProtocol, base_url: Arc<str>, action: impl Into<String>) -> Self {
        Self {
            protocol,
            request_id: Uuid::new_v4().to_string(),
            base_url,
            action: action.into(),
        }
    }

    pub fn error(&self, code: SnsErrorCode) -> SnsError {
        SnsError {
            code,
            request_id: self.request_id.clone(),
            protocol: self.protocol,
        }
    }

    pub fn internal_error(&self, source: impl std::fmt::Display) -> SnsError {
        tracing::error!(error = %source, "SNS internal error");
        self.error(SnsErrorCode::InternalError)
    }

    pub fn ok<T: Serialize>(&self, body: T) -> Response {
        match self.protocol {
            SnsProtocol::Json => {
                ([("content-type", "application/x-amz-json-1.0")], Json(body)).into_response()
            }
            SnsProtocol::Query => {
                let inner = serde_json::to_value(&body).unwrap_or(serde_json::Value::Null);
                let xml = sns_xml_response(&inner, &self.action, &self.request_id);
                ([("content-type", "text/xml")], xml).into_response()
            }
        }
    }

    pub fn empty_ok(&self) -> Response {
        match self.protocol {
            SnsProtocol::Json => (axum::http::StatusCode::OK, "{}").into_response(),
            SnsProtocol::Query => {
                let xml = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<{}Response xmlns="https://sns.amazonaws.com/doc/2010-03-31/">
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</{}Response>"#,
                    self.action, self.request_id, self.action
                );
                ([("content-type", "text/xml")], xml).into_response()
            }
        }
    }

    pub fn topic_arn(&self, name: &str) -> String {
        format!("arn:aws:sns:{}:{}:{}", REGION, ACCOUNT, name)
    }

    pub fn subscription_arn(&self, topic: &str, queue: &str) -> String {
        format!("arn:aws:sns:{}:{}:{}:{}", REGION, ACCOUNT, topic, queue)
    }

    /// Extract topic name from a topic ARN or return the input as-is (for name-only callers).
    pub fn topic_name_from_arn<'a>(&self, arn: &'a str) -> Option<&'a str> {
        if arn.starts_with("arn:") {
            // arn:aws:sns:region:account:name
            arn.splitn(7, ':').nth(5).filter(|s| !s.is_empty())
        } else {
            Some(arn)
        }
    }

    /// Parse a subscription ARN into (topic_name, queue_name).
    pub fn parse_subscription_arn(&self, arn: &str) -> Option<(String, String)> {
        // arn:aws:sns:region:account:topic:queue  (7 colon-separated parts)
        let parts: Vec<&str> = arn.splitn(8, ':').collect();
        if parts.len() >= 7 {
            Some((parts[5].to_string(), parts[6].to_string()))
        } else {
            None
        }
    }

    pub fn queue_endpoint(&self, queue_name: &str) -> String {
        format!(
            "{}/{}/{}",
            self.base_url.trim_end_matches('/'),
            ACCOUNT,
            queue_name
        )
    }
}

fn sns_xml_response(value: &serde_json::Value, action: &str, request_id: &str) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<{}Response xmlns=\"https://sns.amazonaws.com/doc/2010-03-31/\">\n  <{}Result>\n",
        action, action
    );
    if let serde_json::Value::Object(map) = value {
        for (key, val) in map {
            append_xml_field(&mut xml, key, val, 2);
        }
    }
    xml.push_str(&format!(
        "  </{}Result>\n  <ResponseMetadata>\n    <RequestId>{}</RequestId>\n  </ResponseMetadata>\n</{}Response>",
        action, request_id, action
    ));
    xml
}

fn append_xml_field(xml: &mut String, key: &str, val: &serde_json::Value, depth: usize) {
    let indent = "  ".repeat(depth);
    match val {
        serde_json::Value::Array(arr) => {
            for item in arr {
                xml.push_str(&format!("{}<member>\n", indent));
                if let serde_json::Value::Object(m) = item {
                    for (k, v) in m {
                        append_xml_field(xml, k, v, depth + 1);
                    }
                }
                xml.push_str(&format!("{}</member>\n", indent));
            }
        }
        serde_json::Value::Object(m) => {
            xml.push_str(&format!("{}<{}>\n", indent, key));
            for (k, v) in m {
                append_xml_field(xml, k, v, depth + 1);
            }
            xml.push_str(&format!("{}</{}>\n", indent, key));
        }
        serde_json::Value::Null => {}
        _ => {
            let text = val
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| val.to_string());
            xml.push_str(&format!(
                "{}<{}>{}</{}>\n",
                indent,
                key,
                escape_xml(&text),
                key
            ));
        }
    }
}

fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
