use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use uuid::Uuid;

use crate::sqs::error::{SqsError, SqsErrorCode, SqsProtocol};

/// Per-request context threaded through SQS action handlers.
/// Carries the protocol variant and request ID needed to produce
/// correctly-formatted responses and errors.
#[derive(Clone)]
pub struct SqsContext {
    pub protocol: SqsProtocol,
    pub request_id: String,
    pub base_url: String,
}

impl SqsContext {
    pub fn new(protocol: SqsProtocol, base_url: String) -> Self {
        Self {
            protocol,
            request_id: Uuid::new_v4().to_string(),
            base_url,
        }
    }

    pub fn error(&self, code: SqsErrorCode) -> SqsError {
        SqsError {
            code,
            request_id: self.request_id.clone(),
            protocol: self.protocol,
        }
    }

    pub fn internal_error(&self, source: impl std::fmt::Display) -> SqsError {
        tracing::error!(error = %source, "internal error");
        self.error(SqsErrorCode::InternalError)
    }

    pub fn ok<T: Serialize>(&self, body: T) -> Response {
        match self.protocol {
            SqsProtocol::Json => (
                [("content-type", "application/x-amz-json-1.0")],
                Json(body),
            )
                .into_response(),
            SqsProtocol::Query => {
                // Wrap in a standard SQS Query response envelope
                let inner = serde_json::to_value(&body).unwrap_or(serde_json::Value::Null);
                let xml = json_to_xml_response(&inner, &self.request_id);
                (
                    [("content-type", "text/xml")],
                    xml,
                )
                    .into_response()
            }
        }
    }

    pub fn empty_ok(&self) -> Response {
        match self.protocol {
            SqsProtocol::Json => (axum::http::StatusCode::OK, "{}").into_response(),
            SqsProtocol::Query => {
                let xml = format!(
                    r#"<?xml version="1.0" encoding="UTF-8"?>
<ResponseMetadata>
  <RequestId>{}</RequestId>
</ResponseMetadata>"#,
                    self.request_id
                );
                ([("content-type", "text/xml")], xml).into_response()
            }
        }
    }

    pub fn queue_url(&self, queue_name: &str) -> String {
        format!("{}/000000000000/{}", self.base_url.trim_end_matches('/'), queue_name)
    }
}

fn json_to_xml_response(value: &serde_json::Value, request_id: &str) -> String {
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Response>\n");
    json_value_to_xml(value, &mut xml, 1);
    xml.push_str(&format!(
        "  <ResponseMetadata>\n    <RequestId>{}</RequestId>\n  </ResponseMetadata>\n",
        request_id
    ));
    xml.push_str("</Response>");
    xml
}

fn json_value_to_xml(value: &serde_json::Value, xml: &mut String, depth: usize) {
    let indent = "  ".repeat(depth);
    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map {
                match val {
                    serde_json::Value::Array(arr) => {
                        for item in arr {
                            xml.push_str(&format!("{}<{}>\n", indent, key));
                            json_value_to_xml(item, xml, depth + 1);
                            xml.push_str(&format!("{}</{}>\n", indent, key));
                        }
                    }
                    serde_json::Value::Object(_) => {
                        xml.push_str(&format!("{}<{}>\n", indent, key));
                        json_value_to_xml(val, xml, depth + 1);
                        xml.push_str(&format!("{}</{}>\n", indent, key));
                    }
                    serde_json::Value::Null => {}
                    _ => {
                        let text = val.as_str().map(|s| s.to_string()).unwrap_or_else(|| val.to_string());
                        xml.push_str(&format!("{}<{}>{}</{}>\n", indent, key, escape_xml(&text), key));
                    }
                }
            }
        }
        _ => {
            let text = value.as_str().map(|s| s.to_string()).unwrap_or_else(|| value.to_string());
            xml.push_str(&format!("{}{}\n", indent, escape_xml(&text)));
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
