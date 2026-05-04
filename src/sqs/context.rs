use std::sync::Arc;

use axum::Json;
use axum::response::{IntoResponse, Response};
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
    pub base_url: Arc<str>,
    pub action: String,
}

impl SqsContext {
    pub fn new(protocol: SqsProtocol, base_url: Arc<str>, action: impl Into<String>) -> Self {
        Self {
            protocol,
            request_id: Uuid::new_v4().to_string(),
            base_url,
            action: action.into(),
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
            SqsProtocol::Json => {
                ([("content-type", "application/x-amz-json-1.0")], Json(body)).into_response()
            }
            SqsProtocol::Query => {
                let inner = serde_json::to_value(&body).unwrap_or(serde_json::Value::Null);
                let xml = action_xml_response(&inner, &self.action, &self.request_id);
                ([("content-type", "text/xml")], xml).into_response()
            }
        }
    }

    pub fn empty_ok(&self) -> Response {
        match self.protocol {
            SqsProtocol::Json => (axum::http::StatusCode::OK, "{}").into_response(),
            SqsProtocol::Query => {
                let xml = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                     <{}Response>\n  \
                       <ResponseMetadata>\n    \
                         <RequestId>{}</RequestId>\n  \
                       </ResponseMetadata>\n\
                     </{}Response>",
                    self.action, self.request_id, self.action,
                );
                ([("content-type", "text/xml")], xml).into_response()
            }
        }
    }

    pub fn queue_url(&self, queue_name: &str) -> String {
        format!(
            "{}/000000000000/{}",
            self.base_url.trim_end_matches('/'),
            queue_name
        )
    }
}

/// Generates a standard SQS Query response envelope:
/// `<{Action}Response><{Action}Result>…</{Action}Result><ResponseMetadata>…</ResponseMetadata></{Action}Response>`
fn action_xml_response(value: &serde_json::Value, action: &str, request_id: &str) -> String {
    let mut xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <{action}Response>\n  \
           <{action}Result>\n",
    );
    if let serde_json::Value::Object(map) = value {
        for (key, val) in map {
            json_value_to_xml(val, key, &mut xml, 2);
        }
    }
    xml.push_str(&format!(
        "  </{action}Result>\n  \
         <ResponseMetadata>\n    \
           <RequestId>{request_id}</RequestId>\n  \
         </ResponseMetadata>\n\
         </{action}Response>",
    ));
    xml
}

fn json_value_to_xml(value: &serde_json::Value, tag: &str, xml: &mut String, depth: usize) {
    let indent = "  ".repeat(depth);
    match value {
        serde_json::Value::Array(arr) => {
            for item in arr {
                xml.push_str(&format!("{indent}<{tag}>\n"));
                if let serde_json::Value::Object(m) = item {
                    for (k, v) in m {
                        json_value_to_xml(v, k, xml, depth + 1);
                    }
                } else {
                    json_value_to_xml(item, tag, xml, depth + 1);
                }
                xml.push_str(&format!("{indent}</{tag}>\n"));
            }
        }
        serde_json::Value::Object(m) => {
            xml.push_str(&format!("{indent}<{tag}>\n"));
            for (k, v) in m {
                json_value_to_xml(v, k, xml, depth + 1);
            }
            xml.push_str(&format!("{indent}</{tag}>\n"));
        }
        serde_json::Value::Null => {}
        _ => {
            let text = value
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| value.to_string());
            xml.push_str(&format!("{indent}<{tag}>{}</{tag}>\n", escape_xml(&text),));
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
