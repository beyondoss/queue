use std::collections::HashMap;

use crate::sqs::context::SqsContext;
use crate::sqs::error::{SqsError, SqsErrorCode};
use crate::sqs::types::MessageAttribute;

pub fn queue_name_from_url<'a>(
    queue_url: Option<&'a str>,
    ctx: &SqsContext,
) -> Result<String, SqsError> {
    let url = queue_url.ok_or_else(|| ctx.error(SqsErrorCode::NonExistentQueue))?;
    // Expect: http://host:port/000000000000/{queue_name}
    url.rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .ok_or_else(|| ctx.error(SqsErrorCode::NonExistentQueue))
}

pub fn md5_of(s: &str) -> String {
    format!("{:x}", md5::compute(s.as_bytes()))
}

pub fn message_attributes_to_headers(
    attrs: &HashMap<String, MessageAttribute>,
    group_id: &Option<String>,
    dedup_id: &Option<String>,
) -> Option<serde_json::Value> {
    if attrs.is_empty() && group_id.is_none() && dedup_id.is_none() {
        return None;
    }

    let mut map = serde_json::Map::new();

    for (name, attr) in attrs {
        map.insert(
            name.clone(),
            serde_json::json!({
                "DataType": attr.data_type,
                "StringValue": attr.string_value,
            }),
        );
    }

    if let Some(gid) = group_id {
        map.insert("x-pgmq-group".to_string(), serde_json::Value::String(gid.clone()));
    }
    if let Some(did) = dedup_id {
        map.insert("x-pgmq-dedup-id".to_string(), serde_json::Value::String(did.clone()));
    }

    Some(serde_json::Value::Object(map))
}
