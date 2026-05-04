use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// Query protocol sends all values as strings; these helpers accept both number and string.
fn de_i32<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i32, D::Error> {
    struct V;
    impl<'de> serde::de::Visitor<'de> for V {
        type Value = i32;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("integer or string")
        }
        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<i32, E> {
            Ok(v as i32)
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<i32, E> {
            Ok(v as i32)
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<i32, E> {
            v.parse().map_err(E::custom)
        }
    }
    d.deserialize_any(V)
}

fn de_opt_i32<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<i32>, D::Error> {
    de_i32(d).map(Some)
}

// ---- Common ----

#[derive(Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct MessageAttribute {
    pub data_type: String,
    pub string_value: Option<String>,
    pub binary_value: Option<String>,
}

// ---- SendMessage ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SendMessageRequest {
    pub queue_url: Option<String>,
    pub message_body: String,
    #[serde(default, deserialize_with = "de_i32")]
    pub delay_seconds: i32,
    #[serde(default)]
    pub message_attributes: HashMap<String, MessageAttribute>,
    pub message_group_id: Option<String>,
    pub message_deduplication_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SendMessageResponse {
    pub message_id: String,
    #[serde(rename = "MD5OfMessageBody")]
    pub md5_of_message_body: String,
}

// ---- SendMessageBatch ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SendMessageBatchRequestEntry {
    pub id: String,
    pub message_body: String,
    #[serde(default, deserialize_with = "de_i32")]
    pub delay_seconds: i32,
    #[serde(default)]
    pub message_attributes: HashMap<String, MessageAttribute>,
    pub message_group_id: Option<String>,
    pub message_deduplication_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SendMessageBatchRequest {
    pub queue_url: Option<String>,
    pub entries: Vec<SendMessageBatchRequestEntry>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SendMessageBatchResultEntry {
    pub id: String,
    pub message_id: String,
    #[serde(rename = "MD5OfMessageBody")]
    pub md5_of_message_body: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SendMessageBatchResponse {
    pub successful: Vec<SendMessageBatchResultEntry>,
    pub failed: Vec<serde_json::Value>,
}

// ---- ReceiveMessage ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ReceiveMessageRequest {
    pub queue_url: Option<String>,
    #[serde(default = "default_max_messages", deserialize_with = "de_i32")]
    pub max_number_of_messages: i32,
    #[serde(default, deserialize_with = "de_opt_i32")]
    pub visibility_timeout: Option<i32>,
    #[serde(default, deserialize_with = "de_i32")]
    pub wait_time_seconds: i32,
    #[serde(default)]
    pub attribute_names: Vec<String>,
    #[serde(default)]
    pub message_attribute_names: Vec<String>,
}

fn default_max_messages() -> i32 {
    1
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SqsMessage {
    pub message_id: String,
    pub receipt_handle: String,
    pub body: String,
    #[serde(rename = "MD5OfBody")]
    pub md5_of_body: String,
    pub attributes: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub message_attributes: HashMap<String, MessageAttribute>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ReceiveMessageResponse {
    pub messages: Vec<SqsMessage>,
}

// ---- DeleteMessage ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteMessageRequest {
    pub queue_url: Option<String>,
    pub receipt_handle: String,
}

// ---- DeleteMessageBatch ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteMessageBatchRequestEntry {
    pub id: String,
    pub receipt_handle: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteMessageBatchRequest {
    pub queue_url: Option<String>,
    pub entries: Vec<DeleteMessageBatchRequestEntry>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteMessageBatchResultEntry {
    pub id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteMessageBatchResponse {
    pub successful: Vec<DeleteMessageBatchResultEntry>,
    pub failed: Vec<serde_json::Value>,
}

// ---- ChangeMessageVisibility ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ChangeMessageVisibilityRequest {
    pub queue_url: Option<String>,
    pub receipt_handle: String,
    #[serde(deserialize_with = "de_i32")]
    pub visibility_timeout: i32,
}

// ---- ChangeMessageVisibilityBatch ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ChangeMessageVisibilityBatchRequestEntry {
    pub id: String,
    pub receipt_handle: String,
    #[serde(deserialize_with = "de_i32")]
    pub visibility_timeout: i32,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ChangeMessageVisibilityBatchRequest {
    pub queue_url: Option<String>,
    pub entries: Vec<ChangeMessageVisibilityBatchRequestEntry>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ChangeMessageVisibilityBatchResultEntry {
    pub id: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ChangeMessageVisibilityBatchResponse {
    pub successful: Vec<ChangeMessageVisibilityBatchResultEntry>,
    pub failed: Vec<serde_json::Value>,
}

// ---- CreateQueue ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateQueueRequest {
    pub queue_name: String,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateQueueResponse {
    pub queue_url: String,
}

// ---- DeleteQueue ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteQueueRequest {
    pub queue_url: Option<String>,
}

// ---- GetQueueUrl ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetQueueUrlRequest {
    pub queue_name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetQueueUrlResponse {
    pub queue_url: String,
}

// ---- GetQueueAttributes ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetQueueAttributesRequest {
    pub queue_url: Option<String>,
    #[serde(default)]
    pub attribute_names: Vec<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetQueueAttributesResponse {
    pub attributes: HashMap<String, String>,
}

// ---- ListQueues ----

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ListQueuesRequest {
    pub queue_name_prefix: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListQueuesResponse {
    pub queue_urls: Vec<String>,
}

// ---- PurgeQueue ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PurgeQueueRequest {
    pub queue_url: Option<String>,
}
