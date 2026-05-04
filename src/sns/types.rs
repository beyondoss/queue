use serde::{Deserialize, Serialize};

// ---- CreateTopic ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateTopicRequest {
    pub name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CreateTopicResponse {
    pub topic_arn: String,
}

// ---- DeleteTopic ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteTopicRequest {
    pub topic_arn: String,
}

// ---- ListTopics ----

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct TopicEntry {
    pub topic_arn: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListTopicsResponse {
    pub topics: Vec<TopicEntry>,
}

// ---- Subscribe ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SubscribeRequest {
    pub topic_arn: String,
    pub protocol: String,
    pub endpoint: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SubscribeResponse {
    pub subscription_arn: String,
}

// ---- Unsubscribe ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct UnsubscribeRequest {
    pub subscription_arn: String,
}

// ---- ListSubscriptions ----

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct SubscriptionEntry {
    pub subscription_arn: String,
    pub owner: String,
    pub protocol: String,
    pub endpoint: String,
    pub topic_arn: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListSubscriptionsResponse {
    pub subscriptions: Vec<SubscriptionEntry>,
}

// ---- ListSubscriptionsByTopic ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ListSubscriptionsByTopicRequest {
    pub topic_arn: String,
}

// ---- GetTopicAttributes ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetTopicAttributesRequest {
    pub topic_arn: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetTopicAttributesResponse {
    pub attributes: std::collections::HashMap<String, String>,
}

// ---- SetTopicAttributes ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SetTopicAttributesRequest {
    pub topic_arn: String,
    pub attribute_name: String,
    pub attribute_value: Option<String>,
}

// ---- GetSubscriptionAttributes ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetSubscriptionAttributesRequest {
    pub subscription_arn: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct GetSubscriptionAttributesResponse {
    pub attributes: std::collections::HashMap<String, String>,
}

// ---- Publish ----

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PublishRequest {
    pub topic_arn: Option<String>,
    pub target_arn: Option<String>,
    pub message: String,
    pub subject: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct PublishResponse {
    pub message_id: String,
}
