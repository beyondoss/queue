//! Integration tests for POST /v1/previews.

use serde_json::json;

use crate::helpers::{TestClient, test_env};

#[tokio::test]
async fn preview_cron_returns_canonical_and_next_fires() {
    let _ = test_env();
    let client = TestClient::new();

    let body = client
        .post(
            "/v1/previews",
            &json!({
                "cron": "0 9 * * 1-5",
                "timezone": "UTC",
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(body["cron"], "0 9 * * 1-5");
    assert!(
        body["human_readable"]
            .as_str()
            .unwrap()
            .contains("weekdays")
    );
    let next_fires = body["next_fires"].as_array().unwrap();
    assert!(!next_fires.is_empty());
}

#[tokio::test]
async fn preview_every_translates_to_cron() {
    let _ = test_env();
    let client = TestClient::new();

    let body = client
        .post("/v1/previews", &json!({ "every": "5m" }))
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(body["cron"], "*/5 * * * *");
}

#[tokio::test]
async fn preview_when_translates_natural_language() {
    let _ = test_env();
    let client = TestClient::new();

    let body = client
        .post(
            "/v1/previews",
            &json!({ "when": "every weekday at 9am", "timezone": "America/New_York" }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(body["cron"], "0 9 * * 1-5");
    assert_eq!(body["timezone"], "America/New_York");
    assert!(
        body["human_readable"]
            .as_str()
            .unwrap()
            .contains("America/New_York")
    );
}

#[tokio::test]
async fn preview_fire_at_one_shot() {
    let _ = test_env();
    let client = TestClient::new();

    let future = chrono::Utc::now() + chrono::Duration::hours(1);
    let body = client
        .post("/v1/previews", &json!({ "fire_at": future.to_rfc3339() }))
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert!(body["cron"].is_null());
    assert!(body["fire_at"].is_string());
    assert_eq!(body["next_fires"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn preview_bad_expression_returns_400() {
    let _ = test_env();
    let client = TestClient::new();

    let resp = client
        .post("/v1/previews", &json!({ "when": "every weekdays at 9am" }))
        .await
        .assert_status(400);
    let err: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
    assert_eq!(err["error"]["code"], "schedule_invalid");
}

#[tokio::test]
async fn preview_empty_returns_400() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/previews", &json!({}))
        .await
        .assert_status(400);
}

#[tokio::test]
async fn preview_ambiguous_returns_400() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/previews",
            &json!({ "cron": "* * * * *", "every": "5m" }),
        )
        .await
        .assert_status(400);
}
