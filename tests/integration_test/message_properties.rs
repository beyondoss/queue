use crate::helpers::{TestClient, test_env};

#[tokio::test]
async fn test_message_read_count_increments() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_rc_q" }))
        .await
        .assert_status(201);
    client
        .post(
            "/v1/queues/test_rc_q/messages",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await
        .assert_status(201);

    // First receive: read_count = 1
    let first = client
        .get("/v1/queues/test_rc_q/messages?max=1&wait=0&vt=1")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(
        first[0]["read_count"], 1,
        "first receive must set read_count=1"
    );

    // Wait for vt to expire and receive again
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let second = client
        .get("/v1/queues/test_rc_q/messages?max=1&wait=0&vt=30")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(
        second[0]["read_count"], 2,
        "read_count must increment on each receive"
    );
}

#[tokio::test]
async fn test_message_headers_round_trip() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_headers_q" }),
        )
        .await
        .assert_status(201);
    client
        .post(
            "/v1/queues/test_headers_q/messages",
            &serde_json::json!({
                "message": { "x": 1 },
                "headers": { "x-source": "test", "x-priority": "high" }
            }),
        )
        .await
        .assert_status(201);

    let msgs = client
        .get("/v1/queues/test_headers_q/messages?max=1&wait=0&vt=30")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let msg = &msgs.as_array().unwrap()[0];
    assert_eq!(msg["headers"]["x-source"], "test");
    assert_eq!(msg["headers"]["x-priority"], "high");
}
