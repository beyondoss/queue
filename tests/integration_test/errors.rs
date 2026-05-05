use crate::helpers::{TestClient, test_env};

#[tokio::test]
async fn test_delete_nonexistent_queue_is_idempotent() {
    let _ = test_env();
    let client = TestClient::new();

    // Delete a queue that has never been created — must not error
    client
        .delete("/v1/queues/test_never_existed_xyzzy_q")
        .await
        .assert_status(204);
}

#[tokio::test]
async fn test_invalid_receipt_handle_returns_error() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_badreceipt_q" }),
        )
        .await
        .assert_status(201);

    // CreateQueue + SendMessage so queue exists
    let create = client
        .sqs(
            "CreateQueue",
            &serde_json::json!({ "QueueName": "test_badreceipt_q" }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let queue_url = create["QueueUrl"].as_str().unwrap().to_string();

    client
        .sqs(
            "SendMessage",
            &serde_json::json!({ "QueueUrl": queue_url, "MessageBody": "hi" }),
        )
        .await
        .assert_status(200);

    // DeleteMessage with a completely invalid receipt handle
    let resp = client
        .sqs(
            "DeleteMessage",
            &serde_json::json!({
                "QueueUrl": queue_url,
                "ReceiptHandle": "not-a-valid-base64url-receipt-handle!!!"
            }),
        )
        .await;
    assert!(
        resp.status == 400 || resp.status == 404,
        "invalid receipt handle must return 4xx, got {}",
        resp.status
    );
}

#[tokio::test]
async fn test_delete_nonexistent_message_returns_404() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_del404_q" }),
        )
        .await
        .assert_status(201);

    // Message ID 999999999 has never been inserted
    client
        .delete("/v1/queues/test_del404_q/messages/999999999")
        .await
        .assert_status(404);
}

#[tokio::test]
async fn test_send_to_nonexistent_queue_returns_404() {
    let _ = test_env();
    let client = TestClient::new();

    let res = client
        .post(
            "/v1/queues/no_such_queue_xyz/messages",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await;
    res.assert_status(404);
}

#[tokio::test]
async fn test_invalid_queue_name_returns_400() {
    let _ = test_env();
    let client = TestClient::new();

    // Invalid characters
    let res = client
        .post("/v1/queues", &serde_json::json!({ "name": "INVALID!" }))
        .await;
    res.assert_status(400);

    // Name too long (>48 chars)
    let long_name = "a".repeat(49);
    let res = client
        .post("/v1/queues", &serde_json::json!({ "name": long_name }))
        .await;
    res.assert_status(400);

    // Uppercase letters
    let res = client
        .post("/v1/queues", &serde_json::json!({ "name": "MyQueue" }))
        .await;
    res.assert_status(400);
}
