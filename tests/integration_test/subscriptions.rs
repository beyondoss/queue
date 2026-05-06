use crate::helpers::{TestClient, test_env};

#[tokio::test]
async fn test_sqs_subscriptions_unaffected_by_http_delivery() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_http_sqs_fanout_q" }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/events/sqs.fanout.*/subscriptions",
            &serde_json::json!({ "queue_name": "test_http_sqs_fanout_q" }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/events/sqs.fanout.event",
            &serde_json::json!({ "message": { "data": "hello" } }),
        )
        .await
        .assert_status(201);

    let msgs = client
        .get("/v1/queues/test_http_sqs_fanout_q/messages?max=1&wait=0")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let arr = msgs.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["message"]["data"], "hello");
}

#[tokio::test]
async fn test_sqs_subscription_unsubscribe() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_unsub_q" }))
        .await
        .assert_status(201);

    // Subscribe via SNS JSON protocol using ARN format endpoint to exercise
    // the ARN path in subscribe.rs (splits on ':' rather than '/').
    let sub_resp = client
        .sns(
            "Subscribe",
            &serde_json::json!({
                "TopicArn": "arn:aws:sns:us-east-1:000000000000:unsub.*",
                "Protocol": "sqs",
                "Endpoint": "arn:aws:sqs:us-east-1:000000000000:test_unsub_q",
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let sub_arn = sub_resp["SubscriptionArn"]
        .as_str()
        .expect("SubscriptionArn")
        .to_string();

    // Publish — must arrive
    client
        .sns(
            "Publish",
            &serde_json::json!({
                "TopicArn": "arn:aws:sns:us-east-1:000000000000:unsub.test",
                "Message": r#"{"before":"unsub"}"#,
            }),
        )
        .await
        .assert_status(200);

    let before = client
        .get("/v1/queues/test_unsub_q/messages?max=1&wait=0&vt=30")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(
        before.as_array().unwrap().len(),
        1,
        "message must arrive before unsubscribe"
    );
    // Drain
    let msg_id = before[0]["id"].as_i64().unwrap();
    client
        .delete(&format!("/v1/queues/test_unsub_q/messages/{msg_id}"))
        .await
        .assert_status(204);

    // Unsubscribe
    client
        .sns(
            "Unsubscribe",
            &serde_json::json!({ "SubscriptionArn": sub_arn }),
        )
        .await
        .assert_status(200);

    // Publish again — must NOT arrive
    client
        .sns(
            "Publish",
            &serde_json::json!({
                "TopicArn": "arn:aws:sns:us-east-1:000000000000:unsub.test",
                "Message": r#"{"after":"unsub"}"#,
            }),
        )
        .await
        .assert_status(200);

    let after = client
        .get("/v1/queues/test_unsub_q/messages?max=1&wait=0&vt=30")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(
        after.as_array().unwrap().len(),
        0,
        "no message must arrive after unsubscribe"
    );
}

#[tokio::test]
async fn test_subscribe_invalid_protocol_returns_400() {
    let _ = test_env();
    let client = TestClient::new();

    let res = client
        .post(
            "/v1/events/test.invalid.proto/subscriptions",
            &serde_json::json!({
                "protocol": "smtp",
                "endpoint": "smtp://mail.example.com",
            }),
        )
        .await;
    res.assert_status(400);
}

#[tokio::test]
async fn test_unsubscribe_nonexistent_returns_404() {
    let _ = test_env();
    let client = TestClient::new();

    let res = client
        .delete("/v1/events/test.pattern/subscriptions/999999999")
        .await;
    res.assert_status(404);
}
