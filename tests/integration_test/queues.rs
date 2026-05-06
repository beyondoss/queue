use crate::helpers::{TestClient, test_env};

#[tokio::test]
async fn test_create_and_list_queue() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_list_q" }))
        .await
        .assert_status(201);

    let body = client
        .get("/v1/queues")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let queues = body.as_array().expect("expected array");
    assert!(
        queues.iter().any(|q| q["name"] == "test_list_q"),
        "test_list_q not found in {queues:?}"
    );
}

#[tokio::test]
async fn test_create_queue_idempotent() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_idem_q" }))
        .await
        .assert_status(201);
    // second create should not error
    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_idem_q" }))
        .await
        .assert_status(201);
}

#[tokio::test]
async fn test_delete_queue() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_drop_q" }))
        .await
        .assert_status(201);

    client
        .delete("/v1/queues/test_drop_q")
        .await
        .assert_status(204);
}

#[tokio::test]
async fn test_purge_queue() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_purge_q" }))
        .await
        .assert_status(201);

    for i in 0..5 {
        client
            .post(
                "/v1/queues/test_purge_q/messages",
                &serde_json::json!({ "message": { "i": i } }),
            )
            .await
            .assert_status(201);
    }

    let purge = client
        .post("/v1/queues/test_purge_q/purge", &serde_json::json!({}))
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(purge["deleted"], 5);

    let metrics = client
        .get("/v1/queues/test_purge_q")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(metrics["queue_length"], 0);
}

#[tokio::test]
async fn test_delete_queue_cascades_to_subscriptions() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_cascade_q" }),
        )
        .await
        .assert_status(201);

    // Bind the queue to a topic.
    client
        .post(
            "/v1/events/test.cascade.topic/subscriptions",
            &serde_json::json!({ "queue_name": "test_cascade_q" }),
        )
        .await
        .assert_status(201);

    // Confirm subscription exists.
    let subs = client
        .get("/v1/queues/test_cascade_q/subscriptions")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(subs.as_array().unwrap().len(), 1);

    // Delete the queue — subscription must cascade away.
    client
        .delete("/v1/queues/test_cascade_q")
        .await
        .assert_status(204);

    // Publish should succeed with 0 matches (no dangling subscription).
    let res = client
        .post(
            "/v1/events/test.cascade.topic",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    assert_eq!(res["queues_matched"], 0);
}
