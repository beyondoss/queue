use crate::helpers::{TestClient, test_env};

#[tokio::test]
async fn test_send_receive_delete() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_srd_q" }))
        .await
        .assert_status(201);

    // send
    let send_resp = client
        .post(
            "/v1/queues/test_srd_q/messages",
            &serde_json::json!({ "message": { "hello": "world" } }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();

    let msg_id = send_resp["id"].as_i64().expect("expected id");
    assert!(msg_id > 0);

    // receive
    let msgs = client
        .get("/v1/queues/test_srd_q/messages?max=1&wait=0&vt=30")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();

    let msgs = msgs.as_array().expect("expected array");
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["id"], msg_id);
    assert_eq!(msgs[0]["message"]["hello"], "world");

    // delete
    client
        .delete(&format!("/v1/queues/test_srd_q/messages/{msg_id}"))
        .await
        .assert_status(204);

    // queue is now empty
    let empty = client
        .get("/v1/queues/test_srd_q/messages?max=1&wait=0&vt=1")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(empty.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_send_with_delay() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_delay_q" }))
        .await
        .assert_status(201);

    // send with 60s delay
    client
        .post(
            "/v1/queues/test_delay_q/messages",
            &serde_json::json!({ "message": { "x": 1 }, "delay": 60 }),
        )
        .await
        .assert_status(201);

    // message should not be visible
    let msgs = client
        .get("/v1/queues/test_delay_q/messages?max=1&wait=0&vt=1")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(
        msgs.as_array().unwrap().len(),
        0,
        "delayed message should not be visible"
    );
}

#[tokio::test]
async fn test_send_batch() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_batch_q" }))
        .await
        .assert_status(201);

    // send batch of 3
    let resp = client
        .post(
            "/v1/queues/test_batch_q/messages",
            &serde_json::json!([
                { "message": { "n": 1 } },
                { "message": { "n": 2 } },
                { "message": { "n": 3 } },
            ]),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();

    let ids = resp["ids"].as_array().expect("expected ids array");
    assert_eq!(ids.len(), 3);
}

#[tokio::test]
async fn test_batch_delete() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_bdel_q" }))
        .await
        .assert_status(201);

    // send 3
    let r = client
        .post(
            "/v1/queues/test_bdel_q/messages",
            &serde_json::json!([
                { "message": { "n": 1 } },
                { "message": { "n": 2 } },
                { "message": { "n": 3 } },
            ]),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();

    let ids: Vec<i64> = r["ids"]
        .as_array()
        .expect("expected ids array")
        .iter()
        .map(|v| v.as_i64().expect("id must be integer"))
        .collect();

    // delete first two
    client
        .delete_json(
            "/v1/queues/test_bdel_q/messages",
            &serde_json::json!({ "ids": &ids[..2] }),
        )
        .await
        .assert_status(200);

    // one message remains
    let metrics = client
        .get("/v1/queues/test_bdel_q")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(metrics["queue_length"], 1);
}

#[tokio::test]
async fn test_batch_delete_with_nonexistent_ids() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_bdel_ne_q" }),
        )
        .await
        .assert_status(201);

    let r = client
        .post(
            "/v1/queues/test_bdel_ne_q/messages",
            &serde_json::json!([
                { "message": { "n": 1 } },
                { "message": { "n": 2 } },
            ]),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    let ids: Vec<i64> = r["ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();

    // Batch delete one valid ID + one ID that never existed
    let resp = client
        .delete_json(
            "/v1/queues/test_bdel_ne_q/messages",
            &serde_json::json!({ "ids": [ids[0], 999_999_999i64] }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();

    // Exactly 1 message was actually deleted (response is an array of deleted IDs)
    let deleted_ids = resp["deleted"]
        .as_array()
        .expect("deleted must be an array");
    assert_eq!(
        deleted_ids.len(),
        1,
        "only the existing message should be returned as deleted"
    );

    // The second message is still in the queue
    let metrics = client
        .get("/v1/queues/test_bdel_ne_q")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(
        metrics["queue_length"], 1,
        "the surviving message must still be in the queue"
    );
}

#[tokio::test]
async fn test_batch_send_per_message_delays() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_pmdelay_q" }),
        )
        .await
        .assert_status(201);

    // First message available immediately; second delayed 60 seconds.
    let res = client
        .post(
            "/v1/queues/test_pmdelay_q/messages",
            &serde_json::json!([
                { "message": { "a": 1 }, "delay": 0 },
                { "message": { "b": 2 }, "delay": 60 },
            ]),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    assert_eq!(res["ids"].as_array().unwrap().len(), 2);

    // Only the message with delay=0 should be visible right now.
    let msgs = client
        .get("/v1/queues/test_pmdelay_q/messages?max=10&wait=0&vt=1")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let arr = msgs.as_array().unwrap();
    assert_eq!(arr.len(), 1, "only the immediate message should be visible");
    assert_eq!(arr[0]["message"]["a"], 1, "wrong message delivered first");
}

#[tokio::test]
async fn test_empty_batch_send_returns_400() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_empty_batch_q" }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/queues/test_empty_batch_q/messages",
            &serde_json::json!([]),
        )
        .await
        .assert_status(400);
}

#[tokio::test]
async fn test_concurrent_receive_no_duplicates() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_concurrent_q" }),
        )
        .await
        .assert_status(201);

    // Enqueue 10 messages
    for i in 0i32..10 {
        client
            .post(
                "/v1/queues/test_concurrent_q/messages",
                &serde_json::json!({ "message": { "i": i } }),
            )
            .await
            .assert_status(201);
    }

    // Two workers receive concurrently — each pulling up to 5
    let base = test_env().url.clone();
    let http = reqwest::Client::new();
    let http2 = http.clone();
    let base2 = base.clone();

    let (a, b) = tokio::join!(
        async move {
            http.get(format!(
                "{base}/v1/queues/test_concurrent_q/messages?max=5&wait=0&vt=30"
            ))
            .header(reqwest::header::AUTHORIZATION, "Bearer test")
            .send()
            .await
            .expect("recv a")
            .json::<serde_json::Value>()
            .await
            .expect("json a")
        },
        async move {
            http2
                .get(format!(
                    "{base2}/v1/queues/test_concurrent_q/messages?max=5&wait=0&vt=30"
                ))
                .header(reqwest::header::AUTHORIZATION, "Bearer test")
                .send()
                .await
                .expect("recv b")
                .json::<serde_json::Value>()
                .await
                .expect("json b")
        }
    );

    let a_ids: std::collections::HashSet<i64> = a
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_i64().unwrap())
        .collect();
    let b_ids: std::collections::HashSet<i64> = b
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_i64().unwrap())
        .collect();

    let overlap: Vec<_> = a_ids.intersection(&b_ids).collect();
    assert!(
        overlap.is_empty(),
        "SKIP LOCKED must prevent duplicate delivery; overlap: {overlap:?}"
    );
    assert_eq!(
        a_ids.len() + b_ids.len(),
        10,
        "all 10 messages must be delivered exactly once"
    );
}
