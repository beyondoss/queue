use crate::helpers::{TestClient, test_env};

#[tokio::test]
async fn test_fifo_create_send_receive() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_fifo_q", "fifo": true }),
        )
        .await
        .assert_status(201);

    let send = client
        .post(
            "/v1/queues/test_fifo_q/messages",
            &serde_json::json!({ "message": { "hello": "fifo" }, "group_id": "grp1" }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();

    let msg_id = send["id"].as_i64().expect("expected id");
    assert!(msg_id > 0);

    let msgs = client
        .get("/v1/queues/test_fifo_q/messages?max=1&wait=0&vt=1&fifo=true")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();

    let msgs = msgs.as_array().unwrap();
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["id"], msg_id);
    assert_eq!(msgs[0]["message"]["hello"], "fifo");
}

#[tokio::test]
async fn test_fifo_within_group_ordering() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_fifo_order_q", "fifo": true }),
        )
        .await
        .assert_status(201);

    // send 3 messages to the same group — must be delivered in msg_id ASC order
    for n in 1i64..=3 {
        client
            .post(
                "/v1/queues/test_fifo_order_q/messages",
                &serde_json::json!({ "message": { "n": n }, "group_id": "grp" }),
            )
            .await
            .assert_status(201);
    }

    let mut prev_id = 0i64;
    for expected_n in 1i64..=3 {
        let msgs = client
            .get("/v1/queues/test_fifo_order_q/messages?max=1&wait=0&vt=30&fifo=true")
            .await
            .assert_status(200)
            .json::<serde_json::Value>();
        let msgs = msgs.as_array().unwrap();
        assert_eq!(msgs.len(), 1, "expected message {expected_n}");
        let id = msgs[0]["id"].as_i64().unwrap();
        assert_eq!(msgs[0]["message"]["n"], expected_n, "wrong ordering");
        assert!(id > prev_id, "msg_id must be ascending");
        prev_id = id;
        // delete to release the group lock before next read
        client
            .delete(&format!("/v1/queues/test_fifo_order_q/messages/{id}"))
            .await
            .assert_status(204);
    }
}

#[tokio::test]
async fn test_fifo_group_locking() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_fifo_lock_q", "fifo": true }),
        )
        .await
        .assert_status(201);

    // grp1: two messages
    client
        .post(
            "/v1/queues/test_fifo_lock_q/messages",
            &serde_json::json!({ "message": { "n": 1 }, "group_id": "grp1" }),
        )
        .await
        .assert_status(201);
    client
        .post(
            "/v1/queues/test_fifo_lock_q/messages",
            &serde_json::json!({ "message": { "n": 2 }, "group_id": "grp1" }),
        )
        .await
        .assert_status(201);

    // grp2: one message
    let grp2 = client
        .post(
            "/v1/queues/test_fifo_lock_q/messages",
            &serde_json::json!({ "message": { "n": 3 }, "group_id": "grp2" }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    let grp2_id = grp2["id"].as_i64().unwrap();

    // read grp1's first message with long vt — puts grp1 in-flight
    let m = client
        .get("/v1/queues/test_fifo_lock_q/messages?max=1&wait=0&vt=60&fifo=true")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let first = m.as_array().unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0]["message"]["n"], 1, "should read grp1 first");

    // grp1 is locked (in-flight) — next read must skip it and deliver grp2
    let m2 = client
        .get("/v1/queues/test_fifo_lock_q/messages?max=1&wait=0&vt=1&fifo=true")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let second = m2.as_array().unwrap();
    assert_eq!(second.len(), 1, "should deliver grp2 while grp1 is locked");
    assert_eq!(second[0]["id"], grp2_id);
    assert_eq!(second[0]["message"]["n"], 3);
}

#[tokio::test]
async fn test_fifo_send_batch() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_fifo_batch_q", "fifo": true }),
        )
        .await
        .assert_status(201);

    let resp = client
        .post(
            "/v1/queues/test_fifo_batch_q/messages",
            &serde_json::json!([
                { "message": { "n": 1 }, "group_id": "a" },
                { "message": { "n": 2 }, "group_id": "a" },
                { "message": { "n": 3 }, "group_id": "b" },
            ]),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();

    let ids = resp["ids"].as_array().expect("expected ids array");
    assert_eq!(ids.len(), 3);
    let id_set: std::collections::HashSet<i64> = ids.iter().map(|v| v.as_i64().unwrap()).collect();
    assert_eq!(id_set.len(), 3, "all ids must be distinct");
}

#[tokio::test]
async fn test_fifo_deduplication() {
    let _ = test_env();
    let client = TestClient::new();

    // Create FIFO queue via SQS JSON
    let create = client
        .sqs(
            "CreateQueue",
            &serde_json::json!({
                "QueueName": "test_fifo_dedup_q.fifo",
                "Attributes": { "FifoQueue": "true" }
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let queue_url = create["QueueUrl"].as_str().expect("QueueUrl").to_string();

    // Send first message with a deduplication ID
    let send1 = client
        .sqs(
            "SendMessage",
            &serde_json::json!({
                "QueueUrl": queue_url,
                "MessageBody": r#"{"n":1}"#,
                "MessageGroupId": "grp",
                "MessageDeduplicationId": "dedup-001"
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let msg_id1 = send1["MessageId"].as_str().expect("MessageId").to_string();

    // Send a duplicate — same group + dedup ID
    let send2 = client
        .sqs(
            "SendMessage",
            &serde_json::json!({
                "QueueUrl": queue_url,
                "MessageBody": r#"{"n":2}"#,
                "MessageGroupId": "grp",
                "MessageDeduplicationId": "dedup-001"
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let msg_id2 = send2["MessageId"].as_str().expect("MessageId").to_string();

    // SQS spec: duplicate sends must return the same MessageId.
    // ON CONFLICT DO UPDATE in send_fifo returns the original row's msg_id.
    assert_eq!(
        msg_id1, msg_id2,
        "duplicate deduplication_id must yield the same MessageId"
    );

    // Only 1 message must be visible in the queue
    let recv = client
        .sqs(
            "ReceiveMessage",
            &serde_json::json!({
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": 10,
                "WaitTimeSeconds": 0,
                "VisibilityTimeout": 30
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let msgs = recv["Messages"]
        .as_array()
        .map(|v| v.as_slice())
        .unwrap_or(&[]);
    assert_eq!(msgs.len(), 1, "deduplicated send must appear exactly once");
}

#[tokio::test]
async fn test_fifo_concurrent_producers_ordered_delivery() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_fifo_conc_q", "fifo": true }),
        )
        .await
        .assert_status(201);

    let env = test_env();
    let base = env.url.clone();

    // 3 concurrent producers, 5 messages each, all to group "g1"
    let mut tasks = tokio::task::JoinSet::new();
    for producer in 0..3usize {
        let b = base.clone();
        let http = reqwest::Client::new();
        tasks.spawn(async move {
            let mut ids = Vec::new();
            for i in 0..5usize {
                let resp: serde_json::Value = http
                    .post(format!("{b}/v1/queues/test_fifo_conc_q/messages"))
                    .header(reqwest::header::AUTHORIZATION, "Bearer test")
                    .json(&serde_json::json!({
                        "message": { "producer": producer, "i": i },
                        "group_id": "g1"
                    }))
                    .send()
                    .await
                    .expect("send")
                    .json()
                    .await
                    .expect("json");
                ids.push(resp["id"].as_i64().expect("id"));
            }
            ids
        });
    }

    let mut all_sent_ids: Vec<i64> = Vec::new();
    while let Some(res) = tasks.join_next().await {
        all_sent_ids.extend(res.expect("producer task"));
    }
    all_sent_ids.sort();
    assert_eq!(all_sent_ids.len(), 15, "all 15 sends must succeed");

    // Receive all 15 one by one — FIFO within group must preserve msg_id ASC order
    let mut prev_id = 0i64;
    for _ in 0..15 {
        let msgs = client
            .get("/v1/queues/test_fifo_conc_q/messages?max=1&wait=0&vt=30&fifo=true")
            .await
            .assert_status(200)
            .json::<serde_json::Value>();
        let arr = msgs.as_array().unwrap();
        assert_eq!(arr.len(), 1, "must receive one message per call");
        let id = arr[0]["id"].as_i64().unwrap();
        assert!(
            id > prev_id,
            "msg_id must be strictly ascending: {id} must be > {prev_id}"
        );
        prev_id = id;
        client
            .delete(&format!("/v1/queues/test_fifo_conc_q/messages/{id}"))
            .await
            .assert_status(204);
    }
}
