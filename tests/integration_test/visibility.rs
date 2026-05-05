use crate::helpers::{TestClient, test_env};

#[tokio::test]
async fn test_change_visibility() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_vt_q" }))
        .await
        .assert_status(201);

    let send = client
        .post(
            "/v1/queues/test_vt_q/messages",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    let msg_id = send["id"].as_i64().unwrap();

    // hide it for 60s
    client
        .patch(
            &format!("/v1/queues/test_vt_q/messages/{msg_id}"),
            &serde_json::json!({ "vt": 60 }),
        )
        .await
        .assert_status(200);

    // not visible
    let empty = client
        .get("/v1/queues/test_vt_q/messages?max=1&wait=0&vt=1")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(empty.as_array().unwrap().len(), 0, "should be hidden");

    // reveal it
    client
        .patch(
            &format!("/v1/queues/test_vt_q/messages/{msg_id}"),
            &serde_json::json!({ "vt": 0 }),
        )
        .await
        .assert_status(200);

    let visible = client
        .get("/v1/queues/test_vt_q/messages?max=1&wait=0&vt=0")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(visible.as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_visibility_timeout_expiry_redelivers() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_vt_expiry_q" }),
        )
        .await
        .assert_status(201);
    let send = client
        .post(
            "/v1/queues/test_vt_expiry_q/messages",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    let msg_id = send["id"].as_i64().unwrap();

    // Lock with 1-second vt
    let first = client
        .get("/v1/queues/test_vt_expiry_q/messages?max=1&wait=0&vt=1")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(first.as_array().unwrap().len(), 1);
    assert_eq!(first[0]["id"], msg_id);

    // Wait for the lock to expire
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Must be re-receivable — core at-least-once guarantee
    let second = client
        .get("/v1/queues/test_vt_expiry_q/messages?max=1&wait=0&vt=30")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let msgs = second.as_array().unwrap();
    assert_eq!(msgs.len(), 1, "message must reappear after vt expires");
    assert_eq!(msgs[0]["id"], msg_id, "must be the same message");
}

#[tokio::test]
async fn test_long_poll_wait_receives_message() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_longpoll_q" }),
        )
        .await
        .assert_status(201);

    let env = test_env();
    let base = env.url.clone();
    let http = reqwest::Client::new();

    // Start a long-poll receive (5-second wait) in the background
    let recv_task = tokio::spawn(async move {
        http.get(format!(
            "{base}/v1/queues/test_longpoll_q/messages?max=1&wait=5&vt=30"
        ))
        .header(reqwest::header::AUTHORIZATION, "Bearer test")
        .send()
        .await
        .expect("long-poll GET")
        .json::<serde_json::Value>()
        .await
        .expect("json")
    });

    // Producer fires after 300ms — queue was empty when the receive started
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    client
        .post(
            "/v1/queues/test_longpoll_q/messages",
            &serde_json::json!({ "message": { "wake": "up" } }),
        )
        .await
        .assert_status(201);

    let msgs = tokio::time::timeout(std::time::Duration::from_secs(6), recv_task)
        .await
        .expect("long-poll did not complete within timeout")
        .expect("task panicked");

    let arr = msgs.as_array().expect("expected array");
    assert_eq!(
        arr.len(),
        1,
        "long-poll must return the message sent mid-wait"
    );
    assert_eq!(arr[0]["message"]["wake"], "up");
}

#[tokio::test]
async fn test_sqs_change_visibility_batch() {
    let _ = test_env();
    let client = TestClient::new();

    // Create queue and send two messages
    let create = client
        .sqs(
            "CreateQueue",
            &serde_json::json!({ "QueueName": "test_cmvb_q" }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let queue_url = create["QueueUrl"].as_str().expect("QueueUrl").to_string();

    client
        .sqs(
            "SendMessage",
            &serde_json::json!({ "QueueUrl": queue_url, "MessageBody": "msg1" }),
        )
        .await
        .assert_status(200);
    client
        .sqs(
            "SendMessage",
            &serde_json::json!({ "QueueUrl": queue_url, "MessageBody": "msg2" }),
        )
        .await
        .assert_status(200);

    // Receive both with a long vt (locked)
    let recv = client
        .sqs(
            "ReceiveMessage",
            &serde_json::json!({
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": 2,
                "WaitTimeSeconds": 0,
                "VisibilityTimeout": 60
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let msgs = recv["Messages"].as_array().expect("Messages");
    assert_eq!(msgs.len(), 2);

    // Build batch entries that reset vt to 0 (immediately visible)
    let entries: Vec<serde_json::Value> = msgs
        .iter()
        .enumerate()
        .map(|(i, m)| {
            serde_json::json!({
                "Id": format!("e{i}"),
                "ReceiptHandle": m["ReceiptHandle"],
                "VisibilityTimeout": 0
            })
        })
        .collect();

    let batch = client
        .sqs(
            "ChangeMessageVisibilityBatch",
            &serde_json::json!({ "QueueUrl": queue_url, "Entries": entries }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let successful = batch["Successful"].as_array().expect("Successful");
    assert_eq!(successful.len(), 2, "both entries must succeed");

    // Both messages must be receivable again immediately
    let after = client
        .sqs(
            "ReceiveMessage",
            &serde_json::json!({
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": 2,
                "WaitTimeSeconds": 0,
                "VisibilityTimeout": 30
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let after_msgs = after["Messages"]
        .as_array()
        .map(|v| v.as_slice())
        .unwrap_or(&[]);
    assert_eq!(
        after_msgs.len(),
        2,
        "both messages must be visible after batch vt reset"
    );
}

#[tokio::test]
async fn test_change_visibility_missing_message_returns_404() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_cv_miss_q" }),
        )
        .await
        .assert_status(201);

    let res = client
        .patch(
            "/v1/queues/test_cv_miss_q/messages/99999999",
            &serde_json::json!({ "vt": 60 }),
        )
        .await;
    res.assert_status(404);
}
