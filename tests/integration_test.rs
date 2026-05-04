mod helpers;

use helpers::{TestClient, test_env};

// ── queue lifecycle ───────────────────────────────────────────────────────────

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

// ── send / receive / delete round-trip ───────────────────────────────────────

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

// ── batch delete ─────────────────────────────────────────────────────────────

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

// ── visibility change ─────────────────────────────────────────────────────────

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

// ── purge ─────────────────────────────────────────────────────────────────────

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

// ── FIFO queue ────────────────────────────────────────────────────────────────

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

// ── healthz ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_healthz() {
    let _ = test_env();
    // healthz bypasses auth middleware
    let env = test_env();
    let res = reqwest::get(format!("{}/healthz", env.url))
        .await
        .expect("GET /healthz");
    assert_eq!(res.status().as_u16(), 200);
}

// ── HTTP webhook delivery ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_http_delivery_raw() {
    let _ = test_env();
    let client = TestClient::new();
    let webhook = helpers::TestWebhook::start().await;

    // Subscribe HTTP endpoint (raw delivery by default)
    client
        .post(
            "/v1/topics/orders.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url }),
        )
        .await
        .assert_status(201);

    // Publish via REST API
    client
        .post(
            "/v1/topics/orders.placed",
            &serde_json::json!({ "message": { "id": 42 } }),
        )
        .await
        .assert_status(201);

    let deliveries = webhook.wait_for(1, std::time::Duration::from_secs(5)).await;
    // raw_delivery=true → raw payload posted directly (not SNS envelope)
    assert_eq!(deliveries[0], serde_json::json!({ "id": 42 }));
}

#[tokio::test]
async fn test_http_delivery_envelope() {
    let _ = test_env();
    let client = TestClient::new();
    let webhook = helpers::TestWebhook::start().await;

    // Subscribe with envelope=true to get SNS wrapper
    client
        .post(
            "/v1/topics/events.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url, "envelope": true }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/topics/events.created",
            &serde_json::json!({ "message": { "type": "created" } }),
        )
        .await
        .assert_status(201);

    let deliveries = webhook.wait_for(1, std::time::Duration::from_secs(5)).await;
    let msg = &deliveries[0];
    assert_eq!(msg["Type"], "Notification");
    assert!(msg["MessageId"].is_string());
    assert!(msg["Signature"].is_string());
    assert!(!msg["Signature"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn test_http_delivery_retry_on_failure() {
    let _ = test_env();
    let client = TestClient::new();
    // First request returns 500, second returns 200.
    // Backoff after attempt 1 is 10s, so the retry arrives ~10s later.
    let webhook = helpers::TestWebhook::with_status_sequence(vec![500, 200]).await;

    client
        .post(
            "/v1/topics/retry.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/topics/retry.test",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await
        .assert_status(201);

    // Wait for 2 deliveries: the initial failure (500) and the successful retry (200).
    // Backoff after attempt 1 is 10s; allow 15s total.
    let deliveries = webhook
        .wait_for(2, std::time::Duration::from_secs(15))
        .await;
    assert_eq!(deliveries.len(), 2);
    // Both attempts carry the same payload
    assert_eq!(deliveries[1], serde_json::json!({ "x": 1 }));
}

#[tokio::test]
async fn test_http_delivery_unsubscribe_cancels_pending() {
    let _ = test_env();
    let client = TestClient::new();
    let webhook = helpers::TestWebhook::start().await;

    // Subscribe and get the subscription id from the response
    let sub = client
        .post(
            "/v1/topics/cancel.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    let sub_id = sub["id"].as_i64().expect("subscription id");

    // Publish — creates an http_deliveries row
    client
        .post(
            "/v1/topics/cancel.me",
            &serde_json::json!({ "message": { "x": 99 } }),
        )
        .await
        .assert_status(201);

    // Unsubscribe — CASCADE deletes http_deliveries rows
    client
        .delete(&format!("/v1/topics/cancel.*/subscriptions/{sub_id}"))
        .await
        .assert_status(204);

    // Give worker a moment to see the empty table — delivery should NOT arrive
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        webhook.received_count(),
        0,
        "delivery should have been cancelled"
    );
}

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
            "/v1/topics/sqs.fanout.*/subscriptions",
            &serde_json::json!({ "queue_name": "test_http_sqs_fanout_q" }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/topics/sqs.fanout.event",
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
async fn test_http_delivery_dead_letter() {
    let env = test_env();
    let client = TestClient::new();
    // Endpoint that always returns 500 — delivery will never succeed.
    let webhook =
        helpers::TestWebhook::with_status_sequence(std::iter::repeat(500u16).take(10).collect())
            .await;

    client
        .post(
            "/v1/topics/deadletter.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/topics/deadletter.test",
            &serde_json::json!({ "message": { "fail": true } }),
        )
        .await
        .assert_status(201);

    // Wait for the first delivery attempt to be recorded.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // The row should exist with attempt >= 1.
    let row = sqlx::query!(
        r#"SELECT id AS "id!", attempt AS "attempt!" FROM queue.http_deliveries
           WHERE endpoint = $1 ORDER BY id DESC LIMIT 1"#,
        webhook.url,
    )
    .fetch_optional(&env.pool)
    .await
    .unwrap()
    .expect("expected an http_deliveries row after first attempt");
    assert!(
        row.attempt >= 1,
        "delivery worker should have attempted at least once"
    );

    // Fast-forward: set attempt = max_attempts to simulate exhaustion.
    sqlx::query!(
        "UPDATE queue.http_deliveries SET attempt = max_attempts WHERE id = $1",
        row.id,
    )
    .execute(&env.pool)
    .await
    .unwrap();

    // Give the worker a couple of poll cycles.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    // Row must still exist — exhausted rows are retained for inspection, not deleted.
    let still_there = sqlx::query!(
        r#"SELECT id AS "id!" FROM queue.http_deliveries WHERE id = $1"#,
        row.id,
    )
    .fetch_optional(&env.pool)
    .await
    .unwrap();
    assert!(
        still_there.is_some(),
        "dead-lettered row must remain for inspection"
    );
}

#[tokio::test]
async fn test_sns_subscribe_http_and_publish() {
    let _ = test_env();
    let client = TestClient::new();
    let webhook = helpers::TestWebhook::start().await;

    // Subscribe via SNS wire protocol (JSON). SNS subs default to envelope delivery (raw_delivery=false).
    let sub_resp = client
        .sns(
            "Subscribe",
            &serde_json::json!({
                "TopicArn": "arn:aws:sns:us-east-1:000000000000:sns-wh.*",
                "Protocol": "http",
                "Endpoint": webhook.url,
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert!(
        sub_resp["SubscriptionArn"]
            .as_str()
            .unwrap_or("")
            .contains("sns-wh"),
        "SubscriptionArn should reference the topic: {sub_resp}"
    );

    // Publish via SNS wire protocol.
    let pub_resp = client
        .sns(
            "Publish",
            &serde_json::json!({
                "TopicArn": "arn:aws:sns:us-east-1:000000000000:sns-wh.created",
                "Message": r#"{"event":"user_signed_up"}"#,
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert!(
        pub_resp["MessageId"].is_string(),
        "Publish must return a MessageId"
    );

    // The delivery worker should POST the SNS envelope to the webhook.
    let deliveries = webhook.wait_for(1, std::time::Duration::from_secs(5)).await;
    let msg = &deliveries[0];

    // Envelope fields required by the SNS spec.
    assert_eq!(msg["Type"], "Notification");
    assert!(msg["MessageId"].is_string());
    assert_eq!(
        msg["TopicArn"],
        "arn:aws:sns:us-east-1:000000000000:sns-wh.created"
    );
    assert_eq!(msg["Message"], r#"{"event":"user_signed_up"}"#);
    // RSA-2048 signature must be present and non-empty.
    assert!(
        msg["Signature"]
            .as_str()
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        "SNS envelope must carry a non-empty Signature"
    );
    assert_eq!(msg["SignatureVersion"], "2");
}
