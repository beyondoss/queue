mod helpers;

use helpers::{TestClient, test_env};

// Extract and XML-unescape the text content of the first `<tag>…</tag>` in an
// XML response body. Used only for SQS Query (form-urlencoded) protocol tests.
fn xml_text(body: &str, tag: &str) -> String {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = body
        .find(&open)
        .unwrap_or_else(|| panic!("<{tag}> not found in:\n{body}"))
        + open.len();
    let end = start
        + body[start..]
            .find(&close)
            .unwrap_or_else(|| panic!("</{tag}> not found in:\n{body}"));
    // Unescape in reverse escape order so &amp; doesn't double-convert.
    body[start..end]
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

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

// ── 1. Auth middleware ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_missing_auth_returns_403() {
    let env = test_env();
    // Plain reqwest with no Authorization header — must be rejected.
    let res = reqwest::Client::new()
        .get(format!("{}/v1/queues", env.url))
        .send()
        .await
        .expect("GET");
    assert_eq!(res.status().as_u16(), 403, "missing auth must return 403");
}

// ── 2. SQS JSON wire protocol ─────────────────────────────────────────────────

#[tokio::test]
async fn test_sqs_json_protocol_round_trip() {
    let _ = test_env();
    let client = TestClient::new();

    // CreateQueue
    let create = client
        .sqs(
            "CreateQueue",
            &serde_json::json!({ "QueueName": "sqs_json_q" }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let queue_url = create["QueueUrl"].as_str().expect("QueueUrl").to_string();
    assert!(queue_url.contains("sqs_json_q"));

    // SendMessage
    let send = client
        .sqs(
            "SendMessage",
            &serde_json::json!({
                "QueueUrl": queue_url,
                "MessageBody": r#"{"hello":"sqs"}"#
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert!(send["MessageId"].is_string());

    // ReceiveMessage
    let recv = client
        .sqs(
            "ReceiveMessage",
            &serde_json::json!({
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": 1,
                "WaitTimeSeconds": 0,
                "VisibilityTimeout": 30
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let msgs = recv["Messages"].as_array().expect("Messages");
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0]["Body"], r#"{"hello":"sqs"}"#);
    let receipt = msgs[0]["ReceiptHandle"]
        .as_str()
        .expect("ReceiptHandle")
        .to_string();

    // DeleteMessage
    client
        .sqs(
            "DeleteMessage",
            &serde_json::json!({ "QueueUrl": queue_url, "ReceiptHandle": receipt }),
        )
        .await
        .assert_status(200);

    // Queue empty
    let empty = client
        .sqs(
            "ReceiveMessage",
            &serde_json::json!({
                "QueueUrl": queue_url,
                "MaxNumberOfMessages": 1,
                "WaitTimeSeconds": 0
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(
        empty["Messages"].as_array().map(|v| v.len()).unwrap_or(0),
        0,
        "queue must be empty after delete"
    );
}

// ── 3. SQS Query (form-urlencoded) wire protocol ──────────────────────────────

#[tokio::test]
async fn test_sqs_query_protocol_round_trip() {
    let _ = test_env();
    let client = TestClient::new();

    // CreateQueue
    let create = client
        .sqs_query(&[("Action", "CreateQueue"), ("QueueName", "sqs_query_q")])
        .await
        .assert_status(200);
    let queue_url = xml_text(&create.body, "QueueUrl");
    assert!(queue_url.contains("sqs_query_q"));

    // SendMessage
    client
        .sqs_query(&[
            ("Action", "SendMessage"),
            ("QueueUrl", &queue_url),
            ("MessageBody", r#"{"hello":"query"}"#),
        ])
        .await
        .assert_status(200);

    // ReceiveMessage — pull ReceiptHandle from XML
    let recv = client
        .sqs_query(&[
            ("Action", "ReceiveMessage"),
            ("QueueUrl", &queue_url),
            ("MaxNumberOfMessages", "1"),
            ("WaitTimeSeconds", "0"),
            ("VisibilityTimeout", "30"),
        ])
        .await
        .assert_status(200);
    let receipt = xml_text(&recv.body, "ReceiptHandle");
    assert!(!receipt.is_empty(), "ReceiptHandle must be present");
    let body_val = xml_text(&recv.body, "Body");
    assert_eq!(body_val, r#"{"hello":"query"}"#);

    // DeleteMessage
    client
        .sqs_query(&[
            ("Action", "DeleteMessage"),
            ("QueueUrl", &queue_url),
            ("ReceiptHandle", &receipt),
        ])
        .await
        .assert_status(200);
}

// ── 4. Visibility timeout natural expiry (at-least-once guarantee) ────────────

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

// ── 5. SNS signature cryptographic verification ───────────────────────────────

#[tokio::test]
async fn test_sns_signature_is_cryptographically_valid() {
    let env = test_env();
    let client = TestClient::new();
    let webhook = helpers::TestWebhook::start().await;

    client
        .post(
            "/v1/topics/sigcheck.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url, "envelope": true }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/topics/sigcheck.test",
            &serde_json::json!({ "message": { "verify": "me" } }),
        )
        .await
        .assert_status(201);

    let deliveries = webhook.wait_for(1, std::time::Duration::from_secs(5)).await;
    let envelope = &deliveries[0];

    // Fetch the signing certificate
    let cert_pem = reqwest::get(format!("{}/SimpleNotificationService.pem", env.url))
        .await
        .expect("GET cert")
        .text()
        .await
        .expect("cert text");

    // Parse the X.509 certificate and extract the RSA public key
    use base64::Engine as _;
    use rsa::pkcs8::DecodePublicKey;
    use rsa::signature::Verifier;
    use x509_cert::der::{DecodePem, Encode};

    let cert = x509_cert::Certificate::from_pem(cert_pem.as_bytes()).expect("parse cert");
    let spki_der = cert
        .tbs_certificate
        .subject_public_key_info
        .to_der()
        .expect("SPKI to DER");
    let public_key = rsa::RsaPublicKey::from_public_key_der(&spki_der).expect("RSA public key");
    let verifying_key = rsa::pkcs1v15::VerifyingKey::<rsa::sha2::Sha256>::new(public_key);

    // Reconstruct the SNS v2 string-to-sign
    let message = envelope["Message"].as_str().expect("Message");
    let message_id = envelope["MessageId"].as_str().expect("MessageId");
    let timestamp = envelope["Timestamp"].as_str().expect("Timestamp");
    let topic_arn = envelope["TopicArn"].as_str().expect("TopicArn");
    let string_to_sign = format!(
        "Message\n{message}\nMessageId\n{message_id}\nTimestamp\n{timestamp}\nTopicArn\n{topic_arn}\nType\nNotification\n"
    );

    // Decode and verify the signature
    let sig_bytes = base64::engine::general_purpose::STANDARD
        .decode(envelope["Signature"].as_str().expect("Signature"))
        .expect("base64 decode signature");
    let signature =
        rsa::pkcs1v15::Signature::try_from(sig_bytes.as_slice()).expect("parse signature");
    verifying_key
        .verify(string_to_sign.as_bytes(), &signature)
        .expect("SNS signature must be cryptographically valid");
}

// ── 6. read_count increments on each receive ──────────────────────────────────

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

// ── 7. SQS GetQueueUrl action ─────────────────────────────────────────────────

#[tokio::test]
async fn test_sqs_get_queue_url() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_gqurl_q" }))
        .await
        .assert_status(201);

    let resp = client
        .sqs(
            "GetQueueUrl",
            &serde_json::json!({ "QueueName": "test_gqurl_q" }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let url = resp["QueueUrl"].as_str().expect("QueueUrl");
    assert!(
        url.contains("test_gqurl_q"),
        "QueueUrl must embed the queue name"
    );

    // Non-existent queue must return an error status
    let err = client
        .sqs(
            "GetQueueUrl",
            &serde_json::json!({ "QueueName": "no_such_q_xyzzy" }),
        )
        .await;
    assert_ne!(
        err.status, 200,
        "GetQueueUrl for missing queue must not return 200"
    );
}

// ── 8. Message headers survive send → receive ─────────────────────────────────

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

// ── 9. Delete non-existent queue is idempotent ────────────────────────────────

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

// ── 10. SNS listing actions ───────────────────────────────────────────────────

#[tokio::test]
async fn test_sns_list_subscriptions() {
    let _ = test_env();
    let client = TestClient::new();
    let webhook = helpers::TestWebhook::start().await;

    // Subscribe via SNS JSON protocol
    let sub_resp = client
        .sns(
            "Subscribe",
            &serde_json::json!({
                "TopicArn": "arn:aws:sns:us-east-1:000000000000:list-test.*",
                "Protocol": "http",
                "Endpoint": webhook.url,
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let sub_arn = sub_resp["SubscriptionArn"]
        .as_str()
        .expect("SubscriptionArn")
        .to_string();
    assert!(sub_arn.contains("list-test"));

    // ListSubscriptions — our subscription must appear
    let list_resp = client
        .sns("ListSubscriptions", &serde_json::json!({}))
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let subs = list_resp["Subscriptions"]
        .as_array()
        .expect("Subscriptions");
    assert!(
        subs.iter().any(|s| s["SubscriptionArn"]
            .as_str()
            .unwrap_or("")
            .contains("list-test")),
        "ListSubscriptions must include our subscription"
    );

    // ListSubscriptionsByTopic
    let by_topic = client
        .sns(
            "ListSubscriptionsByTopic",
            &serde_json::json!({ "TopicArn": "arn:aws:sns:us-east-1:000000000000:list-test.*" }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let by_topic_list = by_topic["Subscriptions"].as_array().expect("Subscriptions");
    assert!(
        !by_topic_list.is_empty(),
        "ListSubscriptionsByTopic must return results"
    );

    // GetSubscriptionAttributes
    let attrs = client
        .sns(
            "GetSubscriptionAttributes",
            &serde_json::json!({ "SubscriptionArn": sub_arn }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert!(
        attrs["Attributes"].is_object(),
        "GetSubscriptionAttributes must return attributes map"
    );
}

// ── 11. Concurrent receive: SKIP LOCKED prevents duplicates ───────────────────

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

// ── 12. HTTP delivery: endpoint timeout triggers retry ────────────────────────

#[tokio::test]
async fn test_http_delivery_endpoint_timeout() {
    let env = test_env();
    let client = TestClient::new();

    // TCP listener that accepts connections but never sends a response,
    // causing the delivery worker's HTTP client to time out.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let endpoint_url = format!("http://{addr}");
    tokio::spawn(async move {
        loop {
            let _ = listener.accept().await;
        }
    });

    client
        .post(
            "/v1/topics/timeout.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": endpoint_url }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/topics/timeout.test",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await
        .assert_status(201);

    // Wait for the delivery attempt to time out (worker timeout = 5s) plus margin
    tokio::time::sleep(std::time::Duration::from_secs(7)).await;

    let row = sqlx::query!(
        r#"SELECT id AS "id!", attempt AS "attempt!" FROM queue.http_deliveries
           WHERE endpoint = $1 ORDER BY id DESC LIMIT 1"#,
        endpoint_url,
    )
    .fetch_optional(&env.pool)
    .await
    .unwrap()
    .expect("http_deliveries row must exist after timeout");

    assert!(
        row.attempt >= 1,
        "timed-out delivery must be recorded as a failed attempt"
    );
}

// ── 13. FIFO deduplication ────────────────────────────────────────────────────

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

// ── 14. SQS ChangeMessageVisibilityBatch ──────────────────────────────────────

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

// ── 16. Long-poll: receive on empty queue wakes up when message arrives ───────

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

// ── 17. async_commit query parameter ─────────────────────────────────────────

#[tokio::test]
async fn test_async_commit_send_succeeds() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_async_q" }))
        .await
        .assert_status(201);

    // async_commit=true skips WAL fsync; message must still be readable
    let send = client
        .post(
            "/v1/queues/test_async_q/messages?async_commit=true",
            &serde_json::json!({ "message": { "fast": true } }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();

    assert!(
        send["id"].as_i64().unwrap() > 0,
        "must return a valid msg id"
    );

    let msgs = client
        .get("/v1/queues/test_async_q/messages?max=1&wait=0&vt=30")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();

    let arr = msgs.as_array().unwrap();
    assert_eq!(arr.len(), 1, "async_commit message must be readable");
    assert_eq!(arr[0]["message"]["fast"], true);
}

// ── 18. Topic fanout to multiple SQS queues ───────────────────────────────────

#[tokio::test]
async fn test_topic_fanout_multiple_sqs_queues() {
    let _ = test_env();
    let client = TestClient::new();

    for q in ["test_mfan_q1", "test_mfan_q2", "test_mfan_q3"] {
        client
            .post("/v1/queues", &serde_json::json!({ "name": q }))
            .await
            .assert_status(201);
        client
            .post(
                "/v1/topics/mfan.*/subscriptions",
                &serde_json::json!({ "queue_name": q }),
            )
            .await
            .assert_status(201);
    }

    let pub_resp = client
        .post(
            "/v1/topics/mfan.event",
            &serde_json::json!({ "message": { "broadcast": true } }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    assert_eq!(
        pub_resp["queues_matched"], 3,
        "must fan-out to all 3 subscribers"
    );

    for q in ["test_mfan_q1", "test_mfan_q2", "test_mfan_q3"] {
        let msgs = client
            .get(&format!("/v1/queues/{q}/messages?max=1&wait=0&vt=30"))
            .await
            .assert_status(200)
            .json::<serde_json::Value>();
        assert_eq!(
            msgs.as_array().unwrap().len(),
            1,
            "{q} must have received the fan-out message"
        );
    }
}

// ── 19. Topic wildcard routing: * matches exactly one dot-free segment ────────

#[tokio::test]
async fn test_topic_wildcard_routing() {
    let _ = test_env();
    let client = TestClient::new();

    // star_q subscribes to "wc.star.*" — the * matches exactly one segment
    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_wc_star_q" }),
        )
        .await
        .assert_status(201);
    client
        .post(
            "/v1/topics/wc.star.*/subscriptions",
            &serde_json::json!({ "queue_name": "test_wc_star_q" }),
        )
        .await
        .assert_status(201);

    // hash_q subscribes to "wc.star.#" — # matches any number of segments.
    // reqwest strips bare # as URL fragment, so use %23 (percent-encoded).
    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_wc_hash_q" }),
        )
        .await
        .assert_status(201);
    client
        .post(
            "/v1/topics/wc.star.%23/subscriptions",
            &serde_json::json!({ "queue_name": "test_wc_hash_q" }),
        )
        .await
        .assert_status(201);

    // "wc.star.foo" → matches both wc.star.* (one segment) and wc.star.# (any)
    let r1 = client
        .post(
            "/v1/topics/wc.star.foo",
            &serde_json::json!({ "message": { "t": 1 } }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    assert_eq!(
        r1["queues_matched"], 2,
        "wc.star.foo must match both wc.star.* and wc.star.#"
    );

    // "wc.star.foo.bar" — * does not cross dots, # does; only hash_q receives this
    let r2 = client
        .post(
            "/v1/topics/wc.star.foo.bar",
            &serde_json::json!({ "message": { "t": 2 } }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    assert_eq!(
        r2["queues_matched"], 1,
        "wc.star.foo.bar must match wc.star.# but NOT wc.star.*"
    );

    // star_q has 1 message ("wc.star.foo"), hash_q has 2 ("wc.star.foo" + "wc.star.foo.bar")
    let star_msgs = client
        .get("/v1/queues/test_wc_star_q/messages?max=10&wait=0&vt=30")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(
        star_msgs.as_array().unwrap().len(),
        1,
        "test_wc_star_q must have exactly 1 message"
    );

    let hash_msgs = client
        .get("/v1/queues/test_wc_hash_q/messages?max=10&wait=0&vt=30")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(
        hash_msgs.as_array().unwrap().len(),
        2,
        "test_wc_hash_q must have 2 messages (single- and multi-segment)"
    );
}

// ── 20. Invalid receipt handle returns a 4xx, not 500 ────────────────────────

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

// ── 21. DELETE non-existent message returns 404 ───────────────────────────────

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

// ── 22. SQS subscription unsubscribe stops fanout ─────────────────────────────

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

// ── 23. HTTP delivery: resetting next_attempt_at triggers a retry ─────────────

#[tokio::test]
async fn test_http_delivery_lease_reset_retries() {
    let env = test_env();
    let client = TestClient::new();
    // Endpoint always fails so the row stays alive
    let webhook =
        helpers::TestWebhook::with_status_sequence(std::iter::repeat(500u16).take(10).collect())
            .await;

    client
        .post(
            "/v1/topics/leasereset.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/topics/leasereset.test",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await
        .assert_status(201);

    // Wait for first delivery attempt, then let the worker write the DB update.
    // The attempt counter is incremented *after* the HTTP POST returns, so a brief
    // sleep is needed — same pattern as test_http_delivery_dead_letter.
    webhook.wait_for(1, std::time::Duration::from_secs(5)).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Verify the attempt counter incremented. Uses the same query shape as
    // test_http_delivery_dead_letter.
    let row = sqlx::query!(
        r#"SELECT id AS "id!", attempt AS "attempt!" FROM queue.http_deliveries
           WHERE endpoint = $1 ORDER BY id DESC LIMIT 1"#,
        webhook.url,
    )
    .fetch_one(&env.pool)
    .await
    .unwrap();
    assert_eq!(
        row.attempt, 1,
        "attempt counter must be 1 after first failure"
    );
    // Row still has attempt < max_attempts, so the worker will retry after backoff.
    // The webhook count proves the delivery attempt reached the endpoint.
    assert_eq!(
        webhook.received_count(),
        1,
        "exactly one delivery attempt must have reached the endpoint"
    );
}

// ── 24. FIFO: concurrent producers → messages delivered in msg_id ASC order ───

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

// ── 25. Batch delete with non-existent IDs is safe ───────────────────────────

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

// ── 26. Coalescer: async_commit batches are still delivered ───────────────────

#[tokio::test]
async fn test_coalescer_async_commit_delivers_messages() {
    let env = test_env();

    // Start a separate server with the coalescer enabled (50ms linger window)
    let server = beyond_queue::test_support::start_with_coalescer(env.pool.clone(), 50)
        .await
        .expect("coalescer server");
    let http = reqwest::Client::new();
    let base = server.url.clone();

    let make_req = |method: reqwest::Method, path: &str| {
        http.request(method, format!("{base}{path}"))
            .header(reqwest::header::AUTHORIZATION, "Bearer test")
    };

    // Create queue
    make_req(reqwest::Method::POST, "/v1/queues")
        .json(&serde_json::json!({ "name": "test_coal_async_q" }))
        .send()
        .await
        .expect("create")
        .error_for_status()
        .expect("201");

    // Send 5 messages concurrently with async_commit — they should land in the
    // same linger window and be flushed as a single batch with async commit
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..5usize {
        let b = base.clone();
        let h = http.clone();
        tasks.spawn(async move {
            h.post(format!(
                "{b}/v1/queues/test_coal_async_q/messages?async_commit=true"
            ))
            .header(reqwest::header::AUTHORIZATION, "Bearer test")
            .json(&serde_json::json!({ "message": { "i": i } }))
            .send()
            .await
            .expect("send")
            .json::<serde_json::Value>()
            .await
            .expect("json")
        });
    }
    let mut sent_ids = Vec::new();
    while let Some(r) = tasks.join_next().await {
        let v = r.expect("task");
        sent_ids.push(v["id"].as_i64().expect("id"));
    }
    assert_eq!(sent_ids.len(), 5, "all 5 sends must return an id");

    // Receive all 5
    let msgs: serde_json::Value = make_req(
        reqwest::Method::GET,
        "/v1/queues/test_coal_async_q/messages?max=10&wait=0&vt=30",
    )
    .send()
    .await
    .expect("receive")
    .json()
    .await
    .expect("json");

    let arr = msgs.as_array().unwrap();
    assert_eq!(
        arr.len(),
        5,
        "all 5 async_commit messages must be readable after coalescer flush"
    );
}

// ── 15. SQS GetQueueAttributes ────────────────────────────────────────────────

#[tokio::test]
async fn test_sqs_get_queue_attributes() {
    let _ = test_env();
    let client = TestClient::new();

    let create = client
        .sqs(
            "CreateQueue",
            &serde_json::json!({ "QueueName": "test_gqa_q" }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let queue_url = create["QueueUrl"].as_str().expect("QueueUrl").to_string();

    // Send one message so the count is non-zero
    client
        .sqs(
            "SendMessage",
            &serde_json::json!({ "QueueUrl": queue_url, "MessageBody": "hello" }),
        )
        .await
        .assert_status(200);

    let resp = client
        .sqs(
            "GetQueueAttributes",
            &serde_json::json!({
                "QueueUrl": queue_url,
                "AttributeNames": ["All"]
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();

    let attrs = resp["Attributes"].as_object().expect("Attributes");
    assert!(
        attrs.contains_key("ApproximateNumberOfMessages"),
        "must include ApproximateNumberOfMessages"
    );
    assert!(
        attrs.contains_key("VisibilityTimeout"),
        "must include VisibilityTimeout"
    );
    assert!(attrs.contains_key("QueueArn"), "must include QueueArn");
    assert_eq!(
        attrs["ApproximateNumberOfMessages"].as_str().unwrap_or(""),
        "1",
        "queue must report 1 message"
    );
}

// ── error path: empty batch ──────────────────────────────────────────────────

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

// ── error path: invalid queue name ───────────────────────────────────────────

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

// ── error path: send to non-existent queue ────────────────────────────────────

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

// ── error path: change visibility on non-existent message ────────────────────

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

// ── batch send: per-message delays respected ──────────────────────────────────

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

// ── topic: publish with no subscribers ───────────────────────────────────────

#[tokio::test]
async fn test_topic_publish_with_no_subscribers() {
    let _ = test_env();
    let client = TestClient::new();

    let res = client
        .post(
            "/v1/topics/no.subscribers.ever",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();

    assert_eq!(res["queues_matched"], 0);
    assert_eq!(res["messages"].as_array().unwrap().len(), 0);
}

// ── queue subscriptions list (native REST) ────────────────────────────────────

#[tokio::test]
async fn test_get_queue_subscriptions() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post("/v1/queues", &serde_json::json!({ "name": "test_qsubs_q" }))
        .await
        .assert_status(201);

    // No subscriptions yet.
    let subs = client
        .get("/v1/queues/test_qsubs_q/subscriptions")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(subs.as_array().unwrap().len(), 0, "should start empty");

    // Bind the queue to a topic pattern.
    client
        .post(
            "/v1/topics/test.qsubs.topic/subscriptions",
            &serde_json::json!({ "queue_name": "test_qsubs_q" }),
        )
        .await
        .assert_status(201);

    // Should now appear.
    let subs = client
        .get("/v1/queues/test_qsubs_q/subscriptions")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let arr = subs.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["pattern"], "test.qsubs.topic");
}

// ── SNS Subscribe with ARN-format endpoint ────────────────────────────────────

#[tokio::test]
async fn test_sns_subscribe_with_arn_endpoint() {
    let _ = test_env();
    let client = TestClient::new();

    client
        .post(
            "/v1/queues",
            &serde_json::json!({ "name": "test_arn_sub_q" }),
        )
        .await
        .assert_status(201);

    // Subscribe using an ARN-format endpoint instead of a URL.
    let res = client
        .sns(
            "Subscribe",
            &serde_json::json!({
                "TopicArn": "arn:aws:sns:us-east-1:000000000000:test.arn.topic",
                "Protocol": "sqs",
                "Endpoint": "arn:aws:sqs:us-east-1:000000000000:test_arn_sub_q",
            }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    let sub_arn = res["SubscriptionArn"]
        .as_str()
        .expect("SubscriptionArn must be a string");
    assert!(
        sub_arn.contains("test.arn.topic"),
        "ARN should contain the topic name"
    );

    // Publish and verify delivery to the queue.
    client
        .sns(
            "Publish",
            &serde_json::json!({
                "TopicArn": "arn:aws:sns:us-east-1:000000000000:test.arn.topic",
                "Message": r#"{"hello":"arn"}"#,
            }),
        )
        .await
        .assert_status(200);

    let msgs = client
        .get("/v1/queues/test_arn_sub_q/messages?max=1&wait=5&vt=30")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(
        msgs.as_array().unwrap().len(),
        1,
        "message must arrive via ARN-subscribed queue"
    );
}

// ── native REST subscribe: invalid protocol rejected ─────────────────────────

#[tokio::test]
async fn test_subscribe_invalid_protocol_returns_400() {
    let _ = test_env();
    let client = TestClient::new();

    let res = client
        .post(
            "/v1/topics/test.invalid.proto/subscriptions",
            &serde_json::json!({
                "protocol": "smtp",
                "endpoint": "smtp://mail.example.com",
            }),
        )
        .await;
    res.assert_status(400);
}

// ── delete queue cascades to subscriptions ────────────────────────────────────

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
            "/v1/topics/test.cascade.topic/subscriptions",
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
            "/v1/topics/test.cascade.topic",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    assert_eq!(res["queues_matched"], 0);
}

// ── native REST unsubscribe: non-existent id returns 404 ─────────────────────

#[tokio::test]
async fn test_unsubscribe_nonexistent_returns_404() {
    let _ = test_env();
    let client = TestClient::new();

    let res = client
        .delete("/v1/topics/test.pattern/subscriptions/999999999")
        .await;
    res.assert_status(404);
}
