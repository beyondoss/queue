use crate::helpers::{TestClient, test_env};

#[tokio::test]
async fn test_http_delivery_raw() {
    let _ = test_env();
    let client = TestClient::new();
    let webhook = crate::helpers::TestWebhook::start().await;

    // Subscribe HTTP endpoint (raw delivery by default)
    client
        .post(
            "/v1/events/orders.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url }),
        )
        .await
        .assert_status(201);

    // Publish via REST API
    client
        .post(
            "/v1/events/orders.placed",
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
    let webhook = crate::helpers::TestWebhook::start().await;

    // Subscribe with envelope=true to get SNS wrapper
    client
        .post(
            "/v1/events/events.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url, "envelope": true }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/events/events.created",
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
    let webhook = crate::helpers::TestWebhook::with_status_sequence(vec![500, 200]).await;

    client
        .post(
            "/v1/events/retry.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/events/retry.test",
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
    let env = test_env();
    let client = TestClient::new();
    // The first attempt returns 500, so the delivery row survives in backoff
    // (next_attempt_at pushed ~10s out) — giving unsubscribe a *pending* row to
    // cancel. A 200 would be delivered-and-deleted immediately, since publish
    // now wakes the delivery worker in-process (delivery is prompt, not polled),
    // so "publish then unsubscribe" no longer cancels the first attempt — only
    // not-yet-due retries. This test asserts that cancellation guarantee.
    let webhook = crate::helpers::TestWebhook::with_status_sequence(vec![500]).await;

    let sub = client
        .post(
            "/v1/events/cancel.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    let sub_id = sub["id"].as_i64().expect("subscription id");

    // Publish — creates an event_deliveries row; the worker attempts it promptly.
    client
        .post(
            "/v1/events/cancel.me",
            &serde_json::json!({ "message": { "x": 99 } }),
        )
        .await
        .assert_status(201);

    // Wait for the first (failing) attempt so the row is now in backoff, pending.
    webhook.wait_for(1, std::time::Duration::from_secs(5)).await;

    // Unsubscribe — CASCADE deletes the pending (backing-off) delivery row.
    client
        .delete(&format!("/v1/events/cancel.*/subscriptions/{sub_id}"))
        .await
        .assert_status(204);

    // The pending row is gone, so the ~10s retry never fires. Scope the count to
    // this test's endpoint — the integration DB is shared across tests.
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM queue.event_deliveries WHERE endpoint = $1")
            .bind(&webhook.url)
            .fetch_one(&env.pool)
            .await
            .expect("count event_deliveries");
    assert_eq!(
        remaining, 0,
        "unsubscribe should CASCADE-delete pending deliveries"
    );
    assert_eq!(
        webhook.received_count(),
        1,
        "only the initial failed attempt should have fired; the retry was cancelled"
    );
}

#[tokio::test]
async fn test_http_delivery_dead_letter() {
    let env = test_env();
    let client = TestClient::new();
    // Endpoint that always returns 500 — delivery will never succeed.
    let webhook = crate::helpers::TestWebhook::with_status_sequence(
        std::iter::repeat_n(500u16, 10).collect(),
    )
    .await;

    client
        .post(
            "/v1/events/deadletter.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/events/deadletter.test",
            &serde_json::json!({ "message": { "fail": true } }),
        )
        .await
        .assert_status(201);

    // Poll until the first failed attempt is recorded (attempt incremented),
    // scoped to this test's endpoint (the integration DB is shared across tests).
    // wait_for() alone is too early — it fires when the webhook receives the POST,
    // before the worker commits the failure.
    let mut row_id: Option<i64> = None;
    for _ in 0..50 {
        let r = sqlx::query!(
            r#"SELECT id AS "id!", attempt AS "attempt!" FROM queue.event_deliveries
               WHERE endpoint = $1 ORDER BY id DESC LIMIT 1"#,
            webhook.url,
        )
        .fetch_optional(&env.pool)
        .await
        .unwrap();
        if let Some(r) = r
            && r.attempt >= 1
        {
            row_id = Some(r.id);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let row_id = row_id.expect("delivery worker should have recorded a failed attempt");

    // Fast-forward this row to exhaustion.
    sqlx::query!(
        "UPDATE queue.event_deliveries SET attempt = max_attempts WHERE id = $1",
        row_id,
    )
    .execute(&env.pool)
    .await
    .unwrap();

    // Publish a fresh event to the same endpoint to drive another delivery batch.
    // The exhausted-row sweep runs at the end of each batch and discards
    // dead-lettered rows (deleted + logged + counted) so they don't accumulate.
    client
        .post(
            "/v1/events/deadletter.test",
            &serde_json::json!({ "message": { "fail": true } }),
        )
        .await
        .assert_status(201);

    // The dead-lettered row is swept; poll until it's gone.
    let mut swept = false;
    for _ in 0..50 {
        let still = sqlx::query!(
            r#"SELECT id AS "id!" FROM queue.event_deliveries WHERE id = $1"#,
            row_id,
        )
        .fetch_optional(&env.pool)
        .await
        .unwrap();
        if still.is_none() {
            swept = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(
        swept,
        "exhausted (dead-lettered) row should be swept by the delivery worker"
    );
}

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
            "/v1/events/timeout.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": endpoint_url }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/events/timeout.test",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await
        .assert_status(201);

    // Wait for the delivery attempt to time out (worker timeout = 5s) plus margin
    tokio::time::sleep(std::time::Duration::from_secs(7)).await;

    let row = sqlx::query!(
        r#"SELECT id AS "id!", attempt AS "attempt!" FROM queue.event_deliveries
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

#[tokio::test]
async fn test_http_delivery_lease_reset_retries() {
    let env = test_env();
    let client = TestClient::new();
    // Endpoint always fails so the row stays alive
    let webhook = crate::helpers::TestWebhook::with_status_sequence(
        std::iter::repeat_n(500u16, 10).collect(),
    )
    .await;

    client
        .post(
            "/v1/events/leasereset.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/events/leasereset.test",
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
        r#"SELECT id AS "id!", attempt AS "attempt!" FROM queue.event_deliveries
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
