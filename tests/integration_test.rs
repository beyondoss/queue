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

    let body = client.get("/v1/queues").await.assert_status(200).json::<serde_json::Value>();
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

    client.delete("/v1/queues/test_drop_q").await.assert_status(204);
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
    assert_eq!(msgs.as_array().unwrap().len(), 0, "delayed message should not be visible");
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
        .post("/v1/queues", &serde_json::json!({ "name": "test_fifo_q", "fifo": true }))
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
        .post("/v1/queues", &serde_json::json!({ "name": "test_fifo_order_q", "fifo": true }))
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
        .post("/v1/queues", &serde_json::json!({ "name": "test_fifo_lock_q", "fifo": true }))
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
        .post("/v1/queues", &serde_json::json!({ "name": "test_fifo_batch_q", "fifo": true }))
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
    let id_set: std::collections::HashSet<i64> =
        ids.iter().map(|v| v.as_i64().unwrap()).collect();
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
