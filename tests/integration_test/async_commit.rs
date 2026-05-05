use crate::helpers::test_env;

#[tokio::test]
async fn test_async_commit_send_succeeds() {
    let _ = test_env();
    let client = crate::helpers::TestClient::new();

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
