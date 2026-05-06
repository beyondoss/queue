use crate::helpers::{TestClient, test_env};

#[tokio::test]
async fn test_sns_subscribe_http_and_publish() {
    let _ = test_env();
    let client = TestClient::new();
    let webhook = crate::helpers::TestWebhook::start().await;

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

#[tokio::test]
async fn test_sns_signature_is_cryptographically_valid() {
    let env = test_env();
    let client = TestClient::new();
    let webhook = crate::helpers::TestWebhook::start().await;

    client
        .post(
            "/v1/events/sigcheck.*/subscriptions",
            &serde_json::json!({ "protocol": "https", "endpoint": webhook.url, "envelope": true }),
        )
        .await
        .assert_status(201);

    client
        .post(
            "/v1/events/sigcheck.test",
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

#[tokio::test]
async fn test_sns_list_subscriptions() {
    let _ = test_env();
    let client = TestClient::new();
    let webhook = crate::helpers::TestWebhook::start().await;

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
                "/v1/events/mfan.*/subscriptions",
                &serde_json::json!({ "queue_name": q }),
            )
            .await
            .assert_status(201);
    }

    let pub_resp = client
        .post(
            "/v1/events/mfan.event",
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
            "/v1/events/wc.star.*/subscriptions",
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
            "/v1/events/wc.star.%23/subscriptions",
            &serde_json::json!({ "queue_name": "test_wc_hash_q" }),
        )
        .await
        .assert_status(201);

    // "wc.star.foo" → matches both wc.star.* (one segment) and wc.star.# (any)
    let r1 = client
        .post(
            "/v1/events/wc.star.foo",
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
            "/v1/events/wc.star.foo.bar",
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

#[tokio::test]
async fn test_topic_publish_with_no_subscribers() {
    let _ = test_env();
    let client = TestClient::new();

    let res = client
        .post(
            "/v1/events/no.subscribers.ever",
            &serde_json::json!({ "message": { "x": 1 } }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();

    assert_eq!(res["queues_matched"], 0);
    assert_eq!(res["messages"].as_array().unwrap().len(), 0);
}

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
            "/v1/events/test.qsubs.topic/subscriptions",
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
