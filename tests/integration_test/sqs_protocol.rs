use crate::helpers::{TestClient, test_env};

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
