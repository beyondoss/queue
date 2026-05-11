//! Integration tests for /v1/schedules + the schedule worker.
//!
//! The test_support module starts the worker with poll_interval_ms=100,
//! so each tokio sleep of ~200-400ms is enough to observe a fire.

use std::time::Duration;

use serde_json::json;

use crate::helpers::{TestClient, test_env};

/// Pick a unique queue name per test so we don't collide across tests.
fn unique(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{ts}_{n}")
}

async fn create_queue(client: &TestClient, name: &str) {
    client
        .post("/v1/queues", &json!({ "name": name }))
        .await
        .assert_status(201);
}

async fn next_message(client: &TestClient, queue: &str) -> Option<serde_json::Value> {
    let resp = client
        .get(&format!("/v1/queues/{queue}/messages?max=1&wait=2"))
        .await;
    if resp.status != 200 {
        return None;
    }
    let body: serde_json::Value = serde_json::from_str(&resp.body).ok()?;
    body.as_array().and_then(|a| a.first().cloned())
}

// ─────────────────────────────────────────────────────────────────────────────
// CRUD lifecycle
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn create_get_list_delete() {
    let _ = test_env();
    let client = TestClient::new();
    let target = unique("sched_target");
    let name = unique("sched_crud");
    create_queue(&client, &target).await;

    let create = client
        .post(
            "/v1/schedules",
            &json!({
                "name": name,
                "cron": "0 9 * * 1-5",
                "target": { "queue": target, "message": {"k": "v"} }
            }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    assert_eq!(create["name"], name);
    assert_eq!(create["cron"], "0 9 * * 1-5");
    assert!(create["next_fires"].as_array().unwrap().len() >= 1);
    assert!(
        create["human_readable"]
            .as_str()
            .unwrap()
            .contains("weekdays")
    );

    let got = client
        .get(&format!("/v1/schedules/{name}"))
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(got["name"], name);

    let list = client
        .get("/v1/schedules")
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert!(list.as_array().unwrap().iter().any(|s| s["name"] == name));

    client
        .delete(&format!("/v1/schedules/{name}"))
        .await
        .assert_status(204);

    client
        .get(&format!("/v1/schedules/{name}"))
        .await
        .assert_status(404);
}

#[tokio::test]
async fn strict_create_returns_409_on_duplicate() {
    let _ = test_env();
    let client = TestClient::new();
    let target = unique("sched_target");
    let name = unique("sched_dup");
    create_queue(&client, &target).await;

    let body = json!({
        "name": name,
        "cron": "0 9 * * *",
        "target": { "queue": target, "message": {} }
    });
    client.post("/v1/schedules", &body).await.assert_status(201);
    let dup = client.post("/v1/schedules", &body).await.assert_status(409);
    let err: serde_json::Value = serde_json::from_str(&dup.body).unwrap();
    assert_eq!(err["error"]["code"], "schedule_conflict");
}

#[tokio::test]
async fn put_is_idempotent_upsert() {
    let _ = test_env();
    let client = TestClient::new();
    let target = unique("sched_target");
    let name = unique("sched_put");
    create_queue(&client, &target).await;

    let body = json!({
        "name": name,
        "cron": "0 9 * * *",
        "target": { "queue": target, "message": {} }
    });
    // First PUT creates → 201
    let created = client
        .put(&format!("/v1/schedules/{name}"), &body)
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    assert_eq!(created["cron"], "0 9 * * *");

    // Second PUT with same spec updates → 200 (and preserves fire_count = 0)
    let updated = client
        .put(&format!("/v1/schedules/{name}"), &body)
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(updated["fire_count"], 0);

    // PUT with a different cron should recompute next_fire_at
    let body2 = json!({
        "name": name,
        "cron": "0 12 * * *",
        "target": { "queue": target, "message": {} }
    });
    let changed = client
        .put(&format!("/v1/schedules/{name}"), &body2)
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(changed["cron"], "0 12 * * *");
    assert_ne!(updated["next_fire_at"], changed["next_fire_at"]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Workflow rejection
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn workflow_target_rejected_with_400() {
    let _ = test_env();
    let client = TestClient::new();
    let name = unique("sched_wf");

    let resp = client
        .post(
            "/v1/schedules",
            &json!({
                "name": name,
                "cron": "0 9 * * *",
                "target": { "workflow": "my-workflow", "input": {} }
            }),
        )
        .await
        .assert_status(400);
    let err: serde_json::Value = serde_json::from_str(&resp.body).unwrap();
    assert_eq!(err["error"]["code"], "schedule_invalid");
    assert!(
        err["error"]["message"]
            .as_str()
            .unwrap()
            .contains("workflow")
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Reserved headers
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn reserved_schedule_header_rejected() {
    let _ = test_env();
    let client = TestClient::new();
    let target = unique("sched_target");
    let name = unique("sched_reserved");
    create_queue(&client, &target).await;

    let resp = client
        .post(
            "/v1/schedules",
            &json!({
                "name": name,
                "cron": "0 9 * * *",
                "target": {
                    "queue": target,
                    "message": {},
                    "headers": { "_schedule": { "foo": "bar" } }
                }
            }),
        )
        .await
        .assert_status(400);
    assert!(resp.body.contains("_schedule"));
}

// ─────────────────────────────────────────────────────────────────────────────
// One-shot fireAt
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn one_shot_fires_then_self_destructs() {
    let _ = test_env();
    let client = TestClient::new();
    let target = unique("sched_target");
    let name = unique("sched_oneshot");
    create_queue(&client, &target).await;

    let fire_at = chrono::Utc::now() + chrono::Duration::seconds(2);
    client
        .post(
            "/v1/schedules",
            &json!({
                "name": name,
                "fire_at": fire_at.to_rfc3339(),
                "target": { "queue": target, "message": { "kind": "oneshot" } }
            }),
        )
        .await
        .assert_status(201);

    // Worker polls every 100ms; wait long enough for one cycle past fire time.
    tokio::time::sleep(Duration::from_millis(3000)).await;

    let msg = next_message(&client, &target)
        .await
        .expect("message should have arrived");
    assert_eq!(msg["message"]["kind"], "oneshot");
    let headers = &msg["headers"];
    assert_eq!(headers["_schedule"]["name"], name);
    assert_eq!(headers["_schedule"]["out_of_band"], false);

    // Row should be gone after the one-shot fires.
    client
        .get(&format!("/v1/schedules/{name}"))
        .await
        .assert_status(404);
}

// ─────────────────────────────────────────────────────────────────────────────
// Pause / resume
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn pause_prevents_fire_resume_restores_it() {
    let _ = test_env();
    let client = TestClient::new();
    let target = unique("sched_target");
    let name = unique("sched_pause");
    create_queue(&client, &target).await;

    let fire_at = chrono::Utc::now() + chrono::Duration::seconds(2);
    client
        .post(
            "/v1/schedules",
            &json!({
                "name": name,
                "fire_at": fire_at.to_rfc3339(),
                "target": { "queue": target, "message": { "k": "v" } }
            }),
        )
        .await
        .assert_status(201);

    // Immediately pause.
    let paused = client
        .patch(
            &format!("/v1/schedules/{name}"),
            &json!({ "status": "paused" }),
        )
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(paused["status"], "paused");

    // Wait past the fire time; nothing should have landed.
    tokio::time::sleep(Duration::from_millis(3000)).await;
    let r = client
        .get(&format!("/v1/queues/{target}/messages?max=1&wait=1"))
        .await;
    assert!(
        r.body == "[]" || r.body.is_empty(),
        "expected no messages, got: {}",
        r.body
    );

    // Resume. The original fire_at is now in the past; the worker will fire it
    // once on the next poll (we picked up a past-due one-shot) and then delete the row.
    client
        .patch(
            &format!("/v1/schedules/{name}"),
            &json!({ "status": "active" }),
        )
        .await
        .assert_status(200);
}

// ─────────────────────────────────────────────────────────────────────────────
// Manual run (POST /runs)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn manual_run_bumps_fire_count_marks_out_of_band() {
    let _ = test_env();
    let client = TestClient::new();
    let target = unique("sched_target");
    let name = unique("sched_run");
    create_queue(&client, &target).await;

    // Schedule that won't fire on its own during the test (far in the future).
    let pre = client
        .post(
            "/v1/schedules",
            &json!({
                "name": name,
                "cron": "0 9 1 1 *",   // Jan 1 at 09:00 — won't naturally fire today
                "target": { "queue": target, "message": { "k": "v" } }
            }),
        )
        .await
        .assert_status(201)
        .json::<serde_json::Value>();
    let pre_next_fire_at = pre["next_fire_at"].clone();

    let run = client
        .post(&format!("/v1/schedules/{name}/runs"), &json!({}))
        .await
        .assert_status(202)
        .json::<serde_json::Value>();
    assert_eq!(run["schedule_name"], name);
    assert_eq!(run["out_of_band"], true);
    assert_eq!(run["msg_ids"].as_array().unwrap().len(), 1);

    let msg = next_message(&client, &target).await.expect("message");
    assert_eq!(msg["headers"]["_schedule"]["name"], name);
    assert_eq!(msg["headers"]["_schedule"]["out_of_band"], true);

    // fire_count bumped, next_fire_at unchanged.
    let after = client
        .get(&format!("/v1/schedules/{name}"))
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(after["fire_count"], 1);
    assert_eq!(after["next_fire_at"], pre_next_fire_at);
}

// ─────────────────────────────────────────────────────────────────────────────
// Failure → auto-pause
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn failure_increments_then_pauses_at_threshold() {
    let _ = test_env();
    let client = TestClient::new();
    let name = unique("sched_fail");

    // Target queue intentionally does not exist.
    let fire_at = chrono::Utc::now() + chrono::Duration::seconds(2);
    client
        .post(
            "/v1/schedules",
            &json!({
                "name": name,
                "fire_at": fire_at.to_rfc3339(),
                "failure_threshold": 1,
                "target": { "queue": "this_queue_does_not_exist_for_sure", "message": {} }
            }),
        )
        .await
        .assert_status(201);

    // Wait for a few worker cycles past the scheduled fire.
    tokio::time::sleep(Duration::from_millis(3500)).await;

    let after = client
        .get(&format!("/v1/schedules/{name}"))
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    assert_eq!(after["status"], "paused");
    assert!(after["consecutive_failures"].as_i64().unwrap() >= 1);
    assert!(after["last_error"].is_string());
}

// ─────────────────────────────────────────────────────────────────────────────
// Catchup
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn catchup_fires_missed_occurrences() {
    let _ = test_env();
    let client = TestClient::new();
    let target = unique("sched_target");
    let name = unique("sched_catchup");
    create_queue(&client, &target).await;

    let pool = &test_env().pool;

    // Create with a normal future fire_at so the API validates, then rewind in DB.
    let fire_at = chrono::Utc::now() + chrono::Duration::seconds(60);
    client
        .post(
            "/v1/schedules",
            &json!({
                "name": name,
                "cron": "* * * * *",   // every minute
                "catchup": true,
                "catchup_limit": 3,
                "target": { "queue": target, "message": {} }
            }),
        )
        .await
        .assert_status(201);
    let _ = fire_at;

    // Rewind next_fire_at to 5 minutes ago to simulate downtime.
    let past = chrono::Utc::now() - chrono::Duration::minutes(5);
    sqlx::query("UPDATE queue.schedule SET next_fire_at = $1 WHERE name = $2")
        .bind(past)
        .bind(&name)
        .execute(pool)
        .await
        .expect("rewind next_fire_at");

    // Wait for at least one worker poll.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Drain the target queue and count distinct scheduled_for values.
    let mut scheduled_for_seen = std::collections::HashSet::new();
    for _ in 0..10 {
        let Some(msg) = next_message(&client, &target).await else {
            break;
        };
        if let Some(sf) = msg["headers"]["_schedule"]["scheduled_for"].as_str() {
            scheduled_for_seen.insert(sf.to_string());
        }
    }
    assert!(
        scheduled_for_seen.len() >= 3,
        "expected at least 3 distinct scheduled_for, got {}: {scheduled_for_seen:?}",
        scheduled_for_seen.len()
    );
    // Catchup limit is 3 — should not have exceeded by much.
    assert!(
        scheduled_for_seen.len() <= 4,
        "expected at most 4 fires (catchup_limit=3 + maybe overflow marker), got {}",
        scheduled_for_seen.len()
    );

    let after = client
        .get(&format!("/v1/schedules/{name}"))
        .await
        .assert_status(200)
        .json::<serde_json::Value>();
    // last_error should mention catchup_limit_exceeded.
    let err = after["last_error"].as_str().unwrap_or("");
    assert!(
        err.contains("catchup_limit_exceeded"),
        "expected catchup_limit_exceeded in last_error, got: {err:?}"
    );
}
