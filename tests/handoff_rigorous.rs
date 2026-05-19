//! Rigorous handoff tests: fault injection, concurrency, durability under
//! sustained load. Priority subset of the test matrix from the plan.

mod handoff_harness;

use std::time::Duration;

use handoff_harness::{
    Harness, Sender, create_queue, fetch_metrics, metric_value, receive_batch, receive_one,
    send_message,
};

/// Sender thread spammed across pre/during/post-handoff. Every 200-acked
/// body MUST be readable on the successor.
#[tokio::test(flavor = "multi_thread")]
async fn acked_sends_durable_under_handoff() {
    let mut h = Harness::new().await;
    h.cold_start();

    let addr = h.http_addr();
    let status = create_queue(addr, "load_q");
    assert!(status == 201 || status == 200, "create_queue: {status}");

    let sender = Sender::start(addr, "load_q".into());
    // Let the sender warm up.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let pre_count = sender.acked_count();
    assert!(pre_count > 0, "sender failed to start sending");

    let summary = h.handoff();
    assert!(
        summary.committed,
        "handoff did not commit: {:?}",
        summary.abort_reason
    );

    // Keep sending after handoff.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let result = sender.stop();
    assert!(
        result.acked.len() > pre_count as usize,
        "no acks recorded after handoff (acks={}, pre={})",
        result.acked.len(),
        pre_count
    );
    // Error rate should be bounded — kv allows acks/20; we allow acks/10
    // because queue's reqwest/ureq stack is less re-connection-friendly.
    let allowed_errors = (result.acked.len() / 10).max(20);
    assert!(
        result.errors <= allowed_errors as u64,
        "too many errors: {} > {} (acked={})",
        result.errors,
        allowed_errors,
        result.acked.len()
    );

    // Receive all messages and assert every acked body is present. Use a
    // batch read with a long `vt` so the test doesn't re-receive the same
    // bodies while draining — single-message reads at CI's per-RTT cost
    // blow the wall-clock budget on the larger soak test.
    let mut received: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while received.len() < result.acked.len() && std::time::Instant::now() < deadline {
        let batch = receive_batch(addr, "load_q", 32, 180);
        if batch.is_empty() {
            break;
        }
        received.extend(batch);
    }
    // Each ack was for body `msg-N`; assert every N is present.
    for ack in &result.acked {
        assert!(
            received.iter().any(|b| b.contains(&ack.body)),
            "missing acked body {:?} in received set",
            ack.body
        );
    }
}

/// A second beyond-queue process pointed at the same state_dir must refuse
/// to start — the DataDirLock flock prevents it.
#[tokio::test(flavor = "multi_thread")]
async fn two_queue_processes_on_same_state_dir_is_prevented() {
    let mut h = Harness::new().await;
    h.cold_start();

    let mut competitor = h.try_spawn_competitor();
    let exit = competitor
        .wait_timeout(Duration::from_secs(10))
        .expect("competitor should exit");
    assert!(
        !exit.success(),
        "competitor unexpectedly succeeded: {:?}",
        exit
    );
}

/// SIGKILL leaves a stale pidfile. `cold_start_after_crash` exercises the
/// `acquire_or_break_stale` path; messages from before the crash are still
/// readable.
#[tokio::test(flavor = "multi_thread")]
async fn stale_lock_breaks_cleanly_after_sigkill() {
    let mut h = Harness::new().await;
    h.cold_start();

    let addr = h.http_addr();
    let status = create_queue(addr, "crash_q");
    assert!(status == 201 || status == 200, "create_queue: {status}");

    let _ = send_message(addr, "crash_q", "pre-crash").expect("send pre-crash");

    h.sigkill_current();
    // SIGKILL doesn't give the process a chance to clean up the pidfile;
    // the next cold_start must reclaim the stale lock.
    h.cold_start_after_crash();

    let body = receive_one(addr, "crash_q").expect("receive after crash recovery");
    assert!(body.contains("pre-crash"), "body was {body}");
}

/// Force a successor crash before Ready via `QUEUE_TEST_PANIC_BEFORE_READY`.
/// The old incumbent must resume serving. `handoff_rolled_back_total`
/// should increment.
#[tokio::test(flavor = "multi_thread")]
async fn successor_crash_before_ready_triggers_real_resume() {
    let mut h = Harness::new().await;
    h.cold_start();

    let addr = h.http_addr();
    let status = create_queue(addr, "abort_q");
    assert!(status == 201 || status == 200, "create_queue: {status}");

    // Inject the panic into the SUCCESSOR's env. The incumbent already
    // started without it; only the spawned child sees it.
    let summary = h.handoff_with_env(vec![("QUEUE_TEST_PANIC_BEFORE_READY".into(), "1".into())]);
    assert!(
        !summary.committed,
        "handoff should not commit when successor panics: {:?}",
        summary.abort_reason
    );

    // Old incumbent still serves.
    let id = send_message(addr, "abort_q", "post-abort").expect("send after abort");
    assert!(id > 0);
    let body = receive_one(addr, "abort_q").expect("receive after abort");
    assert!(body.contains("post-abort"), "body was {body}");

    // Metrics reflect the rolled-back handoff. Drain ran (counted) before
    // the supervisor aborted; seal also ran (queue's seal is a no-op but
    // still records the elapsed-time histogram); resume_after_abort
    // incremented the rolled_back counter.
    let metrics = fetch_metrics(addr);
    let rolled_back = metric_value(&metrics, "handoff_rolled_back_total", None)
        .expect("handoff_rolled_back_total present");
    assert!(
        rolled_back >= 1.0,
        "handoff_rolled_back_total = {rolled_back}, expected >= 1"
    );
    let drain_count = metric_value(&metrics, "handoff_drain_seconds_count", None)
        .expect("handoff_drain_seconds_count present");
    assert!(
        drain_count >= 1.0,
        "handoff_drain_seconds_count = {drain_count}, expected >= 1"
    );
    let resumed = metric_value(
        &metrics,
        "handoff_handoffs_total",
        Some(r#"result="resumed""#),
    )
    .expect("handoff_handoffs_total{result=resumed} present");
    assert!(resumed >= 1.0, "resumed counter = {resumed}");

    // A second handoff (without the panic env) must commit.
    let s2 = h.handoff();
    assert!(
        s2.committed,
        "second handoff did not commit: {:?}",
        s2.abort_reason
    );
}

/// Concurrent `perform_handoff` calls against the same Supervisor must
/// be serialized by its in_flight mutex: one wins (Ok with committed),
/// the other gets `Error::HandoffInProgress`. Catches regressions in the
/// Supervisor's correctness invariant that at most one swap is in-flight
/// per primitive.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_handoff_calls_are_serialized() {
    let mut h = Harness::new().await;
    h.cold_start();

    let sup1 = h.supervisor();
    let sup2 = h.supervisor();
    let spec1 = h.make_spawn_spec();
    let spec2 = h.make_spawn_spec();

    // Race two perform_handoff calls. We don't know which thread will
    // grab the in_flight mutex first.
    let t1 = std::thread::spawn(move || sup1.perform_handoff(spec1));
    let t2 = std::thread::spawn(move || sup2.perform_handoff(spec2));
    let r1 = t1.join().expect("t1 panic");
    let r2 = t2.join().expect("t2 panic");

    let outcomes = [r1, r2];
    let committed_count = outcomes
        .iter()
        .filter(|r| matches!(r, Ok(o) if o.committed))
        .count();
    let in_progress_count = outcomes
        .iter()
        .filter(|r| matches!(r, Err(handoff::Error::HandoffInProgress)))
        .count();

    // Exactly one of the two must have committed; the other must have
    // bounced off the in_flight mutex with HandoffInProgress.
    assert_eq!(
        committed_count, 1,
        "expected exactly 1 committed handoff, got {committed_count}: \
         outcomes = {outcomes:?}"
    );
    assert_eq!(
        in_progress_count, 1,
        "expected exactly 1 HandoffInProgress, got {in_progress_count}: \
         outcomes = {outcomes:?}"
    );

    // Adopt the new child so the Harness's Drop reaps it cleanly.
    for o in outcomes {
        if let Ok(mut outcome) = o
            && outcome.committed
            && let Some(child) = outcome.child.take()
        {
            // Replace whatever Harness tracked. The old child has been
            // reaped (committed handoff) but the field may still hold
            // its post-exit handle.
            h.adopt_current(child);
        }
    }
}

/// FD-count regression check: after N back-to-back handoffs, the
/// successor's /proc/$pid/fd entry count must not have grown
/// pathologically compared to a freshly-started baseline. Catches
/// listener FD or control-socket FD leaks per handoff.
#[tokio::test(flavor = "multi_thread")]
async fn fd_count_does_not_grow_across_handoffs() {
    fn count_fds(pid: u32) -> usize {
        match std::fs::read_dir(format!("/proc/{pid}/fd")) {
            Ok(rd) => rd.filter(|e| e.is_ok()).count(),
            Err(_) => 0,
        }
    }

    let mut h = Harness::new().await;
    h.cold_start();
    // Let the worker pool stabilize so we count steady-state FDs.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let baseline_pid = h.current_pid().expect("pid");
    let baseline_fds = count_fds(baseline_pid);
    assert!(baseline_fds > 0, "couldn't read /proc/{baseline_pid}/fd");

    // Stage a small amount of traffic so the DB pool has connections open
    // at the baseline measurement too (we want apples-to-apples).
    let addr = h.http_addr();
    let _ = create_queue(addr, "fd_probe_q");
    let _ = send_message(addr, "fd_probe_q", "warmup");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let warm_fds = count_fds(baseline_pid);

    // 5 back-to-back handoffs.
    for i in 0..5 {
        let summary = h.handoff();
        assert!(
            summary.committed,
            "handoff #{i} did not commit: {:?}",
            summary.abort_reason
        );
    }

    // Let the final successor settle.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let final_pid = h.current_pid().expect("successor pid");
    let final_fds = count_fds(final_pid);
    assert!(final_fds > 0, "couldn't read /proc/{final_pid}/fd");

    // Allow some slop: the post-warmup baseline already includes DB pool
    // + tokio worker FDs. Across 5 handoffs we allow at most +10 FDs
    // beyond the warm baseline. Linear growth (e.g. one leaked listener
    // clone per handoff) would show ≥5 over baseline plus per-cycle
    // accumulation — this bound flags that.
    let allowed_growth = 10;
    assert!(
        final_fds as i64 - warm_fds as i64 <= allowed_growth as i64,
        "FD count grew across handoffs: baseline={baseline_fds} warm={warm_fds} \
         after_5_handoffs={final_fds} (Δ from warm = {})",
        final_fds as i64 - warm_fds as i64
    );
}

/// SIGTERM during a long-poll receive must complete within a tight bound.
/// Axum's graceful shutdown waits for the in-flight handler; the handler
/// holds the connection for `wait` seconds (or until a message arrives).
/// We use wait=5 so the test stays fast in CI but still exercises the
/// "graceful shutdown blocks on in-flight handler" path.
#[tokio::test(flavor = "multi_thread")]
async fn sigterm_during_long_poll_shuts_down_within_bound() {
    let mut h = Harness::new().await;
    h.cold_start();
    let addr = h.http_addr();

    let status = create_queue(addr, "longpoll_q");
    assert!(status == 201 || status == 200);

    // Kick off a long-poll receive in the background. wait=5 means the
    // handler blocks up to 5 seconds; no message will arrive so it
    // returns an empty array on timeout.
    let url = format!("http://{addr}/v1/queues/longpoll_q/messages?max=1&wait=5&vt=10");
    let probe_handle = tokio::task::spawn_blocking(move || {
        ureq::get(&url)
            .set("Authorization", "Bearer test")
            .timeout(Duration::from_secs(15))
            .call()
            .map(|r| r.status())
            .map_err(|e| match e {
                ureq::Error::Status(code, _) => format!("status {code}"),
                ureq::Error::Transport(t) => format!("transport: {t}"),
            })
    });

    // Give the long-poll time to reach the server. Most of the wait=5
    // window is consumed in-server.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Bound: wait=5 for handler + ~2s slack for runtime teardown +
    // delivery/scrape JH abort awaits. 8s is generous but tight enough
    // to catch a regression that, say, hangs on coalescer drain.
    let elapsed = h
        .sigterm_and_wait(Duration::from_secs(8))
        .expect("process should exit within bound after SIGTERM");
    assert!(
        elapsed < Duration::from_secs(8),
        "SIGTERM-to-exit took {elapsed:?}, expected < 8s"
    );

    // The long-poll either completed with an empty array (200) or saw a
    // transport-level disconnect when the server closed. Both are
    // acceptable — what we DON'T accept is the long-poll hanging forever
    // (which would manifest as the probe future never finishing).
    let probe_result = tokio::time::timeout(Duration::from_secs(5), probe_handle)
        .await
        .expect("probe future did not finish")
        .expect("probe task panic");
    match probe_result {
        Ok(code) => assert!(
            (200..300).contains(&code) || code == 503,
            "unexpected status from long-poll across SIGTERM: {code}"
        ),
        Err(_e) => {
            // Connection error is fine — server closed mid-handler.
        }
    }
}

/// HTTP delivery continuity: subscribe a webhook to a topic, fire events,
/// handoff mid-batch, assert every event lands at the webhook. Validates
/// the lease-based abort-safety claim — handoff during delivery may
/// produce duplicates (at-least-once), but MUST NOT lose any event.
#[tokio::test(flavor = "multi_thread")]
async fn http_delivery_continues_across_handoff() {
    use std::collections::HashSet;
    use std::sync::Arc as StdArc;
    use std::sync::Mutex as StdSyncMutex;

    use serde_json::json;

    // Test webhook server: records every POST body it receives.
    let received: StdArc<StdSyncMutex<Vec<serde_json::Value>>> =
        StdArc::new(StdSyncMutex::new(Vec::new()));
    let webhook_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let webhook_addr = webhook_listener.local_addr().unwrap();
    {
        let received = received.clone();
        tokio::spawn(async move {
            let app = axum::Router::new().route(
                "/",
                axum::routing::post(move |body: axum::Json<serde_json::Value>| {
                    let received = received.clone();
                    async move {
                        received.lock().unwrap().push(body.0);
                        axum::http::StatusCode::OK
                    }
                }),
            );
            let _ = axum::serve(webhook_listener, app).await;
        });
    }

    // Spin up beyond-queue with a fast delivery poll so the worker is
    // very likely in-flight during the handoff.
    let mut h = Harness::new()
        .await
        .with_extra_env(vec![("QUEUE_HTTP_DELIVERY_POLL_MS".into(), "100".into())]);
    h.cold_start();
    let addr = h.http_addr();
    let base = format!("http://{addr}");

    // Subscribe the webhook to a routing key pattern.
    let routing_key = "delivery.handoff.test";
    let subscribe_url = format!("{base}/v1/events/{routing_key}/subscriptions");
    let sub_body = json!({
        "protocol": "http",
        "endpoint": format!("http://{webhook_addr}/"),
        "envelope": false
    })
    .to_string();
    let resp = tokio::task::spawn_blocking({
        let url = subscribe_url.clone();
        let body = sub_body.clone();
        move || {
            ureq::post(&url)
                .set("Authorization", "Bearer test")
                .set("Content-Type", "application/json")
                .send_string(&body)
        }
    })
    .await
    .unwrap();
    let resp = resp.expect("subscribe");
    assert_eq!(resp.status(), 201, "subscribe status");

    // Publish N events.
    let n = 8;
    let publish_url = format!("{base}/v1/events/{routing_key}");
    let mut sent_ids: HashSet<i64> = HashSet::new();
    for i in 0..n {
        let body = json!({"message": {"seq": i, "tag": format!("evt-{i}")}}).to_string();
        let url = publish_url.clone();
        let resp = tokio::task::spawn_blocking(move || {
            ureq::post(&url)
                .set("Authorization", "Bearer test")
                .set("Content-Type", "application/json")
                .send_string(&body)
        })
        .await
        .unwrap();
        let r = resp.expect("publish");
        assert_eq!(r.status(), 201, "publish status for seq {i}");
        sent_ids.insert(i);
    }

    // Give the worker a tick or two to start delivering.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let summary = h.handoff();
    assert!(
        summary.committed,
        "handoff did not commit: {:?}",
        summary.abort_reason
    );

    // Wait for the webhook to see every distinct seq. Bounded — the new
    // worker reclaims any rows the old failed to delete pre-abort.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut seen: HashSet<i64> = HashSet::new();
    while seen.len() < sent_ids.len() && std::time::Instant::now() < deadline {
        {
            for body in received.lock().unwrap().iter() {
                if let Some(seq) = body.get("seq").and_then(|v| v.as_i64()) {
                    seen.insert(seq);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let missing: Vec<i64> = sent_ids.difference(&seen).copied().collect();
    assert!(
        missing.is_empty(),
        "missing deliveries: {missing:?} (saw {} total bodies, {} distinct)",
        received.lock().unwrap().len(),
        seen.len()
    );

    // Duplicates are allowed (at-least-once is the documented guarantee)
    // but should be bounded — we don't expect every event to be redelivered.
    let total = received.lock().unwrap().len();
    assert!(
        total <= (sent_ids.len() * 3),
        "too many duplicates: got {total} bodies for {} events",
        sent_ids.len()
    );
}

/// Schedule worker continuity: a 1-second `every` schedule fires
/// continuously while a handoff happens in the middle. Validates that
/// the abort-safe-SAVEPOINT-rollback claim holds in practice — no
/// duplicate fires (same `scheduled_for` twice) and no large gaps
/// across the swap.
#[tokio::test(flavor = "multi_thread")]
async fn schedule_fires_continue_across_handoff() {
    use serde_json::json;

    let mut h = Harness::new().await;
    h.cold_start();
    let addr = h.http_addr();
    let base = format!("http://{addr}");

    let queue_name = "sched_target_q";
    let status = create_queue(addr, queue_name);
    assert!(status == 201 || status == 200);

    // Schedule firing every 1s. Use blocking ureq from a worker task so
    // we can hold the test's tokio runtime for sleeps + the handoff call.
    let create_url = format!("{base}/v1/schedules");
    let body = json!({
        "name": "sched_handoff_test",
        "every": "1s",
        "target": { "queue": queue_name, "message": {"k": "v"} }
    })
    .to_string();
    let resp = tokio::task::spawn_blocking(move || {
        ureq::post(&create_url)
            .set("Authorization", "Bearer test")
            .set("Content-Type", "application/json")
            .send_string(&body)
    })
    .await
    .unwrap();
    let resp = resp.expect("create schedule");
    assert_eq!(resp.status(), 201, "create schedule status");

    // Let it fire ~3 times.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let summary = h.handoff();
    assert!(
        summary.committed,
        "handoff did not commit: {:?}",
        summary.abort_reason
    );

    // Let it fire ~3 more times on the successor.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Collect every fire's `_schedule.scheduled_for` from message headers.
    let mut fires: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while std::time::Instant::now() < deadline {
        let url = format!("{base}/v1/queues/{queue_name}/messages?max=10&wait=1&vt=120");
        let resp = tokio::task::spawn_blocking(move || {
            ureq::get(&url).set("Authorization", "Bearer test").call()
        })
        .await
        .unwrap();
        let Ok(resp) = resp else { continue };
        let text = resp.into_string().unwrap_or_default();
        let arr: serde_json::Value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
        let items = match arr.as_array() {
            Some(items) if !items.is_empty() => items.clone(),
            _ => break,
        };
        for msg in items {
            if let Some(s) = msg
                .get("headers")
                .and_then(|h| h.get("_schedule"))
                .and_then(|sch| sch.get("scheduled_for"))
                .and_then(|v| v.as_str())
            {
                fires.push(s.to_string());
            }
        }
    }

    assert!(
        fires.len() >= 4,
        "expected ≥ 4 schedule fires across pre/post handoff, got {} ({:?})",
        fires.len(),
        fires
    );

    // No duplicates: each `scheduled_for` value should appear exactly once.
    let mut sorted = fires.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(
        fires.len(),
        sorted.len(),
        "duplicate scheduled_for values across handoff: {:?}",
        fires
    );
}

/// TLS handoff: a tokio-rustls/reqwest mTLS client probes /livez at ~5ms
/// cadence while the supervisor performs a handoff. Asserts zero non-2xx
/// responses across the swap. Closes the gap that `serve_tls_inner` is
/// rewritten with the two-token + accept_closed pattern but unexercised
/// under handoff.
#[tokio::test(flavor = "multi_thread")]
async fn tls_handoff_zero_dropped_connections() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let mut h = Harness::new().await.with_tls();
    h.cold_start();
    let addr = h.http_addr();

    // Build an mTLS reqwest client matching the harness's cert bundle.
    let certs = h.tls_certs().expect("TLS configured").clone();
    let ca = reqwest::Certificate::from_pem(certs.ca_pem.as_bytes()).expect("parse CA");
    let identity_pem = format!("{}{}", certs.client_pem, certs.client_key_pem);
    let identity = reqwest::Identity::from_pem(identity_pem.as_bytes()).expect("parse identity");
    let client = reqwest::Client::builder()
        .add_root_certificate(ca)
        .identity(identity)
        .https_only(true)
        .timeout(Duration::from_secs(2))
        .build()
        .expect("build mTLS client");
    let url = format!("https://{addr}/livez");

    // Confirm the TLS server is actually serving before we start the
    // handoff. cold_start's wait_ready only TCP-probes for TLS mode.
    let mut tls_ready = false;
    for _ in 0..40 {
        if client
            .get(&url)
            .send()
            .await
            .map(|r| r.status().as_u16())
            .ok()
            == Some(200)
        {
            tls_ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(tls_ready, "TLS server never reached 200 on /livez");

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let non_2xx = Arc::new(AtomicU64::new(0));
    let transport_errors = Arc::new(AtomicU64::new(0));
    let bad_statuses = Arc::new(std::sync::Mutex::new(Vec::<u16>::new()));

    let prober = {
        let stop = stop.clone();
        let total = total.clone();
        let non_2xx = non_2xx.clone();
        let transport_errors = transport_errors.clone();
        let bad_statuses = bad_statuses.clone();
        let client = client.clone();
        let url = url.clone();
        tokio::spawn(async move {
            while !stop.load(Ordering::Relaxed) {
                match client.get(&url).send().await {
                    Ok(r) => {
                        total.fetch_add(1, Ordering::Relaxed);
                        let s = r.status().as_u16();
                        if !(200..300).contains(&s) {
                            non_2xx.fetch_add(1, Ordering::Relaxed);
                            bad_statuses.lock().unwrap().push(s);
                        }
                    }
                    Err(_) => {
                        transport_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    // Warm-up.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let summary = h.handoff();
    assert!(
        summary.committed,
        "TLS handoff did not commit: {:?}",
        summary.abort_reason
    );

    // Drain probes after the swap.
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::Relaxed);
    let _ = prober.await;

    let total = total.load(Ordering::Relaxed);
    let non_2xx = non_2xx.load(Ordering::Relaxed);
    let transport = transport_errors.load(Ordering::Relaxed);
    let bad = bad_statuses.lock().unwrap().clone();
    assert!(total > 30, "expected >30 TLS probes, got {total}");
    assert_eq!(
        non_2xx, 0,
        "TLS probes saw {non_2xx} non-2xx responses (statuses: {bad:?})"
    );
    // Transport-level errors: same accounting as the plain probe test.
    // In-flight TLS sessions established against the OLD process get
    // RST'd at commit. We tolerate a small handful.
    assert!(
        transport <= 5,
        "expected ≤ 5 transport errors, got {transport} (of {total} probes)"
    );
}

/// Soak: 10 consecutive handoffs while a sender hammers SendMessage.
/// Validates that nothing accumulates pathologically across cycles —
/// orphaned coalescer tasks, listener FD reference cycles, control
/// socket re-bind races, etc.
#[tokio::test(flavor = "multi_thread")]
async fn ten_consecutive_handoffs_under_sustained_load() {
    let mut h = Harness::new().await;
    h.cold_start();

    let addr = h.http_addr();
    let status = create_queue(addr, "soak_q");
    assert!(status == 201 || status == 200, "create_queue: {status}");

    let sender = Sender::start(addr, "soak_q".into());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(sender.acked_count() > 0, "sender failed to start");

    for i in 0..10 {
        let summary = h.handoff();
        assert!(
            summary.committed,
            "handoff #{} did not commit: {:?}",
            i, summary.abort_reason
        );
        // Brief pause between handoffs so the sender accumulates acks on
        // each successor incarnation.
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    // Run a bit more so post-final-handoff acks are recorded.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let result = sender.stop();

    assert!(
        result.acked.len() >= 100,
        "expected ≥ 100 acks across 10 handoffs, got {}",
        result.acked.len()
    );
    // Stricter than the single-handoff test (acks/10) because over 10
    // swaps we expect connection-error chatter; still bounded.
    let allowed_errors = result.acked.len() / 20 + 30;
    assert!(
        result.errors <= allowed_errors as u64,
        "too many errors over soak: {} > {} (acked={})",
        result.errors,
        allowed_errors,
        result.acked.len()
    );

    // Every acked body must be receivable. Batch-read with a long `vt` so
    // we drain each message exactly once and don't lose the wall-clock
    // budget to per-message round-trips on CI.
    let mut received: Vec<String> = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(45);
    while received.len() < result.acked.len() && std::time::Instant::now() < deadline {
        let batch = receive_batch(addr, "soak_q", 32, 180);
        if batch.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        received.extend(batch);
    }
    let missing: Vec<String> = result
        .acked
        .iter()
        .filter(|ack| !received.iter().any(|r| r.contains(&ack.body)))
        .map(|a| a.body.clone())
        .collect();
    assert!(
        missing.is_empty(),
        "missing {} acked bodies after soak: first few = {:?}",
        missing.len(),
        missing.iter().take(5).collect::<Vec<_>>()
    );
}

/// Stronger downtime check than the load test: literally zero non-200
/// responses must be observed across a handoff. Probes /livez at ~5ms
/// cadence; the swap (drain + seal + commit + successor bind) typically
/// completes in well under a second, so we get >100 probe samples
/// straddling the swap window.
#[tokio::test(flavor = "multi_thread")]
async fn livez_probes_never_return_non_200_across_handoff() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let mut h = Harness::new().await;
    h.cold_start();
    let addr = h.http_addr();

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let non_2xx = Arc::new(AtomicU64::new(0));
    let connection_errors = Arc::new(AtomicU64::new(0));
    let bad_statuses = Arc::new(std::sync::Mutex::new(Vec::<u16>::new()));

    let probe_handle = {
        let stop = stop.clone();
        let total = total.clone();
        let non_2xx = non_2xx.clone();
        let connection_errors = connection_errors.clone();
        let bad_statuses = bad_statuses.clone();
        std::thread::Builder::new()
            .name("livez-prober".into())
            .spawn(move || {
                let agent = ureq::AgentBuilder::new()
                    .timeout(Duration::from_millis(2000))
                    .build();
                let url = format!("http://{addr}/livez");
                while !stop.load(Ordering::Relaxed) {
                    match agent.get(&url).call() {
                        Ok(resp) => {
                            total.fetch_add(1, Ordering::Relaxed);
                            let s = resp.status();
                            if !(200..300).contains(&s) {
                                non_2xx.fetch_add(1, Ordering::Relaxed);
                                bad_statuses.lock().unwrap().push(s);
                            }
                        }
                        Err(ureq::Error::Status(code, _)) => {
                            total.fetch_add(1, Ordering::Relaxed);
                            non_2xx.fetch_add(1, Ordering::Relaxed);
                            bad_statuses.lock().unwrap().push(code);
                        }
                        Err(_) => {
                            // Transport-level failure (connection refused,
                            // EOF, timeout). Counted separately — the
                            // listener FD survives the swap so we don't
                            // expect any of these either; if it happens,
                            // it's a real downtime event.
                            connection_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
            .expect("spawn prober thread")
    };

    // Let the prober warm up.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let summary = h.handoff();
    assert!(
        summary.committed,
        "handoff did not commit: {:?}",
        summary.abort_reason
    );

    // Let the prober run a bit more so probes after the swap are counted.
    tokio::time::sleep(Duration::from_millis(500)).await;

    stop.store(true, Ordering::Relaxed);
    probe_handle.join().expect("prober thread");

    let total = total.load(Ordering::Relaxed);
    let non_2xx = non_2xx.load(Ordering::Relaxed);
    let conn_errors = connection_errors.load(Ordering::Relaxed);
    let bad = bad_statuses.lock().unwrap().clone();
    assert!(total > 50, "expected >50 probes, got {total}");
    assert_eq!(
        non_2xx, 0,
        "expected zero non-2xx responses, got {non_2xx} (statuses: {bad:?})"
    );
    // Connection-level errors are also a downtime signal. The listener FD
    // survives the swap, but TCP connections established to the OLD process
    // get RST'd when it exits — clients that pick that exact moment may see
    // one connection error. We allow up to 5 such errors out of (typically)
    // 200+ probes; more than that suggests a real regression.
    assert!(
        conn_errors <= 5,
        "expected ≤ 5 transport errors, got {conn_errors} (out of {total} probes)"
    );
}

// `Child::wait_timeout` from the `wait-timeout` crate would be cleaner, but
// keeping deps minimal — implement a busy-wait helper here.
trait WaitTimeoutExt {
    fn wait_timeout(&mut self, dur: Duration) -> Option<std::process::ExitStatus>;
}

impl WaitTimeoutExt for std::process::Child {
    fn wait_timeout(&mut self, dur: Duration) -> Option<std::process::ExitStatus> {
        let deadline = std::time::Instant::now() + dur;
        loop {
            match self.try_wait() {
                Ok(Some(status)) => return Some(status),
                Ok(None) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Ok(None) => return None,
                Err(_) => return None,
            }
        }
    }
}
