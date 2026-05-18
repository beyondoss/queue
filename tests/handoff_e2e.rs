//! End-to-end handoff happy-path tests. Spins up a real beyond-queue
//! process under a `Supervisor`, drives a full handoff cycle, and verifies
//! message durability across the swap.

mod handoff_harness;

use handoff_harness::{Harness, create_queue, receive_one, send_message};

#[tokio::test(flavor = "multi_thread")]
async fn messages_survive_handoff() {
    let mut h = Harness::new().await;
    h.cold_start();

    let addr = h.http_addr();
    let status = create_queue(addr, "survive_q");
    assert!(status == 201 || status == 200, "create_queue: {status}");

    let id = send_message(addr, "survive_q", "before-handoff").expect("send before handoff");
    assert!(id > 0, "msg_id should be positive");

    let summary = h.handoff();
    assert!(
        summary.committed,
        "handoff did not commit: {:?}",
        summary.abort_reason
    );

    // Successor sees the message.
    let body = receive_one(addr, "survive_q").expect("receive after handoff");
    assert!(body.contains("before-handoff"), "body was {body}");

    // Successor accepts new sends too.
    let id2 = send_message(addr, "survive_q", "after-handoff").expect("send after handoff");
    assert!(id2 > id, "second msg_id should advance: {id2} <= {id}");

    let body2 = receive_one(addr, "survive_q").expect("receive second");
    assert!(body2.contains("after-handoff"), "second body was {body2}");
}

#[tokio::test(flavor = "multi_thread")]
async fn back_to_back_handoffs() {
    let mut h = Harness::new().await;
    h.cold_start();

    let addr = h.http_addr();
    let status = create_queue(addr, "back_to_back_q");
    assert!(status == 201 || status == 200, "create_queue: {status}");

    let _ = send_message(addr, "back_to_back_q", "msg-1").expect("send 1");

    let s1 = h.handoff();
    assert!(
        s1.committed,
        "first handoff did not commit: {:?}",
        s1.abort_reason
    );

    let _ = send_message(addr, "back_to_back_q", "msg-2").expect("send 2");

    let s2 = h.handoff();
    assert!(
        s2.committed,
        "second handoff did not commit: {:?}",
        s2.abort_reason
    );

    let _ = send_message(addr, "back_to_back_q", "msg-3").expect("send 3");

    // All three messages should be receivable now.
    let mut got: Vec<String> = Vec::new();
    for _ in 0..3 {
        if let Some(body) = receive_one(addr, "back_to_back_q") {
            got.push(body);
        }
    }
    assert!(
        got.iter().any(|b| b.contains("msg-1")),
        "missing msg-1 in {got:?}"
    );
    assert!(
        got.iter().any(|b| b.contains("msg-2")),
        "missing msg-2 in {got:?}"
    );
    assert!(
        got.iter().any(|b| b.contains("msg-3")),
        "missing msg-3 in {got:?}"
    );
}
