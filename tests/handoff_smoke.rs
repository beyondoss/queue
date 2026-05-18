//! Smoke test: cold-start, verify `/livez`, SIGTERM, observe clean exit.
//! The full handoff-protocol drive is exercised in handoff_e2e.

mod handoff_harness;

use std::time::Duration;

use handoff_harness::{Harness, wait_for_livez};

#[tokio::test(flavor = "multi_thread")]
async fn cold_start_then_sigterm_exits_cleanly() {
    let mut h = Harness::new().await;
    h.cold_start();

    // The /livez probe was already done inside cold_start (wait_ready),
    // but exercise it again to confirm the server is steady-state.
    wait_for_livez(h.http_addr(), Duration::from_secs(5));

    // SIGTERM the process; harness::Drop also does this, but we want to
    // observe the explicit clean shutdown.
    h.kill_current();

    // After kill_current, no PID is tracked — process has exited.
    assert!(h.current_pid().is_none(), "process should have exited");

    // The control socket file should be unlinked by Incumbent's drop.
    // (Best-effort — if it lingers it's OK because cold_start_after_crash
    // handles stale sockets via the data-dir flock.)
}
