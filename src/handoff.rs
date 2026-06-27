//! Bridges the sync `handoff::Drainable` trait into queue's tokio runtime.
//!
//! Topology:
//! - `Incumbent::serve` runs on a dedicated OS thread (NOT a tokio worker).
//! - `QueueHandoff` carries `tokio::runtime::Handle` so its sync hooks can
//!   `block_on` into the runtime.
//! - The axum task is driven by `with_graceful_shutdown(...)` over a select
//!   of `outer_token.cancelled()` (SIGTERM) and the per-cycle inner token
//!   (handoff drain). Either triggers graceful shutdown.
//! - `drain()` cancels the inner token and awaits the axum task's
//!   `JoinHandle` up to the supplied deadline.
//! - `seal()` is a no-op for queue: Postgres holds all durable state and
//!   drain already awaited every in-flight handler.
//! - `resume_after_abort()` builds a fresh `AppState` (with a fresh
//!   coalescer task if `QUEUE_LINGER_MS > 0`), spawns a new axum task on a
//!   cloned listener FD, and installs both into shared cells. The old
//!   coalescer task drains naturally once the old `AppState` clones go out
//!   of scope.
//!
//! The axum `JoinHandle` lives in an `Arc<Mutex<Option<...>>>` shared with
//! the main task. Either side may `take()` it: `drain` takes it during a
//! handoff; the main task takes it during a SIGTERM shutdown. Whoever takes
//! it first awaits it.

use std::net::TcpListener as StdTcpListener;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use axum::Router;
use handoff::{DrainReport, Drainable, SealReport, StateSnapshot};
use sqlx::PgPool;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::AppState;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::signing::Signer;

#[derive(Clone)]
pub struct TlsParts {
    pub cert: String,
    pub key: String,
    pub ca: String,
}

/// Shared slot for the running axum task's `JoinHandle`. Either the handoff
/// drain or the SIGTERM main-thread cleanup may `take()` it; whoever takes
/// it first is the awaiter.
pub type ServerJhSlot = Arc<StdMutex<Option<JoinHandle<anyhow::Result<()>>>>>;

/// Ingredients sufficient to (re)build an `AppState` + fresh coalescer task.
/// Captured at startup, used both by the initial spawn and by
/// `resume_after_abort` to build a fresh state without sharing it through
/// `QueueHandoff` (so that final shutdown doesn't have to coordinate with
/// the handoff thread to drop the coalescer's last `Sender`).
pub struct Rebuild {
    pub pool: PgPool,
    pub config: Arc<Config>,
    pub signer: Arc<Signer>,
    pub base_url: Arc<str>,
    pub metrics: Arc<Metrics>,
    pub delivery_notify: Arc<tokio::sync::Notify>,
    pub schedule_notify: Arc<tokio::sync::Notify>,
}

impl Rebuild {
    /// Build a fresh `AppState` and (when coalescing is enabled) a fresh
    /// coalescer task. The returned `JoinHandle` is discarded on the
    /// resume path — the task drains naturally once its `AppState` clones
    /// drop.
    pub fn build_state(&self, rt: &Handle) -> (AppState, Option<JoinHandle<()>>) {
        let (coalescer, coalescer_jh) = if self.config.linger_ms > 0 {
            let _g = rt.enter();
            let (c, j) = crate::ops::coalesce::start(
                self.pool.clone(),
                self.config.linger_ms,
                self.metrics.clone(),
            );
            (Some(c), Some(j))
        } else {
            (None, None)
        };
        let state = AppState {
            pool: self.pool.clone(),
            config: self.config.clone(),
            base_url: self.base_url.clone(),
            coalescer,
            signer: self.signer.clone(),
            metrics: self.metrics.clone(),
            delivery_notify: self.delivery_notify.clone(),
            schedule_notify: self.schedule_notify.clone(),
        };
        (state, coalescer_jh)
    }
}

pub struct QueueHandoff {
    rt: Handle,
    /// Canonical TCP listener. Every server-task spawn `try_clone`s this.
    listener: Arc<StdTcpListener>,
    tls: Option<TlsParts>,
    /// Outer cancellation token (SIGTERM). Cloned into every spawned server
    /// task; the main thread cancels it on signal receipt.
    outer_token: CancellationToken,
    /// Used by the TLS accept loop to short-circuit pending accepts during
    /// drain. Plain `axum::serve` uses the tokens instead; this flag is set
    /// during drain and cleared during resume so the TLS loop resumes
    /// accepting.
    pub accept_closed: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
    /// Shared with the main task (see [`ServerJhSlot`]).
    server_jh: ServerJhSlot,
    /// Per-cycle cancellation token wired into `with_graceful_shutdown`.
    /// Cancelled by `drain`; replaced fresh by `resume_after_abort`.
    inner_token: StdMutex<CancellationToken>,
    rebuild: Rebuild,
}

impl QueueHandoff {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rt: Handle,
        listener: Arc<StdTcpListener>,
        tls: Option<TlsParts>,
        outer_token: CancellationToken,
        accept_closed: Arc<AtomicBool>,
        metrics: Arc<Metrics>,
        server_jh: ServerJhSlot,
        initial_inner_token: CancellationToken,
        rebuild: Rebuild,
    ) -> Self {
        Self {
            rt,
            listener,
            tls,
            outer_token,
            accept_closed,
            metrics,
            server_jh,
            inner_token: StdMutex::new(initial_inner_token),
            rebuild,
        }
    }
}

impl Drainable for QueueHandoff {
    fn drain(&self, deadline: Instant) -> handoff::Result<DrainReport> {
        let started = Instant::now();
        self.accept_closed.store(true, Ordering::SeqCst);

        let inner_token = self.inner_token.lock().expect("poisoned").clone();
        inner_token.cancel();

        let jh = self.server_jh.lock().expect("poisoned").take();
        if let Some(jh) = jh {
            let timeout = deadline.saturating_duration_since(Instant::now());
            self.rt.block_on(async {
                match tokio::time::timeout(timeout, jh).await {
                    Ok(Ok(Ok(()))) => {}
                    Ok(Ok(Err(e))) => {
                        tracing::error!(error = %e, "server task error during drain")
                    }
                    Ok(Err(join_err)) => {
                        tracing::error!(error = %join_err, "server task panicked during drain")
                    }
                    Err(_) => tracing::warn!("server task did not finish within drain deadline"),
                }
            });
        }

        let open_conns = self.metrics.http_connections_active.get().max(0.0) as u32;
        self.metrics
            .handoff_drain_seconds
            .observe(started.elapsed().as_secs_f64());
        Ok(DrainReport {
            open_conns_remaining: open_conns,
            accept_closed: true,
        })
    }

    fn seal(&self) -> handoff::Result<SealReport> {
        // No-op for queue: Postgres holds all durable state; drain already
        // awaited every in-flight handler (each of which awaited its
        // coalescer flush). Nothing pending to sync at this point. We
        // still record the elapsed time so dashboards built against kv's
        // `handoff_seal_seconds` work.
        let started = Instant::now();
        self.metrics
            .handoff_seal_seconds
            .observe(started.elapsed().as_secs_f64());
        Ok(SealReport::default())
    }

    fn resume_after_abort(&self) -> handoff::Result<()> {
        // Build a fresh AppState + coalescer for the new cycle. The old
        // coalescer task drains naturally once its `AppState` clones drop
        // (the previous axum task has already exited, taking its clone with
        // it; the main thread's clone lasts until process shutdown).
        let (state, _new_coalescer_jh) = self.rebuild.build_state(&self.rt);
        let app = crate::build_router(state);

        let new_inner = CancellationToken::new();
        let new_jh = spawn_server_task(
            self.listener.clone(),
            self.tls.as_ref(),
            app,
            self.outer_token.clone(),
            new_inner.clone(),
            self.accept_closed.clone(),
            &self.rt,
        )
        .map_err(|e| handoff::Error::Protocol(format!("respawn server task: {e}")))?;

        *self.server_jh.lock().expect("poisoned") = Some(new_jh);
        *self.inner_token.lock().expect("poisoned") = new_inner;
        self.accept_closed.store(false, Ordering::SeqCst);
        self.metrics.handoff_rolled_back_total.inc();
        self.metrics
            .handoff_handoffs_total
            .with_label_values(&["resumed"])
            .inc();
        Ok(())
    }

    fn snapshot_state(&self) -> StateSnapshot {
        StateSnapshot {
            shard_count: 1,
            open_conns: self.metrics.http_connections_active.get().max(0.0) as u32,
            last_revision_per_shard: Vec::new(),
        }
    }
}

/// Spawn the axum (or TLS) server task. Clones the std listener FD so
/// subsequent calls (e.g. from `resume_after_abort`) can spawn additional
/// tasks pointing at the same kernel accept queue.
///
/// The graceful-shutdown future fires on either `outer_token` (SIGTERM) or
/// `inner_token` (handoff drain), so both paths trigger axum's in-flight
/// request draining.
pub fn spawn_server_task(
    listener: Arc<StdTcpListener>,
    tls: Option<&TlsParts>,
    app: Router,
    outer_token: CancellationToken,
    inner_token: CancellationToken,
    accept_closed: Arc<AtomicBool>,
    rt: &Handle,
) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
    let std_clone = listener.try_clone()?;
    std_clone.set_nonblocking(true)?;
    // `from_std` registers the FD with tokio's reactor and so requires an
    // active tokio runtime context. `spawn_server_task` is called both from
    // the main task (in-context) and from `resume_after_abort` on the
    // handoff control OS thread (no context); `rt.enter()` makes both safe.
    let _g = rt.enter();
    let tokio_listener = tokio::net::TcpListener::from_std(std_clone)?;

    let jh = match tls {
        None => {
            let outer = outer_token.clone();
            let inner = inner_token.clone();
            rt.spawn(async move {
                axum::serve(tokio_listener, app)
                    .with_graceful_shutdown(async move {
                        tokio::select! {
                            _ = outer.cancelled() => {},
                            _ = inner.cancelled() => {},
                        }
                    })
                    .await
                    .map_err(Into::into)
            })
        }
        Some(parts) => {
            let parts = parts.clone();
            let outer = outer_token.clone();
            let inner = inner_token.clone();
            rt.spawn(async move {
                crate::serve_tls_inner(
                    tokio_listener,
                    &parts.cert,
                    &parts.key,
                    &parts.ca,
                    app,
                    outer,
                    inner,
                    accept_closed,
                )
                .await
            })
        }
    };
    Ok(jh)
}
