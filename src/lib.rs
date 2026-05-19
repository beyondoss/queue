pub mod cli;
pub mod config;
pub mod db;
pub mod error;
pub mod handoff;
pub mod metrics;
pub mod middleware;
pub mod ops;
pub mod routes;
pub mod schedule;
pub mod signing;
pub mod sns;
pub mod sqs;
pub mod telemetry;
pub mod test_support;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{MatchedPath, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use sqlx::PgPool;
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;
use tower::{ServiceBuilder, ServiceExt};
use tower_http::{
    catch_panic::CatchPanicLayer,
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    timeout::TimeoutLayer,
    trace::{MakeSpan, TraceLayer},
};

use config::Config;
use error::{ApiError, DbPoolTimeout};
use metrics::Metrics;
use ops::coalesce::Coalescer;
use signing::Signer;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    /// Config is Arc-wrapped so AppState::clone() is cheap (Arc increment, no String copies).
    pub config: Arc<Config>,
    /// Precomputed base URL — Arc<str> clone is a single atomic increment.
    pub base_url: Arc<str>,
    /// Write coalescer for non-FIFO sends. None when QUEUE_LINGER_MS=0.
    pub coalescer: Option<Coalescer>,
    /// RSA-2048 signer for SNS notification envelopes.
    pub signer: Arc<Signer>,
    pub metrics: Arc<Metrics>,
}

/// Parse an AWS service request body: returns `Ok((is_json, action_name, parsed_body))`.
/// Returns `Err(Response)` if the body claims to be JSON but fails to parse.
/// `service_prefix` is e.g. `"AmazonSQS."` or `"AmazonSNS."`.
// Response is large but callers always return it immediately — no stack cost in practice.
#[allow(clippy::result_large_err)]
pub fn parse_service_body(
    headers: &HeaderMap,
    body: &Bytes,
    service_prefix: &str,
) -> Result<(bool, String, serde_json::Value), Response> {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if content_type.contains("application/x-amz-json-1.0") {
        let target = headers
            .get("x-amz-target")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let action = target
            .strip_prefix(service_prefix)
            .unwrap_or(target)
            .to_string();
        let value = serde_json::from_slice(body).map_err(|_| {
            ApiError::BadRequest("invalid JSON request body".into()).into_response()
        })?;
        Ok((true, action, value))
    } else {
        let map: HashMap<String, String> = form_urlencoded::parse(body).into_owned().collect();
        let action = map.get("Action").cloned().unwrap_or_default();
        let value = serde_json::to_value(&map).unwrap_or(serde_json::json!({}));
        Ok((false, action, value))
    }
}

pub async fn serve(config: Config) -> anyhow::Result<()> {
    let otel_config = telemetry::OtelConfig {
        enabled: config.otlp_enabled,
        otlp_endpoint: config.otlp_endpoint.clone(),
        service_name: "beyond-queue".into(),
        sample_rate: config.otlp_sample_rate,
    };
    let _otel_guard = telemetry::init(&otel_config, vec![], &config.log_level)?;

    // 1. Role detection before any other thread spawn. `detect_role` reads
    //    the inherited-FD env vars and clears them; safe to call early.
    let (inherited_http, mut successor) = match ::handoff::detect_role()
        .map_err(|e| anyhow::anyhow!("handoff::detect_role: {e}"))?
    {
        ::handoff::Role::ColdStart { mut inherited } => {
            tracing::info!(inherited_listeners = ?inherited.names(), "cold start");
            (inherited.take("http"), None)
        }
        ::handoff::Role::Successor(s) => {
            let build_id = env!("CARGO_PKG_VERSION").as_bytes().to_vec();
            let s = s
                .handshake(build_id)
                .map_err(|e| anyhow::anyhow!("handshake: {e}"))?;
            tracing::info!(handoff_id = %s.handoff_id(), "handshake complete; waiting for Begin");
            let mut s = s
                .wait_for_begin()
                .map_err(|e| anyhow::anyhow!("wait_for_begin: {e}"))?;
            tracing::info!(handoff_id = %s.handoff_id(), "Begin received");
            (s.take_listener("http"), Some(s))
        }
    };

    // Keep the supervisor's per-recv liveness timer (10s) alive while the
    // successor's slow init runs (DB pool warm-up, state rebuild, TLS load).
    // Dropped explicitly just before `announce_and_bind` so the main thread
    // is the sole writer when `Ready` goes on the wire.
    let heartbeat_guard = successor.as_ref().map(|s| s.start_heartbeats());

    // 2. Acquire the data-dir flock. ColdStart: break_stale recovers from a
    //    crashed predecessor. Successor: the prior incumbent has released
    //    after `SealComplete` so this succeeds immediately.
    let _ = std::fs::create_dir_all(&config.handoff_state_dir);
    let data_dir_lock = ::handoff::DataDirLock::acquire_or_break_stale(&config.handoff_state_dir)
        .map_err(|e| {
        anyhow::anyhow!(
            "acquire data-dir lock {}: {e}",
            config.handoff_state_dir.display()
        )
    })?;

    // 3. Bind the HTTP listener: inherited if Successor, fresh otherwise.
    let std_listener: std::net::TcpListener = match inherited_http {
        Some(l) => {
            tracing::info!(addr = ?l.local_addr().ok(), "HTTP listening on inherited fd");
            l
        }
        None => {
            let l = std::net::TcpListener::bind(&config.address)?;
            tracing::info!(address = %config.address, "HTTP listening (fresh bind)");
            l
        }
    };
    std_listener.set_nonblocking(true)?;
    let listener_arc = Arc::new(std_listener);

    // 4. DB pool + workers.
    let pool = db::connect(&config.database_url, config.max_connections).await?;
    let metrics = Arc::new(Metrics::new());

    let delivery_handle = if config.http_delivery_enabled {
        tracing::info!("HTTP delivery worker enabled");
        Some(ops::delivery::start(
            pool.clone(),
            ops::delivery::DeliveryConfig {
                poll_interval_ms: config.http_delivery_poll_ms,
                delivery_timeout_secs: config.http_delivery_timeout_secs,
                batch_size: config.http_delivery_batch_size,
            },
            metrics.clone(),
        )?)
    } else {
        None
    };

    let schedule_handle = if config.schedule_enabled {
        tracing::info!("schedule worker enabled");
        Some(ops::schedule_worker::start(
            pool.clone(),
            ops::schedule_worker::ScheduleWorkerConfig {
                poll_interval_ms: config.schedule_poll_ms,
                batch_size: config.schedule_batch_size,
            },
            metrics.clone(),
        ))
    } else {
        None
    };

    let scrape_handle = start_queue_depth_scrape(pool.clone(), metrics.clone());

    // 5. TLS validation (up-front; misconfig surfaces immediately).
    let tls_parts = match (&config.tls_cert, &config.tls_key, &config.tls_ca) {
        (Some(c), Some(k), Some(ca)) => Some(handoff::TlsParts {
            cert: c.clone(),
            key: k.clone(),
            ca: ca.clone(),
        }),
        (None, None, None) => None,
        _ => anyhow::bail!(
            "BEYOND_TLS_CERT, BEYOND_TLS_KEY, and BEYOND_TLS_CA must all be set or all unset"
        ),
    };

    // 6. Build the rebuild ingredients and the initial AppState.
    let signer = Arc::new(Signer::generate()?);
    let base_url: Arc<str> = config.base_url().into();
    let listening_address = config.address.clone();
    let handoff_socket_path = config.handoff_socket_path.clone();
    let config_arc = Arc::new(config);
    let rebuild = handoff::Rebuild {
        pool: pool.clone(),
        config: config_arc.clone(),
        signer: signer.clone(),
        base_url: base_url.clone(),
        metrics: metrics.clone(),
    };
    let rt = Handle::current();
    let (initial_state, coalescer_handle) = rebuild.build_state(&rt);
    if config_arc.linger_ms > 0 {
        tracing::info!(linger_ms = config_arc.linger_ms, "write coalescer enabled");
    }
    let initial_app = build_router(initial_state.clone());

    // 7. Cancellation tokens + accept_closed flag + server task spawn.
    let outer_token = CancellationToken::new();
    let initial_inner_token = CancellationToken::new();
    let accept_closed = Arc::new(AtomicBool::new(false));
    let initial_server_jh = handoff::spawn_server_task(
        listener_arc.clone(),
        tls_parts.as_ref(),
        initial_app,
        outer_token.clone(),
        initial_inner_token.clone(),
        accept_closed.clone(),
        &rt,
    )?;
    let server_jh: handoff::ServerJhSlot = Arc::new(StdMutex::new(Some(initial_server_jh)));

    // 8. Build QueueHandoff (sharing the server_jh slot with the main task).
    let queue_handoff = handoff::QueueHandoff::new(
        rt.clone(),
        listener_arc.clone(),
        tls_parts.clone(),
        outer_token.clone(),
        accept_closed.clone(),
        metrics.clone(),
        server_jh.clone(),
        initial_inner_token,
        rebuild,
    );

    // 9. Signal handling via signal-hook → atomic flag polled below.
    let shutdown = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&shutdown))?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&shutdown))?;

    // 10. Bind control socket / announce_and_bind. Stop the heartbeat
    //     thread first so the main thread is the sole writer for `Ready`.
    drop(heartbeat_guard);
    let incumbent = match successor.take() {
        Some(s) => {
            #[cfg(feature = "test-hooks")]
            if std::env::var("QUEUE_TEST_PANIC_BEFORE_READY").is_ok() {
                std::process::exit(42);
            }
            let snapshot = ::handoff::ReadinessSnapshot {
                listening_on: vec![listening_address.clone()],
                healthz_ok: true,
                advertised_revision_per_shard: Vec::new(),
            };
            s.announce_and_bind(snapshot, &handoff_socket_path, data_dir_lock)
                .map_err(|e| anyhow::anyhow!("announce_and_bind: {e}"))?
        }
        None => ::handoff::Incumbent::bind_cold_start(&handoff_socket_path, data_dir_lock)
            .map_err(|e| anyhow::anyhow!("bind handoff control socket: {e}"))?,
    }
    .with_build_id(env!("CARGO_PKG_VERSION").as_bytes().to_vec());

    // 11. Handoff control thread. Sets `handoff_committed` (and the
    //     `shutdown` flag) on a successful commit so the main task knows
    //     to take the "handoff drained the server task already" cleanup
    //     path versus the "SIGTERM, we still need to drain axum" path.
    let handoff_committed = Arc::new(AtomicBool::new(false));
    let handoff_shutdown = Arc::clone(&shutdown);
    let handoff_committed_flag = Arc::clone(&handoff_committed);
    let handoff_metrics = Arc::clone(&metrics);
    std::thread::Builder::new()
        .name("queue-handoff".into())
        .spawn(move || match incumbent.serve(queue_handoff) {
            Ok(()) => {
                handoff_metrics
                    .handoff_handoffs_total
                    .with_label_values(&["committed"])
                    .inc();
                tracing::info!("handoff committed; signaling main to exit");
                handoff_committed_flag.store(true, Ordering::Relaxed);
                handoff_shutdown.store(true, Ordering::Relaxed);
            }
            Err(e) => {
                tracing::error!(error = %e, "handoff control thread exited with error");
            }
        })?;

    // 12. Main wait loop.
    while !shutdown.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // 13. Cleanup branches by reason for exit.
    if handoff_committed.load(Ordering::Relaxed) {
        // Handoff committed — drain() already awaited the server task and
        // the handoff thread has returned (which dropped QueueHandoff and
        // its rebuild ingredients). The active axum task's `AppState`
        // clone was dropped when it returned, so the only remaining
        // `Coalescer` sender is `initial_state.coalescer`. Drop our
        // local clone and the canonical `initial_state` to close the
        // channel.
        drop(initial_state);
        if let Some(jh) = coalescer_handle
            && tokio::time::timeout(Duration::from_secs(10), jh)
                .await
                .is_err()
        {
            tracing::warn!("coalescer did not drain within shutdown deadline");
        }
    } else {
        // SIGTERM (or another error path). The server task is still live
        // and the handoff thread is still blocked in `incumbent.serve`.
        // Cancel the outer token to trigger axum's graceful shutdown, take
        // the JH from the shared slot, await it (so long-poll handlers
        // finish), then drop state to drain the coalescer.
        tracing::info!("shutdown signal received, draining connections");
        outer_token.cancel();
        let jh = server_jh.lock().expect("poisoned").take();
        if let Some(jh) = jh {
            // Long-poll receives top out around 30s; 35s leaves headroom.
            match tokio::time::timeout(Duration::from_secs(35), jh).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(e))) => {
                    tracing::error!(error = %e, "server task error during shutdown")
                }
                Ok(Err(je)) => tracing::error!(error = %je, "server task panicked during shutdown"),
                Err(_) => {
                    tracing::warn!("server task did not drain within shutdown deadline")
                }
            }
        }
        drop(initial_state);
        // QueueHandoff still holds nothing that blocks coalescer drain
        // (no AppState clone), so dropping `initial_state` is sufficient.
        if let Some(jh) = coalescer_handle
            && tokio::time::timeout(Duration::from_secs(10), jh)
                .await
                .is_err()
        {
            tracing::warn!("coalescer did not drain within shutdown deadline");
        }
    }

    if let Some(jh) = delivery_handle {
        jh.abort();
        let _ = jh.await;
    }
    if let Some(jh) = schedule_handle {
        jh.abort();
        let _ = jh.await;
    }
    scrape_handle.abort();
    let _ = scrape_handle.await;

    tracing::info!("shutdown complete");
    Ok(())
}

/// TLS accept loop. Spawned as a tokio task by [`handoff::spawn_server_task`].
///
/// Drives the same `outer_token` + `inner_token` graceful-shutdown signals
/// as the plain `axum::serve` path, plus an `accept_closed` flag that
/// short-circuits pending accepts during drain so we don't race a
/// just-accepted TLS connection past the cancellation.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_tls_inner(
    listener: tokio::net::TcpListener,
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
    app: Router,
    outer_token: CancellationToken,
    inner_token: CancellationToken,
    accept_closed: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use hyper_util::server::conn::auto::Builder;
    use rustls::RootCertStore;
    use rustls::ServerConfig;
    use rustls::server::WebPkiClientVerifier;
    use tokio_rustls::TlsAcceptor;

    let server_certs = tls_load_certs(cert_path)?;
    let server_key = tls_load_key(key_path)?;
    let ca_certs = tls_load_certs(ca_path)?;

    let mut ca_store = RootCertStore::empty();
    for cert in ca_certs {
        ca_store.add(cert)?;
    }

    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let verifier = WebPkiClientVerifier::builder_with_provider(
        std::sync::Arc::new(ca_store),
        provider.clone(),
    )
    .build()?;

    let mut cfg = ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_client_cert_verifier(verifier)
        .with_single_cert(server_certs, server_key)?;
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    let acceptor = TlsAcceptor::from(std::sync::Arc::new(cfg));

    loop {
        if accept_closed.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(25)).await;
            continue;
        }
        tokio::select! {
            result = listener.accept() => {
                let (tcp, _) = result?;
                let acceptor = acceptor.clone();
                let app = app.clone();
                tokio::spawn(async move {
                    match acceptor.accept(tcp).await {
                        Ok(tls_stream) => {
                            let io = TokioIo::new(tls_stream);
                            let svc = hyper::service::service_fn(move |req: axum::http::Request<hyper::body::Incoming>| app.clone().oneshot(req));
                            Builder::new(TokioExecutor::new())
                                .serve_connection_with_upgrades(io, svc)
                                .await
                                .ok();
                        }
                        Err(e) => tracing::debug!(error = %e, "TLS handshake failed"),
                    }
                });
            }
            _ = outer_token.cancelled() => break,
            _ = inner_token.cancelled() => break,
        }
    }
    Ok(())
}

/// Test/library helper. Bind an axum server (with optional TLS) on an
/// already-bound `tokio::net::TcpListener` without any of the handoff or
/// signal plumbing. Used by the TLS integration test.
pub async fn serve_with_listener(
    listener: tokio::net::TcpListener,
    tls: Option<(String, String, String)>,
    app: Router,
) -> anyhow::Result<()> {
    if let Some((cert, key, ca)) = tls {
        // Use a never-cancelling token + always-false accept flag so the
        // accept loop runs forever (until the listener is closed).
        let token = CancellationToken::new();
        let accept_closed = Arc::new(AtomicBool::new(false));
        serve_tls_inner(
            listener,
            &cert,
            &key,
            &ca,
            app,
            token.clone(),
            token,
            accept_closed,
        )
        .await
    } else {
        axum::serve(listener, app).await?;
        Ok(())
    }
}

fn tls_load_certs(path: &str) -> anyhow::Result<Vec<rustls::pki_types::CertificateDer<'static>>> {
    let f = std::fs::File::open(path)?;
    rustls_pemfile::certs(&mut std::io::BufReader::new(f))
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn tls_load_key(path: &str) -> anyhow::Result<rustls::pki_types::PrivateKeyDer<'static>> {
    let f = std::fs::File::open(path)?;
    rustls_pemfile::private_key(&mut std::io::BufReader::new(f))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {path}"))
}

fn is_sns_query_action(action: &str) -> bool {
    matches!(
        action,
        "CreateTopic"
            | "DeleteTopic"
            | "ListTopics"
            | "Subscribe"
            | "Unsubscribe"
            | "ListSubscriptions"
            | "ListSubscriptionsByTopic"
            | "Publish"
            | "GetTopicAttributes"
            | "SetTopicAttributes"
            | "GetSubscriptionAttributes"
            | "ConfirmSubscription"
    )
}

async fn gateway_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let target = headers
        .get("x-amz-target")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if target.starts_with("AmazonSNS.") {
        return sns::handle_service_request(state, headers, body).await;
    }

    // Query-protocol (form-urlencoded) SNS requests carry no X-Amz-Target header.
    // Detect them by peeking at the Action field in the body.
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !content_type.contains("application/x-amz-json-1.0") {
        let action = form_urlencoded::parse(&body)
            .find(|(k, _)| k == "Action")
            .map(|(_, v)| v.into_owned())
            .unwrap_or_default();
        if is_sns_query_action(&action) {
            return sns::handle_service_request(state, headers, body).await;
        }
    }

    sqs::handle_service_request(state, headers, body).await
}

/// Propagates W3C trace context (`traceparent`/`tracestate`) from incoming
/// requests so spans are children of the caller's trace, not fresh roots.
#[derive(Clone, Default)]
struct OtelMakeSpan;

impl<B> MakeSpan<B> for OtelMakeSpan {
    fn make_span(&mut self, request: &axum::http::Request<B>) -> tracing::Span {
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;

        let span = tracing::info_span!(
            "http.request",
            otel.kind = "server",
            http.method = request.method().as_str(),
            http.target = %request.uri(),
            http.flavor = ?request.version(),
            http.route = tracing::field::Empty,
            http.status_code = tracing::field::Empty,
        );
        let _ = span.set_parent(telemetry::extract_trace_context(request.headers()));
        span
    }
}

pub fn build_router(state: AppState) -> Router {
    use axum::Json;
    use axum::extract::DefaultBodyLimit;
    use axum::middleware::from_fn;
    use routes::ApiDoc;
    use utoipa::OpenApi;

    let openapi = ApiDoc::openapi();

    let api = Router::new()
        .nest("/v1", routes::router())
        .route(
            "/v1/openapi.json",
            get(move || async move { Json(openapi) }),
        )
        .route("/", post(gateway_handler))
        .merge(sqs::router())
        .layer(DefaultBodyLimit::max(4 * 1024 * 1024))
        .layer(from_fn(middleware::auth::require_auth));

    Router::new()
        .merge(api)
        .route("/livez", get(healthz_live))
        .route("/readyz", get(healthz_ready))
        .route("/metrics", get(metrics_handler))
        .route("/SimpleNotificationService.pem", get(serve_signing_cert))
        .route_layer(from_fn_with_state(state.clone(), record_metrics))
        .layer(
            ServiceBuilder::new()
                .layer(SetRequestIdLayer::x_request_id(MakeRequestUuid))
                .layer(PropagateRequestIdLayer::x_request_id())
                .layer(TraceLayer::new_for_http().make_span_with(OtelMakeSpan))
                .layer(TimeoutLayer::with_status_code(
                    axum::http::StatusCode::REQUEST_TIMEOUT,
                    Duration::from_secs(30),
                ))
                .layer(CatchPanicLayer::new()),
        )
        .with_state(state)
}

async fn record_metrics(State(state): State<AppState>, req: Request, next: Next) -> Response {
    state.metrics.http_connections_active.inc();
    let method = req.method().clone();
    let path = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string())
        .unwrap_or_else(|| "<unmatched>".to_string());
    tracing::Span::current().record("http.route", &path);
    let timer = state
        .metrics
        .http_request_duration_seconds
        .with_label_values(&[method.as_str(), &path]);
    let start = Instant::now();

    let response = next.run(req).await;

    state.metrics.http_connections_active.dec();
    let status = response.status().as_u16();
    state
        .metrics
        .http_requests_total
        .with_label_values(&[method.as_str(), &path, &status.to_string()])
        .inc();
    timer.observe(start.elapsed().as_secs_f64());
    let pool_size = state.pool.size() as usize;
    let pool_idle = state.pool.num_idle();
    state.metrics.db_pool_size.set(pool_size as f64);
    state.metrics.db_pool_idle.set(pool_idle as f64);
    state
        .metrics
        .db_pool_active
        .set((pool_size - pool_idle) as f64);
    if response.extensions().get::<DbPoolTimeout>().is_some() {
        state.metrics.db_pool_acquire_timeouts_total.inc();
    }

    tracing::Span::current().record("http.status_code", status);

    response
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        state.metrics.encode(),
    )
        .into_response()
}

async fn serve_signing_cert(State(state): State<AppState>) -> impl IntoResponse {
    (
        [("content-type", "application/x-pem-file")],
        state.signer.cert_pem().to_string(),
    )
}

async fn healthz_live() -> impl IntoResponse {
    #[derive(serde::Serialize)]
    struct HealthzResponse {
        status: &'static str,
        version: &'static str,
    }
    (
        StatusCode::OK,
        axum::Json(HealthzResponse {
            status: "ok",
            version: env!("CARGO_PKG_VERSION"),
        }),
    )
}

async fn healthz_ready(State(state): State<AppState>) -> impl IntoResponse {
    #[derive(serde::Serialize)]
    struct HealthzResponse {
        status: &'static str,
        version: &'static str,
    }

    let db_ok = sqlx::query!("SELECT 1 AS ping")
        .fetch_one(&state.pool)
        .await
        .is_ok();

    if db_ok {
        (
            StatusCode::OK,
            axum::Json(HealthzResponse {
                status: "ok",
                version: env!("CARGO_PKG_VERSION"),
            }),
        )
            .into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(HealthzResponse {
                status: "degraded",
                version: env!("CARGO_PKG_VERSION"),
            }),
        )
            .into_response()
    }
}

fn start_queue_depth_scrape(pool: PgPool, metrics: Arc<Metrics>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match ops::queue_admin::all_queue_depths(&pool).await {
                Ok(snapshots) => {
                    for s in snapshots {
                        metrics
                            .queue_depth
                            .with_label_values(&[&s.queue_name])
                            .set(s.visible as f64);
                        metrics
                            .queue_in_flight
                            .with_label_values(&[&s.queue_name])
                            .set(s.in_flight as f64);
                    }
                }
                Err(e) => tracing::warn!(error = %e, "queue depth scrape failed"),
            }
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
    })
}
