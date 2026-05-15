pub mod cli;
pub mod config;
pub mod db;
pub mod error;
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
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{MatchedPath, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use sqlx::PgPool;
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
    /// Write coalescer for non-FIFO sends. None when LINGER_MS=0.
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

    let pool = db::connect(&config.database_url, config.max_connections).await?;

    let metrics = Arc::new(Metrics::new());

    let (coalescer, coalescer_handle) = if config.linger_ms > 0 {
        tracing::info!(linger_ms = config.linger_ms, "write coalescer enabled");
        let (c, h) = ops::coalesce::start(pool.clone(), config.linger_ms, metrics.clone());
        (Some(c), Some(h))
    } else {
        (None, None)
    };

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

    let signer = Arc::new(Signer::generate()?);
    let base_url: Arc<str> = config.base_url().into();
    let address = config.address.clone();
    let state = AppState {
        pool,
        config: Arc::new(config),
        base_url,
        coalescer,
        signer,
        metrics,
    };

    let app = build_router(state.clone());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    let tls = match (
        &state.config.tls_cert,
        &state.config.tls_key,
        &state.config.tls_ca,
    ) {
        (Some(cert), Some(key), Some(ca)) => Some((cert.clone(), key.clone(), ca.clone())),
        (None, None, None) => None,
        _ => anyhow::bail!(
            "BEYOND_TLS_CERT, BEYOND_TLS_KEY, and BEYOND_TLS_CA must all be set or all unset"
        ),
    };
    tracing::info!(address = %address, tls = tls.is_some(), "listening");
    if let Some((cert, key, ca)) = tls {
        serve_tls(listener, &cert, &key, &ca, app).await?;
    } else {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
    }

    tracing::info!("draining workers");

    scrape_handle.abort();
    let _ = scrape_handle.await;

    // Delivery: abort the task (lease-based design makes this abort-safe —
    // any mid-flight rows resurface after their lease expires).
    if let Some(h) = delivery_handle {
        h.abort();
        let _ = h.await;
    }

    // Schedule worker: abort is safe — the outer transaction either committed
    // (all advances persisted) or aborted (next iteration re-claims the rows).
    if let Some(h) = schedule_handle {
        h.abort();
        let _ = h.await;
    }

    // Coalescer: AppState (and the Coalescer sender) was dropped when axum::serve
    // returned, closing the channel. The task exits naturally on the next recv().
    if let Some(h) = coalescer_handle
        && tokio::time::timeout(Duration::from_secs(10), h)
            .await
            .is_err()
    {
        tracing::warn!("coalescer did not drain within shutdown deadline");
    }

    tracing::info!("shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            tracing::error!(error = %e, "ctrl+c handler failed");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => tracing::error!(error = %e, "failed to install SIGTERM handler"),
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received, draining connections");
}

async fn serve_tls(
    listener: tokio::net::TcpListener,
    cert_path: &str,
    key_path: &str,
    ca_path: &str,
    app: Router,
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
            _ = shutdown_signal() => break,
        }
    }
    Ok(())
}

pub async fn serve_with_listener(
    listener: tokio::net::TcpListener,
    tls: Option<(String, String, String)>,
    app: Router,
) -> anyhow::Result<()> {
    if let Some((cert, key, ca)) = tls {
        serve_tls(listener, &cert, &key, &ca, app).await
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
