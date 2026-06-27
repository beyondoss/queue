use std::net::SocketAddr;
use std::sync::Arc;

use sqlx::PgPool;
use tokio::net::TcpListener;

use crate::AppState;
use crate::config::Config;
use crate::ops::{delivery, schedule_worker};
use crate::signing::Signer;

pub struct TestServer {
    pub url: String,
    pub addr: SocketAddr,
}

/// Start a test server with the write coalescer enabled.
/// Uses the provided pool directly; no delivery worker is started.
pub async fn start_with_coalescer(pool: PgPool, linger_ms: u64) -> anyhow::Result<TestServer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let config = Config {
        database_url: "postgres://test".into(),
        address: addr.to_string(),
        default_visibility_timeout: 30,
        max_connections: 5,
        linger_ms,
        log_level: "error".into(),
        otlp_enabled: false,
        otlp_endpoint: "http://localhost:4317".into(),
        otlp_sample_rate: 0.1,
        base_url_override: Some(format!("http://{addr}")),
        http_delivery_enabled: false,
        http_delivery_poll_ms: 50,
        http_delivery_timeout_secs: 5,
        http_delivery_batch_size: 50,
        schedule_enabled: false,
        schedule_poll_ms: 100,
        schedule_batch_size: 32,
        schedule_preview_count: 5,
        schedule_list_max: 1000,
        tls_cert: None,
        tls_key: None,
        tls_ca: None,
        handoff_state_dir: std::path::PathBuf::from("/tmp"),
        handoff_socket_path: std::path::PathBuf::from("/tmp/queue-test-unused.sock"),
    };

    let metrics = Arc::new(crate::metrics::Metrics::new());
    let (coalescer, _) = crate::ops::coalesce::start(pool.clone(), linger_ms, metrics.clone());
    let base_url: Arc<str> = config.base_url().into();
    let signer = Arc::new(Signer::generate()?);
    let state = AppState {
        pool,
        config: Arc::new(config),
        base_url,
        coalescer: Some(coalescer),
        signer,
        metrics,
        delivery_notify: Arc::new(tokio::sync::Notify::new()),
        schedule_notify: Arc::new(tokio::sync::Notify::new()),
    };
    let app = crate::build_router(state);

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    Ok(TestServer {
        url: format!("http://{addr}"),
        addr,
    })
}

pub async fn start(pool: PgPool, database_url: String) -> anyhow::Result<TestServer> {
    // Initialize tracing for tests — ignore errors if already initialized.
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::ERROR)
        .try_init();

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let config = Config {
        database_url,
        address: addr.to_string(),
        default_visibility_timeout: 30,
        max_connections: 5,
        linger_ms: 0,
        log_level: "error".into(),
        otlp_enabled: false,
        otlp_endpoint: "http://localhost:4317".into(),
        otlp_sample_rate: 0.1,
        base_url_override: Some(format!("http://{addr}")),
        http_delivery_enabled: true,
        http_delivery_poll_ms: 50,
        http_delivery_timeout_secs: 5,
        http_delivery_batch_size: 50,
        schedule_enabled: true,
        schedule_poll_ms: 100,
        schedule_batch_size: 32,
        schedule_preview_count: 5,
        schedule_list_max: 1000,
        tls_cert: None,
        tls_key: None,
        tls_ca: None,
        handoff_state_dir: std::path::PathBuf::from("/tmp"),
        handoff_socket_path: std::path::PathBuf::from("/tmp/queue-test-unused.sock"),
    };

    // Shared in-process wakeups: the same handles the route handlers poke must
    // be the ones the workers wait on, so the test server wires them together.
    let delivery_notify = Arc::new(tokio::sync::Notify::new());
    let schedule_notify = Arc::new(tokio::sync::Notify::new());

    // Start delivery worker with fast poll for tests; detach the handle since
    // tokio keeps the task alive until the runtime shuts down.
    drop(delivery::start(
        pool.clone(),
        delivery::DeliveryConfig {
            poll_interval_ms: 50,
            delivery_timeout_secs: 5,
            batch_size: 50,
        },
        Arc::new(crate::metrics::Metrics::new()),
        delivery_notify.clone(),
    )?);

    drop(schedule_worker::start(
        pool.clone(),
        schedule_worker::ScheduleWorkerConfig {
            poll_interval_ms: 100,
            batch_size: 32,
        },
        Arc::new(crate::metrics::Metrics::new()),
        schedule_notify.clone(),
        delivery_notify.clone(),
    ));

    let base_url: Arc<str> = config.base_url().into();
    let signer = Arc::new(Signer::generate()?);
    let state = AppState {
        pool,
        config: Arc::new(config),
        base_url,
        coalescer: None,
        signer,
        metrics: Arc::new(crate::metrics::Metrics::new()),
        delivery_notify,
        schedule_notify,
    };
    let app = crate::build_router(state);

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    Ok(TestServer {
        url: format!("http://{addr}"),
        addr,
    })
}
