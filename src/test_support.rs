use std::net::SocketAddr;
use std::sync::Arc;

use sqlx::PgPool;
use tokio::net::TcpListener;

use crate::AppState;
use crate::config::Config;
use crate::ops::delivery;
use crate::signing::Signer;

pub struct TestServer {
    pub url: String,
    pub addr: SocketAddr,
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
        base_url_override: Some(format!("http://{addr}")),
        http_delivery_enabled: true,
        http_delivery_poll_ms: 50,
        http_delivery_timeout_secs: 5,
    };

    // Start delivery worker with fast poll for tests
    delivery::start(
        pool.clone(),
        delivery::DeliveryConfig {
            poll_interval_ms: 50,
            delivery_timeout_secs: 5,
            batch_size: 50,
        },
    );

    let base_url: Arc<str> = config.base_url().into();
    let signer = Arc::new(Signer::generate());
    let state = AppState {
        pool,
        config: Arc::new(config),
        base_url,
        coalescer: None,
        signer,
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
