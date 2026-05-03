use std::net::SocketAddr;

use sqlx::PgPool;
use tokio::net::TcpListener;

use crate::AppState;
use crate::config::Config;

pub struct TestServer {
    pub url: String,
    pub addr: SocketAddr,
}

pub async fn start(pool: PgPool, database_url: String) -> anyhow::Result<TestServer> {
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
    };

    let state = AppState {
        pool,
        config,
        coalescer: None,
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
