pub mod config;
pub mod db;
pub mod error;
pub mod middleware;
pub mod ops;
pub mod routes;
pub mod sns;
pub mod sqs;
pub mod test_support;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::middleware::from_fn;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use clap::Parser;
use sqlx::PgPool;
use tracing_subscriber::EnvFilter;

use config::Config;
use ops::coalesce::Coalescer;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub config: Config,
    /// Write coalescer for non-FIFO sends. None when LINGER_MS=0.
    pub coalescer: Option<Coalescer>,
}

pub async fn run() -> anyhow::Result<()> {
    let config = Config::parse();

    init_tracing(&config);

    let pool = db::connect(&config.database_url, config.max_connections).await?;

    let coalescer = if config.linger_ms > 0 {
        tracing::info!(linger_ms = config.linger_ms, "write coalescer enabled");
        Some(ops::coalesce::start(pool.clone(), config.linger_ms))
    } else {
        None
    };

    let state = AppState {
        pool,
        config: config.clone(),
        coalescer,
    };

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(&config.address).await?;
    tracing::info!(address = %config.address, "listening");
    axum::serve(listener, app).await?;
    Ok(())
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
        sns::handle_service_request(state, headers, body).await
    } else {
        sqs::handle_service_request(state, headers, body).await
    }
}

pub fn build_router(state: AppState) -> Router {
    let api = Router::new()
        .nest("/v1", routes::router())
        .route("/", post(gateway_handler))
        .merge(sqs::router())
        .layer(from_fn(middleware::auth::require_auth));

    Router::new()
        .merge(api)
        .route("/healthz", get(healthz))
        .with_state(state)
}

async fn healthz() -> impl IntoResponse {
    axum::http::StatusCode::OK
}

fn init_tracing(config: &Config) {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&config.log_level))
        .json()
        .init();
}
