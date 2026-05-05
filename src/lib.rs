pub mod cli;
pub mod config;
pub mod db;
pub mod error;
pub mod middleware;
pub mod ops;
pub mod routes;
pub mod signing;
pub mod sns;
pub mod sqs;
pub mod test_support;

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::middleware::from_fn;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use sqlx::PgPool;
use tracing_subscriber::EnvFilter;

use config::Config;
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
}

/// Parse an AWS service request body: returns (is_json, action_name, parsed_body).
/// `service_prefix` is e.g. `"AmazonSQS."` or `"AmazonSNS."`.
pub fn parse_service_body(
    headers: &HeaderMap,
    body: &Bytes,
    service_prefix: &str,
) -> (bool, String, serde_json::Value) {
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
        let value = serde_json::from_slice(body).unwrap_or(serde_json::json!({}));
        (true, action, value)
    } else {
        let map: HashMap<String, String> = form_urlencoded::parse(body).into_owned().collect();
        let action = map.get("Action").cloned().unwrap_or_default();
        let value = serde_json::to_value(&map).unwrap_or(serde_json::json!({}));
        (false, action, value)
    }
}

pub async fn serve(config: Config) -> anyhow::Result<()> {
    init_tracing(&config);

    let pool = db::connect(&config.database_url, config.max_connections).await?;

    let mut _coalescer_handle = None;
    let coalescer = if config.linger_ms > 0 {
        tracing::info!(linger_ms = config.linger_ms, "write coalescer enabled");
        let (c, h) = ops::coalesce::start(pool.clone(), config.linger_ms);
        _coalescer_handle = Some(h);
        Some(c)
    } else {
        None
    };

    let mut _delivery_handle = None;
    if config.http_delivery_enabled {
        tracing::info!("HTTP delivery worker enabled");
        _delivery_handle = Some(ops::delivery::start(
            pool.clone(),
            ops::delivery::DeliveryConfig {
                poll_interval_ms: config.http_delivery_poll_ms,
                delivery_timeout_secs: config.http_delivery_timeout_secs,
                batch_size: 50,
            },
        )?);
    }

    let signer = Arc::new(Signer::generate());
    let base_url: Arc<str> = config.base_url().into();
    let address = config.address.clone();
    let state = AppState {
        pool,
        config: Arc::new(config),
        base_url,
        coalescer,
        signer,
    };

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(&address).await?;
    tracing::info!(address = %address, "listening");
    axum::serve(listener, app).await?;
    Ok(())
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

pub fn build_router(state: AppState) -> Router {
    use axum::Json;
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
        .layer(from_fn(middleware::auth::require_auth));

    Router::new()
        .merge(api)
        .route("/healthz", get(healthz))
        .route("/SimpleNotificationService.pem", get(serve_signing_cert))
        .with_state(state)
}

async fn serve_signing_cert(State(state): State<AppState>) -> impl IntoResponse {
    (
        [("content-type", "application/x-pem-file")],
        state.signer.cert_pem().to_string(),
    )
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
