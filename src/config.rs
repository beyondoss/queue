use clap::Args;

#[derive(Debug, Clone, Args)]
pub struct Config {
    /// PostgreSQL connection string.
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// HTTP bind address.
    #[arg(long, env = "ADDRESS", default_value = "0.0.0.0:9324")]
    pub address: String,

    /// Default visibility timeout in seconds when client doesn't specify one.
    #[arg(long, env = "DEFAULT_VISIBILITY_TIMEOUT", default_value = "30")]
    pub default_visibility_timeout: i32,

    /// sqlx pool maximum connections.
    #[arg(long, env = "MAX_CONNECTIONS", default_value = "10")]
    pub max_connections: u32,

    /// Tracing filter directive (e.g. "info", "beyond_queue=debug").
    #[arg(long, env = "LOG_LEVEL", default_value = "info")]
    pub log_level: String,

    /// Enable OpenTelemetry OTLP export.
    #[arg(long, env = "OTLP_ENABLED", default_value = "false")]
    pub otlp_enabled: bool,

    /// OTLP gRPC collector endpoint.
    #[arg(long, env = "OTLP_ENDPOINT", default_value = "http://localhost:4317")]
    pub otlp_endpoint: String,

    /// OTLP trace sample rate (0.0 = never, 1.0 = always, 0.1 = 10%).
    /// Only effective when OTLP_ENABLED=true.
    #[arg(long, env = "OTLP_SAMPLE_RATE", default_value_t = 0.1)]
    pub otlp_sample_rate: f64,

    /// Write coalescing linger window in milliseconds.
    ///
    /// Non-FIFO sends are held for up to this duration and flushed as a single
    /// batch, turning N WAL fsyncs into 1. Set to 0 to disable coalescing.
    /// Tradeoff: up to LINGER_MS added tail latency; messages in flight are
    /// lost on crash (same as any in-flight request).
    #[arg(long, env = "LINGER_MS", default_value = "0")]
    pub linger_ms: u64,

    /// Public base URL used to construct SQS queue URLs returned to clients.
    /// Defaults to http://{address}.
    #[arg(long, env = "BASE_URL")]
    pub base_url_override: Option<String>,

    /// Enable HTTP/HTTPS webhook delivery worker.
    #[arg(long, env = "HTTP_DELIVERY_ENABLED", default_value = "true")]
    pub http_delivery_enabled: bool,

    /// Delivery worker poll interval in milliseconds.
    #[arg(long, env = "HTTP_DELIVERY_POLL_MS", default_value = "1000")]
    pub http_delivery_poll_ms: u64,

    /// Delivery worker per-request timeout in seconds.
    #[arg(long, env = "HTTP_DELIVERY_TIMEOUT_SECS", default_value = "5")]
    pub http_delivery_timeout_secs: u64,

    /// Delivery worker maximum rows to claim per poll cycle.
    #[arg(long, env = "HTTP_DELIVERY_BATCH_SIZE", default_value = "50")]
    pub http_delivery_batch_size: i64,

    /// Enable the schedule worker (cron / every / when triggers).
    #[arg(long, env = "SCHEDULE_ENABLED", default_value = "true")]
    pub schedule_enabled: bool,

    /// Schedule worker poll interval in milliseconds.
    ///
    /// Floor on fire latency. With the partial index `WHERE status = 'active'`
    /// an empty schedule table costs one sub-millisecond probe per poll.
    #[arg(long, env = "SCHEDULE_POLL_MS", default_value = "1000")]
    pub schedule_poll_ms: u64,

    /// Schedule worker maximum rows to claim per poll cycle.
    #[arg(long, env = "SCHEDULE_BATCH_SIZE", default_value = "32")]
    pub schedule_batch_size: i64,

    /// Number of upcoming fire timestamps to project in API responses
    /// (`next_fires` array on schedules and previews).
    #[arg(long, env = "SCHEDULE_PREVIEW_COUNT", default_value = "5")]
    pub schedule_preview_count: usize,

    /// Hard cap on `GET /v1/schedules` response size.
    #[arg(long, env = "SCHEDULE_LIST_MAX", default_value = "1000")]
    pub schedule_list_max: usize,

    /// Path to the PEM-encoded TLS certificate for this service.
    /// When all three BEYOND_TLS_* vars are set, the server switches to mTLS.
    #[arg(long, env = "BEYOND_TLS_CERT")]
    pub tls_cert: Option<String>,

    /// Path to the PEM-encoded TLS private key for this service.
    #[arg(long, env = "BEYOND_TLS_KEY")]
    pub tls_key: Option<String>,

    /// Path to the PEM-encoded CA certificate used to verify client certificates.
    #[arg(long, env = "BEYOND_TLS_CA")]
    pub tls_ca: Option<String>,
}

impl Config {
    pub fn base_url(&self) -> String {
        self.base_url_override
            .clone()
            .unwrap_or_else(|| format!("http://{}", self.address))
    }
}
