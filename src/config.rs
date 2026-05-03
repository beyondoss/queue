use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(name = "beyond-queue")]
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
}

impl Config {
    pub fn base_url(&self) -> String {
        self.base_url_override
            .clone()
            .unwrap_or_else(|| format!("http://{}", self.address))
    }
}
