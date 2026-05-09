#[allow(unused_imports)]
use prometheus::{
    Counter, CounterVec, Encoder as _, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec,
    Opts, Registry, TextEncoder,
};

macro_rules! define_metrics {
    (
        $(#[$struct_meta:meta])*
        $vis:vis struct $name:ident {
            $(
                $metric_type:ident $field:ident($metric_name:literal)
                $([$($label:literal),+ $(,)?])?
                $(buckets = $buckets:expr)?
                => $help:literal
            ),* $(,)?
        }
    ) => {
        $(#[$struct_meta])*
        $vis struct $name {
            pub registry: Registry,
            $(pub $field: define_metrics!(@field_type $metric_type $([$($label),+])?),)*
        }

        impl $name {
            pub fn new() -> Self {
                let registry = Registry::new();
                $(
                    let $field = define_metrics!(
                        @create $metric_type $metric_name $help
                        $([$($label),+])?
                        $(buckets = $buckets)?
                    );
                    registry.register(Box::new($field.clone())).expect("metric not yet registered");
                )*
                Self { registry, $($field,)* }
            }

            #[allow(dead_code)]
            pub fn registry(&self) -> &Registry { &self.registry }

            pub fn encode(&self) -> String {
                let mut buf = Vec::new();
                TextEncoder::new().encode(&self.registry.gather(), &mut buf)
                    .expect("encoding to vec never fails");
                String::from_utf8(buf).expect("prometheus outputs valid utf-8")
            }
        }

        impl Default for $name {
            fn default() -> Self { Self::new() }
        }
    };

    (@field_type counter) => { Counter };
    (@field_type counter [$($label:literal),+]) => { CounterVec };
    (@field_type counter_vec) => { CounterVec };
    (@field_type counter_vec [$($label:literal),+]) => { CounterVec };
    (@field_type gauge) => { Gauge };
    (@field_type gauge [$($label:literal),+]) => { GaugeVec };
    (@field_type gauge_vec) => { GaugeVec };
    (@field_type gauge_vec [$($label:literal),+]) => { GaugeVec };
    (@field_type histogram) => { Histogram };
    (@field_type histogram [$($label:literal),+]) => { HistogramVec };
    (@field_type histogram_vec) => { HistogramVec };
    (@field_type histogram_vec [$($label:literal),+]) => { HistogramVec };

    (@create counter $name:literal $help:literal) => {
        Counter::new($name, $help).expect("valid metric")
    };
    (@create counter $name:literal $help:literal [$($label:literal),+]) => {
        CounterVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create counter_vec $name:literal $help:literal [$($label:literal),+]) => {
        CounterVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create gauge $name:literal $help:literal) => {
        Gauge::new($name, $help).expect("valid metric")
    };
    (@create gauge $name:literal $help:literal [$($label:literal),+]) => {
        GaugeVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create gauge_vec $name:literal $help:literal [$($label:literal),+]) => {
        GaugeVec::new(Opts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal) => {
        Histogram::with_opts(HistogramOpts::new($name, $help)).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal buckets = $buckets:expr) => {
        Histogram::with_opts(
            HistogramOpts::new($name, $help).buckets($buckets.to_vec())
        ).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal [$($label:literal),+]) => {
        HistogramVec::new(HistogramOpts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create histogram $name:literal $help:literal [$($label:literal),+] buckets = $buckets:expr) => {
        HistogramVec::new(
            HistogramOpts::new($name, $help).buckets($buckets.to_vec()),
            &[$($label),+],
        ).expect("valid metric")
    };
    (@create histogram_vec $name:literal $help:literal [$($label:literal),+]) => {
        HistogramVec::new(HistogramOpts::new($name, $help), &[$($label),+]).expect("valid metric")
    };
    (@create histogram_vec $name:literal $help:literal [$($label:literal),+] buckets = $buckets:expr) => {
        HistogramVec::new(
            HistogramOpts::new($name, $help).buckets($buckets.to_vec()),
            &[$($label),+],
        ).expect("valid metric")
    };
}

// Bucket sets grouped by operation latency profile.

/// HTTP request latency — from 5ms fast-path to 2.5s slow requests.
const HTTP_BUCKETS: &[f64] = &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5];
/// Database-backed operations — 1ms fast-path through 1s slow queries.
const DB_OP_BUCKETS: &[f64] = &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0];
/// Outbound HTTP webhook delivery — 50ms minimum round-trip to 10s timeout.
const DELIVERY_BUCKETS: &[f64] = &[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0];

define_metrics! {
    pub struct Metrics {
        counter_vec http_requests_total("http_requests_total")["method", "path", "status"]
            => "Total HTTP requests",
        histogram_vec http_request_duration_seconds("http_request_duration_seconds")["method", "path"]
            buckets = HTTP_BUCKETS
            => "HTTP request duration in seconds",
        gauge http_connections_active("http_connections_active")
            => "HTTP requests currently in flight",
        counter_vec messages_sent_total("queue_messages_sent_total")["queue"]
            => "Total messages enqueued",
        counter_vec messages_received_total("queue_messages_received_total")["queue"]
            => "Total messages delivered to consumers",
        counter_vec messages_deleted_total("queue_messages_deleted_total")["queue"]
            => "Total messages deleted (acknowledged)",
        counter_vec messages_redelivered_total("queue_messages_redelivered_total")["queue"]
            => "Total messages received with read_count > 1 (consumer did not ack before vt expiry)",
        histogram_vec message_send_duration_seconds("queue_message_send_duration_seconds")["queue"]
            buckets = DB_OP_BUCKETS
            => "Message send operation duration in seconds",
        histogram_vec message_receive_duration_seconds("queue_message_receive_duration_seconds")["queue"]
            buckets = DB_OP_BUCKETS
            => "Message receive operation duration in seconds",
        histogram_vec message_delete_duration_seconds("queue_message_delete_duration_seconds")["queue"]
            buckets = DB_OP_BUCKETS
            => "Message delete operation duration in seconds",
        histogram_vec message_age_at_receive_seconds("queue_message_age_at_receive_seconds")["queue"]
            buckets = [0.1, 0.5, 1.0, 5.0, 15.0, 30.0, 60.0, 300.0, 900.0, 3600.0]
            => "Message age when received (time from enqueue to first delivery) in seconds",
        counter_vec delivery_attempts_total("queue_delivery_attempts_total")["outcome"]
            => "HTTP webhook delivery attempts by outcome (success|failure)",
        histogram_vec delivery_attempt_duration_seconds("queue_delivery_attempt_duration_seconds")["outcome"]
            buckets = DELIVERY_BUCKETS
            => "HTTP webhook delivery attempt duration in seconds",
        counter delivery_exhausted_total("queue_delivery_exhausted_total")
            => "Webhook deliveries permanently abandoned after exhausting max_attempts",
        histogram coalescer_flush_batch_size("queue_coalescer_flush_batch_size")
            buckets = [1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 500.0, 1000.0]
            => "Number of messages per coalescer flush batch",
        gauge_vec queue_depth("queue_depth")["queue"]
            => "Current number of visible messages in the queue",
        gauge_vec queue_in_flight("queue_in_flight")["queue"]
            => "Current number of in-flight (consumer-locked or delayed) messages",
        gauge db_pool_size("db_pool_size")
            => "Current total connections in the database pool (idle + active)",
        gauge db_pool_idle("db_pool_idle")
            => "Current idle connections in the database pool",
        gauge db_pool_active("db_pool_active")
            => "Current active (checked-out) connections in the database pool",
        counter db_pool_acquire_timeouts_total("db_pool_acquire_timeouts_total")
            => "Total database pool acquire timeout errors (pool exhausted)",
    }
}
