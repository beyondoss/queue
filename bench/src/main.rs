use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;

#[derive(Parser)]
#[command(name = "bench")]
struct Args {
    #[command(subcommand)]
    command: Command,

    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Set synchronous_commit = off on every pool connection (async-commit treatment).
    #[arg(long)]
    async_commit: bool,
}

#[derive(Clone, Copy, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Profile {
    /// ~5k msgs per scenario, fast feedback.
    Quick,
    /// ~100k msgs per scenario, stable numbers.
    Full,
}

#[derive(Subcommand)]
enum Command {
    /// Benchmark send throughput (single or batch).
    Send {
        #[arg(long, default_value = "bench_send")]
        queue: String,
        #[arg(long, default_value = "10000")]
        count: u64,
        #[arg(long, default_value = "1")]
        concurrency: usize,
        /// Messages per send call. >1 uses pgmq._send_batch.
        #[arg(long, default_value = "1")]
        batch_size: usize,
    },
    /// Benchmark receive throughput (drains a pre-filled queue).
    Receive {
        #[arg(long, default_value = "bench_recv")]
        queue: String,
        #[arg(long, default_value = "10000")]
        count: u64,
        #[arg(long, default_value = "1")]
        concurrency: usize,
    },
    /// Benchmark end-to-end round trip: send + delete per message.
    RoundTrip {
        #[arg(long, default_value = "bench_rt")]
        queue: String,
        #[arg(long, default_value = "1000")]
        count: u64,
        #[arg(long, default_value = "1")]
        concurrency: usize,
    },
    /// Run the full scenario matrix and optionally write results to JSON.
    RunAll {
        #[arg(long, default_value = "quick")]
        profile: Profile,
        /// Write results to this JSON file for later comparison.
        #[arg(long)]
        output: Option<String>,
        /// Also run FIFO scenarios (read_fifo / send_fifo).
        #[arg(long)]
        fifo: bool,
        /// Also run OSS grouped scenarios against this URL (read_grouped_rr).
        /// If provided, runs pgmq.read_grouped_rr on this separate database.
        #[arg(long)]
        oss_url: Option<String>,
    },
    /// Print a diff table comparing two run-all JSON output files.
    Compare {
        before: String,
        after: String,
    },
}

#[derive(Serialize, Deserialize)]
struct BenchResult {
    scenario: String,
    async_commit: bool,
    total_ops: u64,
    elapsed_secs: f64,
    msgs_per_sec: f64,
    p50_us: u64,
    p99_us: u64,
    p999_us: u64,
}

// ---------------------------------------------------------------------------
// entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Compare needs no DB connection.
    if let Command::Compare { ref before, ref after } = args.command {
        return compare(before, after);
    }

    let url = args.database_url.context("DATABASE_URL is required")?;
    let pool_size = (peak_concurrency(&args.command) + 4) as u32;
    let pool = Arc::new(
        build_pool(&url, pool_size, args.async_commit)
            .await
            .context("failed to connect to database")?,
    );

    match args.command {
        Command::Send { queue, count, concurrency, batch_size } => {
            let r = run_send(&pool, &queue, count, concurrency, batch_size, args.async_commit).await?;
            print_results(&[r]);
        }
        Command::Receive { queue, count, concurrency } => {
            let r = run_receive(&pool, &queue, count, concurrency, args.async_commit).await?;
            print_results(&[r]);
        }
        Command::RoundTrip { queue, count, concurrency } => {
            let r = run_round_trip(&pool, &queue, count, concurrency, args.async_commit).await?;
            print_results(&[r]);
        }
        Command::RunAll { profile, output, fifo, oss_url } => {
            let oss_pool = if let Some(ref u) = oss_url {
                let p = build_pool(u, 36, args.async_commit)
                    .await
                    .context("failed to connect to oss database")?;
                Some(Arc::new(p))
            } else {
                None
            };
            let results = run_all(&pool, oss_pool.as_ref(), profile, fifo, args.async_commit).await?;
            print_results(&results);
            if let Some(path) = output {
                std::fs::write(&path, serde_json::to_string_pretty(&results)?)?;
                tracing::info!(path, "results written");
            }
        }
        Command::Compare { .. } => unreachable!(),
    }

    Ok(())
}

fn peak_concurrency(cmd: &Command) -> usize {
    match cmd {
        Command::Send { concurrency, .. } => *concurrency,
        Command::Receive { concurrency, .. } => *concurrency,
        Command::RoundTrip { concurrency, .. } => *concurrency,
        Command::RunAll { .. } => 32,
        Command::Compare { .. } => 0,
    }
}

// ---------------------------------------------------------------------------
// pool
// ---------------------------------------------------------------------------

async fn build_pool(url: &str, max: u32, async_commit: bool) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(max)
        .after_connect(move |conn, _| {
            Box::pin(async move {
                // Suppress pgmq.create "relation already exists" notices.
                sqlx::query("SET client_min_messages = WARNING")
                    .execute(&mut *conn)
                    .await?;
                if async_commit {
                    sqlx::query("SET synchronous_commit = off")
                        .execute(&mut *conn)
                        .await?;
                }
                Ok(())
            })
        })
        .connect(url)
        .await
        .map_err(Into::into)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

async fn ensure_queue(pool: &PgPool, queue: &str) -> Result<()> {
    sqlx::query("SELECT pgmq.create($1)").bind(queue).execute(pool).await?;
    Ok(())
}

async fn ensure_fifo_queue(pool: &PgPool, queue: &str) -> Result<()> {
    sqlx::query("SELECT pgmq.create_fifo($1)").bind(queue).execute(pool).await?;
    Ok(())
}

async fn purge_queue(pool: &PgPool, queue: &str) -> Result<()> {
    sqlx::query("SELECT pgmq.purge_queue($1)").bind(queue).execute(pool).await?;
    Ok(())
}

async fn warmup(pool: &PgPool, queue: &str, n: u32) -> Result<()> {
    for _ in 0..n {
        let (msg_id,): (i64,) = sqlx::query_as(
            "SELECT pgmq.send($1, '{\"w\":1}'::jsonb, NULL::jsonb, clock_timestamp())",
        )
        .bind(queue)
        .fetch_one(pool)
        .await?;
        sqlx::query("SELECT pgmq.delete($1, $2)").bind(queue).bind(msg_id).execute(pool).await?;
    }
    Ok(())
}

/// Fill queue with `count` messages using batch inserts of 500.
async fn fill_queue(pool: &PgPool, queue: &str, count: u64) -> Result<()> {
    const BATCH: usize = 500;
    let msgs: Vec<serde_json::Value> = (0..BATCH).map(|i| serde_json::json!({"b": i})).collect();

    let full = count as usize / BATCH;
    let rem = count as usize % BATCH;

    for _ in 0..full {
        sqlx::query(
            "SELECT pgmq._send_batch($1, $2::jsonb[], NULL::jsonb[], clock_timestamp())",
        )
        .bind(queue)
        .bind(&msgs)
        .execute(pool)
        .await?;
    }
    if rem > 0 {
        let tail: Vec<serde_json::Value> = (0..rem).map(|i| serde_json::json!({"b": i})).collect();
        sqlx::query(
            "SELECT pgmq._send_batch($1, $2::jsonb[], NULL::jsonb[], clock_timestamp())",
        )
        .bind(queue)
        .bind(&tail)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// Fill a FIFO queue: `groups` round-robin group IDs, total `count` messages.
async fn fill_fifo_queue(pool: &PgPool, queue: &str, count: u64, groups: usize) -> Result<()> {
    const BATCH: usize = 100;
    let mut inserted = 0u64;
    while inserted < count {
        let n = ((count - inserted) as usize).min(BATCH);
        for i in 0..n {
            let gid = format!("g{}", (inserted as usize + i) % groups);
            sqlx::query(
                "SELECT pgmq.send_fifo($1, '{\"b\":1}'::jsonb, $2)",
            )
            .bind(queue)
            .bind(&gid)
            .execute(pool)
            .await?;
        }
        inserted += n as u64;
    }
    Ok(())
}

/// Fill a standard queue with header-based groups for OSS read_grouped_rr comparison.
async fn fill_grouped_queue(pool: &PgPool, queue: &str, count: u64, groups: usize) -> Result<()> {
    const BATCH: usize = 100;
    let mut inserted = 0u64;
    while inserted < count {
        let n = ((count - inserted) as usize).min(BATCH);
        for i in 0..n {
            let gid = format!("g{}", (inserted as usize + i) % groups);
            let headers = serde_json::json!({"x-pgmq-group": gid});
            sqlx::query(
                "SELECT pgmq.send($1, '{\"b\":1}'::jsonb, $2::jsonb, clock_timestamp())",
            )
            .bind(queue)
            .bind(&headers)
            .execute(pool)
            .await?;
        }
        inserted += n as u64;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// send
// ---------------------------------------------------------------------------

async fn run_send(
    pool: &Arc<PgPool>,
    queue: &str,
    count: u64,
    concurrency: usize,
    batch_size: usize,
    async_commit: bool,
) -> Result<BenchResult> {
    ensure_queue(pool, queue).await?;
    warmup(pool, queue, 20).await?;
    purge_queue(pool, queue).await?;

    let total_batches = count.div_ceil(batch_size as u64) as usize;
    let ops_per_worker = total_batches.div_ceil(concurrency);
    let total_msgs = ops_per_worker * concurrency * batch_size;

    let start = Instant::now();
    let mut set: JoinSet<Result<Vec<u64>>> = JoinSet::new();

    for _ in 0..concurrency {
        let pool = Arc::clone(pool);
        let queue = queue.to_string();
        set.spawn(async move {
            let mut samples = Vec::with_capacity(ops_per_worker);
            for _ in 0..ops_per_worker {
                let t = Instant::now();
                if batch_size == 1 {
                    sqlx::query(
                        "SELECT pgmq.send($1, '{\"b\":1}'::jsonb, NULL::jsonb, clock_timestamp())",
                    )
                    .bind(&queue)
                    .execute(pool.as_ref())
                    .await?;
                } else {
                    let msgs: Vec<serde_json::Value> =
                        (0..batch_size).map(|i| serde_json::json!({"b": i})).collect();
                    sqlx::query(
                        "SELECT pgmq._send_batch($1, $2::jsonb[], NULL::jsonb[], clock_timestamp())",
                    )
                    .bind(&queue)
                    .bind(&msgs)
                    .execute(pool.as_ref())
                    .await?;
                }
                samples.push(t.elapsed().as_micros() as u64);
            }
            Ok(samples)
        });
    }

    let mut hist = Histogram::<u64>::new(3)?;
    while let Some(result) = set.join_next().await {
        for us in result?? {
            hist.record(us)?;
        }
    }

    let elapsed = start.elapsed();
    let scenario = if batch_size == 1 {
        format!("send c={concurrency}")
    } else {
        format!("send c={concurrency} b={batch_size}")
    };

    Ok(BenchResult {
        scenario,
        async_commit,
        total_ops: total_msgs as u64,
        elapsed_secs: elapsed.as_secs_f64(),
        msgs_per_sec: total_msgs as f64 / elapsed.as_secs_f64(),
        p50_us: hist.value_at_quantile(0.50),
        p99_us: hist.value_at_quantile(0.99),
        p999_us: hist.value_at_quantile(0.999),
    })
}

// ---------------------------------------------------------------------------
// send fifo
// ---------------------------------------------------------------------------

async fn run_send_fifo(
    pool: &Arc<PgPool>,
    queue: &str,
    count: u64,
    concurrency: usize,
    groups: usize,
    async_commit: bool,
) -> Result<BenchResult> {
    ensure_fifo_queue(pool, queue).await?;
    purge_queue(pool, queue).await?;

    let per_worker = count.div_ceil(concurrency as u64) as usize;
    let total_msgs = per_worker * concurrency;

    let start = Instant::now();
    let mut set: JoinSet<Result<Vec<u64>>> = JoinSet::new();

    for w in 0..concurrency {
        let pool = Arc::clone(pool);
        let queue = queue.to_string();
        set.spawn(async move {
            let mut samples = Vec::with_capacity(per_worker);
            for i in 0..per_worker {
                let gid = format!("g{}", (w * per_worker + i) % groups);
                let t = Instant::now();
                sqlx::query("SELECT pgmq.send_fifo($1, '{\"b\":1}'::jsonb, $2)")
                    .bind(&queue)
                    .bind(&gid)
                    .execute(pool.as_ref())
                    .await?;
                samples.push(t.elapsed().as_micros() as u64);
            }
            Ok(samples)
        });
    }

    let mut hist = Histogram::<u64>::new(3)?;
    while let Some(result) = set.join_next().await {
        for us in result?? {
            hist.record(us)?;
        }
    }

    let elapsed = start.elapsed();
    Ok(BenchResult {
        scenario: format!("fifo-send c={concurrency} g={groups}"),
        async_commit,
        total_ops: total_msgs as u64,
        elapsed_secs: elapsed.as_secs_f64(),
        msgs_per_sec: total_msgs as f64 / elapsed.as_secs_f64(),
        p50_us: hist.value_at_quantile(0.50),
        p99_us: hist.value_at_quantile(0.99),
        p999_us: hist.value_at_quantile(0.999),
    })
}

// ---------------------------------------------------------------------------
// receive
// ---------------------------------------------------------------------------

async fn run_receive(
    pool: &Arc<PgPool>,
    queue: &str,
    count: u64,
    concurrency: usize,
    async_commit: bool,
) -> Result<BenchResult> {
    ensure_queue(pool, queue).await?;
    purge_queue(pool, queue).await?;
    fill_queue(pool.as_ref(), queue, count + 200).await?;

    let per_worker = count / concurrency as u64;
    let start = Instant::now();

    let mut set: JoinSet<Result<(u64, Vec<u64>)>> = JoinSet::new();
    for _ in 0..concurrency {
        let pool = Arc::clone(pool);
        let queue = queue.to_string();
        set.spawn(async move {
            let mut samples = Vec::new();
            let mut received = 0u64;
            while received < per_worker {
                let want = (per_worker - received).min(10) as i32;
                let t = Instant::now();
                let rows: Vec<(i64,)> =
                    sqlx::query_as("SELECT msg_id FROM pgmq.read($1, 30, $2)")
                        .bind(&queue)
                        .bind(want)
                        .fetch_all(pool.as_ref())
                        .await?;
                if rows.is_empty() {
                    break;
                }
                let n = rows.len() as u64;
                let us_per_msg = t.elapsed().as_micros() as u64 / n;
                for _ in 0..n {
                    samples.push(us_per_msg);
                }
                received += n;
            }
            Ok((received, samples))
        });
    }

    let mut hist = Histogram::<u64>::new(3)?;
    let mut total = 0u64;
    while let Some(result) = set.join_next().await {
        let (n, samples) = result??;
        total += n;
        for us in samples {
            hist.record(us)?;
        }
    }

    let elapsed = start.elapsed();
    Ok(BenchResult {
        scenario: format!("receive c={concurrency}"),
        async_commit,
        total_ops: total,
        elapsed_secs: elapsed.as_secs_f64(),
        msgs_per_sec: total as f64 / elapsed.as_secs_f64(),
        p50_us: hist.value_at_quantile(0.50),
        p99_us: hist.value_at_quantile(0.99),
        p999_us: hist.value_at_quantile(0.999),
    })
}

// ---------------------------------------------------------------------------
// receive — sharded (simulates partitioned queue: one queue per worker)
// ---------------------------------------------------------------------------
//
// Measures the upper bound of what hash-partitioned queue tables would provide:
// zero inter-worker heap-page contention. Compare against run_receive at the
// same concurrency; the gap is what partitioning could buy.

async fn run_receive_sharded(
    pool: &Arc<PgPool>,
    base_queue: &str,
    count: u64,
    shards: usize,
    async_commit: bool,
) -> Result<BenchResult> {
    let per_shard = count / shards as u64;
    for s in 0..shards {
        let qname = format!("{base_queue}_s{s}");
        ensure_queue(pool, &qname).await?;
        purge_queue(pool, &qname).await?;
        fill_queue(pool.as_ref(), &qname, per_shard + 100).await?;
    }

    let start = Instant::now();
    let mut set: JoinSet<Result<(u64, Vec<u64>)>> = JoinSet::new();

    for s in 0..shards {
        let pool = Arc::clone(pool);
        let queue = format!("{base_queue}_s{s}");
        set.spawn(async move {
            let mut samples = Vec::new();
            let mut received = 0u64;
            while received < per_shard {
                let want = (per_shard - received).min(10) as i32;
                let t = Instant::now();
                let rows: Vec<(i64,)> =
                    sqlx::query_as("SELECT msg_id FROM pgmq.read($1, 30, $2)")
                        .bind(&queue)
                        .bind(want)
                        .fetch_all(pool.as_ref())
                        .await?;
                if rows.is_empty() {
                    break;
                }
                let n = rows.len() as u64;
                let us_per_msg = t.elapsed().as_micros() as u64 / n;
                for _ in 0..n {
                    samples.push(us_per_msg);
                }
                received += n;
            }
            Ok((received, samples))
        });
    }

    let mut hist = Histogram::<u64>::new(3)?;
    let mut total = 0u64;
    while let Some(result) = set.join_next().await {
        let (n, samples) = result??;
        total += n;
        for us in samples {
            hist.record(us)?;
        }
    }

    let elapsed = start.elapsed();
    Ok(BenchResult {
        scenario: format!("receive-sharded c={shards}"),
        async_commit,
        total_ops: total,
        elapsed_secs: elapsed.as_secs_f64(),
        msgs_per_sec: total as f64 / elapsed.as_secs_f64(),
        p50_us: hist.value_at_quantile(0.50),
        p99_us: hist.value_at_quantile(0.99),
        p999_us: hist.value_at_quantile(0.999),
    })
}

// ---------------------------------------------------------------------------
// receive fifo — read + delete cycle
//
// FIFO queues enforce exclusive group processing: once messages from group G
// are in-flight (vt > now), no other consumer can read from G. A simple
// "drain with long VT" test stalls once all groups are locked. Instead we
// measure read + immediate delete, which models real FIFO processing and keeps
// groups available throughout the run.
// ---------------------------------------------------------------------------

async fn run_receive_fifo(
    pool: &Arc<PgPool>,
    queue: &str,
    count: u64,
    concurrency: usize,
    groups: usize,
    async_commit: bool,
) -> Result<BenchResult> {
    ensure_fifo_queue(pool, queue).await?;
    purge_queue(pool, queue).await?;
    fill_fifo_queue(pool.as_ref(), queue, count + 200, groups).await?;

    let per_worker = count / concurrency as u64;
    let start = Instant::now();

    let mut set: JoinSet<Result<(u64, Vec<u64>)>> = JoinSet::new();
    for _ in 0..concurrency {
        let pool = Arc::clone(pool);
        let queue = queue.to_string();
        set.spawn(async move {
            let mut samples = Vec::new();
            let mut received = 0u64;
            while received < per_worker {
                let want = (per_worker - received).min(10) as i32;
                let t = Instant::now();
                let rows: Vec<(i64,)> =
                    sqlx::query_as("SELECT msg_id FROM pgmq.read_fifo($1, 30, $2)")
                        .bind(&queue)
                        .bind(want)
                        .fetch_all(pool.as_ref())
                        .await?;
                if rows.is_empty() {
                    break;
                }
                let ids: Vec<i64> = rows.iter().map(|(id,)| *id).collect();
                sqlx::query("SELECT pgmq.delete($1, $2::bigint[])")
                    .bind(&queue)
                    .bind(&ids)
                    .execute(pool.as_ref())
                    .await?;
                let n = ids.len() as u64;
                let us_per_msg = t.elapsed().as_micros() as u64 / n;
                for _ in 0..n {
                    samples.push(us_per_msg);
                }
                received += n;
            }
            Ok((received, samples))
        });
    }

    let mut hist = Histogram::<u64>::new(3)?;
    let mut total = 0u64;
    while let Some(result) = set.join_next().await {
        let (n, samples) = result??;
        total += n;
        for us in samples {
            hist.record(us)?;
        }
    }

    let elapsed = start.elapsed();
    Ok(BenchResult {
        scenario: format!("fifo-recv c={concurrency} g={groups}"),
        async_commit,
        total_ops: total,
        elapsed_secs: elapsed.as_secs_f64(),
        msgs_per_sec: total as f64 / elapsed.as_secs_f64(),
        p50_us: hist.value_at_quantile(0.50),
        p99_us: hist.value_at_quantile(0.99),
        p999_us: hist.value_at_quantile(0.999),
    })
}

// ---------------------------------------------------------------------------
// receive — OSS pgmq read_grouped_rr (header-based groups)
//
// Same read+delete cycle as run_receive_fifo: OSS grouped reads also lock
// groups via in-flight VT, so we delete immediately to keep groups available.
// ---------------------------------------------------------------------------

async fn run_receive_grouped_rr(
    pool: &Arc<PgPool>,
    queue: &str,
    count: u64,
    concurrency: usize,
    groups: usize,
    label_prefix: &str,
    async_commit: bool,
) -> Result<BenchResult> {
    ensure_queue(pool, queue).await?;
    purge_queue(pool, queue).await?;
    fill_grouped_queue(pool.as_ref(), queue, count + 200, groups).await?;

    let per_worker = count / concurrency as u64;
    let start = Instant::now();

    let mut set: JoinSet<Result<(u64, Vec<u64>)>> = JoinSet::new();
    for _ in 0..concurrency {
        let pool = Arc::clone(pool);
        let queue = queue.to_string();
        set.spawn(async move {
            let mut samples = Vec::new();
            let mut received = 0u64;
            while received < per_worker {
                let want = (per_worker - received).min(10) as i32;
                let t = Instant::now();
                let rows: Vec<(i64,)> =
                    sqlx::query_as("SELECT msg_id FROM pgmq.read_grouped_rr($1, 30, $2)")
                        .bind(&queue)
                        .bind(want)
                        .fetch_all(pool.as_ref())
                        .await?;
                if rows.is_empty() {
                    break;
                }
                let ids: Vec<i64> = rows.iter().map(|(id,)| *id).collect();
                sqlx::query("SELECT pgmq.delete($1, $2::bigint[])")
                    .bind(&queue)
                    .bind(&ids)
                    .execute(pool.as_ref())
                    .await?;
                let n = ids.len() as u64;
                let us_per_msg = t.elapsed().as_micros() as u64 / n;
                for _ in 0..n {
                    samples.push(us_per_msg);
                }
                received += n;
            }
            Ok((received, samples))
        });
    }

    let mut hist = Histogram::<u64>::new(3)?;
    let mut total = 0u64;
    while let Some(result) = set.join_next().await {
        let (n, samples) = result??;
        total += n;
        for us in samples {
            hist.record(us)?;
        }
    }

    let elapsed = start.elapsed();
    Ok(BenchResult {
        scenario: format!("{label_prefix}-grouped-rr c={concurrency} g={groups}"),
        async_commit,
        total_ops: total,
        elapsed_secs: elapsed.as_secs_f64(),
        msgs_per_sec: total as f64 / elapsed.as_secs_f64(),
        p50_us: hist.value_at_quantile(0.50),
        p99_us: hist.value_at_quantile(0.99),
        p999_us: hist.value_at_quantile(0.999),
    })
}

// ---------------------------------------------------------------------------
// round trip
// ---------------------------------------------------------------------------

async fn run_round_trip(
    pool: &Arc<PgPool>,
    queue: &str,
    count: u64,
    concurrency: usize,
    async_commit: bool,
) -> Result<BenchResult> {
    ensure_queue(pool, queue).await?;
    purge_queue(pool, queue).await?;
    warmup(pool, queue, 20).await?;

    let per_worker = count / concurrency as u64;
    let start = Instant::now();

    let mut set: JoinSet<Result<Vec<u64>>> = JoinSet::new();
    for _ in 0..concurrency {
        let pool = Arc::clone(pool);
        let queue = queue.to_string();
        set.spawn(async move {
            let mut samples = Vec::with_capacity(per_worker as usize);
            for _ in 0..per_worker {
                let t = Instant::now();
                let (msg_id,): (i64,) = sqlx::query_as(
                    "SELECT pgmq.send($1, '{\"b\":1}'::jsonb, NULL::jsonb, clock_timestamp())",
                )
                .bind(&queue)
                .fetch_one(pool.as_ref())
                .await?;
                sqlx::query("SELECT pgmq.delete($1, $2)")
                    .bind(&queue)
                    .bind(msg_id)
                    .execute(pool.as_ref())
                    .await?;
                samples.push(t.elapsed().as_micros() as u64);
            }
            Ok(samples)
        });
    }

    let mut hist = Histogram::<u64>::new(3)?;
    let mut total = 0u64;
    while let Some(result) = set.join_next().await {
        let samples = result??;
        total += samples.len() as u64;
        for us in samples {
            hist.record(us)?;
        }
    }

    let elapsed = start.elapsed();
    Ok(BenchResult {
        scenario: format!("round-trip c={concurrency}"),
        async_commit,
        total_ops: total,
        elapsed_secs: elapsed.as_secs_f64(),
        msgs_per_sec: total as f64 / elapsed.as_secs_f64(),
        p50_us: hist.value_at_quantile(0.50),
        p99_us: hist.value_at_quantile(0.99),
        p999_us: hist.value_at_quantile(0.999),
    })
}

// ---------------------------------------------------------------------------
// run-all matrix
// ---------------------------------------------------------------------------

async fn run_all(
    pool: &Arc<PgPool>,
    oss_pool: Option<&Arc<PgPool>>,
    profile: Profile,
    fifo: bool,
    async_commit: bool,
) -> Result<Vec<BenchResult>> {
    let (send_n, recv_n, rt_n): (u64, u64, u64) = match profile {
        Profile::Quick => (5_000, 5_000, 500),
        Profile::Full => (100_000, 100_000, 10_000),
    };

    let mut out = Vec::new();

    macro_rules! scenario {
        ($label:expr, $fut:expr) => {{
            tracing::info!("  {}...", $label);
            out.push($fut.await?);
        }};
    }

    tracing::info!("=== send ===");
    scenario!(
        format!("send c=1  ({send_n} msgs)"),
        run_send(pool, "bench_send", send_n, 1, 1, async_commit)
    );
    scenario!(
        format!("send c=8  ({send_n} msgs)"),
        run_send(pool, "bench_send", send_n, 8, 1, async_commit)
    );
    scenario!(
        format!("send c=32 ({send_n} msgs)"),
        run_send(pool, "bench_send", send_n, 32, 1, async_commit)
    );
    scenario!(
        format!("send b=100 c=1  ({send_n} msgs)"),
        run_send(pool, "bench_send", send_n, 1, 100, async_commit)
    );
    scenario!(
        format!("send b=100 c=8  ({send_n} msgs)"),
        run_send(pool, "bench_send", send_n, 8, 100, async_commit)
    );

    tracing::info!("=== receive ===");
    scenario!(
        format!("receive c=1  ({recv_n} msgs)"),
        run_receive(pool, "bench_recv", recv_n, 1, async_commit)
    );
    scenario!(
        format!("receive c=8  ({recv_n} msgs)"),
        run_receive(pool, "bench_recv", recv_n, 8, async_commit)
    );
    scenario!(
        format!("receive-sharded c=8  ({recv_n} msgs, 1 queue/worker)"),
        run_receive_sharded(pool, "bench_recv_sh", recv_n, 8, async_commit)
    );

    tracing::info!("=== round trip ===");
    scenario!(
        format!("round-trip c=1  ({rt_n} msgs)"),
        run_round_trip(pool, "bench_rt", rt_n, 1, async_commit)
    );
    scenario!(
        format!("round-trip c=8  ({rt_n} msgs)"),
        run_round_trip(pool, "bench_rt", rt_n, 8, async_commit)
    );

    if fifo {
        tracing::info!("=== fifo send ===");
        // g=8: simulate 8 independent order groups (e.g., 8 customers)
        scenario!(
            format!("fifo-send c=1 g=8  ({send_n} msgs)"),
            run_send_fifo(pool, "bench_fsend", send_n, 1, 8, async_commit)
        );
        scenario!(
            format!("fifo-send c=8 g=8  ({send_n} msgs)"),
            run_send_fifo(pool, "bench_fsend", send_n, 8, 8, async_commit)
        );
        // g=100: wider fan — more groups than workers
        scenario!(
            format!("fifo-send c=8 g=100  ({send_n} msgs)"),
            run_send_fifo(pool, "bench_fsend100", send_n, 8, 100, async_commit)
        );

        tracing::info!("=== fifo recv ===");
        scenario!(
            format!("fifo-recv c=1 g=8  ({recv_n} msgs)"),
            run_receive_fifo(pool, "bench_frecv8", recv_n, 1, 8, async_commit)
        );
        scenario!(
            format!("fifo-recv c=1 g=100  ({recv_n} msgs)"),
            run_receive_fifo(pool, "bench_frecv100", recv_n, 1, 100, async_commit)
        );
        scenario!(
            format!("fifo-recv c=8 g=100  ({recv_n} msgs)"),
            run_receive_fifo(pool, "bench_frecv100c", recv_n, 8, 100, async_commit)
        );
    }

    if let Some(oss) = oss_pool {
        tracing::info!("=== oss receive (read_grouped_rr) ===");
        scenario!(
            format!("oss recv c=1  ({recv_n} msgs)"),
            run_receive(oss, "bench_recv", recv_n, 1, async_commit)
        );
        scenario!(
            format!("oss recv c=8  ({recv_n} msgs)"),
            run_receive(oss, "bench_recv", recv_n, 8, async_commit)
        );
        scenario!(
            format!("oss-grouped-rr c=1 g=8  ({recv_n} msgs)"),
            run_receive_grouped_rr(oss, "bench_grr8", recv_n, 1, 8, "oss", async_commit)
        );
        scenario!(
            format!("oss-grouped-rr c=1 g=100  ({recv_n} msgs)"),
            run_receive_grouped_rr(oss, "bench_grr100", recv_n, 1, 100, "oss", async_commit)
        );
        scenario!(
            format!("oss-grouped-rr c=8 g=100  ({recv_n} msgs)"),
            run_receive_grouped_rr(oss, "bench_grr100c", recv_n, 8, 100, "oss", async_commit)
        );
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// output
// ---------------------------------------------------------------------------

fn print_results(results: &[BenchResult]) {
    println!(
        "\n{:<34} {:>8} {:>10} {:>10} {:>8} {:>8} {:>8}",
        "scenario", "msgs", "elapsed", "msgs/s", "p50µs", "p99µs", "p999µs"
    );
    println!("{}", "─".repeat(94));
    for r in results {
        let tag = if r.async_commit { "*" } else { "" };
        println!(
            "{:<34} {:>8} {:>10} {:>10.0} {:>8} {:>8} {:>8}",
            format!("{}{}", r.scenario, tag),
            r.total_ops,
            format!("{:.2}s", r.elapsed_secs),
            r.msgs_per_sec,
            r.p50_us,
            r.p99_us,
            r.p999_us,
        );
    }
    if results.iter().any(|r| r.async_commit) {
        println!("  * async_commit = off");
    }
    println!();
}

fn compare(before_path: &str, after_path: &str) -> Result<()> {
    let before: Vec<BenchResult> =
        serde_json::from_str(&std::fs::read_to_string(before_path)?)?;
    let after: Vec<BenchResult> =
        serde_json::from_str(&std::fs::read_to_string(after_path)?)?;

    println!(
        "\n{:<34} {:>12} {:>12} {:>10}   {:>10} {:>10} {:>10}",
        "scenario", "msgs/s (A)", "msgs/s (B)", "Δ msgs/s", "p99µs (A)", "p99µs (B)", "Δ p99"
    );
    println!("{}", "─".repeat(106));

    for a in &before {
        if let Some(b) = after.iter().find(|b| b.scenario == a.scenario) {
            let dt = (b.msgs_per_sec - a.msgs_per_sec) / a.msgs_per_sec * 100.0;
            let dp99 = (b.p99_us as f64 - a.p99_us as f64) / a.p99_us as f64 * 100.0;
            println!(
                "{:<34} {:>12.0} {:>12.0} {:>+10.1}%   {:>10} {:>10} {:>+10.1}%",
                a.scenario, a.msgs_per_sec, b.msgs_per_sec, dt, a.p99_us, b.p99_us, dp99,
            );
        }
    }
    println!();
    Ok(())
}
