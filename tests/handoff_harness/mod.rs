//! End-to-end harness for handoff integration tests. Mirrors kv's
//! `tests/handoff_harness/mod.rs` shape, adapted for queue's HTTP +
//! Postgres architecture.
//!
//! What gets exercised by going through here:
//! - `handoff::detect_role()` — both `ColdStart` and `Successor` branches
//!   on real processes.
//! - `LISTEN_FDS` / `LISTEN_FDNAMES` env-var inheritance via `fork+exec`
//!   `dup2` in `pre_exec`.
//! - `DataDirLock::acquire_or_break_stale` on a real on-disk flock between
//!   old and new processes.
//! - `Incumbent::serve` running inside beyond-queue;
//!   `Supervisor::perform_handoff` running here; both sides actually
//!   speaking the wire protocol over a real Unix socket.

#![allow(dead_code)]

use std::io::ErrorKind;
use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::OnceLock;
use std::thread;
use std::time::{Duration, Instant};

use handoff::{HandoffId, SpawnSpec, Supervisor};
use sqlx::{Connection, PgConnection, PgPool};
use tempfile::TempDir;
use testcontainers::ImageExt;
use testcontainers::core::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

const QUEUE_BINARY: &str = env!("CARGO_BIN_EXE_beyond-queue");

/// Shared Postgres testcontainer. Started lazily on the first `Harness::new`
/// call. Each test gets its own database within this instance so schema
/// changes don't cross-contaminate.
///
/// We do **not** hold a long-lived `PgPool` here: each `#[tokio::test]` gets
/// its own runtime, and a pool's background tasks die when the runtime
/// that created them drops. Tests instead open a fresh `PgConnection` for
/// each admin op (CREATE/DROP DATABASE, terminate_backend).
pub struct SharedPg {
    pub admin_url: String,
    _container: ContainerAsync<Postgres>,
}

static SHARED_PG: OnceLock<SharedPg> = OnceLock::new();

fn cleanup_orphaned_containers() {
    let Ok(out) = std::process::Command::new("docker")
        .args([
            "ps",
            "-q",
            "--filter",
            "label=org.testcontainers.managed-by=testcontainers",
        ])
        .output()
    else {
        return;
    };
    let ids: Vec<&str> = std::str::from_utf8(&out.stdout)
        .unwrap_or_default()
        .split_whitespace()
        .collect();
    if !ids.is_empty() {
        let _ = std::process::Command::new("docker")
            .arg("rm")
            .arg("-f")
            .args(&ids)
            .status();
    }
}

/// Bring up (or return) the shared Postgres testcontainer. Async; safe to
/// call from any tokio runtime.
pub async fn shared_pg() -> &'static SharedPg {
    if let Some(p) = SHARED_PG.get() {
        return p;
    }
    cleanup_orphaned_containers();
    let container = Postgres::default()
        .with_tag("18")
        .start()
        .await
        .expect("postgres testcontainer");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let admin_url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let val = SharedPg {
        admin_url,
        _container: container,
    };
    let _ = SHARED_PG.set(val);
    SHARED_PG.get().expect("set or get")
}

/// Create a fresh database within the shared Postgres, load the queue
/// schema + hot_paths, return its connection URL + a per-test pool.
///
/// Uses a fresh single-shot `PgConnection` for the admin op so the
/// connection's lifecycle is tied to this test's runtime (no cross-runtime
/// pool background tasks).
pub async fn fresh_db() -> (String, PgPool) {
    let pg = shared_pg().await;
    let dbname = format!(
        "queue_test_{}",
        uuid::Uuid::new_v4()
            .simple()
            .to_string()
            .chars()
            .take(16)
            .collect::<String>()
    );

    {
        let mut admin = PgConnection::connect(&pg.admin_url)
            .await
            .expect("connect admin");
        sqlx::query(&format!(r#"CREATE DATABASE "{dbname}""#))
            .execute(&mut admin)
            .await
            .expect("create test database");
        let _ = admin.close().await;
    }

    let port = pg
        .admin_url
        .rsplit_once(':')
        .and_then(|(_, p)| p.split('/').next())
        .expect("port in url");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/{dbname}");
    let pool = sqlx::pool::PoolOptions::<sqlx::Postgres>::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect to fresh test database");

    let schema_sql = include_str!("../../beyond-queue-extension/sql/schema.sql");
    let hot_paths_sql = include_str!("../fixtures/hot_paths.sql");

    sqlx::raw_sql(schema_sql)
        .execute(&pool)
        .await
        .expect("schema setup");
    sqlx::raw_sql(hot_paths_sql)
        .execute(&pool)
        .await
        .expect("hot_paths setup");

    (url, pool)
}

/// One in-progress handoff scenario.
pub struct Harness {
    binary: PathBuf,
    _temp: TempDir,
    state_dir: PathBuf,
    control_socket: PathBuf,
    journal_path: PathBuf,
    http_listener: TcpListener,
    http_addr: SocketAddr,
    database_url: String,
    pub pool: PgPool,
    extra_args: Vec<String>,
    extra_env: Vec<(String, String)>,
    /// Set by `with_tls()`. Exposed so tests can build matching mTLS clients.
    tls_certs: Option<CertBundle>,
    current: Option<Child>,
    supervisor: Arc<Supervisor>,
}

/// PEM-encoded test cert material. Minted by [`Harness::with_tls`].
#[derive(Clone)]
pub struct CertBundle {
    pub ca_pem: String,
    pub server_pem: String,
    pub server_key_pem: String,
    pub client_pem: String,
    pub client_key_pem: String,
}

#[derive(Debug)]
pub struct HandoffSummary {
    pub committed: bool,
    pub abort_reason: Option<String>,
    pub handoff_id: HandoffId,
    pub elapsed: Duration,
}

impl Harness {
    /// Allocate temp dir + ephemeral HTTP port + fresh database.
    /// Does **NOT** start beyond-queue yet (call [`Self::cold_start`]).
    pub async fn new() -> Self {
        let binary = PathBuf::from(QUEUE_BINARY);
        let temp = tempfile::tempdir().expect("tempdir");
        let state_dir = temp.path().join("state");
        std::fs::create_dir_all(&state_dir).expect("create state dir");
        let control_socket = state_dir.join("control.sock");
        let journal_path = temp.path().join("handoff-state.bin");

        let http_listener = TcpListener::bind("127.0.0.1:0").expect("bind http");
        let http_addr = http_listener.local_addr().unwrap();

        let (database_url, pool) = fresh_db().await;

        let supervisor = Supervisor::new(&control_socket)
            .expect("Supervisor::new")
            .with_listener("http", http_listener.as_raw_fd())
            .with_journal(journal_path.clone());
        let supervisor = Arc::new(supervisor);

        Self {
            binary,
            _temp: temp,
            state_dir,
            control_socket,
            journal_path,
            http_listener,
            http_addr,
            database_url,
            pool,
            extra_args: Vec::new(),
            extra_env: Vec::new(),
            tls_certs: None,
            current: None,
            supervisor,
        }
    }

    /// Mint a CA + server + client cert into the temp dir and set the
    /// BEYOND_TLS_* env vars on every spawn. Call before `cold_start`.
    /// Returns the cert bundle so tests can build matching mTLS clients.
    pub fn with_tls(mut self) -> Self {
        assert!(self.current.is_none(), "configure TLS before cold_start");
        let certs = generate_test_certs();
        let cert_path = self._temp.path().join("server.pem");
        let key_path = self._temp.path().join("server.key");
        let ca_path = self._temp.path().join("ca.pem");
        std::fs::write(&cert_path, &certs.server_pem).expect("write server cert");
        std::fs::write(&key_path, &certs.server_key_pem).expect("write server key");
        std::fs::write(&ca_path, &certs.ca_pem).expect("write ca");
        self.extra_env.extend([
            (
                "BEYOND_TLS_CERT".into(),
                cert_path.to_str().unwrap().to_string(),
            ),
            (
                "BEYOND_TLS_KEY".into(),
                key_path.to_str().unwrap().to_string(),
            ),
            (
                "BEYOND_TLS_CA".into(),
                ca_path.to_str().unwrap().to_string(),
            ),
        ]);
        self.tls_certs = Some(certs);
        self
    }

    pub fn tls_certs(&self) -> Option<&CertBundle> {
        self.tls_certs.as_ref()
    }

    /// Append extra CLI args to every spawn. Set before `cold_start`.
    pub fn with_extra_args(mut self, args: Vec<String>) -> Self {
        assert!(self.current.is_none(), "set extra args before cold_start");
        self.extra_args = args;
        self
    }

    /// Append extra env vars to every spawn. Set before `cold_start`.
    pub fn with_extra_env(mut self, env: Vec<(String, String)>) -> Self {
        assert!(self.current.is_none(), "set extra env before cold_start");
        self.extra_env = env;
        self
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    /// Spawn the first beyond-queue process (no `HANDOFF_ROLE`, so
    /// `Role::ColdStart`). Blocks until `/livez` returns 200.
    pub fn cold_start(&mut self) -> &mut Self {
        self.cold_start_with_env(Vec::new())
    }

    /// Like `cold_start` but with extra env vars for the cold-start child.
    pub fn cold_start_with_env(&mut self, env: Vec<(String, String)>) -> &mut Self {
        assert!(self.current.is_none(), "queue already running");
        let listener_fds = vec![("http".to_string(), self.http_listener.as_raw_fd())];
        let args = self.queue_args();
        let child_env = self.merged_env(&env);
        let child =
            spawn_cold_start_with_inherited_and_env(&self.binary, &args, &listener_fds, &child_env);
        self.current = Some(child);
        self.wait_ready();
        self
    }

    /// Drive a full happy-path handoff: spawn successor, run Hello → Commit.
    /// Reaps the old child on commit. Blocks until the successor is
    /// reachable on the same port.
    pub fn handoff(&mut self) -> HandoffSummary {
        self.handoff_with_env(Vec::new())
    }

    /// Like `handoff` but with extra env vars for the successor process.
    pub fn handoff_with_env(&mut self, env: Vec<(String, String)>) -> HandoffSummary {
        let started = Instant::now();
        let args = self.queue_args();
        let child_env = self.merged_env(&env);
        let spec = SpawnSpec {
            binary: self.binary.clone(),
            args,
            env: child_env,
            deadline: Duration::from_secs(60),
            drain_grace: Duration::from_secs(10),
        };
        let mut outcome = self
            .supervisor
            .perform_handoff(spec)
            .expect("perform_handoff");

        if outcome.committed {
            if let Some(mut old) = self.current.take() {
                let _ = old.wait();
            }
            self.current = outcome.child.take();
            self.wait_ready();
        }

        HandoffSummary {
            committed: outcome.committed,
            abort_reason: outcome.abort_reason,
            handoff_id: outcome.handoff_id,
            elapsed: started.elapsed(),
        }
    }

    /// Block until the control socket exists and the HTTP listener is
    /// reachable. For plaintext servers, also checks that `/livez` returns
    /// 200. For TLS servers we only TCP-probe — tests should build their
    /// own mTLS client for the HTTP-level check.
    pub fn wait_ready(&self) {
        // Generous timeout: under CI load (shared CPU, postgres in container,
        // build artifacts on slow disk) cold start of the queue subprocess
        // can take ~15-20s.
        assert!(
            wait_for_path(&self.control_socket, Duration::from_secs(30)),
            "control socket {:?} never appeared",
            self.control_socket
        );
        if self.tls_certs.is_some() {
            wait_for_tcp(self.http_addr, Duration::from_secs(30));
        } else {
            wait_for_livez(self.http_addr, Duration::from_secs(30));
        }
    }

    /// SIGTERM the current child. Best-effort; ignores reap failures.
    pub fn kill_current(&mut self) {
        if let Some(mut c) = self.current.take() {
            // SIGTERM lets the binary's graceful shutdown run.
            unsafe { libc::kill(c.id() as i32, libc::SIGTERM) };
            let _ = c.wait();
        }
    }

    /// Adopt an externally-created Child as the harness's tracked
    /// `current`. Used by tests that call `perform_handoff` directly
    /// (bypassing `handoff()`) so the harness's Drop reaps it.
    pub fn adopt_current(&mut self, child: Child) {
        // If something else is still tracked, kill it first.
        self.sigkill_current();
        self.current = Some(child);
        // Wait for readiness so subsequent test logic doesn't race the
        // successor's bind.
        self.wait_ready();
    }

    /// SIGTERM the current child and bound the wait. Returns
    /// `Some(elapsed)` if it exited cleanly within `timeout`, `None` if it
    /// hung past the deadline. Used by the shutdown-latency test.
    pub fn sigterm_and_wait(&mut self, timeout: Duration) -> Option<Duration> {
        let Some(mut c) = self.current.take() else {
            return Some(Duration::ZERO);
        };
        let started = Instant::now();
        unsafe { libc::kill(c.id() as i32, libc::SIGTERM) };
        let deadline = started + timeout;
        loop {
            match c.try_wait() {
                Ok(Some(_status)) => return Some(started.elapsed()),
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(50));
                }
                Ok(None) => {
                    // Hung past deadline. Force-kill to avoid leaking.
                    let _ = c.kill();
                    let _ = c.wait();
                    return None;
                }
                Err(_) => return Some(started.elapsed()),
            }
        }
    }

    /// SIGKILL — hard crash, leaves a stale pidfile. Use
    /// `cold_start_after_crash` to exercise the stale-break path.
    pub fn sigkill_current(&mut self) {
        if let Some(mut c) = self.current.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// Cold-start again on the same state_dir + listener after a SIGKILL.
    pub fn cold_start_after_crash(&mut self) -> &mut Self {
        assert!(self.current.is_none(), "kill current child first");
        let listener_fds = vec![("http".to_string(), self.http_listener.as_raw_fd())];
        let args = self.queue_args();
        let child_env = self.merged_env(&[]);
        let child =
            spawn_cold_start_with_inherited_and_env(&self.binary, &args, &listener_fds, &child_env);
        self.current = Some(child);
        self.wait_ready();
        self
    }

    /// Try to start a second beyond-queue process pointed at the same
    /// state_dir on a different port + control socket. Should fail to
    /// acquire the flock.
    pub fn try_spawn_competitor(&self) -> Child {
        let extra_http = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = extra_http.local_addr().unwrap().port();
        drop(extra_http);
        let other_socket = self._temp.path().join("competitor-control.sock");

        let mut cmd = Command::new(&self.binary);
        cmd.args([
            "serve",
            "--address",
            &format!("127.0.0.1:{port}"),
            "--handoff-state-dir",
            self.state_dir.to_str().unwrap(),
            "--handoff-socket-path",
            other_socket.to_str().unwrap(),
        ]);
        cmd.env("DATABASE_URL", &self.database_url);
        cmd.env("LOG_LEVEL", "error");
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        cmd.spawn().expect("spawn competitor")
    }

    // ── Inspection ───────────────────────────────────────────────────────

    pub fn http_addr(&self) -> SocketAddr {
        self.http_addr
    }

    pub fn http_url(&self) -> String {
        format!("http://{}", self.http_addr)
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn control_socket(&self) -> &Path {
        &self.control_socket
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    pub fn current_pid(&self) -> Option<u32> {
        self.current.as_ref().map(|c| c.id())
    }

    pub fn supervisor(&self) -> Arc<Supervisor> {
        Arc::clone(&self.supervisor)
    }

    /// Build a `SpawnSpec` matching the harness's defaults — used by
    /// tests that call `perform_handoff` directly instead of through
    /// `handoff()` (e.g. concurrent-handoff serialization tests).
    pub fn make_spawn_spec(&self) -> SpawnSpec {
        SpawnSpec {
            binary: self.binary.clone(),
            args: self.queue_args(),
            env: self.merged_env(&[]),
            deadline: Duration::from_secs(60),
            drain_grace: Duration::from_secs(10),
        }
    }

    // ── Internals ────────────────────────────────────────────────────────

    fn queue_args(&self) -> Vec<String> {
        let mut v = vec![
            "serve".into(),
            "--handoff-state-dir".into(),
            self.state_dir.to_str().unwrap().into(),
            "--handoff-socket-path".into(),
            self.control_socket.to_str().unwrap().into(),
        ];
        v.extend(self.extra_args.iter().cloned());
        v
    }

    fn merged_env(&self, extra: &[(String, String)]) -> Vec<(String, String)> {
        let mut env = vec![
            ("DATABASE_URL".into(), self.database_url.clone()),
            // QUEUE_ADDRESS is irrelevant when LISTEN_FDS is set (the
            // inherited listener wins), but the SDK tests have shown
            // mishaps when the env isn't set, so keep it for parity.
            ("QUEUE_ADDRESS".into(), self.http_addr.to_string()),
            // Cap pool size aggressively so back-to-back tests don't
            // exhaust the shared testcontainer Postgres's
            // max_connections budget while torn-down processes wait
            // for their connections to be reclaimed.
            ("QUEUE_MAX_CONNECTIONS".into(), "3".into()),
            ("LOG_LEVEL".into(), "error".into()),
        ];
        env.extend(self.extra_env.iter().cloned());
        env.extend(extra.iter().cloned());
        env
    }

    /// Drop the per-harness database to free its connection slots.
    /// Tolerant of "database does not exist".
    async fn drop_database(admin_url: &str, dbname: &str) {
        let Ok(mut admin) = PgConnection::connect(admin_url).await else {
            return;
        };
        let _ = sqlx::query(&format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = '{dbname}' AND pid <> pg_backend_pid()"
        ))
        .execute(&mut admin)
        .await;
        let _ = sqlx::query(&format!(r#"DROP DATABASE IF EXISTS "{dbname}""#))
            .execute(&mut admin)
            .await;
        let _ = admin.close().await;
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.sigkill_current();
        let dbname = self
            .database_url
            .rsplit_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_default();
        if dbname.is_empty() {
            return;
        }
        let admin_url = match SHARED_PG.get() {
            Some(p) => p.admin_url.clone(),
            None => return,
        };
        // Build a one-shot runtime in a fresh thread so we don't
        // require an active tokio context here.
        let _ = std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(r) => r,
                Err(_) => return,
            };
            rt.block_on(async move {
                Self::drop_database(&admin_url, &dbname).await;
            });
        })
        .join();
    }
}

// ── Free helpers ─────────────────────────────────────────────────────────

/// Cold-start spawn that mirrors the production supervisor's FD inheritance:
/// `dup2` each listener FD into FD 3..3+N in the child via `pre_exec`,
/// clearing `FD_CLOEXEC` so the FDs survive `execve`.
pub fn spawn_cold_start_with_inherited_and_env(
    binary: &Path,
    args: &[String],
    listener_fds: &[(String, RawFd)],
    extra_env: &[(String, String)],
) -> Child {
    let mut cmd = Command::new(binary);
    cmd.args(args);
    let names: Vec<String> = listener_fds.iter().map(|(n, _)| n.clone()).collect();
    cmd.env("LISTEN_FDS", listener_fds.len().to_string());
    cmd.env("LISTEN_FDNAMES", names.join(":"));
    for (k, v) in extra_env {
        cmd.env(k, v);
    }
    cmd.stdin(Stdio::null()).stdout(Stdio::null());
    if std::env::var("QUEUE_TEST_LOGS").is_ok() {
        cmd.stderr(Stdio::inherit());
    } else {
        cmd.stderr(Stdio::null());
    }

    let sources: Vec<RawFd> = listener_fds.iter().map(|(_, f)| *f).collect();
    // SAFETY: `pre_exec` runs in the forked child before `execve`. Only
    // async-signal-safe libc calls; no allocations.
    unsafe {
        cmd.pre_exec(move || {
            for (i, src) in sources.iter().enumerate() {
                let dst = 3 + i as RawFd;
                if *src == dst {
                    if libc::fcntl(*src, libc::F_SETFD, 0) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                } else if libc::dup2(*src, dst) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    cmd.spawn().expect("spawn beyond-queue (cold start)")
}

pub fn wait_for_path(path: &Path, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !path.exists() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(25));
    }
    path.exists()
}

pub fn wait_for_tcp(addr: SocketAddr, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(250)) {
            Ok(_) => return,
            Err(_) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(25));
            }
            Err(e) if e.kind() == ErrorKind::TimedOut => continue,
            Err(e) => panic!("wait_for_tcp({addr}): {e}"),
        }
    }
}

/// Block until `GET /livez` on `addr` returns 200.
pub fn wait_for_livez(addr: SocketAddr, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    let url = format!("http://{addr}/livez");
    let agent = ureq::AgentBuilder::new()
        .timeout(Duration::from_millis(500))
        .build();
    loop {
        match agent.get(&url).call() {
            Ok(resp) if resp.status() == 200 => return,
            _ if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
            other => panic!("wait_for_livez({addr}): {other:?}"),
        }
    }
}

// ─── HTTP helpers ────────────────────────────────────────────────────────

/// Blocking `POST /v1/queues` to create a queue.
pub fn create_queue(addr: SocketAddr, name: &str) -> u16 {
    let url = format!("http://{addr}/v1/queues");
    let body = serde_json::json!({"name": name, "fifo": false}).to_string();
    match ureq::post(&url)
        .set("Authorization", "Bearer test")
        .set("Content-Type", "application/json")
        .send_string(&body)
    {
        Ok(resp) => resp.status(),
        Err(ureq::Error::Status(code, _)) => code,
        Err(e) => panic!("create_queue({url}): {e}"),
    }
}

/// Blocking `POST /v1/queues/{name}/messages` with the given body.
pub fn send_message(addr: SocketAddr, queue: &str, body: &str) -> Option<i64> {
    let url = format!("http://{addr}/v1/queues/{queue}/messages");
    let request_body = serde_json::json!({"message": body}).to_string();
    let resp = ureq::post(&url)
        .set("Authorization", "Bearer test")
        .set("Content-Type", "application/json")
        .send_string(&request_body);
    match resp {
        Ok(r) => {
            let json: serde_json::Value =
                serde_json::from_str(&r.into_string().unwrap_or_default()).ok()?;
            json.get("id").and_then(|v| v.as_i64())
        }
        Err(ureq::Error::Status(code, _)) => {
            panic!("send_message({url}): status {code}")
        }
        Err(e) => panic!("send_message({url}): {e}"),
    }
}

/// Blocking `GET /v1/queues/{name}/messages?max=1` and return the first
/// message's body (if any).
pub fn receive_one(addr: SocketAddr, queue: &str) -> Option<String> {
    let url = format!("http://{addr}/v1/queues/{queue}/messages?max=1&wait=2&vt=10");
    let resp = ureq::get(&url).set("Authorization", "Bearer test").call();
    match resp {
        Ok(r) => {
            let text = r.into_string().ok()?;
            let arr: serde_json::Value = serde_json::from_str(&text).ok()?;
            let first = arr.as_array()?.first()?;
            first.get("message").and_then(|m| {
                // `message` may be a string or a JSON value depending on
                // how it was sent; return its text form.
                m.as_str()
                    .map(|s| s.to_string())
                    .or_else(|| Some(m.to_string()))
            })
        }
        Err(ureq::Error::Status(404, _)) => None,
        Err(ureq::Error::Status(code, _)) => panic!("receive_one({url}): status {code}"),
        Err(e) => panic!("receive_one({url}): {e}"),
    }
}

/// Blocking `GET /metrics`. Returns the raw Prometheus text.
pub fn fetch_metrics(addr: SocketAddr) -> String {
    let url = format!("http://{addr}/metrics");
    let resp = ureq::get(&url).call().expect("GET /metrics");
    resp.into_string().expect("body")
}

/// Generate a fresh CA + server cert (SAN: localhost + 127.0.0.1) + client
/// cert. The server cert has both ServerAuth and ClientAuth EKU so the
/// same material can be exercised by a TLS client probing /livez.
pub fn generate_test_certs() -> CertBundle {
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
        SanType,
    };

    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::from_params(&ca_params, &ca_key);

    let server_key = KeyPair::generate().unwrap();
    let mut srv_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    srv_params
        .subject_alt_names
        .push(SanType::IpAddress(std::net::IpAddr::V4(
            std::net::Ipv4Addr::LOCALHOST,
        )));
    srv_params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let server_cert = srv_params.signed_by(&server_key, &issuer).unwrap();

    let client_key = KeyPair::generate().unwrap();
    let mut cli_params = CertificateParams::new(vec!["client".to_string()]).unwrap();
    cli_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_cert = cli_params.signed_by(&client_key, &issuer).unwrap();

    CertBundle {
        ca_pem: ca_cert.pem(),
        server_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    }
}

/// Find a Prometheus metric line by name + label match. Returns the value
/// as f64.
pub fn metric_value(metrics: &str, name: &str, label_match: Option<&str>) -> Option<f64> {
    for line in metrics.lines() {
        if line.starts_with('#') {
            continue;
        }
        if !line.starts_with(name) {
            continue;
        }
        if let Some(needle) = label_match
            && !line.contains(needle)
        {
            continue;
        }
        let v = line.rsplit_once(' ')?.1;
        return v.parse().ok();
    }
    None
}

// ─── Traffic generator ───────────────────────────────────────────────────

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// One acked SendMessage the [`Sender`] has produced.
#[derive(Debug, Clone)]
pub struct AckedSend {
    pub seq: u64,
    pub body: String,
    pub msg_id: i64,
}

/// Stats collected by [`Sender::stop`].
#[derive(Debug)]
pub struct SenderResult {
    pub acked: Vec<AckedSend>,
    pub errors: u64,
    pub elapsed: Duration,
}

/// Background sender thread. Hammers `POST /v1/queues/{queue}/messages`
/// with sequential bodies `"msg-N"` and records every 200-ack. Reconnects
/// automatically across the brief swap window.
pub struct Sender {
    handle: Option<std::thread::JoinHandle<SenderResult>>,
    stop: Arc<AtomicBool>,
    acked_count: Arc<AtomicU64>,
}

impl Sender {
    pub fn start(addr: SocketAddr, queue: String) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let acked_count = Arc::new(AtomicU64::new(0));
        let acked: Arc<Mutex<Vec<AckedSend>>> = Arc::new(Mutex::new(Vec::new()));
        let stop_for_thread = Arc::clone(&stop);
        let count_for_thread = Arc::clone(&acked_count);
        let acked_for_thread = Arc::clone(&acked);

        let handle = std::thread::Builder::new()
            .name("queue-handoff-sender".into())
            .spawn(move || {
                let started = Instant::now();
                let url = format!("http://{addr}/v1/queues/{queue}/messages");
                let agent = ureq::AgentBuilder::new()
                    .timeout(Duration::from_secs(5))
                    .build();
                let mut errors = 0u64;
                let mut seq = 0u64;
                while !stop_for_thread.load(Ordering::Relaxed) {
                    let body = format!("msg-{seq}");
                    let request_body = serde_json::json!({"message": &body}).to_string();
                    match agent
                        .post(&url)
                        .set("Authorization", "Bearer test")
                        .set("Content-Type", "application/json")
                        .send_string(&request_body)
                    {
                        Ok(r) if r.status() == 201 || r.status() == 200 => {
                            let text = r.into_string().unwrap_or_default();
                            let id = serde_json::from_str::<serde_json::Value>(&text)
                                .ok()
                                .and_then(|j| j.get("id").and_then(|v| v.as_i64()))
                                .unwrap_or(0);
                            acked_for_thread.lock().unwrap().push(AckedSend {
                                seq,
                                body,
                                msg_id: id,
                            });
                            count_for_thread.fetch_add(1, Ordering::Relaxed);
                            seq += 1;
                        }
                        _ => {
                            errors += 1;
                            thread::sleep(Duration::from_millis(5));
                        }
                    }
                }
                SenderResult {
                    acked: acked_for_thread.lock().unwrap().clone(),
                    errors,
                    elapsed: started.elapsed(),
                }
            })
            .expect("spawn sender thread");

        Self {
            handle: Some(handle),
            stop,
            acked_count,
        }
    }

    pub fn acked_count(&self) -> u64 {
        self.acked_count.load(Ordering::Relaxed)
    }

    pub fn stop(mut self) -> SenderResult {
        self.stop.store(true, Ordering::SeqCst);
        self.handle
            .take()
            .expect("handle")
            .join()
            .expect("sender panic")
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}
