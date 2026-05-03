use std::sync::OnceLock;

use sqlx::PgPool;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

pub struct TestEnv {
    pub url: String,
    pub pool: PgPool,
}

static TEST_ENV: OnceLock<TestEnv> = OnceLock::new();

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

pub fn test_env() -> &'static TestEnv {
    TEST_ENV.get_or_init(|| {
        cleanup_orphaned_containers();
        let (tx, rx) = std::sync::mpsc::channel();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("test runtime");

            rt.block_on(async move {
                let container = Postgres::default()
                    .with_tag("18")
                    .start()
                    .await
                    .expect("postgres testcontainer");

                let port = container.get_host_port_ipv4(5432).await.expect("port");
                let database_url =
                    format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

                let pool = PgPool::connect(&database_url)
                    .await
                    .expect("connect to test postgres");

                let schema_sql = include_str!("../beyond-queue-extension/sql/schema.sql");
                let hot_paths_sql = include_str!("fixtures/hot_paths.sql");

                sqlx::raw_sql(schema_sql)
                    .execute(&pool)
                    .await
                    .expect("schema setup");
                sqlx::raw_sql(hot_paths_sql)
                    .execute(&pool)
                    .await
                    .expect("hot_paths setup");

                let server = beyond_queue::test_support::start(pool.clone(), database_url)
                    .await
                    .expect("test server");

                tx.send(TestEnv {
                    url: server.url,
                    pool,
                })
                .expect("send TestEnv");

                let _container = container;
                tokio::signal::ctrl_c().await.ok();
            });

            std::process::exit(130);
        });

        rx.recv().expect("test env setup")
    })
}

// ── HTTP client ───────────────────────────────────────────────────────────────

pub struct TestClient {
    inner: reqwest::Client,
    base_url: String,
}

impl TestClient {
    pub fn new() -> Self {
        let env = test_env();
        Self {
            inner: reqwest::Client::new(),
            base_url: env.url.clone(),
        }
    }

    fn req(&self, method: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let url = format!("{}{path}", self.base_url);
        self.inner
            .request(method, url)
            .header(reqwest::header::AUTHORIZATION, "Bearer test")
    }

    pub async fn get(&self, path: &str) -> TestResponse {
        TestResponse::from(
            self.req(reqwest::Method::GET, path)
                .send()
                .await
                .expect("GET"),
        )
        .await
    }

    pub async fn post<B: serde::Serialize>(&self, path: &str, body: &B) -> TestResponse {
        TestResponse::from(
            self.req(reqwest::Method::POST, path)
                .json(body)
                .send()
                .await
                .expect("POST"),
        )
        .await
    }

    pub async fn delete(&self, path: &str) -> TestResponse {
        TestResponse::from(
            self.req(reqwest::Method::DELETE, path)
                .send()
                .await
                .expect("DELETE"),
        )
        .await
    }

    pub async fn delete_json<B: serde::Serialize + ?Sized>(
        &self,
        path: &str,
        body: &B,
    ) -> TestResponse {
        TestResponse::from(
            self.req(reqwest::Method::DELETE, path)
                .json(body)
                .send()
                .await
                .expect("DELETE"),
        )
        .await
    }

    pub async fn patch<B: serde::Serialize>(&self, path: &str, body: &B) -> TestResponse {
        TestResponse::from(
            self.req(reqwest::Method::PATCH, path)
                .json(body)
                .send()
                .await
                .expect("PATCH"),
        )
        .await
    }
}

pub struct TestResponse {
    pub status: u16,
    pub body: String,
}

impl TestResponse {
    async fn from(res: reqwest::Response) -> Self {
        let status = res.status().as_u16();
        let body = res.text().await.unwrap_or_default();
        Self { status, body }
    }

    #[track_caller]
    pub fn assert_status(self, expected: u16) -> Self {
        assert_eq!(
            self.status, expected,
            "expected {expected}, got {}\nbody: {}",
            self.status, self.body
        );
        self
    }

    #[track_caller]
    pub fn json<T: serde::de::DeserializeOwned>(self) -> T {
        serde_json::from_str(&self.body)
            .unwrap_or_else(|e| panic!("deserialize failed: {e}\nbody: {}", self.body))
    }
}
