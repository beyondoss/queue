use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, SanType,
};
use reqwest::Version;
use tempfile::NamedTempFile;

use super::helpers::test_env;

pub struct CertBundle {
    pub ca_pem: String,
    pub server_pem: String,
    pub server_key_pem: String,
    pub client_pem: String,
    pub client_key_pem: String,
}

pub fn generate_test_certs() -> CertBundle {
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

fn write_temp(content: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f
}

fn mtls_client(certs: &CertBundle) -> reqwest::Client {
    let ca = reqwest::Certificate::from_pem(certs.ca_pem.as_bytes()).unwrap();
    let combined = format!("{}{}", certs.client_pem, certs.client_key_pem);
    let identity = reqwest::Identity::from_pem(combined.as_bytes()).unwrap();
    reqwest::Client::builder()
        .add_root_certificate(ca)
        .identity(identity)
        .https_only(true)
        .build()
        .unwrap()
}

async fn start_tls_server(certs: &CertBundle) -> String {
    let env = test_env();
    let cert_file = write_temp(&certs.server_pem);
    let key_file = write_temp(&certs.server_key_pem);
    let ca_file = write_temp(&certs.ca_pem);

    let app = beyond_queue::build_router(beyond_queue::AppState {
        pool: env.pool.clone(),
        config: Arc::new(beyond_queue::config::Config {
            database_url: String::new(),
            address: "127.0.0.1:0".into(),
            default_visibility_timeout: 30,
            max_connections: 5,
            linger_ms: 0,
            log_level: "error".into(),
            otlp_enabled: false,
            otlp_endpoint: "http://localhost:4317".into(),
            otlp_sample_rate: 0.1,
            base_url_override: None,
            http_delivery_enabled: false,
            http_delivery_poll_ms: 50,
            http_delivery_timeout_secs: 5,
            http_delivery_batch_size: 50,
            schedule_enabled: false,
            schedule_poll_ms: 100,
            schedule_batch_size: 32,
            schedule_preview_count: 5,
            schedule_list_max: 1000,
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            handoff_state_dir: std::path::PathBuf::from("/tmp"),
            handoff_socket_path: std::path::PathBuf::from("/tmp/queue-test-tls-unused.sock"),
        }),
        base_url: "https://127.0.0.1".into(),
        coalescer: None,
        signer: Arc::new(beyond_queue::signing::Signer::generate().unwrap()),
        metrics: Arc::new(beyond_queue::metrics::Metrics::new()),
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("https://127.0.0.1:{port}");

    let tls = Some((
        cert_file.path().to_str().unwrap().to_string(),
        key_file.path().to_str().unwrap().to_string(),
        ca_file.path().to_str().unwrap().to_string(),
    ));

    tokio::spawn(async move {
        let _cert = cert_file;
        let _key = key_file;
        let _ca = ca_file;
        beyond_queue::serve_with_listener(listener, tls, app)
            .await
            .ok();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    url
}

// ── tests ────────────────────────────────────────────────────────────────────

/// Valid mTLS client — request succeeds over HTTP/2.
#[tokio::test]
async fn tls_valid_client_gets_h2() {
    let certs = generate_test_certs();
    let url = start_tls_server(&certs).await;

    let client = mtls_client(&certs);
    let res = client
        .get(format!("{url}/livez"))
        .send()
        .await
        .expect("request failed");

    assert_eq!(res.status(), 200);
    assert_eq!(res.version(), Version::HTTP_2);
}

/// No client certificate — server rejects the TLS handshake.
#[tokio::test]
async fn tls_no_client_cert_rejected() {
    let certs = generate_test_certs();
    let url = start_tls_server(&certs).await;

    let ca = reqwest::Certificate::from_pem(certs.ca_pem.as_bytes()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(ca)
        .https_only(true)
        .build()
        .unwrap();

    let err = client.get(format!("{url}/livez")).send().await;
    assert!(err.is_err(), "expected TLS rejection, got: {:?}", err);
}

/// Client cert from a different CA — server rejects it.
#[tokio::test]
async fn tls_wrong_ca_rejected() {
    let server_certs = generate_test_certs();
    let rogue_certs = generate_test_certs();
    let url = start_tls_server(&server_certs).await;

    let client = mtls_client(&rogue_certs);
    let err = client.get(format!("{url}/livez")).send().await;
    assert!(err.is_err(), "expected TLS rejection, got: {:?}", err);
}
