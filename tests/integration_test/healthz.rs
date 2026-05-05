use crate::helpers::test_env;

#[tokio::test]
async fn test_healthz() {
    let _ = test_env();
    // healthz bypasses auth middleware
    let env = test_env();
    let res = reqwest::get(format!("{}/healthz", env.url))
        .await
        .expect("GET /healthz");
    assert_eq!(res.status().as_u16(), 200);
}
