use crate::helpers::test_env;

#[tokio::test]
async fn test_healthz() {
    let _ = test_env();
    let env = test_env();
    let res = reqwest::get(format!("{}/readyz", env.url))
        .await
        .expect("GET /readyz");
    assert_eq!(res.status().as_u16(), 200);
}
