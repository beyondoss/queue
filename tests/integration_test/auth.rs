use crate::helpers::test_env;

#[tokio::test]
async fn test_missing_auth_returns_403() {
    let env = test_env();
    // Plain reqwest with no Authorization header — must be rejected.
    let res = reqwest::Client::new()
        .get(format!("{}/v1/queues", env.url))
        .send()
        .await
        .expect("GET");
    assert_eq!(res.status().as_u16(), 403, "missing auth must return 403");
}
