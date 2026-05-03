use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

/// Verify that an Authorization header is present.
///
/// The SigV4 signature is NOT verified — we accept any credentials. This is the
/// LocalStack/ElasticMQ pattern for private-network deployments where the network
/// boundary is the security layer. AWS SDKs work without any configuration change
/// beyond the endpoint URL.
pub async fn require_auth(request: Request, next: Next) -> Result<Response, StatusCode> {
    if request.headers().contains_key("authorization") {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::FORBIDDEN)
    }
}
