use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, Clone)]
pub enum SnsErrorCode {
    NotFound,
    InvalidParameter,
    AuthorizationError,
    InternalError,
    InvalidClientTokenId,
}

impl SnsErrorCode {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NotFound => "NotFound",
            Self::InvalidParameter => "InvalidParameter",
            Self::AuthorizationError => "AuthorizationError",
            Self::InternalError => "InternalFailure",
            Self::InvalidClientTokenId => "InvalidClientTokenId",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::NotFound => "Topic does not exist.",
            Self::InvalidParameter => "Invalid parameter.",
            Self::AuthorizationError => "Not authorized.",
            Self::InternalError => "We encountered an internal error. Please try again.",
            Self::InvalidClientTokenId => "The security token included in the request is invalid.",
        }
    }

    pub fn http_status(&self) -> StatusCode {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }
}

pub struct SnsError {
    pub code: SnsErrorCode,
    pub request_id: String,
    pub protocol: SnsProtocol,
}

#[derive(Clone, Copy, Debug)]
pub enum SnsProtocol {
    Json,
    Query,
}

impl IntoResponse for SnsError {
    fn into_response(self) -> Response {
        let status = self.code.http_status();
        match self.protocol {
            SnsProtocol::Json => json_error_response(status, &self.code, &self.request_id),
            SnsProtocol::Query => xml_error_response(status, &self.code, &self.request_id),
        }
    }
}

#[derive(Serialize)]
struct JsonErrorBody {
    #[serde(rename = "__type")]
    error_type: String,
    message: String,
}

fn json_error_response(status: StatusCode, code: &SnsErrorCode, _request_id: &str) -> Response {
    let body = JsonErrorBody {
        error_type: code.code().to_string(),
        message: code.message().to_string(),
    };
    (
        status,
        [("content-type", "application/x-amz-json-1.0")],
        axum::Json(body),
    )
        .into_response()
}

fn xml_error_response(status: StatusCode, code: &SnsErrorCode, request_id: &str) -> Response {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://sns.amazonaws.com/doc/2010-03-31/">
  <Error>
    <Type>Sender</Type>
    <Code>{}</Code>
    <Message>{}</Message>
  </Error>
  <RequestId>{}</RequestId>
</ErrorResponse>"#,
        code.code(),
        code.message(),
        request_id,
    );
    (status, [("content-type", "text/xml")], xml).into_response()
}
