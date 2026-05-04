use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, Clone)]
pub enum SqsErrorCode {
    NonExistentQueue,
    InvalidMessageContents,
    ReceiptHandleIsInvalid,
    BatchEntryIdsNotDistinct,
    TooManyEntriesInBatchRequest,
    EmptyBatchRequest,
    InvalidBatchEntryId,
    QueueAlreadyExists,
    InvalidAttributeName,
    InternalError,
}

impl SqsErrorCode {
    pub fn code(&self) -> &'static str {
        match self {
            Self::NonExistentQueue => "AWS.SimpleQueueService.NonExistentQueue",
            Self::InvalidMessageContents => "InvalidMessageContents",
            Self::ReceiptHandleIsInvalid => "ReceiptHandleIsInvalid",
            Self::BatchEntryIdsNotDistinct => "AWS.SimpleQueueService.BatchEntryIdsNotDistinct",
            Self::TooManyEntriesInBatchRequest => {
                "AWS.SimpleQueueService.TooManyEntriesInBatchRequest"
            }
            Self::EmptyBatchRequest => "AWS.SimpleQueueService.EmptyBatchRequest",
            Self::InvalidBatchEntryId => "AWS.SimpleQueueService.InvalidBatchEntryId",
            Self::QueueAlreadyExists => "QueueAlreadyExists",
            Self::InvalidAttributeName => "InvalidAttributeName",
            Self::InternalError => "InternalFailure",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::NonExistentQueue => "The specified queue does not exist.",
            Self::InvalidMessageContents => {
                "The message contains characters outside the allowed set."
            }
            Self::ReceiptHandleIsInvalid => {
                "The input receipt handle is not a valid receipt handle."
            }
            Self::BatchEntryIdsNotDistinct => {
                "Two or more batch entries in the request have the same Id."
            }
            Self::TooManyEntriesInBatchRequest => "Maximum number of entries per request are 10.",
            Self::EmptyBatchRequest => "There is nothing to delete.",
            Self::InvalidBatchEntryId => {
                "A batch entry id can only contain alphanumeric characters, hyphens and underscores."
            }
            Self::QueueAlreadyExists => {
                "A queue already exists with the same name and a different value for attribute."
            }
            Self::InvalidAttributeName => "Unknown attribute.",
            Self::InternalError => "We encountered an internal error. Please try again.",
        }
    }

    pub fn http_status(&self) -> StatusCode {
        match self {
            Self::NonExistentQueue => StatusCode::BAD_REQUEST,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    pub fn sender_or_receiver(&self) -> &'static str {
        match self {
            Self::InternalError => "Receiver",
            _ => "Sender",
        }
    }
}

pub struct SqsError {
    pub code: SqsErrorCode,
    pub request_id: String,
    pub protocol: SqsProtocol,
}

#[derive(Clone, Copy, Debug)]
pub enum SqsProtocol {
    Json,
    Query,
}

impl IntoResponse for SqsError {
    fn into_response(self) -> Response {
        let status = self.code.http_status();
        match self.protocol {
            SqsProtocol::Json => json_error_response(status, &self.code, &self.request_id),
            SqsProtocol::Query => xml_error_response(status, &self.code, &self.request_id),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct JsonErrorBody {
    #[serde(rename = "__type")]
    error_type: String,
    message: String,
}

fn json_error_response(status: StatusCode, code: &SqsErrorCode, _request_id: &str) -> Response {
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

fn xml_error_response(status: StatusCode, code: &SqsErrorCode, request_id: &str) -> Response {
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse>
  <Error>
    <Type>{}</Type>
    <Code>{}</Code>
    <Message>{}</Message>
  </Error>
  <RequestId>{}</RequestId>
</ErrorResponse>"#,
        code.sender_or_receiver(),
        code.code(),
        code.message(),
        request_id,
    );
    (status, [("content-type", "text/xml")], xml).into_response()
}
