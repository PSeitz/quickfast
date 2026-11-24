// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;

use quickwit_actors::AskError;
use quickwit_common::rate_limited_error;
use quickwit_proto::ingest::RateLimitingCause;
use quickwit_proto::ingest::router::{IngestFailure, IngestFailureReason};
use quickwit_proto::types::IndexId;
use quickwit_proto::{GrpcServiceError, ServiceError, ServiceErrorCode, tonic};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
pub enum IngestServiceError {
    #[error("data corruption: {0}")]
    Corruption(String),
    #[error("index `{index_id}` already exists")]
    IndexAlreadyExists { index_id: IndexId },
    #[error("index `{index_id}` not found")]
    IndexNotFound { index_id: IndexId },
    #[error("an internal error occurred: {0}")]
    Internal(String),
    #[error("invalid position: {0}")]
    InvalidPosition(String),
    #[error("io error {0}")]
    IoError(String),
    #[error("rate limited {0}")]
    RateLimited(RateLimitingCause),
    #[error("ingest service is unavailable ({0})")]
    Unavailable(String),
    #[error("bad request ({0})")]
    BadRequest(String),
}

impl From<AskError<IngestServiceError>> for IngestServiceError {
    fn from(error: AskError<IngestServiceError>) -> Self {
        match error {
            AskError::ErrorReply(error) => error,
            AskError::MessageNotDelivered => {
                IngestServiceError::Unavailable("actor not running".to_string())
            }
            AskError::ProcessMessageError => IngestServiceError::Internal(error.to_string()),
        }
    }
}

impl From<io::Error> for IngestServiceError {
    fn from(io_error: io::Error) -> Self {
        IngestServiceError::IoError(io_error.to_string())
    }
}

impl From<IngestFailure> for IngestServiceError {
    fn from(ingest_failure: IngestFailure) -> Self {
        match ingest_failure.reason() {
            IngestFailureReason::Unspecified => {
                IngestServiceError::Internal("unknown error".to_string())
            }
            IngestFailureReason::IndexNotFound => IngestServiceError::IndexNotFound {
                index_id: ingest_failure.index_id,
            },
            IngestFailureReason::SourceNotFound => IngestServiceError::Internal(format!(
                "Ingest source not found for index {}",
                ingest_failure.index_id
            )),
            IngestFailureReason::Internal => {
                IngestServiceError::Internal("internal error".to_string())
            }
            IngestFailureReason::Timeout => {
                IngestServiceError::Internal("request timed out".to_string())
            }
            IngestFailureReason::LoadShedding => {
                IngestServiceError::RateLimited(RateLimitingCause::LoadShedding)
            }
        }
    }
}

impl ServiceError for IngestServiceError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Corruption(err_msg) => {
                rate_limited_error!(
                    limit_per_min = 6,
                    "ingest/corruption internal error: {err_msg}"
                );
                ServiceErrorCode::Internal
            }
            Self::IndexAlreadyExists { .. } => ServiceErrorCode::AlreadyExists,
            Self::IndexNotFound { .. } => ServiceErrorCode::NotFound,
            Self::Internal(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "ingest internal error: {err_msg}");
                ServiceErrorCode::Internal
            }
            Self::InvalidPosition(_) => ServiceErrorCode::BadRequest,
            Self::IoError(io_err) => {
                rate_limited_error!(limit_per_min = 6, "ingest/io internal error: {io_err}");
                ServiceErrorCode::Internal
            }
            Self::RateLimited(_) => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
            Self::BadRequest(_) => ServiceErrorCode::BadRequest,
        }
    }
}

impl quickwit_common::tower::GrpcStatusCode for IngestServiceError {
    fn grpc_status_code(&self) -> tonic::Code {
        self.error_code().grpc_status_code()
    }
}

impl GrpcServiceError for IngestServiceError {
    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_too_many_requests() -> Self {
        Self::RateLimited(RateLimitingCause::Unknown)
    }

    fn new_unavailable(error_msg: String) -> Self {
        Self::Unavailable(error_msg)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("key should contain 16 bytes, got {0}")]
pub struct CorruptedKey(pub usize);

impl From<CorruptedKey> for IngestServiceError {
    fn from(error: CorruptedKey) -> Self {
        IngestServiceError::Corruption(format!("corrupted key: {error:?}"))
    }
}

impl From<IngestServiceError> for tonic::Status {
    fn from(error: IngestServiceError) -> tonic::Status {
        let code = match &error {
            IngestServiceError::Corruption { .. } => tonic::Code::DataLoss,
            IngestServiceError::IndexAlreadyExists { .. } => tonic::Code::AlreadyExists,
            IngestServiceError::IndexNotFound { .. } => tonic::Code::NotFound,
            IngestServiceError::Internal(_) => tonic::Code::Internal,
            IngestServiceError::InvalidPosition(_) => tonic::Code::InvalidArgument,
            IngestServiceError::IoError { .. } => tonic::Code::Internal,
            IngestServiceError::RateLimited(_) => tonic::Code::ResourceExhausted,
            IngestServiceError::Unavailable(_) => tonic::Code::Unavailable,
            IngestServiceError::BadRequest(_) => tonic::Code::InvalidArgument,
        };
        let message = error.to_string();
        tonic::Status::new(code, message)
    }
}
