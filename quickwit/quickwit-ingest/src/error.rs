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

use quickwit_common::rate_limited_error;
use quickwit_proto::ingest::{
    IngestFailure as ProtoIngestFailure, IngestFailureReason, RateLimitingCause,
};
use quickwit_proto::types::IndexId;
use quickwit_proto::{ServiceError, ServiceErrorCode, tonic};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, thiserror::Error, Serialize, Deserialize)]
pub enum IngestServiceError {
    #[error("index `{index_id}` not found")]
    IndexNotFound { index_id: IndexId },
    #[error("ingest source not found for index `{index_id}`")]
    SourceNotFound { index_id: IndexId },
    #[error("an internal error occurred: {0}")]
    Internal(String),
    #[error("request timed out")]
    Timeout,
    #[error("rate limited {0}")]
    RateLimited(RateLimitingCause),
    #[error("ingest service is unavailable ({0})")]
    Unavailable(String),
    #[error("bad request ({0})")]
    BadRequest(String),
}

impl From<ProtoIngestFailure> for IngestServiceError {
    fn from(ingest_failure: ProtoIngestFailure) -> Self {
        match ingest_failure.reason() {
            IngestFailureReason::Unspecified => {
                IngestServiceError::Internal("unknown error".to_string())
            }
            IngestFailureReason::IndexNotFound => IngestServiceError::IndexNotFound {
                index_id: ingest_failure.index_id,
            },
            IngestFailureReason::SourceNotFound => IngestServiceError::SourceNotFound {
                index_id: ingest_failure.index_id,
            },
            IngestFailureReason::Internal => {
                IngestServiceError::Internal("internal error".to_string())
            }
            IngestFailureReason::Timeout => IngestServiceError::Timeout,
            IngestFailureReason::LoadShedding => {
                IngestServiceError::RateLimited(RateLimitingCause::LoadShedding)
            }
        }
    }
}

impl ServiceError for IngestServiceError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::IndexNotFound { .. } => ServiceErrorCode::NotFound,
            Self::SourceNotFound { .. } => ServiceErrorCode::Internal,
            Self::Internal(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "ingest internal error: {err_msg}");
                ServiceErrorCode::Internal
            }
            Self::Timeout => ServiceErrorCode::Internal,
            Self::RateLimited(_) => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
            Self::BadRequest(_) => ServiceErrorCode::BadRequest,
        }
    }
}

impl From<IngestServiceError> for tonic::Status {
    fn from(error: IngestServiceError) -> tonic::Status {
        let code = error.error_code().grpc_status_code();
        tonic::Status::new(code, error.to_string())
    }
}

impl quickwit_common::tower::GrpcStatusCode for IngestServiceError {
    fn grpc_status_code(&self) -> tonic::Code {
        self.error_code().grpc_status_code()
    }
}
