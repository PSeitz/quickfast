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

use std::iter::zip;

use bytes::Bytes;
use quickwit_common::rate_limited_error;
use quickwit_common::tower::MakeLoadShedError;
use serde::{Deserialize, Serialize};

use super::GrpcServiceError;
use crate::types::DocUid;
use crate::{ServiceError, ServiceErrorCode};

pub mod router;

pub mod ingesthree {
    include!("../codegen/quickwit/quickwit.ingest.ingesthree.rs");

    pub const INGESTHREE_FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!("../codegen/quickwit/ingesthree_descriptor.bin");
}

include!("../codegen/quickwit/quickwit.ingest.rs");

pub const INGEST_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/ingest_descriptor.bin");

pub type IngestV2Result<T> = std::result::Result<T, IngestV2Error>;

#[derive(Debug, Copy, Clone, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
pub enum RateLimitingCause {
    #[error("router load shedding")]
    RouterLoadShedding,
    #[error("load shedding")]
    LoadShedding,
    #[error("wal full (memory or disk)")]
    WalFull,
    #[error("circuit breaker")]
    CircuitBreaker,
    #[error("shard rate limiting")]
    ShardRateLimiting,
    #[error("unknown")]
    Unknown,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestV2Error {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests(RateLimitingCause),
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl From<quickwit_common::tower::TimeoutExceeded> for IngestV2Error {
    fn from(_: quickwit_common::tower::TimeoutExceeded) -> IngestV2Error {
        IngestV2Error::Timeout("tower layer timeout".to_string())
    }
}

impl From<quickwit_common::tower::TaskCancelled> for IngestV2Error {
    fn from(task_cancelled: quickwit_common::tower::TaskCancelled) -> IngestV2Error {
        IngestV2Error::Internal(task_cancelled.to_string())
    }
}

impl ServiceError for IngestV2Error {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(error_msg) => {
                rate_limited_error!(limit_per_min = 6, "ingest internal error: {error_msg}");
                ServiceErrorCode::Internal
            }
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests(_) => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl quickwit_common::tower::GrpcStatusCode for IngestV2Error {
    fn grpc_status_code(&self) -> tonic::Code {
        self.error_code().grpc_status_code()
    }
}

impl GrpcServiceError for IngestV2Error {
    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        Self::Timeout(message)
    }

    fn new_too_many_requests() -> Self {
        Self::TooManyRequests(RateLimitingCause::Unknown)
    }

    fn new_unavailable(message: String) -> Self {
        Self::Unavailable(message)
    }
}

impl MakeLoadShedError for IngestV2Error {
    fn make_load_shed_error() -> Self {
        IngestV2Error::TooManyRequests(RateLimitingCause::LoadShedding)
    }
}

impl DocBatchV2 {
    pub fn docs(&self) -> impl Iterator<Item = (DocUid, Bytes)> + Clone + '_ {
        zip(&self.doc_uids, &self.doc_lengths).scan(
            self.doc_buffer.clone(),
            |doc_buffer, (doc_uid, doc_len)| {
                let doc = doc_buffer.split_to(*doc_len as usize);
                Some((*doc_uid, doc))
            },
        )
    }

    pub fn into_docs(self) -> impl Iterator<Item = (DocUid, Bytes)> {
        zip(self.doc_uids, self.doc_lengths).scan(
            self.doc_buffer,
            |doc_buffer, (doc_uid, doc_len)| {
                let doc = doc_buffer.split_to(doc_len as usize);
                Some((doc_uid, doc))
            },
        )
    }

    pub fn is_empty(&self) -> bool {
        self.doc_lengths.is_empty()
    }

    pub fn num_bytes(&self) -> usize {
        self.doc_buffer.len() + self.doc_lengths.len() * 4
    }

    pub fn num_docs(&self) -> usize {
        self.doc_lengths.len()
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(docs: impl IntoIterator<Item = &'static str>) -> Self {
        let mut doc_uids = Vec::new();
        let mut doc_buffer = Vec::new();
        let mut doc_lengths = Vec::new();

        for (doc_uid, doc) in docs.into_iter().enumerate() {
            doc_uids.push(DocUid::for_test(doc_uid as u128));
            doc_buffer.extend(doc.as_bytes());
            doc_lengths.push(doc.len() as u32);
        }
        Self {
            doc_uids,
            doc_buffer: Bytes::from(doc_buffer),
            doc_lengths,
        }
    }
}
