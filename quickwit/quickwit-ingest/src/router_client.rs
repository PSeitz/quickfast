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

//! Dispatch public ingest requests through [`IngesthreeRouter`].

use std::sync::Arc;

use quickwit_proto::ingest::{
    IngestFailure as ProtoIngestFailure, IngestRequestV2, IngestResponseV2, IngestSuccess,
};

use crate::IngestServiceError;
use crate::ingesthree::IngesthreeRouter;

#[cfg(any(test, feature = "testsuite"))]
mockall::mock! {
    #[derive(Debug)]
    pub IngestRouter {
        pub fn ingest(
            &self,
            request: IngestRequestV2,
        ) -> Result<IngestResponse, IngestServiceError>;
    }
}

#[derive(Clone, Debug)]
pub enum IngestRouterClient {
    Ingesthree(Arc<IngesthreeRouter>),
    #[cfg(any(test, feature = "testsuite"))]
    Mock(Arc<tokio::sync::Mutex<MockIngestRouter>>),
}

impl From<IngesthreeRouter> for IngestRouterClient {
    fn from(router: IngesthreeRouter) -> Self {
        Self::Ingesthree(Arc::new(router))
    }
}

impl IngestRouterClient {
    #[cfg(any(test, feature = "testsuite"))]
    pub fn from_mock(mock: MockIngestRouter) -> Self {
        Self::Mock(Arc::new(tokio::sync::Mutex::new(mock)))
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn mocked() -> Self {
        Self::from_mock(MockIngestRouter::new())
    }

    pub async fn ingest(
        &self,
        request: IngestRequestV2,
    ) -> Result<IngestResponse, IngestServiceError> {
        match self {
            Self::Ingesthree(router) => router.ingest(request).await,
            #[cfg(any(test, feature = "testsuite"))]
            Self::Mock(mock) => mock.lock().await.ingest(request),
        }
    }
}

#[derive(Debug, Default)]
pub struct IngestResponse {
    pub successes: Vec<IngestSuccess>,
    pub failures: Vec<IngestFailure>,
}

impl From<IngestResponseV2> for IngestResponse {
    fn from(response: IngestResponseV2) -> Self {
        Self {
            successes: response.successes,
            failures: response.failures.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct IngestFailure {
    pub subrequest_id: u32,
    pub index_id: String,
    pub error: IngestServiceError,
}

impl IngestFailure {
    pub fn subrequest_id(&self) -> u32 {
        self.subrequest_id
    }

    pub fn index_id(&self) -> &str {
        &self.index_id
    }
}

impl From<ProtoIngestFailure> for IngestFailure {
    fn from(failure: ProtoIngestFailure) -> Self {
        let subrequest_id = failure.subrequest_id;
        let index_id = failure.index_id.clone();
        Self {
            subrequest_id,
            index_id,
            error: failure.into(),
        }
    }
}

impl From<IngestFailure> for IngestServiceError {
    fn from(failure: IngestFailure) -> Self {
        failure.error
    }
}

#[cfg(test)]
mod tests {
    use quickwit_proto::ingest::{IngestFailureReason, RateLimitingCause};
    use quickwit_proto::{ServiceError, ServiceErrorCode};

    use super::*;

    #[test]
    fn test_ingesthree_error_mapping_preserves_status_and_message() {
        for (error, code) in [
            (
                IngestServiceError::BadRequest("bad input".into()),
                ServiceErrorCode::BadRequest,
            ),
            (
                IngestServiceError::Unavailable("no destination".into()),
                ServiceErrorCode::Unavailable,
            ),
            (
                IngestServiceError::RateLimited(RateLimitingCause::LoadShedding),
                ServiceErrorCode::TooManyRequests,
            ),
        ] {
            let message = error.to_string();
            assert_eq!(
                error.error_code().http_status_code(),
                code.http_status_code()
            );
            let failure = IngestFailure {
                subrequest_id: 7,
                index_id: "logs".into(),
                error,
            };
            assert_eq!(failure.subrequest_id(), 7);
            assert_eq!(failure.index_id(), "logs");
            let error = IngestServiceError::from(failure);
            assert_eq!(
                error.error_code().http_status_code(),
                code.http_status_code()
            );
            assert_eq!(error.to_string(), message);
        }
    }

    #[test]
    fn test_ingesthree_response_preserves_partial_results() {
        let response = IngestResponse::from(IngestResponseV2 {
            successes: vec![IngestSuccess {
                subrequest_id: 1,
                ..Default::default()
            }],
            failures: vec![ProtoIngestFailure {
                subrequest_id: 2,
                index_id: "logs".into(),
                reason: IngestFailureReason::IndexNotFound as i32,
                ..Default::default()
            }],
        });
        assert_eq!(response.successes[0].subrequest_id, 1);
        assert_eq!(response.failures[0].subrequest_id(), 2);
        assert_eq!(response.failures[0].index_id(), "logs");
        assert!(
            matches!(IngestServiceError::from(response.failures.into_iter().next().unwrap()),
            IngestServiceError::IndexNotFound { index_id } if index_id == "logs")
        );
    }
}
