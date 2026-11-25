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

use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytesize::ByteSize;
use quickwit_common::rate_limited_error;
use quickwit_common::tower::{BalanceChannel, GrpcMetricsLayer};
use serde::{Deserialize, Serialize};
use tonic::codec::CompressionEncoding;
use tracing::Instrument;

use crate::GrpcServiceError;
use crate::error::{
    ServiceError, ServiceErrorCode, grpc_error_to_grpc_status, grpc_status_to_service_error,
};
use crate::grpc::{make_tonic_request, server_span, with_optional_metrics, with_optional_timeout};

include!("../codegen/quickwit/quickwit.compaction.rs");

pub const COMPACTION_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/compaction_descriptor.bin");

pub type CompactionResult<T> = std::result::Result<T, CompactionError>;

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests,
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl ServiceError for CompactionError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "compaction error: {err_msg}");
                ServiceErrorCode::Internal
            }
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl quickwit_common::tower::GrpcStatusCode for CompactionError {
    fn grpc_status_code(&self) -> tonic::Code {
        self.error_code().grpc_status_code()
    }
}

impl GrpcServiceError for CompactionError {
    fn new_internal(message: String) -> Self {
        Self::Internal(message)
    }

    fn new_timeout(message: String) -> Self {
        Self::Timeout(message)
    }

    fn new_too_many_requests() -> Self {
        Self::TooManyRequests
    }

    fn new_unavailable(message: String) -> Self {
        Self::Unavailable(message)
    }
}

#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait CompactionPlannerService: fmt::Debug + Send + Sync + 'static {
    async fn report_status(
        &self,
        request: ReportStatusRequest,
    ) -> CompactionResult<ReportStatusResponse>;
}

#[derive(Debug, Clone)]
pub struct CompactionPlannerServiceClient {
    inner: Arc<dyn CompactionPlannerService>,
    metrics: Option<GrpcMetricsLayer>,
    timeout: Option<Duration>,
}

impl CompactionPlannerServiceClient {
    pub fn new(service: impl CompactionPlannerService) -> Self {
        Self {
            inner: Arc::new(service),
            metrics: None,
            timeout: None,
        }
    }

    pub fn with_metrics(mut self, metrics: GrpcMetricsLayer) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn from_balance_channel(
        balance_channel: BalanceChannel<SocketAddr>,
        max_message_size: ByteSize,
        compression_encoding_opt: Option<CompressionEncoding>,
    ) -> Self {
        let mut client =
            compaction_planner_service_client::CompactionPlannerServiceClient::new(balance_channel)
                .max_decoding_message_size(max_message_size.0 as usize)
                .max_encoding_message_size(max_message_size.0 as usize);
        if let Some(compression_encoding) = compression_encoding_opt {
            client = client
                .accept_compressed(compression_encoding)
                .send_compressed(compression_encoding);
        }
        Self::new(CompactionPlannerServiceGrpcClientAdapter { inner: client })
    }

    pub fn as_grpc_service(
        &self,
        max_message_size: ByteSize,
    ) -> compaction_planner_service_server::CompactionPlannerServiceServer<
        CompactionPlannerServiceGrpcServerAdapter,
    > {
        let adapter = CompactionPlannerServiceGrpcServerAdapter {
            inner: self.clone(),
        };
        compaction_planner_service_server::CompactionPlannerServiceServer::new(adapter)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn from_mock(mock: MockCompactionPlannerService) -> Self {
        Self::new(MockCompactionPlannerServiceWrapper {
            inner: tokio::sync::Mutex::new(mock),
        })
    }
}

#[async_trait::async_trait]
impl CompactionPlannerService for CompactionPlannerServiceClient {
    async fn report_status(
        &self,
        request: ReportStatusRequest,
    ) -> CompactionResult<ReportStatusResponse> {
        with_optional_metrics(
            self.metrics.as_ref(),
            "report_status",
            with_optional_timeout(self.timeout, self.inner.report_status(request)),
        )
        .await
    }
}

#[derive(Debug, Clone)]
struct CompactionPlannerServiceGrpcClientAdapter<T> {
    inner: compaction_planner_service_client::CompactionPlannerServiceClient<T>,
}

#[async_trait::async_trait]
impl<T> CompactionPlannerService for CompactionPlannerServiceGrpcClientAdapter<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + fmt::Debug + Clone + Send + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
    T::Future: Send,
{
    async fn report_status(
        &self,
        request: ReportStatusRequest,
    ) -> CompactionResult<ReportStatusResponse> {
        self.inner
            .clone()
            .report_status(make_tonic_request(request))
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| grpc_status_to_service_error(status, "report_status"))
    }
}

#[derive(Debug)]
pub struct CompactionPlannerServiceGrpcServerAdapter {
    inner: CompactionPlannerServiceClient,
}

#[async_trait::async_trait]
impl compaction_planner_service_server::CompactionPlannerService
    for CompactionPlannerServiceGrpcServerAdapter
{
    async fn report_status(
        &self,
        tonic_request: tonic::Request<ReportStatusRequest>,
    ) -> Result<tonic::Response<ReportStatusResponse>, tonic::Status> {
        let span = server_span(
            tonic_request.metadata(),
            tracing::info_span!("compaction.report_status"),
        );
        async move {
            self.inner
                .report_status(tonic_request.into_inner())
                .await
                .map(tonic::Response::new)
                .map_err(grpc_error_to_grpc_status)
        }
        .instrument(span)
        .await
    }
}

#[cfg(any(test, feature = "testsuite"))]
#[derive(Debug)]
struct MockCompactionPlannerServiceWrapper {
    inner: tokio::sync::Mutex<MockCompactionPlannerService>,
}

#[cfg(any(test, feature = "testsuite"))]
#[async_trait::async_trait]
impl CompactionPlannerService for MockCompactionPlannerServiceWrapper {
    async fn report_status(
        &self,
        request: ReportStatusRequest,
    ) -> CompactionResult<ReportStatusResponse> {
        self.inner.lock().await.report_status(request).await
    }
}
