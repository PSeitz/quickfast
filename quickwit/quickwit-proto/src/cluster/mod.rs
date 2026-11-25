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
use std::sync::Arc;

use bytesize::ByteSize;
use quickwit_common::rate_limited_error;
use quickwit_common::tower::GrpcMetricsLayer;
use serde::{Deserialize, Serialize};
use tonic::codec::CompressionEncoding;
use tracing::Instrument;

use crate::GrpcServiceError;
use crate::error::{
    ServiceError, ServiceErrorCode, grpc_error_to_grpc_status, grpc_status_to_service_error,
};
use crate::grpc::{make_tonic_request, server_span, with_optional_metrics};

include!("../codegen/quickwit/quickwit.cluster.rs");

pub const CLUSTER_PLANE_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/cluster_descriptor.bin");

pub type ClusterResult<T> = std::result::Result<T, ClusterError>;

#[derive(Debug, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClusterError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests,
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl ServiceError for ClusterError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(err_msg) => {
                rate_limited_error!(limit_per_min = 6, "cluster internal error: {err_msg}");
                ServiceErrorCode::Internal
            }
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl quickwit_common::tower::GrpcStatusCode for ClusterError {
    fn grpc_status_code(&self) -> tonic::Code {
        self.error_code().grpc_status_code()
    }
}

impl GrpcServiceError for ClusterError {
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
pub trait ClusterService: fmt::Debug + Send + Sync + 'static {
    async fn fetch_cluster_state(
        &self,
        request: FetchClusterStateRequest,
    ) -> ClusterResult<FetchClusterStateResponse>;
}

#[derive(Debug, Clone)]
pub struct ClusterServiceClient {
    inner: Arc<dyn ClusterService>,
    metrics: Option<GrpcMetricsLayer>,
}

impl ClusterServiceClient {
    pub fn new(service: impl ClusterService) -> Self {
        Self {
            inner: Arc::new(service),
            metrics: None,
        }
    }

    pub fn with_metrics(mut self, metrics: GrpcMetricsLayer) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn from_channel(
        channel: tonic::transport::Channel,
        max_message_size: ByteSize,
        compression_encoding_opt: Option<CompressionEncoding>,
    ) -> Self {
        let mut client = cluster_service_client::ClusterServiceClient::new(channel)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        if let Some(compression_encoding) = compression_encoding_opt {
            client = client
                .accept_compressed(compression_encoding)
                .send_compressed(compression_encoding);
        }
        Self::new(ClusterServiceGrpcClientAdapter { inner: client })
    }

    pub fn as_grpc_service(
        &self,
        max_message_size: ByteSize,
    ) -> cluster_service_server::ClusterServiceServer<ClusterServiceGrpcServerAdapter> {
        let adapter = ClusterServiceGrpcServerAdapter {
            inner: self.clone(),
        };
        cluster_service_server::ClusterServiceServer::new(adapter)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn from_mock(mock: MockClusterService) -> Self {
        Self::new(MockClusterServiceWrapper {
            inner: tokio::sync::Mutex::new(mock),
        })
    }
}

#[async_trait::async_trait]
impl ClusterService for ClusterServiceClient {
    async fn fetch_cluster_state(
        &self,
        request: FetchClusterStateRequest,
    ) -> ClusterResult<FetchClusterStateResponse> {
        with_optional_metrics(
            self.metrics.as_ref(),
            "fetch_cluster_state",
            self.inner.fetch_cluster_state(request),
        )
        .await
    }
}

#[derive(Debug, Clone)]
struct ClusterServiceGrpcClientAdapter<T> {
    inner: cluster_service_client::ClusterServiceClient<T>,
}

#[async_trait::async_trait]
impl<T> ClusterService for ClusterServiceGrpcClientAdapter<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + fmt::Debug + Clone + Send + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
    T::Future: Send,
{
    async fn fetch_cluster_state(
        &self,
        request: FetchClusterStateRequest,
    ) -> ClusterResult<FetchClusterStateResponse> {
        self.inner
            .clone()
            .fetch_cluster_state(make_tonic_request(request))
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| grpc_status_to_service_error(status, "fetch_cluster_state"))
    }
}

#[derive(Debug)]
pub struct ClusterServiceGrpcServerAdapter {
    inner: ClusterServiceClient,
}

#[async_trait::async_trait]
impl cluster_service_server::ClusterService for ClusterServiceGrpcServerAdapter {
    async fn fetch_cluster_state(
        &self,
        tonic_request: tonic::Request<FetchClusterStateRequest>,
    ) -> Result<tonic::Response<FetchClusterStateResponse>, tonic::Status> {
        let span = server_span(
            tonic_request.metadata(),
            tracing::info_span!("cluster.fetch_cluster_state"),
        );
        async move {
            self.inner
                .fetch_cluster_state(tonic_request.into_inner())
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
struct MockClusterServiceWrapper {
    inner: tokio::sync::Mutex<MockClusterService>,
}

#[cfg(any(test, feature = "testsuite"))]
#[async_trait::async_trait]
impl ClusterService for MockClusterServiceWrapper {
    async fn fetch_cluster_state(
        &self,
        request: FetchClusterStateRequest,
    ) -> ClusterResult<FetchClusterStateResponse> {
        self.inner.lock().await.fetch_cluster_state(request).await
    }
}
