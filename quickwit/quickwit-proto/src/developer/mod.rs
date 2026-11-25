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
use tonic::codec::CompressionEncoding;
use tracing::Instrument;

use crate::error::{grpc_error_to_grpc_status, grpc_status_to_service_error};
use crate::grpc::{make_tonic_request, server_span};
use crate::{GrpcServiceError, ServiceError, ServiceErrorCode};

include!("../codegen/quickwit/quickwit.developer.rs");

pub const DEVELOPER_FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!("../codegen/quickwit/developer_descriptor.bin");

pub type DeveloperResult<T> = std::result::Result<T, DeveloperError>;

#[derive(Debug, thiserror::Error, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeveloperError {
    #[error("internal error: {0}")]
    Internal(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("request timed out: {0}")]
    Timeout(String),
    #[error("too many requests")]
    TooManyRequests,
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

impl ServiceError for DeveloperError {
    fn error_code(&self) -> ServiceErrorCode {
        match self {
            Self::Internal(_) => ServiceErrorCode::Internal,
            Self::InvalidArgument(_) => ServiceErrorCode::BadRequest,
            Self::Timeout(_) => ServiceErrorCode::Timeout,
            Self::TooManyRequests => ServiceErrorCode::TooManyRequests,
            Self::Unavailable(_) => ServiceErrorCode::Unavailable,
        }
    }
}

impl quickwit_common::tower::GrpcStatusCode for DeveloperError {
    fn grpc_status_code(&self) -> tonic::Code {
        self.error_code().grpc_status_code()
    }
}

impl GrpcServiceError for DeveloperError {
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

#[async_trait::async_trait]
pub trait DeveloperService: fmt::Debug + Send + Sync + 'static {
    async fn get_debug_info(
        &self,
        request: GetDebugInfoRequest,
    ) -> DeveloperResult<GetDebugInfoResponse>;
}

#[derive(Debug, Clone)]
pub struct DeveloperServiceClient {
    inner: Arc<dyn DeveloperService>,
}

impl DeveloperServiceClient {
    pub fn new(service: impl DeveloperService) -> Self {
        Self {
            inner: Arc::new(service),
        }
    }

    pub fn from_channel(
        channel: tonic::transport::Channel,
        max_message_size: ByteSize,
        compression_encoding_opt: Option<CompressionEncoding>,
    ) -> Self {
        let mut client = developer_service_client::DeveloperServiceClient::new(channel)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        if let Some(compression_encoding) = compression_encoding_opt {
            client = client
                .accept_compressed(compression_encoding)
                .send_compressed(compression_encoding);
        }
        Self::new(DeveloperServiceGrpcClientAdapter { inner: client })
    }

    pub fn as_grpc_service(
        &self,
        max_message_size: ByteSize,
    ) -> developer_service_server::DeveloperServiceServer<DeveloperServiceGrpcServerAdapter> {
        let adapter = DeveloperServiceGrpcServerAdapter {
            inner: self.clone(),
        };
        developer_service_server::DeveloperServiceServer::new(adapter)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize)
    }
}

#[async_trait::async_trait]
impl DeveloperService for DeveloperServiceClient {
    async fn get_debug_info(
        &self,
        request: GetDebugInfoRequest,
    ) -> DeveloperResult<GetDebugInfoResponse> {
        self.inner.get_debug_info(request).await
    }
}

#[derive(Debug, Clone)]
struct DeveloperServiceGrpcClientAdapter<T> {
    inner: developer_service_client::DeveloperServiceClient<T>,
}

#[async_trait::async_trait]
impl<T> DeveloperService for DeveloperServiceGrpcClientAdapter<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + fmt::Debug + Clone + Send + Sync + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
    T::Future: Send,
{
    async fn get_debug_info(
        &self,
        request: GetDebugInfoRequest,
    ) -> DeveloperResult<GetDebugInfoResponse> {
        self.inner
            .clone()
            .get_debug_info(make_tonic_request(request))
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| grpc_status_to_service_error(status, "get_debug_info"))
    }
}

#[derive(Debug)]
pub struct DeveloperServiceGrpcServerAdapter {
    inner: DeveloperServiceClient,
}

#[async_trait::async_trait]
impl developer_service_server::DeveloperService for DeveloperServiceGrpcServerAdapter {
    async fn get_debug_info(
        &self,
        tonic_request: tonic::Request<GetDebugInfoRequest>,
    ) -> Result<tonic::Response<GetDebugInfoResponse>, tonic::Status> {
        let span = server_span(
            tonic_request.metadata(),
            tracing::info_span!("developer.get_debug_info"),
        );
        async move {
            self.inner
                .get_debug_info(tonic_request.into_inner())
                .await
                .map(tonic::Response::new)
                .map_err(grpc_error_to_grpc_status)
        }
        .instrument(span)
        .await
    }
}
