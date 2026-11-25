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

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bytesize::ByteSize;
use quickwit_common::ServiceStream;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::retry::{RetryParams, Retryable};
use quickwit_common::tower::{BalanceChannel, GrpcMetricsLayer, GrpcStatusCode};
use quickwit_common::uri::Uri;
use tokio::sync::{Semaphore, watch};
use tonic::codec::CompressionEncoding;
use tracing::Instrument;

use super::{
    AddSourceRequest, AssignFilesForIndexing, AssignFilesForIndexingResponse, CreateIndexRequest,
    CreateIndexResponse, CreateIndexTemplateRequest, DeleteIndexRequest,
    DeleteIndexTemplatesRequest, DeleteQuery, DeleteSourceRequest, DeleteSplitsRequest, DeleteTask,
    EmptyResponse, GetIndexTemplateRequest, GetIndexTemplateResponse, IndexMetadataRequest,
    IndexMetadataResponse, IndexesMetadataRequest, IndexesMetadataResponse,
    LastDeleteOpstampRequest, LastDeleteOpstampResponse, ListCompletedFilesRequest,
    ListCompletedFilesResponse, ListDeleteTasksRequest, ListDeleteTasksResponse,
    ListIndexTemplatesRequest, ListIndexTemplatesResponse, ListIndexesMetadataRequest,
    ListIndexesMetadataResponse, ListSplitsRequest, ListSplitsResponse, ListStaleSplitsRequest,
    MarkSplitsForDeletionRequest, MetastoreError, MetastoreResult, PublishFileSplits,
    PublishFileSplitsResponse, PublishSplitsRequest, RegisterFileForIndexing,
    RegisterFileForIndexingResponse, StageSplitsRequest, ToggleSourceRequest, UpdateIndexRequest,
    UpdateSourceRequest, UpdateSplitsDeleteOpstampRequest, UpdateSplitsDeleteOpstampResponse,
    metastore_service_client, metastore_service_server,
};
use crate::error::{grpc_error_to_grpc_status, grpc_status_to_service_error};
use crate::grpc::{make_tonic_request, server_span};

pub type MetastoreServiceStream<T> = ServiceStream<MetastoreResult<T>>;

macro_rules! for_each_unary_metastore_rpc {
    ($macro:ident) => {
        $macro! {
            (create_index, CreateIndexRequest, CreateIndexResponse, event),
            (update_index, UpdateIndexRequest, IndexMetadataResponse, event),
            (index_metadata, IndexMetadataRequest, IndexMetadataResponse, plain),
            (indexes_metadata, IndexesMetadataRequest, IndexesMetadataResponse, plain),
            (list_indexes_metadata, ListIndexesMetadataRequest, ListIndexesMetadataResponse, plain),
            (delete_index, DeleteIndexRequest, EmptyResponse, event),
            (register_file_for_indexing, RegisterFileForIndexing, RegisterFileForIndexingResponse, plain),
            (assign_files_for_indexing, AssignFilesForIndexing, AssignFilesForIndexingResponse, plain),
            (publish_file_splits, PublishFileSplits, PublishFileSplitsResponse, plain),
            (list_completed_files, ListCompletedFilesRequest, ListCompletedFilesResponse, plain),
            (stage_splits, StageSplitsRequest, EmptyResponse, plain),
            (publish_splits, PublishSplitsRequest, EmptyResponse, plain),
            (mark_splits_for_deletion, MarkSplitsForDeletionRequest, EmptyResponse, plain),
            (delete_splits, DeleteSplitsRequest, EmptyResponse, plain),
            (add_source, AddSourceRequest, EmptyResponse, event),
            (update_source, UpdateSourceRequest, EmptyResponse, event),
            (toggle_source, ToggleSourceRequest, EmptyResponse, event),
            (delete_source, DeleteSourceRequest, EmptyResponse, event),
            (last_delete_opstamp, LastDeleteOpstampRequest, LastDeleteOpstampResponse, plain),
            (create_delete_task, DeleteQuery, DeleteTask, plain),
            (update_splits_delete_opstamp, UpdateSplitsDeleteOpstampRequest, UpdateSplitsDeleteOpstampResponse, plain),
            (list_delete_tasks, ListDeleteTasksRequest, ListDeleteTasksResponse, plain),
            (list_stale_splits, ListStaleSplitsRequest, ListSplitsResponse, plain),
            (create_index_template, CreateIndexTemplateRequest, EmptyResponse, plain),
            (get_index_template, GetIndexTemplateRequest, GetIndexTemplateResponse, plain),
            (list_index_templates, ListIndexTemplatesRequest, ListIndexTemplatesResponse, plain),
            (delete_index_templates, DeleteIndexTemplatesRequest, EmptyResponse, plain),
        }
    };
}

pub(super) use for_each_unary_metastore_rpc;

#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait::async_trait]
pub trait MetastoreService: fmt::Debug + Send + Sync + 'static {
    async fn check_connectivity(&self) -> anyhow::Result<()>;

    fn endpoints(&self) -> Vec<Uri>;

    async fn create_index(
        &self,
        request: CreateIndexRequest,
    ) -> MetastoreResult<CreateIndexResponse>;
    async fn update_index(
        &self,
        request: UpdateIndexRequest,
    ) -> MetastoreResult<IndexMetadataResponse>;
    async fn index_metadata(
        &self,
        request: IndexMetadataRequest,
    ) -> MetastoreResult<IndexMetadataResponse>;
    async fn indexes_metadata(
        &self,
        request: IndexesMetadataRequest,
    ) -> MetastoreResult<IndexesMetadataResponse>;
    async fn list_indexes_metadata(
        &self,
        request: ListIndexesMetadataRequest,
    ) -> MetastoreResult<ListIndexesMetadataResponse>;
    async fn delete_index(&self, request: DeleteIndexRequest) -> MetastoreResult<EmptyResponse>;
    async fn list_splits(
        &self,
        request: ListSplitsRequest,
    ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>>;
    async fn register_file_for_indexing(
        &self,
        request: RegisterFileForIndexing,
    ) -> MetastoreResult<RegisterFileForIndexingResponse>;
    async fn assign_files_for_indexing(
        &self,
        request: AssignFilesForIndexing,
    ) -> MetastoreResult<AssignFilesForIndexingResponse>;
    async fn publish_file_splits(
        &self,
        request: PublishFileSplits,
    ) -> MetastoreResult<PublishFileSplitsResponse>;
    async fn list_completed_files(
        &self,
        request: ListCompletedFilesRequest,
    ) -> MetastoreResult<ListCompletedFilesResponse>;
    async fn stage_splits(&self, request: StageSplitsRequest) -> MetastoreResult<EmptyResponse>;
    async fn publish_splits(&self, request: PublishSplitsRequest)
    -> MetastoreResult<EmptyResponse>;
    async fn mark_splits_for_deletion(
        &self,
        request: MarkSplitsForDeletionRequest,
    ) -> MetastoreResult<EmptyResponse>;
    async fn delete_splits(&self, request: DeleteSplitsRequest) -> MetastoreResult<EmptyResponse>;
    async fn add_source(&self, request: AddSourceRequest) -> MetastoreResult<EmptyResponse>;
    async fn update_source(&self, request: UpdateSourceRequest) -> MetastoreResult<EmptyResponse>;
    async fn toggle_source(&self, request: ToggleSourceRequest) -> MetastoreResult<EmptyResponse>;
    async fn delete_source(&self, request: DeleteSourceRequest) -> MetastoreResult<EmptyResponse>;
    async fn last_delete_opstamp(
        &self,
        request: LastDeleteOpstampRequest,
    ) -> MetastoreResult<LastDeleteOpstampResponse>;
    async fn create_delete_task(&self, request: DeleteQuery) -> MetastoreResult<DeleteTask>;
    async fn update_splits_delete_opstamp(
        &self,
        request: UpdateSplitsDeleteOpstampRequest,
    ) -> MetastoreResult<UpdateSplitsDeleteOpstampResponse>;
    async fn list_delete_tasks(
        &self,
        request: ListDeleteTasksRequest,
    ) -> MetastoreResult<ListDeleteTasksResponse>;
    async fn list_stale_splits(
        &self,
        request: ListStaleSplitsRequest,
    ) -> MetastoreResult<ListSplitsResponse>;
    async fn create_index_template(
        &self,
        request: CreateIndexTemplateRequest,
    ) -> MetastoreResult<EmptyResponse>;
    async fn get_index_template(
        &self,
        request: GetIndexTemplateRequest,
    ) -> MetastoreResult<GetIndexTemplateResponse>;
    async fn list_index_templates(
        &self,
        request: ListIndexTemplatesRequest,
    ) -> MetastoreResult<ListIndexTemplatesResponse>;
    async fn delete_index_templates(
        &self,
        request: DeleteIndexTemplatesRequest,
    ) -> MetastoreResult<EmptyResponse>;
}

#[derive(Debug, Clone)]
pub struct MetastoreServiceClient {
    inner: Arc<dyn MetastoreService>,
}

impl MetastoreServiceClient {
    pub fn new(service: impl MetastoreService) -> Self {
        Self {
            inner: Arc::new(service),
        }
    }

    pub fn from_balance_channel(
        balance_channel: BalanceChannel<SocketAddr>,
        max_message_size: ByteSize,
        compression_encoding_opt: Option<CompressionEncoding>,
    ) -> Self {
        let connection_addrs_rx = balance_channel.connection_keys_watcher();
        let mut client = metastore_service_client::MetastoreServiceClient::new(balance_channel)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize);
        if let Some(compression_encoding) = compression_encoding_opt {
            client = client
                .accept_compressed(compression_encoding)
                .send_compressed(compression_encoding);
        }
        Self::new(MetastoreServiceGrpcClientAdapter {
            inner: client,
            connection_addrs_rx,
        })
    }

    /// Wraps this client with gRPC client metrics, retries, timeout, and concurrency limiting.
    pub fn with_client_middleware(
        self,
        metrics: GrpcMetricsLayer,
        retry_params: RetryParams,
        timeout: Duration,
        max_concurrency: usize,
    ) -> Self {
        Self::new(MetastoreClientMiddleware {
            inner: self,
            metrics,
            retry_params,
            timeout,
            concurrency_limit: Arc::new(Semaphore::new(max_concurrency)),
        })
    }

    /// Wraps this server-side metastore with gRPC server metrics, load shedding, and optional
    /// event publication on mutating index/source RPCs.
    pub fn with_server_middleware(
        self,
        metrics: GrpcMetricsLayer,
        max_in_flight: usize,
        event_broker_opt: Option<EventBroker>,
    ) -> Self {
        Self::new(MetastoreServerMiddleware {
            inner: self,
            metrics,
            in_flight_limit: Arc::new(Semaphore::new(max_in_flight)),
            event_broker_opt,
        })
    }

    pub fn as_grpc_service(
        &self,
        max_message_size: ByteSize,
    ) -> metastore_service_server::MetastoreServiceServer<MetastoreServiceGrpcServerAdapter> {
        let adapter = MetastoreServiceGrpcServerAdapter {
            inner: self.clone(),
        };
        metastore_service_server::MetastoreServiceServer::new(adapter)
            .accept_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(max_message_size.0 as usize)
            .max_encoding_message_size(max_message_size.0 as usize)
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn from_mock(mock: MockMetastoreService) -> Self {
        Self::new(MockMetastoreServiceWrapper {
            inner: tokio::sync::Mutex::new(mock),
        })
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn mocked() -> Self {
        Self::from_mock(MockMetastoreService::new())
    }
}

macro_rules! impl_client_forward {
    ($(($method:ident, $req:ty, $resp:ty, $kind:ident),)*) => {
        #[async_trait::async_trait]
        impl MetastoreService for MetastoreServiceClient {
            async fn check_connectivity(&self) -> anyhow::Result<()> {
                self.inner.check_connectivity().await
            }

            fn endpoints(&self) -> Vec<Uri> {
                self.inner.endpoints()
            }

            async fn list_splits(
                &self,
                request: ListSplitsRequest,
            ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
                self.inner
                    .list_splits(request)
                    .instrument(tracing::info_span!("metastore.list_splits"))
                    .await
            }

            $(
                async fn $method(&self, request: $req) -> MetastoreResult<$resp> {
                    self.inner
                        .$method(request)
                        .instrument(tracing::info_span!(concat!(
                            "metastore.",
                            stringify!($method)
                        )))
                        .await
                }
            )*
        }
    };
}
for_each_unary_metastore_rpc!(impl_client_forward);

#[derive(Debug)]
struct MetastoreClientMiddleware {
    inner: MetastoreServiceClient,
    metrics: GrpcMetricsLayer,
    retry_params: RetryParams,
    timeout: Duration,
    concurrency_limit: Arc<Semaphore>,
}

impl MetastoreClientMiddleware {
    async fn call<Req, Resp, F, Fut>(
        &self,
        rpc_name: &'static str,
        request: Req,
        mut call_fn: F,
    ) -> MetastoreResult<Resp>
    where
        Req: Clone,
        F: FnMut(Req) -> Fut,
        Fut: Future<Output = MetastoreResult<Resp>>,
    {
        self.metrics
            .instrument_rpc(rpc_name, async {
                let mut num_attempts = 0;
                loop {
                    let result = tokio::time::timeout(self.timeout, async {
                        let _permit = self
                            .concurrency_limit
                            .acquire()
                            .await
                            .expect("metastore concurrency semaphore should stay open");
                        call_fn(request.clone()).await
                    })
                    .await
                    .unwrap_or_else(|_| Err(MetastoreError::Timeout("client".to_string())));

                    match result {
                        Ok(response) => return Ok(response),
                        Err(error) => {
                            num_attempts += 1;
                            if !error.is_retryable()
                                || num_attempts >= self.retry_params.max_attempts
                            {
                                return Err(error);
                            }
                            self.metrics
                                .record_retry(rpc_name, error.grpc_status_code());
                            let delay = self.retry_params.compute_delay(num_attempts);
                            tracing::debug!(
                                num_attempts = %num_attempts,
                                delay_millis = %delay.as_millis(),
                                error = ?error,
                                rpc = rpc_name,
                                "metastore request failed, retrying."
                            );
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            })
            .await
    }
}

macro_rules! impl_client_middleware {
    ($(($method:ident, $req:ty, $resp:ty, $kind:ident),)*) => {
        #[async_trait::async_trait]
        impl MetastoreService for MetastoreClientMiddleware {
            async fn check_connectivity(&self) -> anyhow::Result<()> {
                self.inner.check_connectivity().await
            }

            fn endpoints(&self) -> Vec<Uri> {
                self.inner.endpoints()
            }

            async fn list_splits(
                &self,
                request: ListSplitsRequest,
            ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
                self.call("list_splits", request, |req| self.inner.list_splits(req))
                    .await
            }

            $(
                async fn $method(&self, request: $req) -> MetastoreResult<$resp> {
                    self.call(stringify!($method), request, |req| self.inner.$method(req))
                        .await
                }
            )*
        }
    };
}
for_each_unary_metastore_rpc!(impl_client_middleware);

#[derive(Debug)]
struct MetastoreServerMiddleware {
    inner: MetastoreServiceClient,
    metrics: GrpcMetricsLayer,
    in_flight_limit: Arc<Semaphore>,
    event_broker_opt: Option<EventBroker>,
}

impl MetastoreServerMiddleware {
    async fn handle<Resp, Fut>(&self, rpc_name: &'static str, fut: Fut) -> MetastoreResult<Resp>
    where Fut: Future<Output = MetastoreResult<Resp>> {
        self.metrics
            .instrument_rpc(rpc_name, async {
                let Ok(_permit) = self.in_flight_limit.try_acquire() else {
                    return Err(MetastoreError::TooManyRequests);
                };
                fut.await
            })
            .await
    }
}

macro_rules! server_rpc_body {
    ($self:ident, $method:ident, $request:ident, plain) => {
        $self
            .handle(stringify!($method), $self.inner.$method($request))
            .await
    };
    ($self:ident, $method:ident, $request:ident, event) => {{
        let event_req = $self.event_broker_opt.as_ref().map(|_| $request.clone());
        let response = $self
            .handle(stringify!($method), $self.inner.$method($request))
            .await?;
        if let (Some(broker), Some(event)) = (&$self.event_broker_opt, event_req) {
            broker.publish(event);
        }
        Ok(response)
    }};
}

macro_rules! impl_server_middleware {
    ($(($method:ident, $req:ty, $resp:ty, $kind:ident),)*) => {
        #[async_trait::async_trait]
        impl MetastoreService for MetastoreServerMiddleware {
            async fn check_connectivity(&self) -> anyhow::Result<()> {
                self.inner.check_connectivity().await
            }

            fn endpoints(&self) -> Vec<Uri> {
                self.inner.endpoints()
            }

            async fn list_splits(
                &self,
                request: ListSplitsRequest,
            ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
                self.handle("list_splits", self.inner.list_splits(request))
                    .await
            }

            $(
                async fn $method(&self, request: $req) -> MetastoreResult<$resp> {
                    server_rpc_body!(self, $method, request, $kind)
                }
            )*
        }
    };
}
for_each_unary_metastore_rpc!(impl_server_middleware);

#[derive(Debug, Clone)]
pub struct MetastoreServiceGrpcClientAdapter<T> {
    inner: T,
    connection_addrs_rx: watch::Receiver<HashSet<SocketAddr>>,
}

impl<T> MetastoreServiceGrpcClientAdapter<T> {
    pub fn new(inner: T, connection_addrs_rx: watch::Receiver<HashSet<SocketAddr>>) -> Self {
        Self {
            inner,
            connection_addrs_rx,
        }
    }
}

macro_rules! impl_grpc_client_adapter {
    ($(($method:ident, $req:ty, $resp:ty, $kind:ident),)*) => {
        #[async_trait::async_trait]
        impl<T> MetastoreService
            for MetastoreServiceGrpcClientAdapter<
                metastore_service_client::MetastoreServiceClient<T>,
            >
        where
            T: tonic::client::GrpcService<tonic::body::Body>
                + fmt::Debug
                + Clone
                + Send
                + Sync
                + 'static,
            T::ResponseBody:
                tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
            <T::ResponseBody as tonic::codegen::Body>::Error:
                Into<tonic::codegen::StdError> + Send,
            T::Future: Send,
        {
            async fn check_connectivity(&self) -> anyhow::Result<()> {
                if self.connection_addrs_rx.borrow().is_empty() {
                    anyhow::bail!("no server currently available");
                }
                Ok(())
            }

            fn endpoints(&self) -> Vec<Uri> {
                self.connection_addrs_rx
                    .borrow()
                    .iter()
                    .flat_map(|addr| {
                        Uri::from_str(&format!(
                            "grpc://{addr}/quickwit.metastore.MetastoreService"
                        ))
                    })
                    .collect()
            }

            async fn list_splits(
                &self,
                request: ListSplitsRequest,
            ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
                self.inner
                    .clone()
                    .list_splits(make_tonic_request(request))
                    .await
                    .map(|response| {
                        ServiceStream::from(response.into_inner()).map_err(|status| {
                            grpc_status_to_service_error(status, "list_splits")
                        })
                    })
                    .map_err(|status| grpc_status_to_service_error(status, "list_splits"))
            }

            $(
                async fn $method(&self, request: $req) -> MetastoreResult<$resp> {
                    self.inner
                        .clone()
                        .$method(make_tonic_request(request))
                        .await
                        .map(tonic::Response::into_inner)
                        .map_err(|status| {
                            grpc_status_to_service_error(status, stringify!($method))
                        })
                }
            )*
        }
    };
}
for_each_unary_metastore_rpc!(impl_grpc_client_adapter);

#[derive(Debug)]
pub struct MetastoreServiceGrpcServerAdapter {
    inner: MetastoreServiceClient,
}

macro_rules! impl_grpc_server_adapter {
    ($(($method:ident, $req:ty, $resp:ty, $kind:ident),)*) => {
        #[async_trait::async_trait]
        impl metastore_service_server::MetastoreService for MetastoreServiceGrpcServerAdapter {
            type ListSplitsStream = ServiceStream<tonic::Result<ListSplitsResponse>>;

            async fn list_splits(
                &self,
                tonic_request: tonic::Request<ListSplitsRequest>,
            ) -> Result<tonic::Response<Self::ListSplitsStream>, tonic::Status> {
                let span = server_span(
                    tonic_request.metadata(),
                    tracing::info_span!("metastore.list_splits"),
                );
                async move {
                    self.inner
                        .list_splits(tonic_request.into_inner())
                        .await
                        .map(|stream| {
                            tonic::Response::new(stream.map_err(grpc_error_to_grpc_status))
                        })
                        .map_err(grpc_error_to_grpc_status)
                }
                .instrument(span)
                .await
            }

            $(
                async fn $method(
                    &self,
                    tonic_request: tonic::Request<$req>,
                ) -> Result<tonic::Response<$resp>, tonic::Status> {
                    let span = server_span(
                        tonic_request.metadata(),
                        tracing::info_span!(concat!("metastore.", stringify!($method))),
                    );
                    async move {
                        self.inner
                            .$method(tonic_request.into_inner())
                            .await
                            .map(tonic::Response::new)
                            .map_err(grpc_error_to_grpc_status)
                    }
                    .instrument(span)
                    .await
                }
            )*
        }
    };
}
for_each_unary_metastore_rpc!(impl_grpc_server_adapter);

#[cfg(any(test, feature = "testsuite"))]
#[derive(Debug)]
struct MockMetastoreServiceWrapper {
    inner: tokio::sync::Mutex<MockMetastoreService>,
}

#[cfg(any(test, feature = "testsuite"))]
macro_rules! impl_mock_wrapper {
    ($(($method:ident, $req:ty, $resp:ty, $kind:ident),)*) => {
        #[async_trait::async_trait]
        impl MetastoreService for MockMetastoreServiceWrapper {
            async fn check_connectivity(&self) -> anyhow::Result<()> {
                self.inner.lock().await.check_connectivity().await
            }

            fn endpoints(&self) -> Vec<Uri> {
                futures::executor::block_on(self.inner.lock()).endpoints()
            }

            async fn list_splits(
                &self,
                request: ListSplitsRequest,
            ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
                self.inner.lock().await.list_splits(request).await
            }

            $(
                async fn $method(&self, request: $req) -> MetastoreResult<$resp> {
                    self.inner.lock().await.$method(request).await
                }
            )*
        }
    };
}
#[cfg(any(test, feature = "testsuite"))]
for_each_unary_metastore_rpc!(impl_mock_wrapper);
