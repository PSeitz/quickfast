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
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use quickwit_common::uri::Uri;

use super::service::{MetastoreService, MetastoreServiceClient, MetastoreServiceStream};
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
    MarkSplitsForDeletionRequest, MetastoreResult, PublishFileSplits, PublishFileSplitsResponse,
    PublishSplitsRequest, RegisterFileForIndexing, RegisterFileForIndexingResponse,
    StageSplitsRequest, ToggleSourceRequest, UpdateIndexRequest, UpdateSourceRequest,
    UpdateSplitsDeleteOpstampRequest, UpdateSplitsDeleteOpstampResponse,
};

type InterceptFn<Req, Resp> = Arc<
    dyn Fn(
            MetastoreServiceClient,
            Req,
        ) -> Pin<Box<dyn Future<Output = MetastoreResult<Resp>> + Send>>
        + Send
        + Sync,
>;

#[derive(Default)]
struct MetastoreInterceptor {
    inner: Option<MetastoreServiceClient>,
    index_metadata: Option<InterceptFn<IndexMetadataRequest, IndexMetadataResponse>>,
    assign_files_for_indexing:
        Option<InterceptFn<AssignFilesForIndexing, AssignFilesForIndexingResponse>>,
    publish_file_splits: Option<InterceptFn<PublishFileSplits, PublishFileSplitsResponse>>,
    register_file_for_indexing:
        Option<InterceptFn<RegisterFileForIndexing, RegisterFileForIndexingResponse>>,
}

impl fmt::Debug for MetastoreInterceptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetastoreInterceptor").finish()
    }
}

impl MetastoreInterceptor {
    fn inner(&self) -> &MetastoreServiceClient {
        self.inner.as_ref().expect("inner metastore should be set")
    }
}

impl MetastoreServiceClient {
    pub fn intercept_index_metadata<F, Fut>(self, interceptor: F) -> Self
    where
        F: Fn(MetastoreServiceClient, IndexMetadataRequest) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = MetastoreResult<IndexMetadataResponse>> + Send + 'static,
    {
        Self::new(MetastoreInterceptor {
            inner: Some(self),
            index_metadata: Some(Arc::new(move |inner, req| {
                Box::pin(interceptor(inner, req))
            })),
            ..Default::default()
        })
    }

    pub fn intercept_assign_files_for_indexing<F, Fut>(self, interceptor: F) -> Self
    where
        F: Fn(MetastoreServiceClient, AssignFilesForIndexing) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = MetastoreResult<AssignFilesForIndexingResponse>> + Send + 'static,
    {
        Self::new(MetastoreInterceptor {
            inner: Some(self),
            assign_files_for_indexing: Some(Arc::new(move |inner, req| {
                Box::pin(interceptor(inner, req))
            })),
            ..Default::default()
        })
    }

    pub fn intercept_publish_file_splits<F, Fut>(self, interceptor: F) -> Self
    where
        F: Fn(MetastoreServiceClient, PublishFileSplits) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = MetastoreResult<PublishFileSplitsResponse>> + Send + 'static,
    {
        Self::new(MetastoreInterceptor {
            inner: Some(self),
            publish_file_splits: Some(Arc::new(move |inner, req| {
                Box::pin(interceptor(inner, req))
            })),
            ..Default::default()
        })
    }

    pub fn intercept_register_file_for_indexing<F, Fut>(self, interceptor: F) -> Self
    where
        F: Fn(MetastoreServiceClient, RegisterFileForIndexing) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = MetastoreResult<RegisterFileForIndexingResponse>> + Send + 'static,
    {
        Self::new(MetastoreInterceptor {
            inner: Some(self),
            register_file_for_indexing: Some(Arc::new(move |inner, req| {
                Box::pin(interceptor(inner, req))
            })),
            ..Default::default()
        })
    }
}

macro_rules! delegate_intercept {
    ($self:ident, index_metadata, $req:ident) => {
        if let Some(hook) = &$self.index_metadata {
            hook($self.inner().clone(), $req).await
        } else {
            $self.inner().index_metadata($req).await
        }
    };
    ($self:ident, assign_files_for_indexing, $req:ident) => {
        if let Some(hook) = &$self.assign_files_for_indexing {
            hook($self.inner().clone(), $req).await
        } else {
            $self.inner().assign_files_for_indexing($req).await
        }
    };
    ($self:ident, publish_file_splits, $req:ident) => {
        if let Some(hook) = &$self.publish_file_splits {
            hook($self.inner().clone(), $req).await
        } else {
            $self.inner().publish_file_splits($req).await
        }
    };
    ($self:ident, register_file_for_indexing, $req:ident) => {
        if let Some(hook) = &$self.register_file_for_indexing {
            hook($self.inner().clone(), $req).await
        } else {
            $self.inner().register_file_for_indexing($req).await
        }
    };
    ($self:ident, $method:ident, $req:ident) => {
        $self.inner().$method($req).await
    };
}

macro_rules! impl_interceptor {
    ($(($method:ident, $req:ty, $resp:ty, $kind:ident),)*) => {
        #[async_trait::async_trait]
        impl MetastoreService for MetastoreInterceptor {
            async fn check_connectivity(&self) -> anyhow::Result<()> {
                self.inner().check_connectivity().await
            }

            fn endpoints(&self) -> Vec<Uri> {
                self.inner().endpoints()
            }

            async fn list_splits(
                &self,
                request: ListSplitsRequest,
            ) -> MetastoreResult<MetastoreServiceStream<ListSplitsResponse>> {
                self.inner().list_splits(request).await
            }

            $(
                async fn $method(&self, request: $req) -> MetastoreResult<$resp> {
                    delegate_intercept!(self, $method, request)
                }
            )*
        }
    };
}

super::service::for_each_unary_metastore_rpc!(impl_interceptor);
