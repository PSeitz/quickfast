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

use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};

use anyhow::Context;
use bytesize::ByteSize;
use quickwit_common::{get_from_env_opt, io};
use quickwit_config::{DocsClusteringConfig, IndexerConfig};
use quickwit_ingest::router_client::IngestRouterClient;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::NodeId;
use quickwit_storage::StorageResolver;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::info;

use crate::ingesthree::S3IndexerSupervisor;
use crate::source::{IngestRouterSink, SourceForwarderManager};

#[path = "ingesthree/service.rs"]
mod file_indexing;

/// Name of the indexing directory, usually located at `<data_dir_path>/indexing`.
pub const INDEXING_DIR_NAME: &str = "indexing";

const INDEXING_MAX_WRITE_THROUGHPUT_ENV_KEY: &str = "QW_INDEXING_MAX_WRITE_THROUGHPUT";

static INDEXING_IO_THROUGHPUT_LIMITER: LazyLock<Option<io::Limiter>> = LazyLock::new(|| {
    get_from_env_opt::<ByteSize>(INDEXING_MAX_WRITE_THROUGHPUT_ENV_KEY, false).map(io::limiter)
});

/// Observable counters for [`IndexingService`].
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct IndexingServiceCounters {
    pub num_running_s3_indexers: usize,
    pub source_forwarder_running: bool,
}

struct IndexingServiceState {
    s3_indexers: Vec<S3IndexerSupervisor>,
    source_forwarder_manager_opt: Option<SourceForwarderManager>,
}

struct IndexingServiceInner {
    node_id: NodeId,
    indexing_root_directory: PathBuf,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    max_concurrent_split_uploads: usize,
    indexing_io_throughput_limiter_opt: Option<io::Limiter>,
    clustering_config: Option<DocsClusteringConfig>,
    state: Mutex<IndexingServiceState>,
}

/// Runs `ingesthree` S3 indexer workers and thin pull-source forwarders (`Source → Ingest API`).
#[derive(Clone)]
pub struct IndexingService {
    inner: Arc<IndexingServiceInner>,
}

impl Debug for IndexingService {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexingService")
            .field("node_id", &self.inner.node_id)
            .finish()
    }
}

impl IndexingService {
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        node_id: NodeId,
        data_dir_path: PathBuf,
        indexer_config: IndexerConfig,
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        clustering_config: Option<DocsClusteringConfig>,
        ingest_router_opt: Option<IngestRouterClient>,
    ) -> anyhow::Result<Self> {
        let indexing_root_directory = data_dir_path.join(INDEXING_DIR_NAME);
        match tokio::fs::remove_dir_all(&indexing_root_directory).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to clean indexing directory `{}`",
                        indexing_root_directory.display()
                    )
                });
            }
        }
        tokio::fs::create_dir_all(&indexing_root_directory)
            .await
            .with_context(|| {
                format!(
                    "failed to create indexing directory `{}`",
                    indexing_root_directory.display()
                )
            })?;
        info!(node_id = %node_id, "initializing indexing service");
        let service = Self {
            inner: Arc::new(IndexingServiceInner {
                node_id,
                indexing_root_directory,
                metastore,
                storage_resolver,
                max_concurrent_split_uploads: indexer_config.max_concurrent_split_uploads,
                indexing_io_throughput_limiter_opt: INDEXING_IO_THROUGHPUT_LIMITER.clone(),
                clustering_config,
                state: Mutex::new(IndexingServiceState {
                    s3_indexers: Vec::new(),
                    source_forwarder_manager_opt: None,
                }),
            }),
        };
        let s3_indexers = service.spawn_s3_indexers()?;
        let source_forwarder_manager_opt = ingest_router_opt.map(|router| {
            let sink = Arc::new(IngestRouterSink::new(router));
            SourceForwarderManager::spawn(
                service.inner.metastore.clone(),
                service.inner.storage_resolver.clone(),
                sink,
            )
        });
        {
            let mut state = service.inner.state.lock().await;
            state.s3_indexers = s3_indexers;
            state.source_forwarder_manager_opt = source_forwarder_manager_opt;
        }
        Ok(service)
    }

    pub async fn counters(&self) -> IndexingServiceCounters {
        let state = self.inner.state.lock().await;
        IndexingServiceCounters {
            num_running_s3_indexers: state.s3_indexers.len(),
            source_forwarder_running: state.source_forwarder_manager_opt.is_some(),
        }
    }

    pub fn is_healthy(&self) -> bool {
        true
    }

    /// Stops pull-source forwarders and S3 indexers. Idempotent.
    pub async fn stop(&self) {
        let (forwarder_opt, s3_indexers) = {
            let mut state = self.inner.state.lock().await;
            (
                state.source_forwarder_manager_opt.take(),
                std::mem::take(&mut state.s3_indexers),
            )
        };
        if let Some(manager) = forwarder_opt {
            manager.stop().await;
        }
        futures::future::join_all(s3_indexers.into_iter().map(S3IndexerSupervisor::stop)).await;
    }
}
