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
use async_trait::async_trait;
use bytesize::ByteSize;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Healthz};
use quickwit_common::{get_from_env_opt, io};
use quickwit_config::{DocsClusteringConfig, IndexerConfig};
use quickwit_ingest::router_client::IngestRouterClient;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::NodeId;
use quickwit_storage::StorageResolver;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::ingesthree::S3IndexerSupervisor;
use crate::source::{IngestRouterSink, SourceForwarderManager};
use crate::split_store::IndexingSplitCache;

#[path = "../ingesthree/service.rs"]
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

/// Runs `ingesthree` S3 indexer workers and thin pull-source forwarders (`Source → Ingest API`).
pub struct IndexingService {
    node_id: NodeId,
    indexing_root_directory: PathBuf,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    split_cache: Arc<IndexingSplitCache>,
    max_concurrent_split_uploads: usize,
    indexing_io_throughput_limiter_opt: Option<io::Limiter>,
    clustering_config: Option<DocsClusteringConfig>,
    ingest_router_opt: Option<IngestRouterClient>,
    s3_indexers: Vec<S3IndexerSupervisor>,
    source_forwarder_manager_opt: Option<SourceForwarderManager>,
}

impl Debug for IndexingService {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexingService")
            .field("node_id", &self.node_id)
            .field("num_s3_indexers", &self.s3_indexers.len())
            .finish()
    }
}

impl IndexingService {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        node_id: NodeId,
        data_dir_path: PathBuf,
        indexer_config: IndexerConfig,
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        split_cache: Arc<IndexingSplitCache>,
        clustering_config: Option<DocsClusteringConfig>,
        ingest_router_opt: Option<IngestRouterClient>,
    ) -> anyhow::Result<Self> {
        let indexing_root_directory = data_dir_path.join(INDEXING_DIR_NAME);
        if indexing_root_directory.exists() {
            tokio::fs::remove_dir_all(&indexing_root_directory)
                .await
                .with_context(|| {
                    format!(
                        "failed to clean indexing directory `{}`",
                        indexing_root_directory.display()
                    )
                })?;
        }
        tokio::fs::create_dir_all(&indexing_root_directory)
            .await
            .with_context(|| {
                format!(
                    "failed to create indexing directory `{}`",
                    indexing_root_directory.display()
                )
            })?;
        Ok(Self {
            node_id,
            indexing_root_directory,
            metastore,
            storage_resolver,
            split_cache,
            max_concurrent_split_uploads: indexer_config.max_concurrent_split_uploads,
            indexing_io_throughput_limiter_opt: INDEXING_IO_THROUGHPUT_LIMITER.clone(),
            clustering_config,
            ingest_router_opt,
            s3_indexers: Vec::new(),
            source_forwarder_manager_opt: None,
        })
    }

    fn start_source_forwarders(&mut self) {
        if let Some(router) = self.ingest_router_opt.clone() {
            let sink = Arc::new(IngestRouterSink::new(router));
            self.source_forwarder_manager_opt = Some(SourceForwarderManager::spawn(
                self.metastore.clone(),
                self.storage_resolver.clone(),
                sink,
            ));
        }
    }

    async fn stop_source_forwarders(&mut self) {
        if let Some(manager) = self.source_forwarder_manager_opt.take() {
            manager.stop().await;
        }
    }
}

#[async_trait]
impl Actor for IndexingService {
    type ObservableState = IndexingServiceCounters;

    fn observable_state(&self) -> Self::ObservableState {
        IndexingServiceCounters {
            num_running_s3_indexers: self.s3_indexers.len(),
            source_forwarder_running: self.source_forwarder_manager_opt.is_some(),
        }
    }

    async fn initialize(&mut self, _ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        info!(node_id = %self.node_id, "initializing indexing service");
        self.start_s3_indexers()?;
        self.start_source_forwarders();
        Ok(())
    }

    async fn finalize(
        &mut self,
        _exit_status: &ActorExitStatus,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        ctx.protect_future(async {
            self.stop_source_forwarders().await;
            self.stop_s3_indexers().await;
        })
        .await;
        Ok(())
    }
}

#[async_trait]
impl Handler<Healthz> for IndexingService {
    type Reply = bool;

    async fn handle(
        &mut self,
        _: Healthz,
        _ctx: &ActorContext<Self>,
    ) -> Result<bool, ActorExitStatus> {
        Ok(true)
    }
}
