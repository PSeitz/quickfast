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

#![deny(clippy::disallowed_methods)]

use std::sync::Arc;

use quickwit_actors::{Mailbox, Universe};
use quickwit_config::NodeConfig;
use quickwit_ingest::router_client::IngestRouterClient;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::StorageResolver;
use tracing::info;

pub use crate::actors::{IndexingService, IndexingServiceCounters, PublisherType};
pub use crate::controlled_directory::ControlledDirectory;
pub use crate::local_merge::run_local_merges;
pub use crate::split_store::{
    IndexingSplitCache, IndexingSplitStore, SplitStoreQuota,
    get_tantivy_directory_from_split_bundle,
};

pub mod actors;
mod controlled_directory;
mod docs_clustering;
pub mod ingesthree;
mod local_merge;
pub mod merge_policy;
mod metrics;
pub mod models;
mod no_fsync_mmap_directory;
pub mod source;
#[cfg(test)]
mod split_recovery_tests;
mod split_store;
#[cfg(any(test, feature = "testsuite"))]
mod test_utils;

#[cfg(any(test, feature = "testsuite"))]
pub use test_utils::{MockSplitBuilder, TestSandbox, mock_split, mock_split_meta};

pub use self::source::check_source_connectivity;

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(IndexingServiceCounters)))]
/// Schema used for OpenAPI generation.
pub struct IndexingApiSchemas;

pub async fn start_indexing_service(
    universe: &Universe,
    config: &NodeConfig,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    indexing_split_cache: Arc<IndexingSplitCache>,
    ingest_router: IngestRouterClient,
) -> anyhow::Result<Mailbox<IndexingService>> {
    info!("starting indexer service");
    let indexing_service = IndexingService::new(
        config.node_id.clone(),
        config.data_dir_path.to_path_buf(),
        config.indexer_config.clone(),
        metastore,
        storage_resolver,
        indexing_split_cache,
        config.docs_clustering_config.clone(),
        Some(ingest_router),
    )
    .await?;
    let (indexing_service, _) = universe.spawn_builder().spawn(indexing_service);
    Ok(indexing_service)
}
