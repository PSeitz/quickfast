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

use quickwit_config::NodeConfig;
use quickwit_ingest::router_client::IngestRouterClient;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_storage::StorageResolver;
use tracing::info;

pub use crate::controlled_directory::ControlledDirectory;
pub use crate::doc_processor::{DocProcessorCounters, DocumentConverter};
pub use crate::indexing_service::{INDEXING_DIR_NAME, IndexingService, IndexingServiceCounters};
pub use crate::local_merge::run_local_merges;
pub use crate::merge_executor::{
    MergeExecutionContext, execute_merge_operation, merge_split_attrs,
};
pub use crate::packager::create_packaged_split;
pub use crate::split_store::{IndexingSplitStore, get_tantivy_directory_from_split_bundle};
pub(crate) use crate::uploader::acquire_indexing_upload_permit;
pub use crate::uploader::prepare_split_for_upload;

mod controlled_directory;
mod doc_processor;
mod docs_clustering;
mod indexing_service;
pub mod ingesthree;
mod local_merge;
mod merge_executor;
pub mod merge_policy;
mod metrics;
pub mod models;
mod no_fsync_mmap_directory;
mod packager;
pub mod source;
#[cfg(test)]
mod split_recovery_tests;
mod split_store;
#[cfg(any(test, feature = "testsuite"))]
mod test_utils;
mod uploader;
#[cfg(feature = "vrl")]
mod vrl_processing;

/// Partition ID assigned to overflow splits when the number of partitions exceeds the split limit.
pub(crate) const OTHER_PARTITION_ID: u64 = 3264326757911759461u64;

#[cfg(any(test, feature = "testsuite"))]
pub use test_utils::{MockSplitBuilder, TestSandbox, mock_split, mock_split_meta};

pub use self::source::check_source_connectivity;

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(IndexingServiceCounters)))]
/// Schema used for OpenAPI generation.
pub struct IndexingApiSchemas;

pub async fn start_indexing_service(
    config: &NodeConfig,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    ingest_router: IngestRouterClient,
) -> anyhow::Result<IndexingService> {
    info!("starting indexer service");
    IndexingService::start(
        config.node_id.clone(),
        config.data_dir_path.to_path_buf(),
        config.indexer_config.clone(),
        metastore,
        storage_resolver,
        config.docs_clustering_config.clone(),
        Some(ingest_router),
    )
    .await
}
