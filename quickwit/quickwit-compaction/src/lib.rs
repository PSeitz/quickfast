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

mod compaction_pipeline;
mod compactor_service;
mod metrics;
pub mod planner;

pub type TaskId = String;

pub use compactor_service::{
    CompactorService, CompactorServiceHandle, notify_compactor_decommission,
    wait_for_compactor_decommission,
};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::CompactorConfig;
use quickwit_proto::compaction::CompactionPlannerServiceClient;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::{IndexUid, NodeId, SourceId};
use quickwit_storage::StorageResolver;
use tracing::info;

pub fn source_uid_metrics_label(index_uid: &IndexUid, source_id: &SourceId) -> String {
    let source_uid = format!("{}-{}", index_uid, source_id);
    quickwit_common::metrics::index_label(&source_uid).to_string()
}

#[allow(clippy::too_many_arguments)]
pub async fn start_compactor_service(
    node_id: NodeId,
    compaction_client: CompactionPlannerServiceClient,
    compactor_config: &CompactorConfig,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    event_broker: EventBroker,
    compaction_root_directory: TempDirectory,
) -> anyhow::Result<CompactorServiceHandle> {
    info!("starting compactor service");
    let compactor = CompactorService::new(
        node_id,
        compaction_client,
        compactor_config,
        metastore,
        storage_resolver,
        event_broker,
        compaction_root_directory,
    );
    Ok(compactor.spawn())
}
