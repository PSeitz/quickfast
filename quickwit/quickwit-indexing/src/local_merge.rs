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

use std::sync::Arc;

use quickwit_common::KillSwitch;
use quickwit_common::io::IoControls;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::RetentionPolicy;
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::{
    ListSplitsQuery, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitMetadata,
    SplitState,
};
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService, MetastoreServiceClient};
use time::OffsetDateTime;

use crate::merge_policy::MergePolicy;
use crate::split_store::IndexingSplitStore;
use crate::{MergeExecutionContext, execute_merge_operation};

/// Runs all pending merge operations for `(index_uid, source_id)` locally until the merge policy
/// emits no further operations (including finalize operations).
#[allow(clippy::too_many_arguments)]
pub async fn run_local_merges(
    pipeline_id: MergePipelineId,
    doc_mapper: Arc<DocMapper>,
    indexing_directory: TempDirectory,
    metastore: MetastoreServiceClient,
    split_store: IndexingSplitStore,
    merge_policy: Arc<dyn MergePolicy>,
    retention_policy: Option<RetentionPolicy>,
    max_concurrent_split_uploads: usize,
) -> anyhow::Result<()> {
    let merge_ctx = MergeExecutionContext {
        pipeline_id: pipeline_id.clone(),
        metastore: metastore.clone(),
        doc_mapper,
        split_store,
        merge_policy: merge_policy.clone(),
        retention_policy,
        scratch_directory: indexing_directory,
        download_io_controls: IoControls::default(),
        merge_io_controls: IoControls::default(),
        max_concurrent_split_uploads,
        event_broker: EventBroker::default(),
        merge_execution_semaphore: None,
        kill_switch: KillSwitch::default(),
    };

    let mut finalized = false;
    loop {
        let query = ListSplitsQuery::for_index(pipeline_id.index_uid.clone())
            .with_split_state(SplitState::Published);
        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query)?)
            .await?
            .collect_splits()
            .await?;
        let now = OffsetDateTime::now_utc();
        let mut immature: Vec<SplitMetadata> = splits
            .into_iter()
            .map(|split| split.split_metadata)
            .filter(|meta| meta.source_id == pipeline_id.source_id && !meta.is_mature(now))
            .collect();
        let mut operations = merge_policy.operations(&mut immature);
        if operations.is_empty() && !finalized {
            finalized = true;
            operations = merge_policy.finalize_operations(&mut immature);
        }
        if operations.is_empty() {
            break;
        }
        for operation in operations {
            execute_merge_operation(&merge_ctx, operation).await?;
        }
    }
    Ok(())
}
