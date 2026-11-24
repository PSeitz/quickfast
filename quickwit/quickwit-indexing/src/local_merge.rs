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

use quickwit_actors::{ActorExitStatus, Universe};
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

use crate::actors::{
    MergeExecutor, MergeSplitDownloader, Packager, Publisher, PublisherType, Uploader, UploaderType,
};
use crate::merge_policy::{MergePolicy, MergeSource};
use crate::split_store::IndexingSplitStore;

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
    let universe = Universe::new();
    let publisher = Publisher::new(PublisherType::MergePublisher, metastore.clone());
    let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);
    let uploader = Uploader::new(
        UploaderType::MergeUploader,
        metastore.clone(),
        merge_policy.clone(),
        retention_policy,
        split_store.clone(),
        publisher_mailbox,
        max_concurrent_split_uploads,
        EventBroker::default(),
    );
    let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
    let tag_fields = doc_mapper.tag_named_fields()?;
    let packager = Packager::new("MergePackager", tag_fields, uploader_mailbox);
    let (packager_mailbox, packager_handle) = universe.spawn_builder().spawn(packager);
    let merge_executor = MergeExecutor::new(
        pipeline_id.clone(),
        metastore.clone(),
        doc_mapper,
        IoControls::default(),
        packager_mailbox,
        None,
    );
    let (executor_mailbox, executor_handle) = universe.spawn_builder().spawn(merge_executor);
    let downloader = MergeSplitDownloader {
        scratch_directory: indexing_directory,
        split_store,
        executor_mailbox,
        io_controls: IoControls::default(),
    };
    let (downloader_mailbox, downloader_handle) = universe.spawn_builder().spawn(downloader);

    let mut finalized = false;
    let result: anyhow::Result<()> = async {
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
            let expected_published = operations.len() as u64
                + publisher_handle
                    .process_pending_and_observe()
                    .await
                    .state
                    .num_replace_operations;
            for operation in operations {
                downloader_mailbox
                    .send_message(MergeSource::Operation(operation))
                    .await?;
            }
            downloader_handle.process_pending_and_observe().await;
            executor_handle.process_pending_and_observe().await;
            packager_handle.process_pending_and_observe().await;
            uploader_handle.process_pending_and_observe().await;
            // Uploader spawns background upload tasks; wait until all replace ops are published.
            loop {
                let obs = publisher_handle.process_pending_and_observe().await;
                if obs.state.num_replace_operations >= expected_published {
                    break;
                }
                if uploader_handle.state().is_exit() || publisher_handle.state().is_exit() {
                    anyhow::bail!("merge actor exited before publishing all merged splits");
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            }
        }
        Ok(())
    }
    .await;

    let (pub_exit, _) = publisher_handle.quit().await;
    universe.quit().await;
    result?;
    if !matches!(pub_exit, ActorExitStatus::Success | ActorExitStatus::Quit) {
        anyhow::bail!("merge publisher exited abnormally: {pub_exit}");
    }
    Ok(())
}
