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
use std::iter::FromIterator;
use std::mem;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use anyhow::Context;
use async_trait::async_trait;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::spawn_named_task;
use quickwit_config::RetentionPolicy;
use quickwit_metastore::{SplitMaturity, SplitMetadata, StageSplitsRequestExt};
use quickwit_metrics::{gauge, label_values};
use quickwit_proto::metastore::{
    MetastoreService, MetastoreServiceClient, SplitRecoveryMetadata, StageSplitsRequest,
};
use quickwit_proto::search::{ReportSplit, ReportSplitsRequest};
use quickwit_proto::types::IndexUid;
use quickwit_storage::{SplitPayload, SplitPayloadBuilder};
use serde::Serialize;
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::{Instrument, Span, debug, error, instrument, warn};

use crate::actors::Publisher;
use crate::merge_policy::{MergePolicy, MergeTask};
use crate::metrics::{AVAILABLE_CONCURRENT_UPLOAD_PERMITS, COMPONENT};
use crate::models::{PackagedSplit, PackagedSplitBatch, SplitsUpdate, create_split_metadata};
use crate::split_store::IndexingSplitStore;

/// The following two semaphores ensures that, we have at most `max_concurrent_split_uploads` split
/// uploads can happen at the same time, as configured in the `IndexerConfig`.
///
/// This "budget" is actually split into two semaphores: one for the indexing pipeline and the merge
/// pipeline. The idea is that the merge pipeline is by nature a bit irregular, and we don't want it
/// to stall the indexing pipeline, decreasing its throughput.
static CONCURRENT_UPLOAD_PERMITS_INDEX: OnceLock<Semaphore> = OnceLock::new();
static CONCURRENT_UPLOAD_PERMITS_MERGE: OnceLock<Semaphore> = OnceLock::new();

/// Acquires a permit from the node-wide indexing upload budget.
/// The first caller initializes the budget, which is shared with indexing actors.
pub(crate) async fn acquire_indexing_upload_permit(
    max_concurrent_uploads: usize,
) -> anyhow::Result<SemaphorePermit<'static>> {
    acquire_upload_permit(UploaderType::IndexUploader, max_concurrent_uploads).await
}

async fn acquire_upload_permit(
    uploader_type: UploaderType,
    max_concurrent_uploads: usize,
) -> anyhow::Result<SemaphorePermit<'static>> {
    let (concurrent_upload_permits_once_cell, concurrent_upload_permits_gauge) = match uploader_type
    {
        UploaderType::IndexUploader => (
            &CONCURRENT_UPLOAD_PERMITS_INDEX,
            gauge!(parent: AVAILABLE_CONCURRENT_UPLOAD_PERMITS, labels: [label_values!(COMPONENT => "indexer")]),
        ),
        UploaderType::MergeUploader | UploaderType::DeleteUploader => (
            &CONCURRENT_UPLOAD_PERMITS_MERGE,
            gauge!(parent: AVAILABLE_CONCURRENT_UPLOAD_PERMITS, labels: [label_values!(COMPONENT => "merger")]),
        ),
    };
    let concurrent_upload_permits = concurrent_upload_permits_once_cell
        .get_or_init(|| Semaphore::const_new(max_concurrent_uploads));
    concurrent_upload_permits_gauge.set(concurrent_upload_permits.available_permits() as f64);
    concurrent_upload_permits
        .acquire()
        .await
        .context("the uploader semaphore is closed. (this should never happen)")
}

#[derive(Clone, Copy, Debug)]
pub enum UploaderType {
    IndexUploader,
    MergeUploader,
    DeleteUploader,
}

#[derive(Clone)]
pub struct Uploader {
    uploader_type: UploaderType,
    metastore: MetastoreServiceClient,
    merge_policy: Arc<dyn MergePolicy>,
    retention_policy: Option<RetentionPolicy>,
    split_store: IndexingSplitStore,
    publisher_mailbox: Mailbox<Publisher>,
    max_concurrent_split_uploads: usize,
    counters: UploaderCounters,
    event_broker: EventBroker,
}

impl Uploader {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        uploader_type: UploaderType,
        metastore: MetastoreServiceClient,
        merge_policy: Arc<dyn MergePolicy>,
        retention_policy: Option<RetentionPolicy>,
        split_store: IndexingSplitStore,
        publisher_mailbox: Mailbox<Publisher>,
        max_concurrent_split_uploads: usize,
        event_broker: EventBroker,
    ) -> Uploader {
        Uploader {
            uploader_type,
            metastore,
            merge_policy,
            retention_policy,
            split_store,
            publisher_mailbox,
            max_concurrent_split_uploads,
            counters: Default::default(),
            event_broker,
        }
    }
    async fn acquire_semaphore(
        &self,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<SemaphorePermit<'static>> {
        let _guard = ctx.protect_zone();
        acquire_upload_permit(self.uploader_type, self.max_concurrent_split_uploads).await
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct UploaderCounters {
    pub num_staged_splits: Arc<AtomicU64>,
    pub num_uploaded_splits: Arc<AtomicU64>,
}

#[async_trait]
impl Actor for Uploader {
    type ObservableState = UploaderCounters;

    #[allow(clippy::unused_unit)]
    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        // We do not need a large capacity here...
        // The uploader just spawns tasks that are uploading,
        // so that in a sense, the CONCURRENT_UPLOAD_PERMITS semaphore also acts as
        // a queue capacity.
        //
        // Having a large queue is costly too, because each message is a handle over
        // a split directory. We DO need aggressive backpressure here.
        QueueCapacity::Bounded(0)
    }

    fn name(&self) -> String {
        format!("{:?}", self.uploader_type)
    }

    fn task_metrics_label(&self) -> &'static str {
        match self.uploader_type {
            UploaderType::IndexUploader => "index_uploader",
            UploaderType::MergeUploader => "merge_uploader",
            UploaderType::DeleteUploader => "delete_uploader",
        }
    }
}

#[async_trait]
impl Handler<PackagedSplitBatch> for Uploader {
    type Reply = ();

    #[instrument(name = "uploader",
        parent=batch.batch_parent_span.id(),
        skip_all)]
    async fn handle(
        &mut self,
        batch: PackagedSplitBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let publisher_mailbox = self.publisher_mailbox.clone();

        // The permit will be added back manually to the semaphore the task after it is finished.
        // This is not a valid usage of protected zone here.
        //
        // Protected zone are supposed to be used when the cause for blocking is
        // outside of the responsibility of the current actor.
        // For instance, when sending a message on a downstream actor with a saturated
        // mailbox.
        // This is meant to be fixed with ParallelActors.
        let permit_guard = self.acquire_semaphore(ctx).await?;
        let kill_switch = ctx.kill_switch().clone();
        let split_ids = batch.split_ids();
        if kill_switch.is_dead() {
            warn!(split_ids=?split_ids,"kill switch was activated, cancelling upload");
            return Err(ActorExitStatus::Killed);
        }
        let metastore = self.metastore.clone();
        let split_store = self.split_store.clone();
        let counters = self.counters.clone();
        let index_uid = batch.index_uid();
        let ctx_clone = ctx.clone();
        let merge_policy = self.merge_policy.clone();
        let retention_policy = self.retention_policy.clone();
        debug!(split_ids=?split_ids, "start-stage-and-store-splits");
        let event_broker = self.event_broker.clone();
        spawn_named_task(
            async move {
                let mut split_metadata_list = Vec::with_capacity(batch.splits.len());
                let mut split_payloads = Vec::with_capacity(batch.splits.len());
                let mut report_splits: Vec<ReportSplit> = Vec::with_capacity(batch.splits.len());

                for packaged_split in batch.splits.iter() {
                    let (split_metadata, split_payload) = match prepare_split_for_upload(
                        packaged_split,
                        &merge_policy,
                        retention_policy.as_ref(),
                    ) {
                        Ok(prepared_split) => prepared_split,
                        Err(error) => {
                            error!(
                                ?error,
                                split_id = packaged_split.split_id(),
                                "failed to prepare split for upload"
                            );
                            return;
                        }
                    };

                    report_splits.push(ReportSplit {
                        storage_uri: split_store.remote_uri().to_string(),
                        split_id: packaged_split.split_id().to_string(),
                    });

                    split_metadata_list.push(split_metadata);
                    split_payloads.push(split_payload);
                }

                let stage_splits_request = match StageSplitsRequest::try_from_splits_metadata(
                    index_uid.clone(),
                    split_metadata_list.clone(),
                ) {
                    Ok(stage_splits_request) => stage_splits_request,
                    Err(error) => {
                        error!(?error, "failed to create stage splits request");
                        return;
                    }
                };
                if let Err(error) = metastore.stage_splits(stage_splits_request).await {
                    error!(?error, "failed to stage splits");
                    return;
                };

                counters
                    .num_staged_splits
                    .fetch_add(split_metadata_list.len() as u64, Ordering::Relaxed);

                let mut packaged_splits_and_metadata = Vec::with_capacity(batch.splits.len());

                event_broker.publish(ReportSplitsRequest { report_splits });

                for ((packaged_split, metadata), split_payload) in batch
                    .splits
                    .into_iter()
                    .zip(split_metadata_list)
                    .zip(split_payloads)
                {
                    let upload_result = upload_split(
                        &packaged_split,
                        &metadata,
                        split_payload,
                        &split_store,
                        counters.clone(),
                    )
                    .await;

                    if let Err(error) = upload_result {
                        kill_switch.kill_with_fault(error.context(format!(
                            "failed to upload split `{}`",
                            packaged_split.split_id()
                        )));
                        return;
                    }
                    packaged_splits_and_metadata.push((packaged_split, metadata));
                }

                let splits_update = make_publish_operation(
                    index_uid,
                    packaged_splits_and_metadata,
                    batch.merge_task_opt,
                    batch.batch_parent_span,
                );

                if let Err(error) = ctx_clone
                    .send_message(&publisher_mailbox, splits_update)
                    .await
                {
                    error!(?error, "failed to send splits update to publisher");
                    return;
                }
                // We explicitly drop it in order to force move the permit guard into the async
                // task.
                mem::drop(permit_guard);
            }
            .instrument(Span::current()),
            "upload_single_task",
        );
        Ok(())
    }
}

fn create_split_recovery_metadata(
    split_metadata: &SplitMetadata,
    parent_split_ids: &[quickwit_proto::types::SplitId],
) -> SplitRecoveryMetadata {
    let time_range = split_metadata.time_range.as_ref();
    let time_range_start_inclusive = time_range.map(|range| *range.start());
    let time_range_end_inclusive = time_range.map(|range| *range.end());

    let parent_split_ids = parent_split_ids
        .iter()
        .map(|split_id| split_id.to_string())
        .collect();

    let maturation_period_millis = match split_metadata.maturity {
        SplitMaturity::Mature => None,
        SplitMaturity::Immature { maturation_period } => Some(
            maturation_period
                .as_millis()
                .try_into()
                .expect("maturation period should fit in u64 milliseconds"),
        ),
    };
    SplitRecoveryMetadata {
        split_id: split_metadata.split_id.to_string(),
        index_uid: Some(split_metadata.index_uid.clone()),
        source_id: split_metadata.source_id.clone(),
        node_id: split_metadata.node_id.clone(),
        doc_mapping_uid: Some(split_metadata.doc_mapping_uid),
        partition_id: split_metadata.partition_id,
        num_docs: split_metadata.num_docs as u64,
        uncompressed_docs_size_bytes: split_metadata.uncompressed_docs_size_in_bytes,
        time_range_start_inclusive,
        time_range_end_inclusive,
        create_timestamp: split_metadata.create_timestamp,
        tags: split_metadata.tags.iter().cloned().collect(),
        delete_opstamp: split_metadata.delete_opstamp,
        num_merge_ops: split_metadata.num_merge_ops as u64,
        parent_split_ids,
        maturation_period_millis,
    }
}

/// Builds metadata and an upload payload referencing the packaged split's files.
/// Keep its scratch directory alive and unchanged until the upload finishes.
pub fn prepare_split_for_upload(
    packaged_split: &PackagedSplit,
    merge_policy: &Arc<dyn MergePolicy>,
    retention_policy: Option<&RetentionPolicy>,
) -> anyhow::Result<(SplitMetadata, SplitPayload)> {
    // Footer offsets are unknown at this point, so we use default values.
    let footer_offsets = Default::default();

    let split_metadata = create_split_metadata(
        merge_policy,
        retention_policy,
        &packaged_split.split_attrs,
        packaged_split.tags.clone(),
        footer_offsets,
    );
    let recovery_metadata = create_split_recovery_metadata(
        &split_metadata,
        &packaged_split.split_attrs.replaced_split_ids,
    );
    let serialized_recovery_metadata = recovery_metadata.serialize();
    let split_payload = SplitPayloadBuilder::get_split_payload(
        &packaged_split.split_files,
        &packaged_split.serialized_split_fields,
        Some(&serialized_recovery_metadata),
        &packaged_split.hotcache_bytes,
    )?;
    let split_metadata = SplitMetadata {
        footer_offsets: split_payload.footer_range.clone(),
        ..split_metadata
    };
    Ok((split_metadata, split_payload))
}

fn make_publish_operation(
    index_uid: IndexUid,
    packaged_splits_and_metadatas: Vec<(PackagedSplit, SplitMetadata)>,
    merge_task: Option<MergeTask>,
    parent_span: Span,
) -> SplitsUpdate {
    assert!(!packaged_splits_and_metadatas.is_empty());
    let replaced_split_ids = packaged_splits_and_metadatas
        .iter()
        .flat_map(|(split, _)| split.split_attrs.replaced_split_ids.clone())
        .collect::<HashSet<_>>();
    SplitsUpdate {
        index_uid,
        new_splits: packaged_splits_and_metadatas
            .into_iter()
            .map(|split_and_meta| split_and_meta.1)
            .collect_vec(),
        replaced_split_ids: Vec::from_iter(replaced_split_ids),
        merge_task,
        parent_span,
    }
}

#[instrument(
    level = "info"
    name = "upload",
    fields(split = %packaged_split.split_attrs.split_id),
    skip_all
)]
async fn upload_split(
    packaged_split: &PackagedSplit,
    split_metadata: &SplitMetadata,
    split_payload: SplitPayload,
    split_store: &IndexingSplitStore,
    counters: UploaderCounters,
) -> anyhow::Result<()> {
    split_store
        .store_split(
            split_metadata,
            packaged_split.split_scratch_directory.path(),
            Box::new(split_payload),
        )
        .await?;
    counters.num_uploaded_splits.fetch_add(1, Ordering::SeqCst);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use quickwit_actors::{ObservationType, Universe};
    use quickwit_common::pubsub::EventSubscriber;
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
    use quickwit_proto::types::{DocMappingUid, NodeId, SplitId};
    use quickwit_storage::RamStorage;
    use tantivy::DateTime;

    use super::*;
    use crate::merge_policy::NopMergePolicy;
    use crate::models::{SplitAttrs, SplitsUpdate};

    #[test]
    fn test_split_recovery_metadata_preserves_maturity() {
        let split_metadata = SplitMetadata {
            maturity: SplitMaturity::Immature {
                maturation_period: Duration::from_millis(1_500),
            },
            ..Default::default()
        };

        let recovery_metadata = create_split_recovery_metadata(&split_metadata, &[]);
        assert_eq!(recovery_metadata.maturation_period_millis, Some(1_500));

        let (recovered_metadata, _parent_split_ids) =
            SplitMetadata::try_from_recovery_metadata(recovery_metadata, 1..2).unwrap();
        assert_eq!(recovered_metadata.maturity, split_metadata.maturity);

        let mature_split_metadata = SplitMetadata::default();
        let mature_recovery_metadata = create_split_recovery_metadata(&mature_split_metadata, &[]);
        assert_eq!(mature_recovery_metadata.maturation_period_millis, None);
        let (recovered_mature_metadata, _parent_split_ids) =
            SplitMetadata::try_from_recovery_metadata(mature_recovery_metadata, 1..2).unwrap();
        assert_eq!(recovered_mature_metadata.maturity, SplitMaturity::Mature);
    }

    #[tokio::test]
    async fn test_uploader_single_split() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();

        let node_id = NodeId::from_str("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();

        let event_broker = EventBroker::default();
        let universe = Universe::new();
        let (publisher_mailbox, publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_splits()
            .withf(move |stage_splits_request| -> bool {
                let splits_metadata = stage_splits_request.deserialize_splits_metadata().unwrap();
                let split_metadata = &splits_metadata[0];
                let index_uid: IndexUid = stage_splits_request.index_uid().clone();
                index_uid.index_id == "test-index"
                    && split_metadata.split_id() == "test-split"
                    && split_metadata.time_range == Some(1628203589..=1628203640)
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let ram_storage = RamStorage::default();
        let split_store = IndexingSplitStore::new(Arc::new(ram_storage.clone()));
        let merge_policy = Arc::new(NopMergePolicy);
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            merge_policy,
            None,
            split_store,
            publisher_mailbox,
            4,
            event_broker,
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
        let split_scratch_directory = TempDirectory::for_test();
        uploader_mailbox
            .send_message(PackagedSplitBatch::new(
                vec![PackagedSplit {
                    split_attrs: SplitAttrs {
                        node_id,
                        index_uid,
                        source_id,
                        doc_mapping_uid: DocMappingUid::default(),
                        partition_id: 3u64,
                        time_range: Some(
                            DateTime::from_timestamp_secs(1_628_203_589)
                                ..=DateTime::from_timestamp_secs(1_628_203_640),
                        ),
                        uncompressed_docs_size_in_bytes: 1_000,
                        num_docs: 10,
                        replaced_split_ids: Vec::new(),
                        split_id: "test-split".into(),
                        delete_opstamp: 10,
                        num_merge_ops: 0,
                    },
                    serialized_split_fields: Vec::new(),
                    split_scratch_directory,
                    tags: Default::default(),
                    hotcache_bytes: Vec::new(),
                    split_files: Vec::new(),
                }],
                None,
                Span::none(),
            ))
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let SplitsUpdate {
            index_uid,
            new_splits,
            replaced_split_ids,
            ..
        } = publisher_inbox.recv_typed_message().await.unwrap();

        assert_eq!(index_uid.index_id, "test-index");
        assert_eq!(new_splits.len(), 1);
        assert_eq!(new_splits[0].split_id(), "test-split");
        assert!(replaced_split_ids.is_empty());
        let mut files = ram_storage.list_files().await;
        files.sort();
        assert_eq!(&files, &[PathBuf::from("test-split.split")]);
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_uploader_emits_replace() -> anyhow::Result<()> {
        let node_id = NodeId::from_str("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();

        let universe = Universe::new();
        let (publisher_mailbox, publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_splits()
            .withf(move |stage_splits_request| -> bool {
                let splits_metadata = stage_splits_request.deserialize_splits_metadata().unwrap();
                let is_metadata_valid = splits_metadata.iter().all(|metadata| {
                    ["test-split-1", "test-split-2"].contains(&metadata.split_id().as_str())
                        && metadata.time_range == Some(1628203589..=1628203640)
                });
                let index_uid: IndexUid = stage_splits_request.index_uid().clone();
                index_uid.index_id == "test-index" && is_metadata_valid
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let ram_storage = RamStorage::default();
        let split_store = IndexingSplitStore::new(Arc::new(ram_storage.clone()));
        let merge_policy = Arc::new(NopMergePolicy);
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            merge_policy,
            None,
            split_store,
            publisher_mailbox,
            4,
            EventBroker::default(),
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
        let split_scratch_directory_1 = TempDirectory::for_test();
        let split_scratch_directory_2 = TempDirectory::for_test();
        let packaged_split_1 = PackagedSplit {
            split_attrs: SplitAttrs {
                node_id: node_id.clone(),
                index_uid: index_uid.clone(),
                source_id: source_id.clone(),
                doc_mapping_uid: DocMappingUid::default(),
                split_id: "test-split-1".into(),
                partition_id: 3u64,
                num_docs: 10,
                uncompressed_docs_size_in_bytes: 1_000,
                time_range: Some(
                    DateTime::from_timestamp_secs(1_628_203_589)
                        ..=DateTime::from_timestamp_secs(1_628_203_640),
                ),
                replaced_split_ids: vec![
                    SplitId::from("replaced-split-1"),
                    SplitId::from("replaced-split-2"),
                ],
                delete_opstamp: 0,
                num_merge_ops: 0,
            },
            serialized_split_fields: Vec::new(),
            split_scratch_directory: split_scratch_directory_1,
            tags: Default::default(),
            split_files: Vec::new(),
            hotcache_bytes: Vec::new(),
        };
        let package_split_2 = PackagedSplit {
            split_attrs: SplitAttrs {
                node_id,
                index_uid,
                source_id,
                doc_mapping_uid: DocMappingUid::default(),
                split_id: "test-split-2".into(),
                partition_id: 3u64,
                num_docs: 10,
                uncompressed_docs_size_in_bytes: 1_000,
                time_range: Some(
                    DateTime::from_timestamp_secs(1_628_203_589)
                        ..=DateTime::from_timestamp_secs(1_628_203_640),
                ),
                replaced_split_ids: vec![
                    SplitId::from("replaced-split-1"),
                    SplitId::from("replaced-split-2"),
                ],
                delete_opstamp: 0,
                num_merge_ops: 0,
            },
            serialized_split_fields: Vec::new(),
            split_scratch_directory: split_scratch_directory_2,
            tags: Default::default(),
            split_files: Vec::new(),
            hotcache_bytes: Vec::new(),
        };
        uploader_mailbox
            .send_message(PackagedSplitBatch::new(
                vec![packaged_split_1, package_split_2],
                None,
                Span::none(),
            ))
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let SplitsUpdate {
            index_uid,
            new_splits,
            mut replaced_split_ids,
            ..
        } = publisher_inbox.recv_typed_message().await.unwrap();
        assert_eq!(index_uid.index_id, "test-index");
        // Sort first to avoid test failing.
        replaced_split_ids.sort();
        assert_eq!(new_splits.len(), 2);
        assert_eq!(new_splits[0].split_id(), "test-split-1");
        assert_eq!(new_splits[1].split_id(), "test-split-2");
        assert_eq!(
            &replaced_split_ids,
            &[
                SplitId::from("replaced-split-1"),
                SplitId::from("replaced-split-2"),
            ]
        );

        let mut files = ram_storage.list_files().await;
        files.sort();
        assert_eq!(
            &files,
            &[
                PathBuf::from("test-split-1.split"),
                PathBuf::from("test-split-2.split")
            ]
        );
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_uploader_without_sequencer() -> anyhow::Result<()> {
        let node_id = NodeId::from_str("test-node");
        let index_uid = IndexUid::for_test("test-index", 0);
        let index_uid_clone = index_uid.clone();
        let source_id = "test-source".to_string();

        let universe = Universe::new();
        let (publisher_mailbox, publisher_inbox) = universe.create_test_mailbox::<Publisher>();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_splits()
            .withf(move |stage_splits_request| -> bool {
                stage_splits_request.index_uid() == &index_uid_clone
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let ram_storage = RamStorage::default();
        let split_store = IndexingSplitStore::new(Arc::new(ram_storage.clone()));
        let merge_policy = Arc::new(NopMergePolicy);
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            merge_policy,
            None,
            split_store,
            publisher_mailbox,
            4,
            EventBroker::default(),
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
        let split_scratch_directory = TempDirectory::for_test();
        uploader_mailbox
            .send_message(PackagedSplitBatch::new(
                vec![PackagedSplit {
                    split_attrs: SplitAttrs {
                        node_id,
                        index_uid,
                        source_id,
                        doc_mapping_uid: DocMappingUid::default(),
                        split_id: "test-split".into(),
                        partition_id: 3u64,
                        time_range: None,
                        uncompressed_docs_size_in_bytes: 1_000,
                        num_docs: 10,
                        replaced_split_ids: Vec::new(),
                        delete_opstamp: 10,
                        num_merge_ops: 0,
                    },
                    serialized_split_fields: Vec::new(),
                    split_scratch_directory,
                    tags: Default::default(),
                    hotcache_bytes: Vec::new(),
                    split_files: Vec::new(),
                }],
                None,
                Span::none(),
            ))
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let SplitsUpdate {
            index_uid,
            new_splits,
            replaced_split_ids,
            ..
        } = publisher_inbox.recv_typed_message().await.unwrap();

        assert_eq!(index_uid.index_id, "test-index");
        assert_eq!(new_splits.len(), 1);
        assert!(replaced_split_ids.is_empty());
        universe.assert_quit().await;
        Ok(())
    }

    struct ReportSplitListener {
        report_splits_tx: flume::Sender<ReportSplitsRequest>,
    }

    impl std::fmt::Debug for ReportSplitListener {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.debug_struct("ReportSplitListener").finish()
        }
    }

    #[async_trait]
    impl EventSubscriber<ReportSplitsRequest> for ReportSplitListener {
        async fn handle_event(&mut self, event: ReportSplitsRequest) {
            self.report_splits_tx.send(event).unwrap();
        }
    }

    #[tokio::test]
    async fn test_uploader_notifies_event_broker() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        const SPLIT_ULID_STR: &str = "01HAV29D4XY3D462FS3D8K5Q2H";
        let event_broker = EventBroker::default();
        let (report_splits_tx, report_splits_rx) = flume::unbounded();
        let report_splits_listener = ReportSplitListener { report_splits_tx };

        // we need to keep the handle alive.
        let _subscribe_handle = event_broker.subscribe(report_splits_listener);

        let node_id = NodeId::from_str("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();

        let universe = Universe::new();
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_stage_splits()
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let ram_storage = RamStorage::default();
        let split_store = IndexingSplitStore::new(Arc::new(ram_storage.clone()));
        let merge_policy = Arc::new(NopMergePolicy);
        let (publisher_mailbox, _publisher_inbox) = universe.create_test_mailbox();
        let uploader = Uploader::new(
            UploaderType::IndexUploader,
            MetastoreServiceClient::from_mock(mock_metastore),
            merge_policy,
            None,
            split_store,
            publisher_mailbox,
            4,
            event_broker,
        );
        let (uploader_mailbox, uploader_handle) = universe.spawn_builder().spawn(uploader);
        let split_scratch_directory = TempDirectory::for_test();
        uploader_mailbox
            .send_message(PackagedSplitBatch::new(
                vec![PackagedSplit {
                    split_attrs: SplitAttrs {
                        node_id,
                        index_uid,
                        source_id,
                        doc_mapping_uid: DocMappingUid::default(),
                        partition_id: 3u64,
                        time_range: Some(
                            DateTime::from_timestamp_secs(1_628_203_589)
                                ..=DateTime::from_timestamp_secs(1_628_203_640),
                        ),
                        uncompressed_docs_size_in_bytes: 1_000,
                        num_docs: 10,
                        replaced_split_ids: Vec::new(),
                        split_id: SPLIT_ULID_STR.into(),
                        delete_opstamp: 10,
                        num_merge_ops: 0,
                    },
                    serialized_split_fields: Vec::new(),
                    split_scratch_directory,
                    tags: Default::default(),
                    hotcache_bytes: Vec::new(),
                    split_files: Vec::new(),
                }],
                None,
                Span::none(),
            ))
            .await?;
        assert_eq!(
            uploader_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        mem::drop(uploader_mailbox);
        let report_splits: ReportSplitsRequest = report_splits_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert_eq!(report_splits.report_splits.len(), 1);
        let split = &report_splits.report_splits[0];
        assert_eq!(split.storage_uri, "ram:///");
        assert_eq!(split.split_id, SPLIT_ULID_STR);
        universe.assert_quit().await;
        Ok(())
    }
}
