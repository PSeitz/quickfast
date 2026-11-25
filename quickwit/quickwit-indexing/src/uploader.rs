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

use std::sync::{Arc, OnceLock};

use anyhow::{Context, bail};
use quickwit_common::KillSwitch;
use quickwit_common::pubsub::EventBroker;
use quickwit_config::RetentionPolicy;
use quickwit_metastore::{SplitMaturity, SplitMetadata, StageSplitsRequestExt};
use quickwit_metrics::{gauge, label_values};
use quickwit_proto::metastore::{
    MetastoreService, MetastoreServiceClient, PublishSplitsRequest, SplitRecoveryMetadata,
    StageSplitsRequest,
};
use quickwit_proto::search::{ReportSplit, ReportSplitsRequest};
use quickwit_storage::{SplitPayload, SplitPayloadBuilder};
use tokio::sync::{Semaphore, SemaphorePermit};
use tracing::{debug, info, instrument, warn};

use crate::merge_policy::MergePolicy;
use crate::metrics::{AVAILABLE_CONCURRENT_UPLOAD_PERMITS, COMPONENT, record_published_split};
use crate::models::{PackagedSplit, create_split_metadata};
use crate::split_store::IndexingSplitStore;

/// Two process-wide upload budgets: one for indexing and one for merges/deletes,
/// so irregular merge uploads cannot starve indexing uploads.
static CONCURRENT_UPLOAD_PERMITS_INDEX: OnceLock<Semaphore> = OnceLock::new();
static CONCURRENT_UPLOAD_PERMITS_MERGE: OnceLock<Semaphore> = OnceLock::new();

/// Acquires a permit from the node-wide indexing upload budget.
pub(crate) async fn acquire_indexing_upload_permit(
    max_concurrent_uploads: usize,
) -> anyhow::Result<SemaphorePermit<'static>> {
    acquire_permit(
        &CONCURRENT_UPLOAD_PERMITS_INDEX,
        "indexer",
        max_concurrent_uploads,
    )
    .await
}

/// Acquires a permit from the node-wide merge/delete upload budget.
pub(crate) async fn acquire_merge_upload_permit(
    max_concurrent_uploads: usize,
) -> anyhow::Result<SemaphorePermit<'static>> {
    acquire_permit(
        &CONCURRENT_UPLOAD_PERMITS_MERGE,
        "merger",
        max_concurrent_uploads,
    )
    .await
}

async fn acquire_permit(
    semaphore_cell: &'static OnceLock<Semaphore>,
    component: &'static str,
    max_concurrent_uploads: usize,
) -> anyhow::Result<SemaphorePermit<'static>> {
    let semaphore = semaphore_cell.get_or_init(|| Semaphore::const_new(max_concurrent_uploads));
    let permits_gauge = gauge!(parent: AVAILABLE_CONCURRENT_UPLOAD_PERMITS, labels: [label_values!(COMPONENT => component)]);
    permits_gauge.set(semaphore.available_permits() as f64);
    semaphore.acquire().await.context("upload semaphore closed")
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

/// Stages, uploads, and publishes a single merged/deleted split.
#[allow(clippy::too_many_arguments)]
#[instrument(
    level = "info",
    name = "stage_upload_publish",
    fields(split = %packaged_split.split_attrs.split_id),
    skip_all
)]
pub(crate) async fn stage_upload_and_publish_split(
    packaged_split: PackagedSplit,
    metastore: &MetastoreServiceClient,
    split_store: &IndexingSplitStore,
    merge_policy: &Arc<dyn MergePolicy>,
    retention_policy: Option<&RetentionPolicy>,
    max_concurrent_split_uploads: usize,
    event_broker: &EventBroker,
    kill_switch: &KillSwitch,
) -> anyhow::Result<SplitMetadata> {
    let _permit = acquire_merge_upload_permit(max_concurrent_split_uploads).await?;
    if kill_switch.is_dead() {
        warn!(
            split_id = packaged_split.split_id(),
            "kill switch was activated, cancelling upload"
        );
        bail!("upload cancelled by kill switch");
    }

    let index_uid = packaged_split.index_uid().clone();
    let (split_metadata, split_payload) =
        prepare_split_for_upload(&packaged_split, merge_policy, retention_policy).with_context(
            || {
                format!(
                    "failed to prepare split `{}` for upload",
                    packaged_split.split_id()
                )
            },
        )?;

    debug!(
        split_id = packaged_split.split_id(),
        "start-stage-and-store-split"
    );
    let stage_splits_request = StageSplitsRequest::try_from_splits_metadata(
        index_uid.clone(),
        vec![split_metadata.clone()],
    )
    .context("failed to create stage splits request")?;
    metastore
        .stage_splits(stage_splits_request)
        .await
        .context("failed to stage split")?;

    event_broker.publish(ReportSplitsRequest {
        report_splits: vec![ReportSplit {
            storage_uri: split_store.remote_uri().to_string(),
            split_id: packaged_split.split_id().to_string(),
        }],
    });

    split_store
        .store_split(
            &split_metadata,
            packaged_split.split_scratch_directory.path(),
            Box::new(split_payload),
        )
        .await
        .with_context(|| format!("failed to upload split `{}`", packaged_split.split_id()))?;

    let replaced_split_ids: Vec<String> = packaged_split
        .split_attrs
        .replaced_split_ids
        .iter()
        .map(String::from)
        .collect();
    let publish_splits_request = PublishSplitsRequest {
        index_uid: Some(index_uid.clone()),
        staged_split_ids: vec![split_metadata.split_id.to_string()],
        replaced_split_ids,
    };
    metastore
        .publish_splits(publish_splits_request)
        .await
        .context("failed to publish splits")?;

    record_published_split(&index_uid.index_id, &split_metadata);
    info!(
        num_splits = 1,
        num_docs = split_metadata.num_docs,
        split_size_bytes = split_metadata.footer_offsets.end,
        "publish-new-splits"
    );

    Ok(split_metadata)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use async_trait::async_trait;
    use quickwit_common::pubsub::EventSubscriber;
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_proto::metastore::{EmptyResponse, MockMetastoreService};
    use quickwit_proto::types::{DocMappingUid, IndexUid, NodeId, SplitId};
    use quickwit_storage::RamStorage;
    use tantivy::DateTime;
    use tokio::sync::mpsc;

    use super::*;
    use crate::merge_policy::NopMergePolicy;
    use crate::models::SplitAttrs;

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
    async fn test_stage_upload_and_publish_single_split() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();

        let node_id = NodeId::from_str("test-node");
        let index_uid = IndexUid::new_with_random_ulid("test-index");
        let source_id = "test-source".to_string();
        let split_id = SplitId::from("new-split");

        let event_broker = EventBroker::default();
        let (reported_tx, mut reported_rx) = mpsc::unbounded_channel();
        struct ReportSender(mpsc::UnboundedSender<ReportSplitsRequest>);
        #[async_trait]
        impl EventSubscriber<ReportSplitsRequest> for ReportSender {
            async fn handle_event(&mut self, event: ReportSplitsRequest) {
                let _ = self.0.send(event);
            }
        }
        let _sub = event_broker.subscribe(ReportSender(reported_tx));

        let mut mock_metastore = MockMetastoreService::new();
        let index_uid_for_stage = index_uid.clone();
        mock_metastore
            .expect_stage_splits()
            .withf(move |req| req.index_uid() == &index_uid_for_stage)
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

        let index_uid_for_publish = index_uid.clone();
        mock_metastore
            .expect_publish_splits()
            .withf(move |req| {
                req.index_uid() == &index_uid_for_publish
                    && req.staged_split_ids == vec!["new-split".to_string()]
                    && req.replaced_split_ids == vec!["old-split".to_string()]
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

        let metastore = MetastoreServiceClient::from_mock(mock_metastore);
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::new(storage.clone());
        let scratch_dir = TempDirectory::for_test();
        let file_path = scratch_dir.path().join("meta.json");
        std::fs::write(&file_path, b"{}")?;

        let packaged_split = PackagedSplit {
            serialized_split_fields: Vec::new(),
            split_attrs: SplitAttrs {
                node_id,
                index_uid: index_uid.clone(),
                source_id,
                doc_mapping_uid: DocMappingUid::default(),
                split_id: split_id.clone(),
                partition_id: 0,
                num_docs: 42,
                uncompressed_docs_size_in_bytes: 1000,
                time_range: Some(
                    DateTime::from_timestamp_secs(100)..=DateTime::from_timestamp_secs(200),
                ),
                replaced_split_ids: vec![SplitId::from("old-split")],
                delete_opstamp: 0,
                num_merge_ops: 1,
            },
            split_scratch_directory: scratch_dir,
            tags: Default::default(),
            split_files: vec![file_path],
            hotcache_bytes: vec![1, 2, 3],
        };

        let merge_policy: Arc<dyn MergePolicy> = Arc::new(NopMergePolicy);
        let published = stage_upload_and_publish_split(
            packaged_split,
            &metastore,
            &split_store,
            &merge_policy,
            None,
            4,
            &event_broker,
            &KillSwitch::default(),
        )
        .await?;

        assert_eq!(published.split_id, split_id);
        assert_eq!(published.num_docs, 42);
        let reported = reported_rx
            .recv()
            .await
            .expect("expected ReportSplitsRequest event");
        assert_eq!(reported.report_splits.len(), 1);
        Ok(())
    }
}
