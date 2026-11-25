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
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Instant;

use quickwit_common::KillSwitch;
use quickwit_common::io::{IoControls, Limiter};
use quickwit_common::pubsub::EventBroker;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::RetentionPolicy;
use quickwit_doc_mapper::DocMapper;
use quickwit_indexing::merge_policy::MergeOperation;
use quickwit_indexing::{IndexingSplitStore, MergeExecutionContext, execute_merge_operation};
use quickwit_metrics::{counter, gauge, histogram, label_values};
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_proto::types::{IndexUid, SourceId, SplitId};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::sync::{Semaphore, oneshot};
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::metrics::{
    COMPACTION_DURATION, COMPACTIONS_FAILED, COMPACTIONS_IN_PROGRESS, COMPACTIONS_SUCCEEDED,
    SOURCE_UID_MERGE_LEVEL,
};
use crate::{TaskId, source_uid_metrics_label};

#[derive(Clone, Debug, PartialEq)]
pub enum PipelineStatus {
    InProgress,
    Completed,
    Failed { error: String },
}

impl PipelineStatus {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            PipelineStatus::Completed | PipelineStatus::Failed { .. }
        )
    }
}

pub struct PipelineStatusUpdate {
    pub task_id: TaskId,
    pub index_uid: IndexUid,
    pub source_id: SourceId,
    pub split_ids: HashSet<SplitId>,
    pub status: PipelineStatus,
}

struct RunningCompaction {
    task_handle: JoinHandle<()>,
    result_rx: oneshot::Receiver<Result<(), String>>,
}

/// A single-use compaction pipeline. Processes one merge task and terminates.
///
/// Owned by `CompactorService`, which periodically polls `pipeline_status_update()`.
pub struct CompactionPipeline {
    task_id: TaskId,
    merge_operation: MergeOperation,
    pipeline_id: MergePipelineId,
    status: PipelineStatus,
    kill_switch: KillSwitch,
    scratch_directory: TempDirectory,
    running: Option<RunningCompaction>,
    doc_mapper: Arc<DocMapper>,
    merge_policy: Arc<dyn quickwit_indexing::merge_policy::MergePolicy>,
    retention_policy: Option<RetentionPolicy>,
    metastore: MetastoreServiceClient,
    split_store: IndexingSplitStore,
    io_throughput_limiter: Option<Limiter>,
    max_concurrent_split_uploads: NonZeroUsize,
    event_broker: EventBroker,
    merge_execution_semaphore: Arc<Semaphore>,
    pipeline_start: Option<Instant>,
}

impl Drop for CompactionPipeline {
    fn drop(&mut self) {
        self.kill_switch.kill();
        if let Some(running) = self.running.take() {
            running.task_handle.abort();
        }
    }
}

impl CompactionPipeline {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: TaskId,
        scratch_directory: TempDirectory,
        merge_operation: MergeOperation,
        pipeline_id: MergePipelineId,
        doc_mapper: Arc<DocMapper>,
        merge_policy: Arc<dyn quickwit_indexing::merge_policy::MergePolicy>,
        retention_policy: Option<RetentionPolicy>,
        metastore: MetastoreServiceClient,
        split_store: IndexingSplitStore,
        io_throughput_limiter: Option<Limiter>,
        max_concurrent_split_uploads: NonZeroUsize,
        event_broker: EventBroker,
        merge_execution_semaphore: Arc<Semaphore>,
    ) -> Self {
        CompactionPipeline {
            task_id,
            status: PipelineStatus::InProgress,
            kill_switch: KillSwitch::default(),
            scratch_directory,
            running: None,
            merge_operation,
            pipeline_id,
            doc_mapper,
            merge_policy,
            retention_policy,
            metastore,
            split_store,
            io_throughput_limiter,
            max_concurrent_split_uploads,
            event_broker,
            merge_execution_semaphore,
            pipeline_start: None,
        }
    }

    pub fn status(&self) -> &PipelineStatus {
        &self.status
    }

    /// Returns the latest status update for this compaction task.
    ///
    /// Once terminal (`Completed` or `Failed`), the status is sticky until the
    /// compactor service frees the slot after planner acknowledgement.
    pub fn pipeline_status_update(&mut self) -> PipelineStatusUpdate {
        self.update_status();
        self.build_status_update()
    }

    fn update_status(&mut self) {
        if self.status.is_terminal() {
            return;
        }
        let Some(running) = self.running.as_mut() else {
            return;
        };

        match running.result_rx.try_recv() {
            Ok(Ok(())) => {
                self.record_pipeline_duration();
                info!(task_id = %self.task_id, "compaction pipeline completed");
                self.status = PipelineStatus::Completed;
                self.record_terminal_metrics(true);
                self.running = None;
            }
            Ok(Err(error_msg)) => {
                self.record_pipeline_duration();
                error!(task_id = %self.task_id, error = %error_msg, "compaction pipeline failed");
                self.status = PipelineStatus::Failed { error: error_msg };
                self.record_terminal_metrics(false);
                self.running = None;
            }
            Err(TryRecvError::Closed) => {
                self.record_pipeline_duration();
                let error_msg = "compaction task exited unexpectedly".to_string();
                error!(task_id = %self.task_id, "{error_msg}");
                self.status = PipelineStatus::Failed { error: error_msg };
                self.record_terminal_metrics(false);
                self.running = None;
            }
            Err(TryRecvError::Empty) => {}
        }
    }

    /// Fires once when the pipeline transitions from InProgress to a terminal state.
    /// Pairs with the `COMPACTIONS_IN_PROGRESS.inc()` in `CompactorService::spawn_task`.
    fn record_terminal_metrics(&self, succeeded: bool) {
        let merge_level = self.merge_operation.merge_level().to_string();
        let index_label =
            source_uid_metrics_label(&self.pipeline_id.index_uid, &self.pipeline_id.source_id);
        let labels = label_values!(SOURCE_UID_MERGE_LEVEL => index_label, merge_level);
        gauge!(parent: COMPACTIONS_IN_PROGRESS, labels: [labels.clone()]).dec();
        if succeeded {
            counter!(parent: COMPACTIONS_SUCCEEDED, labels: [labels]).inc();
        } else {
            counter!(parent: COMPACTIONS_FAILED, labels: [labels]).inc();
        }
    }

    fn record_pipeline_duration(&self) {
        if let Some(pipeline_start_time) = self.pipeline_start {
            let elapsed = pipeline_start_time.elapsed().as_secs_f64();
            let merge_level = self.merge_operation.merge_level().to_string();
            let index_label =
                source_uid_metrics_label(&self.pipeline_id.index_uid, &self.pipeline_id.source_id);
            let labels = label_values!(SOURCE_UID_MERGE_LEVEL => index_label, merge_level);
            histogram!(parent: COMPACTION_DURATION, labels: [labels]).observe(elapsed);
        }
    }

    fn build_status_update(&self) -> PipelineStatusUpdate {
        let split_ids = self
            .merge_operation
            .splits
            .iter()
            .map(|split| split.split_id.clone())
            .collect();

        PipelineStatusUpdate {
            task_id: self.task_id.clone(),
            index_uid: self.pipeline_id.index_uid.clone(),
            source_id: self.pipeline_id.source_id.clone(),
            split_ids,
            status: self.status.clone(),
        }
    }

    /// Spawns a background task executing the merge operation end-to-end.
    pub(crate) fn spawn_pipeline(&mut self) -> anyhow::Result<()> {
        info!(
            task_id = %self.task_id,
            pipeline_id = %self.pipeline_id,
            "spawning compaction pipeline"
        );

        let download_io_controls = IoControls::default()
            .set_throughput_limiter_opt(self.io_throughput_limiter.clone())
            .set_component("split_downloader_merge");
        let merge_io_controls = download_io_controls.clone().set_component("merger");

        let merge_ctx = MergeExecutionContext {
            pipeline_id: self.pipeline_id.clone(),
            metastore: self.metastore.clone(),
            doc_mapper: self.doc_mapper.clone(),
            split_store: self.split_store.clone(),
            merge_policy: self.merge_policy.clone(),
            retention_policy: self.retention_policy.clone(),
            scratch_directory: self.scratch_directory.clone(),
            download_io_controls,
            merge_io_controls,
            max_concurrent_split_uploads: self.max_concurrent_split_uploads.get(),
            event_broker: self.event_broker.clone(),
            merge_execution_semaphore: Some(self.merge_execution_semaphore.clone()),
            kill_switch: self.kill_switch.child(),
        };
        let merge_operation = self.merge_operation.clone();
        let (result_tx, result_rx) = oneshot::channel();

        self.pipeline_start = Some(Instant::now());
        let task_handle = tokio::spawn(async move {
            let outcome = execute_merge_operation(&merge_ctx, merge_operation)
                .await
                .map(|_| ())
                .map_err(|err| format!("{err:#}"));
            let _ = result_tx.send(outcome);
        });
        self.running = Some(RunningCompaction {
            task_handle,
            result_rx,
        });
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashSet;
    use std::num::NonZeroUsize;
    use std::sync::Arc;

    use quickwit_common::pubsub::EventBroker;
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_doc_mapper::default_doc_mapper_for_test;
    use quickwit_indexing::IndexingSplitStore;
    use quickwit_indexing::merge_policy::{MergeOperation, default_merge_policy};
    use quickwit_metastore::SplitMetadata;
    use quickwit_proto::indexing::MergePipelineId;
    use quickwit_proto::metastore::{MetastoreServiceClient, MockMetastoreService};
    use quickwit_proto::types::{IndexUid, NodeId, SplitId};
    use quickwit_storage::RamStorage;
    use tokio::sync::Semaphore;

    use super::{CompactionPipeline, PipelineStatus};

    pub fn test_pipeline(task_id: &str, split_ids: &[&str]) -> CompactionPipeline {
        let storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::new(storage);
        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());
        let splits: Vec<SplitMetadata> = split_ids
            .iter()
            .map(|id| SplitMetadata::for_test(SplitId::from(*id)))
            .collect();
        let merge_operation = MergeOperation::new_merge_operation(splits);
        let pipeline_id = MergePipelineId {
            node_id: NodeId::from_str("test-node"),
            index_uid: IndexUid::for_test("test-index", 0),
            source_id: "test-source".to_string(),
        };
        CompactionPipeline::new(
            task_id.to_string(),
            TempDirectory::for_test(),
            merge_operation,
            pipeline_id,
            Arc::new(default_doc_mapper_for_test()),
            default_merge_policy(),
            None,
            metastore,
            split_store,
            None,
            NonZeroUsize::new(2).unwrap(),
            EventBroker::default(),
            Arc::new(Semaphore::new(2)),
        )
    }

    #[tokio::test]
    async fn test_spawn_pipeline_creates_task() {
        let mut pipeline = test_pipeline("task-1", &["split-1", "split-2"]);
        assert!(pipeline.running.is_none());
        pipeline.spawn_pipeline().unwrap();
        assert!(pipeline.running.is_some());
    }

    #[tokio::test]
    async fn test_status_update_unspawned_pipeline() {
        let mut pipeline = test_pipeline("task-1", &["split-1"]);
        let update = pipeline.pipeline_status_update();
        assert_eq!(update.status, PipelineStatus::InProgress);
        assert_eq!(update.task_id, "task-1");
        assert_eq!(update.split_ids, HashSet::from([SplitId::from("split-1")]));
        assert_eq!(update.source_id, "test-source");
        assert_eq!(update.index_uid, IndexUid::for_test("test-index", 0));
    }

    #[tokio::test]
    async fn test_status_update_healthy_pipeline() {
        let mut pipeline = test_pipeline("task-1", &["split-1"]);
        pipeline.spawn_pipeline().unwrap();
        let update = pipeline.pipeline_status_update();
        assert_eq!(update.status, PipelineStatus::InProgress);
    }

    #[tokio::test]
    async fn test_killed_pipeline_fails() {
        let mut pipeline = test_pipeline("task-1", &["split-1"]);
        pipeline.spawn_pipeline().unwrap();

        pipeline.kill_switch.kill();
        tokio::task::yield_now().await;
        let update = pipeline.pipeline_status_update();
        assert!(matches!(update.status, PipelineStatus::Failed { .. }));

        // Calling again still returns Failed (sticky).
        let update = pipeline.pipeline_status_update();
        assert!(matches!(update.status, PipelineStatus::Failed { .. }));
    }
}
