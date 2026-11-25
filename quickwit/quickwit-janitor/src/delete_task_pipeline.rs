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

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use quickwit_common::io::IoControls;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::uri::Uri;
use quickwit_common::{KillSwitch, temp_dir};
use quickwit_config::build_doc_mapper;
use quickwit_indexing::merge_policy::{MergeOperation, merge_policy_from_settings};
use quickwit_indexing::{IndexingSplitStore, MergeExecutionContext, execute_merge_operation};
use quickwit_metastore::IndexMetadataResponseExt;
use quickwit_proto::indexing::MergePipelineId;
use quickwit_proto::metastore::{IndexMetadataRequest, MetastoreService, MetastoreServiceClient};
use quickwit_proto::types::{IndexUid, NodeId};
use quickwit_search::SearchJobPlacer;
use quickwit_storage::Storage;
use tantivy::TrackedObject;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use super::delete_task_planner::DeleteTaskPlanner;

pub struct DeleteTaskPipelineHandle {
    shutdown: CancellationToken,
    kill_switch: KillSwitch,
    planner_handle: JoinHandle<()>,
    worker_handle: JoinHandle<()>,
    #[cfg(test)]
    num_planner_errors: Arc<AtomicU64>,
    #[cfg(test)]
    num_execution_errors: Arc<AtomicU64>,
}

impl DeleteTaskPipelineHandle {
    /// Stops the planner and drains any already-queued delete operations.
    #[cfg(test)]
    pub async fn stop_gracefully(self) {
        self.shutdown.cancel();
        let _ = self.planner_handle.await;
        let _ = self.worker_handle.await;
    }

    /// Cancels in-flight delete operations immediately and awaits task teardown.
    pub async fn kill(self) {
        self.kill_switch.kill();
        self.shutdown.cancel();
        self.planner_handle.abort();
        self.worker_handle.abort();
        let _ = self.planner_handle.await;
        let _ = self.worker_handle.await;
    }

    #[cfg(test)]
    pub fn num_planner_errors(&self) -> u64 {
        self.num_planner_errors.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn num_execution_errors(&self) -> u64 {
        self.num_execution_errors.load(Ordering::Relaxed)
    }
}

pub struct DeleteTaskPipeline {
    index_uid: IndexUid,
    metastore: MetastoreServiceClient,
    search_job_placer: SearchJobPlacer,
    index_storage: Arc<dyn Storage>,
    delete_service_task_dir: PathBuf,
    max_concurrent_split_uploads: usize,
    delete_execution_semaphore: Arc<Semaphore>,
    event_broker: EventBroker,
}

impl DeleteTaskPipeline {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        index_uid: IndexUid,
        metastore: MetastoreServiceClient,
        search_job_placer: SearchJobPlacer,
        index_storage: Arc<dyn Storage>,
        delete_service_task_dir: PathBuf,
        max_concurrent_split_uploads: usize,
        delete_execution_semaphore: Arc<Semaphore>,
        event_broker: EventBroker,
    ) -> Self {
        Self {
            index_uid,
            metastore,
            search_job_placer,
            index_storage,
            delete_service_task_dir,
            max_concurrent_split_uploads,
            delete_execution_semaphore,
            event_broker,
        }
    }

    pub async fn spawn(
        self,
        parent_shutdown: &CancellationToken,
    ) -> anyhow::Result<DeleteTaskPipelineHandle> {
        info!(
            index_uid=%self.index_uid,
            root_dir=%self.delete_service_task_dir.to_str().unwrap(),
            "spawning delete tasks pipeline",
        );
        let index_config = self
            .metastore
            .index_metadata(IndexMetadataRequest::for_index_uid(self.index_uid.clone()))
            .await?
            .deserialize_index_metadata()?
            .into_index_config();
        let split_store = IndexingSplitStore::new(self.index_storage.clone());
        let merge_policy = merge_policy_from_settings(&index_config.indexing_settings);
        let doc_mapper =
            build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)?;
        let pipeline_id = MergePipelineId {
            node_id: NodeId::from_str("unknown"),
            index_uid: self.index_uid.clone(),
            source_id: "unknown".to_string(),
        };
        let merge_io_controls = IoControls::default().set_component("deleter");
        let download_io_controls = merge_io_controls
            .clone()
            .set_component("split_downloader_delete");
        let scratch_directory = temp_dir::Builder::default()
            .join(&self.index_uid.index_id)
            .join(&self.index_uid.incarnation_id.to_string())
            .tempdir_in(&self.delete_service_task_dir)?;
        let kill_switch = KillSwitch::default();
        let merge_ctx = MergeExecutionContext {
            pipeline_id,
            metastore: self.metastore.clone(),
            doc_mapper: doc_mapper.clone(),
            split_store,
            merge_policy,
            retention_policy: index_config.retention_policy_opt.clone(),
            scratch_directory,
            download_io_controls,
            merge_io_controls,
            max_concurrent_split_uploads: self.max_concurrent_split_uploads,
            event_broker: self.event_broker.clone(),
            merge_execution_semaphore: Some(self.delete_execution_semaphore.clone()),
            kill_switch: kill_switch.clone(),
        };

        let (delete_op_tx, delete_op_rx) = mpsc::unbounded_channel();
        let num_execution_errors = Arc::new(AtomicU64::new(0));
        let worker_handle = tokio::spawn(run_delete_worker(
            delete_op_rx,
            merge_ctx,
            num_execution_errors.clone(),
        ));

        let doc_mapper_str = serde_json::to_string(&doc_mapper)?;
        let index_uri: &Uri = &index_config.index_uri;
        let task_planner = DeleteTaskPlanner::new(
            self.index_uid.clone(),
            index_uri.clone(),
            doc_mapper_str,
            self.metastore.clone(),
            self.search_job_placer.clone(),
            delete_op_tx,
        );
        let shutdown = parent_shutdown.child_token();
        let num_planner_errors = Arc::new(AtomicU64::new(0));
        let planner_handle =
            tokio::spawn(task_planner.run(shutdown.clone(), num_planner_errors.clone()));
        Ok(DeleteTaskPipelineHandle {
            shutdown,
            kill_switch,
            planner_handle,
            worker_handle,
            #[cfg(test)]
            num_planner_errors,
            #[cfg(test)]
            num_execution_errors,
        })
    }
}

async fn run_delete_worker(
    mut delete_op_rx: mpsc::UnboundedReceiver<TrackedObject<MergeOperation>>,
    merge_ctx: MergeExecutionContext,
    num_execution_errors: Arc<AtomicU64>,
) {
    while let Some(tracked_op) = delete_op_rx.recv().await {
        if merge_ctx.kill_switch.is_dead() {
            break;
        }
        let operation = (*tracked_op).clone();
        if let Err(error) = execute_merge_operation(&merge_ctx, operation).await {
            if merge_ctx.kill_switch.is_dead() {
                break;
            }
            num_execution_errors.fetch_add(1, Ordering::Relaxed);
            error!(?error, "failed to execute delete operation");
        }
        drop(tracked_op);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_common::pubsub::EventBroker;
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_common::test_utils::wait_until_predicate;
    use quickwit_indexing::TestSandbox;
    use quickwit_metastore::{ListSplitsRequestExt, MetastoreServiceStreamSplitsExt, SplitState};
    use quickwit_proto::metastore::{DeleteQuery, ListSplitsRequest, MetastoreService};
    use quickwit_proto::search::{LeafSearchRequest, LeafSearchResponse};
    use quickwit_search::{
        MockSearchService, SearchError, SearchJobPlacer, searcher_pool_for_test,
    };
    use tokio::sync::Semaphore;
    use tokio_util::sync::CancellationToken;

    use super::DeleteTaskPipeline;

    #[tokio::test]
    async fn test_delete_pipeline_simple() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_id = "test-delete-pipeline-simple";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: i64
                fast: true
        "#;
        let indexing_settings_yaml = r#"
            merge_policy:
                type: no_merge
        "#;
        let test_sandbox = TestSandbox::create(
            index_id,
            doc_mapping_yaml,
            indexing_settings_yaml,
            &["body"],
        )
        .await
        .unwrap();
        let index_uid = test_sandbox.index_uid();
        let docs = vec![
            serde_json::json!({"body": "info", "ts": 0 }),
            serde_json::json!({"body": "info", "ts": 0 }),
            serde_json::json!({"body": "delete", "ts": 0 }),
        ];
        test_sandbox.add_documents(docs).await?;
        let metastore = test_sandbox.metastore();
        metastore
            .create_delete_task(DeleteQuery {
                index_uid: Some(index_uid.clone()),
                start_timestamp: None,
                end_timestamp: None,
                query_ast: quickwit_query::query_ast::qast_json_helper("body:delete", &[]),
            })
            .await
            .unwrap();
        let mut mock_search_service = MockSearchService::new();
        let mut leaf_search_num_failures = 1;
        mock_search_service
            .expect_leaf_search()
            .withf(|leaf_request| -> bool {
                leaf_request
                    .search_request
                    .as_ref()
                    .unwrap()
                    .index_id_patterns
                    == vec!["test-delete-pipeline-simple".to_string()]
            })
            .times(2)
            .returning(move |_: LeafSearchRequest| {
                if leaf_search_num_failures > 0 {
                    leaf_search_num_failures -= 1;
                    return Err(SearchError::Internal("leaf search error".to_string()));
                }
                Ok(LeafSearchResponse {
                    num_hits: 1,
                    ..Default::default()
                })
            });
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let delete_service_task_dir = TempDirectory::for_test();
        let shutdown = CancellationToken::new();
        let pipeline = DeleteTaskPipeline::new(
            test_sandbox.index_uid(),
            metastore.clone(),
            search_job_placer,
            test_sandbox.storage(),
            delete_service_task_dir.path().into(),
            4,
            Arc::new(Semaphore::new(3)),
            EventBroker::default(),
        )
        .spawn(&shutdown)
        .await?;

        wait_until_predicate(
            || {
                let metastore = metastore.clone();
                let index_uid = index_uid.clone();
                async move {
                    let Ok(splits) = metastore
                        .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
                        .await
                        .unwrap()
                        .collect_splits()
                        .await
                    else {
                        return false;
                    };
                    splits.iter().any(|split| {
                        split.split_state == SplitState::Published
                            && split.split_metadata.delete_opstamp == 1
                    })
                }
            },
            Duration::from_secs(30),
            Duration::from_millis(50),
        )
        .await
        .expect("the split with the delete applied should be published");
        assert_eq!(pipeline.num_planner_errors(), 1);
        assert_eq!(pipeline.num_execution_errors(), 0);
        pipeline.stop_gracefully().await;

        let splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
            .await
            .unwrap()
            .collect_splits()
            .await
            .unwrap();
        assert_eq!(splits.len(), 2);
        let published_split = splits
            .iter()
            .find(|split| split.split_state == SplitState::Published)
            .unwrap();
        assert_eq!(published_split.split_metadata.delete_opstamp, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_pipeline_shut_down() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_id = "test-delete-pipeline-shut-down";
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: body
                type: text
              - name: ts
                type: i64
                fast: true
        "#;
        let test_sandbox = TestSandbox::create(index_id, doc_mapping_yaml, "{}", &["body"])
            .await
            .unwrap();
        let metastore = test_sandbox.metastore();
        let mut mock_search_service = MockSearchService::new();
        mock_search_service
            .expect_leaf_search()
            .withf(|leaf_request| -> bool {
                leaf_request
                    .search_request
                    .as_ref()
                    .unwrap()
                    .index_id_patterns
                    == vec!["test-delete-pipeline-shut-down".to_string()]
            })
            .returning(move |_: LeafSearchRequest| {
                Ok(LeafSearchResponse {
                    num_hits: 0,
                    ..Default::default()
                })
            });
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1001", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let delete_service_task_dir = TempDirectory::for_test();
        let shutdown = CancellationToken::new();
        let pipeline = DeleteTaskPipeline::new(
            test_sandbox.index_uid(),
            metastore.clone(),
            search_job_placer,
            test_sandbox.storage(),
            delete_service_task_dir.path().into(),
            4,
            Arc::new(Semaphore::new(3)),
            EventBroker::default(),
        )
        .spawn(&shutdown)
        .await?;

        pipeline.kill().await;
        Ok(())
    }
}
