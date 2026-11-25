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

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use itertools::Itertools;
use quickwit_common::extract_time_range;
use quickwit_common::uri::Uri;
use quickwit_doc_mapper::tag_pruning::extract_tags_from_query;
use quickwit_indexing::merge_policy::MergeOperation;
use quickwit_metastore::{ListSplitsResponseExt, Split, split_tag_filter, split_time_range_filter};
use quickwit_metrics::gauge;
use quickwit_proto::metastore::{
    DeleteTask, LastDeleteOpstampRequest, ListDeleteTasksRequest, ListStaleSplitsRequest,
    MetastoreResult, MetastoreService, MetastoreServiceClient, UpdateSplitsDeleteOpstampRequest,
};
use quickwit_proto::search::SearchRequest;
use quickwit_proto::types::IndexUid;
use quickwit_search::{
    IndexMetasForLeafSearch, SearchJob, SearchJobPlacer, compute_query_complexity_factor,
    jobs_to_leaf_request,
};
use tantivy::{Inventory, TrackedObject};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::metrics::ONGOING_NUM_DELETE_OPERATIONS_TOTAL;

const PLANNER_REFRESH_INTERVAL: Duration = Duration::from_mins(1);
const PLANNER_ERROR_BACKOFF_MIN: Duration = if cfg!(any(test, feature = "testsuite")) {
    Duration::from_millis(50)
} else {
    Duration::from_secs(1)
};
const PLANNER_ERROR_BACKOFF_MAX: Duration = Duration::from_mins(1);
const NUM_STALE_SPLITS_TO_FETCH: usize = 1000;

/// The `DeleteTaskPlanner` plans delete operations on splits for a given index.
/// For each split, the planner checks if there is some documents to delete:
/// - If this is the case, it sends a [`MergeOperation`] to the `MergeExecutor` `MergeOperation` to
///   the `MergeExecutor`.
/// - If there is no document to delete, it updates the split `delete_opstamp` to the latest delete
///   task opstamp.
///
/// Pseudo-algorithm for a given index:
/// 1. Fetches the delete tasks and deduce the last `opstamp`.
/// 2. Fetches the last `N` stale splits ordered by their `delete_opstamp`. A stale split is a split
///    a `delete_opstamp` inferior to the last `opstamp` In theory, this works but... there is one
///    difficulty:
///    - Delete operations do not run on immature splits and they are excluded after fetching stale
///      splits from the metastore as the metastore has no knowledge about the merge policy. If
///      there are more than `N` immature stale splits, the planner will plan no operations.
///      However, this is mitigated by the fact that a merge policy should consider "old split" as
///      mature and an index should not have many immature splits.
///      See tracked issue <https://github.com/quickwit-oss/quickwit/issues/2147>.
/// 3. If there is no stale splits, stop.
/// 4. If there are stale splits, for each split, do:
///    - Get the list of delete queries to apply to this split.
///    - Keep only delete queries that match the split metadata (time range and tags).
///    - If no delete queries remains, then update the split `delete_opstamp` to the latest
///      `opstamp`.
///    - If there are delete queries that match the metadata, do: + Execute delete queries
///      (`leaf_request`) one by one to check if there is a match. + As soon as a hit is returned
///      for a given query, the split is sent to the `MergeExecutor`. + If no delete queries match
///      documents, update the split `delete_opstamp` to the last `opstamp`.
#[derive(Clone)]
pub struct DeleteTaskPlanner {
    index_uid: IndexUid,
    index_uri: Uri,
    doc_mapper_str: String,
    metastore: MetastoreServiceClient,
    search_job_placer: SearchJobPlacer,
    delete_op_tx: mpsc::UnboundedSender<TrackedObject<MergeOperation>>,
    /// Inventory of ongoing delete operations. If everything goes well,
    /// a merge operation is dropped after the publish of the split that underwent
    /// the delete operation.
    /// The inventory is used to avoid sending twice the same delete operation.
    ongoing_delete_operations_inventory: Inventory<MergeOperation>,
}

impl DeleteTaskPlanner {
    pub fn new(
        index_uid: IndexUid,
        index_uri: Uri,
        doc_mapper_str: String,
        metastore: MetastoreServiceClient,
        search_job_placer: SearchJobPlacer,
        delete_op_tx: mpsc::UnboundedSender<TrackedObject<MergeOperation>>,
    ) -> Self {
        Self {
            index_uid,
            index_uri,
            doc_mapper_str,
            metastore,
            search_job_placer,
            delete_op_tx,
            ongoing_delete_operations_inventory: Inventory::new(),
        }
    }

    #[cfg(test)]
    pub fn ongoing_delete_operations_inventory(&self) -> Inventory<MergeOperation> {
        self.ongoing_delete_operations_inventory.clone()
    }

    pub async fn run(mut self, shutdown: CancellationToken, num_errors: Arc<AtomicU64>) {
        let mut error_backoff = PLANNER_ERROR_BACKOFF_MIN;
        loop {
            let sleep_duration = match self.send_delete_operations().await {
                Ok(()) => {
                    error_backoff = PLANNER_ERROR_BACKOFF_MIN;
                    PLANNER_REFRESH_INTERVAL
                }
                Err(error) => {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    num_errors.fetch_add(1, Ordering::Relaxed);
                    error!(
                        index_uid = %self.index_uid,
                        ?error,
                        retry_in = ?error_backoff,
                        "delete task planner pass failed"
                    );
                    let delay = error_backoff;
                    error_backoff = (error_backoff * 2).min(PLANNER_ERROR_BACKOFF_MAX);
                    delay
                }
            };
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(sleep_duration) => {}
            }
        }
    }

    /// Send delete operations for a given `index_id`.
    pub async fn send_delete_operations(&mut self) -> anyhow::Result<()> {
        // Loop until there is no more stale splits.
        loop {
            let last_delete_opstamp_request = LastDeleteOpstampRequest {
                index_uid: Some(self.index_uid.clone()),
            };
            let last_delete_opstamp = self
                .metastore
                .last_delete_opstamp(last_delete_opstamp_request)
                .await?
                .last_delete_opstamp;
            let stale_splits = self
                .get_relevant_stale_splits(self.index_uid.clone(), last_delete_opstamp)
                .await?;
            debug!(
                index_id = self.index_uid.index_id,
                last_delete_opstamp = last_delete_opstamp,
                num_stale_splits = stale_splits.len()
            );

            if stale_splits.is_empty() {
                break;
            }

            let (splits_with_deletes, splits_without_deletes) =
                self.partition_splits_by_deletes(&stale_splits).await?;

            info!(
                "{} splits with deletes, {} splits without deletes.",
                splits_with_deletes.len(),
                splits_without_deletes.len()
            );

            // Updates `delete_opstamp` of splits that won't undergo delete operations.
            let split_ids_without_delete = splits_without_deletes
                .iter()
                .map(|split| split.split_id().to_string())
                .collect_vec();
            let update_splits_delete_opstamp_request = UpdateSplitsDeleteOpstampRequest {
                index_uid: Some(self.index_uid.clone()),
                split_ids: split_ids_without_delete.clone(),
                delete_opstamp: last_delete_opstamp,
            };
            self.metastore
                .update_splits_delete_opstamp(update_splits_delete_opstamp_request)
                .await?;

            // Sends delete operations.
            for split_with_deletes in splits_with_deletes {
                let delete_operation = MergeOperation::new_delete_and_merge_operation(
                    split_with_deletes.split_metadata,
                );
                info!(delete_operation=?delete_operation, "planned delete operation");
                let tracked_delete_operation = self
                    .ongoing_delete_operations_inventory
                    .track(delete_operation);
                self.delete_op_tx
                    .send(tracked_delete_operation)
                    .map_err(|_| anyhow::anyhow!("delete task worker channel closed"))?;
                let index_label =
                    quickwit_common::metrics::index_label(self.index_uid.index_id.as_str())
                        .to_string();
                gauge!(
                    parent: ONGOING_NUM_DELETE_OPERATIONS_TOTAL,
                    "index" => index_label,
                )
                .set(self.ongoing_delete_operations_inventory.list().len() as f64);
            }
        }

        Ok(())
    }

    /// Identifies splits that contain documents to delete and
    /// splits that do not and returns the two groups.
    async fn partition_splits_by_deletes(
        &mut self,
        stale_splits: &[Split],
    ) -> anyhow::Result<(Vec<Split>, Vec<Split>)> {
        let mut splits_without_deletes: Vec<Split> = Vec::new();
        let mut splits_with_deletes: Vec<Split> = Vec::new();

        for stale_split in stale_splits {
            let list_delete_tasks_request = ListDeleteTasksRequest::new(
                self.index_uid.clone(),
                stale_split.split_metadata.delete_opstamp,
            );
            let pending_tasks = self
                .metastore
                .list_delete_tasks(list_delete_tasks_request)
                .await?
                .delete_tasks;

            // Keep only delete tasks that matches the split metadata.
            let pending_and_matching_metadata_tasks = pending_tasks
                .into_iter()
                .filter(|delete_task| {
                    let delete_query = delete_task
                        .delete_query
                        .as_ref()
                        .expect("Delete task must have a delete query.");
                    let time_range = extract_time_range(
                        delete_query.start_timestamp,
                        delete_query.end_timestamp,
                    );
                    // TODO: validate the query at the beginning and return an appropriate error.
                    let delete_query_ast = serde_json::from_str(&delete_query.query_ast)
                        .expect("Failed to deserialize query_ast json");
                    let tags_filter = extract_tags_from_query(delete_query_ast);
                    split_time_range_filter(&stale_split.split_metadata, time_range.as_ref())
                        && split_tag_filter(&stale_split.split_metadata, tags_filter.as_ref())
                })
                .collect_vec();

            // If there is no matching delete tasks,
            // there is no document to delete on this split.
            if pending_and_matching_metadata_tasks.is_empty() {
                splits_without_deletes.push(stale_split.clone());
                continue;
            }

            let has_split_docs_to_delete = self
                .has_split_docs_to_delete(
                    stale_split,
                    &pending_and_matching_metadata_tasks,
                    &self.doc_mapper_str,
                    self.index_uri.as_str(),
                )
                .await?;

            if has_split_docs_to_delete {
                splits_with_deletes.push(stale_split.clone());
            } else {
                splits_without_deletes.push(stale_split.clone());
            }
        }

        Ok((splits_with_deletes, splits_without_deletes))
    }

    /// Executes a `LeafSearchRequest` on the split and returns true
    /// if it matches documents.
    async fn has_split_docs_to_delete(
        &self,
        stale_split: &Split,
        delete_tasks: &[DeleteTask],
        doc_mapper_str: &str,
        index_uri: &str,
    ) -> anyhow::Result<bool> {
        for delete_task in delete_tasks {
            let delete_query = delete_task
                .delete_query
                .as_ref()
                .expect("Delete task must have a delete query.");
            // TODO: resolve with the default fields.
            let search_request = SearchRequest {
                index_id_patterns: vec![delete_query.index_uid().index_id.to_string()],
                query_ast: delete_query.query_ast.clone(),
                start_timestamp: delete_query.start_timestamp,
                end_timestamp: delete_query.end_timestamp,
                ..Default::default()
            };
            let query_complexity_factor = compute_query_complexity_factor(&search_request)?;
            let search_job = SearchJob::new(&stale_split.split_metadata, query_complexity_factor);
            let mut search_client = self
                .search_job_placer
                .assign_job(search_job.clone(), &HashSet::new())
                .await?;
            let mut search_indexes_metas = HashMap::new();
            let index_uri = Uri::from_str(index_uri).context("invalid index URI")?;
            search_indexes_metas.insert(
                delete_query.index_uid().clone(),
                IndexMetasForLeafSearch {
                    doc_mapper_str: doc_mapper_str.to_string(),
                    index_uri,
                },
            );
            let leaf_search_request =
                jobs_to_leaf_request(&search_request, &search_indexes_metas, vec![search_job])?;
            let response = search_client.leaf_search(leaf_search_request).await?;
            if response.num_hits > 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Fetches stale splits from [`quickwit_metastore::Metastore`] and excludes immature splits and
    /// split already among ongoing delete operations.
    async fn get_relevant_stale_splits(
        &mut self,
        index_uid: IndexUid,
        last_delete_opstamp: u64,
    ) -> MetastoreResult<Vec<Split>> {
        let list_stale_splits_request = ListStaleSplitsRequest {
            index_uid: Some(index_uid.clone()),
            delete_opstamp: last_delete_opstamp,
            num_splits: NUM_STALE_SPLITS_TO_FETCH as u64,
        };
        let stale_splits = self
            .metastore
            .list_stale_splits(list_stale_splits_request)
            .await?
            .deserialize_splits()
            .await?;
        debug!(
            index_id = index_uid.index_id,
            last_delete_opstamp = last_delete_opstamp,
            num_stale_splits_from_metastore = stale_splits.len()
        );
        let ongoing_delete_operations = self.ongoing_delete_operations_inventory.list();
        let filtered_splits = stale_splits
            .into_iter()
            .filter(|stale_split| {
                !ongoing_delete_operations.iter().any(|operation| {
                    operation
                        .splits
                        .first()
                        .unwrap() // <- This is safe as we know for sure that an operation is on one split.
                        .split_id()
                        == stale_split.split_id()
                })
            })
            .collect_vec();
        Ok(filtered_splits)
    }
}

#[cfg(test)]
mod tests {
    use quickwit_config::build_doc_mapper;
    use quickwit_indexing::TestSandbox;
    use quickwit_metastore::{
        IndexMetadataResponseExt, ListSplitsRequestExt, MetastoreServiceStreamSplitsExt,
        SplitMetadata,
    };
    use quickwit_proto::metastore::{DeleteQuery, IndexMetadataRequest, ListSplitsRequest};
    use quickwit_proto::search::{LeafSearchRequest, LeafSearchResponse};
    use quickwit_search::{MockSearchService, searcher_pool_for_test};

    use super::*;

    #[tokio::test]
    async fn test_delete_task_planner() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let index_id = "test-delete-task-planner";
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
        .await?;
        let docs = [
            serde_json::json!({"body": "info", "ts": 0 }),
            serde_json::json!({"body": "info", "ts": 0 }),
            serde_json::json!({"body": "delete", "ts": 0 }),
        ];
        // Creates 3 splits
        for doc in docs {
            test_sandbox.add_documents(vec![doc]).await?;
        }
        let metastore = test_sandbox.metastore();
        let index_metadata_request = IndexMetadataRequest::for_index_id(index_id.to_string());
        let index_metadata = metastore
            .index_metadata(index_metadata_request)
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();
        let index_uid = index_metadata.index_uid.clone();
        let index_config = index_metadata.into_index_config();
        let split_metas: Vec<SplitMetadata> = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid.clone()).unwrap())
            .await
            .unwrap()
            .collect_splits_metadata()
            .await
            .unwrap();
        assert_eq!(split_metas.len(), 3);
        let doc_mapper =
            build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)?;
        let doc_mapper_str = serde_json::to_string(&doc_mapper)?;

        // Creates 2 delete tasks, one that will match 1 document,
        // the other that will match no document.

        let body_delete_ast = quickwit_query::query_ast::qast_json_helper("body:delete", &[]);
        let match_nothing_ast =
            quickwit_query::query_ast::qast_json_helper("body:matchnothing", &[]);
        metastore
            .create_delete_task(DeleteQuery {
                index_uid: Some(index_uid.clone()),
                start_timestamp: None,
                end_timestamp: None,
                query_ast: body_delete_ast.clone(),
            })
            .await?;
        metastore
            .create_delete_task(DeleteQuery {
                index_uid: Some(index_uid.clone()),
                start_timestamp: None,
                end_timestamp: None,
                query_ast: match_nothing_ast,
            })
            .await?;
        let mut mock_search_service = MockSearchService::new();

        // We have 2 delete tasks. Each one will trigger a leaf request for each
        // of the 3 splits. This makes 6 requests.
        let split_id_with_doc_to_delete = split_metas[2].split_id.to_string();
        mock_search_service.expect_leaf_search().times(6).returning(
            move |request: LeafSearchRequest| {
                // Search on body:delete should return one hit only on the last split
                // that should contains the doc.
                if request.leaf_requests[0].split_offsets[0].split_id == split_id_with_doc_to_delete
                    && request.search_request.as_ref().unwrap().query_ast == body_delete_ast
                {
                    return Ok(LeafSearchResponse {
                        num_hits: 1,
                        ..Default::default()
                    });
                }
                Ok(LeafSearchResponse {
                    num_hits: 0,
                    ..Default::default()
                })
            },
        );
        let searcher_pool = searcher_pool_for_test([("127.0.0.1:1000", mock_search_service)]);
        let search_job_placer = SearchJobPlacer::new(searcher_pool);
        let (delete_op_tx, mut delete_op_rx) = mpsc::unbounded_channel();
        let mut delete_planner = DeleteTaskPlanner::new(
            index_uid.clone(),
            index_config.index_uri.clone(),
            doc_mapper_str,
            metastore.clone(),
            search_job_placer,
            delete_op_tx,
        );
        delete_planner.send_delete_operations().await?;
        let first_op = delete_op_rx
            .try_recv()
            .expect("expected one planned delete operation");
        assert!(delete_op_rx.try_recv().is_err());
        // The last split will undergo a delete operation.
        assert_eq!(first_op.splits[0].split_id(), split_metas[2].split_id());
        // Check planner inventory tracks the ongoing operation.
        {
            let ongoing = delete_planner.ongoing_delete_operations_inventory().list();
            assert_eq!(ongoing[0].splits[0].split_id(), split_metas[2].split_id());
        }
        // Trigger new plan evaluation and check that we don't have new merge operation.
        delete_planner.send_delete_operations().await?;
        assert!(delete_op_rx.try_recv().is_err());
        // Now drop the current merge operation and check that the planner will plan a new
        // operation.
        drop(first_op);
        assert!(
            delete_planner
                .ongoing_delete_operations_inventory()
                .list()
                .is_empty()
        );

        // Trigger operations planning.
        delete_planner.send_delete_operations().await?;
        let second_op = delete_op_rx
            .try_recv()
            .expect("expected second planned delete operation");
        assert!(delete_op_rx.try_recv().is_err());
        assert_eq!(second_op.splits[0].split_id(), split_metas[2].split_id());
        // The other splits have their delete opstamps updated to the last opstamp (2).
        let all_splits = metastore
            .list_splits(ListSplitsRequest::try_from_index_uid(index_uid).unwrap())
            .await
            .unwrap()
            .collect_splits_metadata()
            .await
            .unwrap();
        assert_eq!(all_splits[0].delete_opstamp, 2);
        assert_eq!(all_splits[1].delete_opstamp, 2);
        // The last split has not yet had its delete opstamp updated.
        assert_eq!(all_splits[2].delete_opstamp, 0);
        Ok(())
    }
}
