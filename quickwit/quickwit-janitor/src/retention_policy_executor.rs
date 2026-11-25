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
use std::time::Duration;

use itertools::Itertools;
use quickwit_config::IndexConfig;
use quickwit_metastore::ListIndexesMetadataResponseExt;
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::IndexUid;
use serde::Serialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::retention_policy_execution::run_execute_retention_policy;

const RUN_INTERVAL: Duration = Duration::from_hours(1);

#[derive(Clone, Debug, Default, Serialize)]
pub struct RetentionPolicyExecutorCounters {
    /// The number of refresh the config passes.
    pub num_refresh_passes: usize,

    /// The number of execution passes.
    pub num_execution_passes: usize,

    /// The number of expired splits.
    pub num_expired_splits: usize,
}

struct ScheduledIndex {
    index_config: IndexConfig,
    next_evaluation_at: Instant,
}

/// Schedules retention policy execution on all indexes.
/// Keeps a cache of indexes with a retention policy configured and periodically refreshes it.
pub struct RetentionPolicyExecutor {
    metastore: MetastoreServiceClient,
    /// Caches retention configs and next scheduled evaluation time by index UID to distinguish
    /// recreated indexes.
    scheduled_indexes: HashMap<IndexUid, ScheduledIndex>,
    counters: RetentionPolicyExecutorCounters,
    counters_tx: watch::Sender<RetentionPolicyExecutorCounters>,
}

impl RetentionPolicyExecutor {
    pub fn new(metastore: MetastoreServiceClient) -> Self {
        let counters = RetentionPolicyExecutorCounters::default();
        let (counters_tx, _) = watch::channel(counters.clone());
        Self {
            metastore,
            scheduled_indexes: HashMap::new(),
            counters,
            counters_tx,
        }
    }

    #[cfg(test)]
    fn subscribe_counters(&self) -> watch::Receiver<RetentionPolicyExecutorCounters> {
        self.counters_tx.subscribe()
    }

    pub fn spawn(mut self, shutdown: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.refresh_index_configs().await;
            let mut next_refresh_at = Instant::now() + RUN_INTERVAL;
            loop {
                let next_deadline = self
                    .scheduled_indexes
                    .values()
                    .map(|entry| entry.next_evaluation_at)
                    .min()
                    .map(|eval_at| eval_at.min(next_refresh_at))
                    .unwrap_or(next_refresh_at);

                tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep_until(next_deadline) => {}
                }

                let now = Instant::now();
                if now >= next_refresh_at {
                    self.refresh_index_configs().await;
                    next_refresh_at = Instant::now() + RUN_INTERVAL;
                }

                let due_index_uids: Vec<IndexUid> = self
                    .scheduled_indexes
                    .iter()
                    .filter(|(_, entry)| entry.next_evaluation_at <= now)
                    .map(|(index_uid, _)| index_uid.clone())
                    .collect();
                for index_uid in due_index_uids {
                    if shutdown.is_cancelled() {
                        return;
                    }
                    self.execute_retention_policy(&index_uid).await;
                }
            }
        })
    }

    /// Refreshes the cached index retention policies from the metastore.
    async fn refresh_index_configs(&mut self) {
        debug!("loading indexes from the metastore");
        self.counters.num_refresh_passes += 1;

        let response = match self
            .metastore
            .list_indexes_metadata(ListIndexesMetadataRequest::all())
            .await
        {
            Ok(response) => response,
            Err(error) => {
                error!(%error, "failed to list indexes from the metastore");
                let _ = self.counters_tx.send(self.counters.clone());
                return;
            }
        };
        let indexes = match response.deserialize_indexes_metadata().await {
            Ok(indexes) => indexes,
            Err(error) => {
                error!(%error, "failed to deserialize indexes metadata");
                let _ = self.counters_tx.send(self.counters.clone());
                return;
            }
        };
        info!("loaded {} indexes from the metastore", indexes.len());

        let deleted_indexes = compute_deleted_indexes(
            self.scheduled_indexes.keys(),
            indexes
                .iter()
                .map(|index_metadata| &index_metadata.index_uid),
        );
        if !deleted_indexes.is_empty() {
            debug!(index_uids=%deleted_indexes.iter().join(", "), "deleting indexes from cache");
            for deleted_index_uid in deleted_indexes {
                self.scheduled_indexes.remove(&deleted_index_uid);
            }
        }
        let now = Instant::now();
        for index_metadata in indexes {
            let index_uid = index_metadata.index_uid.clone();
            let index_config = index_metadata.into_index_config();
            let retention_policy = match &index_config.retention_policy_opt {
                Some(policy) => policy,
                None => {
                    self.scheduled_indexes.remove(&index_uid);
                    continue;
                }
            };

            if let Some(entry) = self.scheduled_indexes.get_mut(&index_uid) {
                entry.index_config = index_config;
                continue;
            }

            if let Ok(next_interval) = retention_policy.duration_until_next_evaluation() {
                info!(index_id=?index_config.index_id, scheduled_in=?next_interval, "retention-policy-schedule-operation");
                self.scheduled_indexes.insert(
                    index_uid,
                    ScheduledIndex {
                        index_config,
                        next_evaluation_at: now + next_interval,
                    },
                );
            } else {
                error!(index_id=%index_config.index_id, "couldn't extract the index next schedule time");
            }
        }
        let _ = self.counters_tx.send(self.counters.clone());
    }

    async fn execute_retention_policy(&mut self, index_uid: &IndexUid) {
        info!(index_id=%index_uid.index_id, "retention-policy-execute-operation");
        self.counters.num_execution_passes += 1;

        let Some(entry) = self.scheduled_indexes.get(index_uid) else {
            debug!(index_uid=%index_uid, "the index was deleted, recreated, or no longer has a retention policy");
            let _ = self.counters_tx.send(self.counters.clone());
            return;
        };

        let retention_policy = entry
            .index_config
            .retention_policy_opt
            .as_ref()
            .expect("Expected index to have retention policy configured.")
            .clone();
        let index_id = entry.index_config.index_id.clone();

        let execution_result = run_execute_retention_policy(
            index_uid.clone(),
            self.metastore.clone(),
            &retention_policy,
        )
        .await;
        match execution_result {
            Ok(splits) => self.counters.num_expired_splits += splits.len(),
            Err(error) => {
                error!(index_id=%index_uid.index_id, error=?error, "failed to execute the retention policy on the index");
            }
        }

        if let Ok(next_interval) = retention_policy.duration_until_next_evaluation() {
            info!(index_id=?index_id, scheduled_in=?next_interval, "retention-policy-schedule-operation");
            if let Some(entry) = self.scheduled_indexes.get_mut(index_uid) {
                entry.next_evaluation_at = Instant::now() + next_interval;
            }
        } else {
            self.scheduled_indexes.remove(index_uid);
            error!(index_id=%index_uid.index_id, "couldn't extract the index next schedule interval");
        }
        let _ = self.counters_tx.send(self.counters.clone());
    }
}

/// Extract the list of deleted indexes.
fn compute_deleted_indexes<'a>(
    cached_indexes: impl Iterator<Item = &'a IndexUid>,
    indexes: impl Iterator<Item = &'a IndexUid>,
) -> HashSet<IndexUid> {
    let cached_set: HashSet<_> = cached_indexes.collect();
    let indexes_set: HashSet<_> = indexes.collect();
    (&cached_set - &indexes_set).into_iter().cloned().collect()
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;

    use mockall::Sequence;
    use quickwit_common::ServiceStream;
    use quickwit_config::{HumanDuration, RetentionPolicy};
    use quickwit_metastore::{
        IndexMetadata, ListSplitsRequestExt, ListSplitsResponseExt, Split, SplitMetadata,
        SplitState,
    };
    use quickwit_proto::metastore::{
        EmptyResponse, ListIndexesMetadataResponse, ListSplitsResponse, MockMetastoreService,
    };

    use super::*;

    fn assert_scheduled_indexes(
        executor: &RetentionPolicyExecutor,
        expected: &[(&'static str, Option<&'static str>)],
    ) {
        let indexes_set: HashSet<_> = executor
            .scheduled_indexes
            .values()
            .map(|entry| {
                (
                    &entry.index_config.index_id,
                    &entry.index_config.retention_policy_opt,
                )
            })
            .collect();

        let expected_indexes: Vec<IndexConfig> = make_indexes(expected)
            .into_iter()
            .map(IndexMetadata::into_index_config)
            .collect();
        let expected_indexes_set: HashSet<_> = expected_indexes
            .iter()
            .map(|im| (&im.index_id, &im.retention_policy_opt))
            .collect();
        assert_eq!(
            indexes_set, expected_indexes_set,
            "Mismatch set of indexes."
        );
    }

    const EVALUATION_SCHEDULE: &str = "hourly";

    fn make_index(index_id: &str, retention_period_opt: Option<&str>) -> IndexConfig {
        let mut index = IndexConfig::for_test(index_id, &format!("ram://indexes/{index_id}"));
        if let Some(retention_period) = retention_period_opt {
            index.retention_policy_opt = Some(RetentionPolicy {
                retention_period: retention_period.to_string(),
                evaluation_schedule: EVALUATION_SCHEDULE.to_string(),
                evaluation_schedule_jitter: Some(
                    HumanDuration::try_from("0s".to_string()).unwrap(),
                ),
            })
        }
        index
    }

    fn make_indexes(index_ids: &[(&str, Option<&str>)]) -> Vec<IndexMetadata> {
        index_ids
            .iter()
            .map(|(index_id, retention_period_opt)| make_index(index_id, *retention_period_opt))
            .map(IndexMetadata::new)
            .collect()
    }

    fn make_split(split_id: &str, time_range: Option<RangeInclusive<i64>>) -> Split {
        Split {
            split_metadata: SplitMetadata {
                split_id: split_id.into(),
                footer_offsets: 5..20,
                time_range,
                ..Default::default()
            },
            split_state: SplitState::Published,
            update_timestamp: 0,
            publish_timestamp: Some(100),
        }
    }

    // Uses the retention policy scheduler to calculate
    // how much time to advance for the execution to take place.
    fn shift_time_by() -> Duration {
        let scheduler = RetentionPolicy {
            retention_period: "".to_string(),
            evaluation_schedule: EVALUATION_SCHEDULE.to_string(),
            evaluation_schedule_jitter: Some(HumanDuration::try_from("0s".to_string()).unwrap()),
        };

        scheduler.duration_until_next_evaluation().unwrap() + Duration::from_secs(1)
    }

    #[tokio::test]
    async fn test_retention_executor_refresh() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();

        let mut sequence = Sequence::new();
        mock_metastore
            .expect_list_splits()
            .times(..)
            .returning(|_| Ok(ServiceStream::empty()));
        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_list_indexes_request| {
                let indexes_metadata = make_indexes(&[
                    ("index-1", Some("1 hour")),
                    ("index-2", Some("1 hour")),
                    ("index-3", None),
                ]);
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });

        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_list_indexes_request| {
                let indexes_metadata = make_indexes(&[
                    ("index-1", Some("1 hour")),
                    ("index-2", Some("2 hour")),
                    ("index-3", Some("1 hour")),
                ]);
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });

        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .in_sequence(&mut sequence)
            .returning(|_list_indexes_request| {
                let indexes_metadata = make_indexes(&[
                    ("index-2", Some("1 hour")),
                    ("index-4", Some("1 hour")),
                    ("index-5", None),
                ]);
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });

        let mut retention_policy_executor =
            RetentionPolicyExecutor::new(MetastoreServiceClient::from_mock(mock_metastore));

        retention_policy_executor.refresh_index_configs().await;
        assert_eq!(retention_policy_executor.counters.num_refresh_passes, 1);
        assert_scheduled_indexes(
            &retention_policy_executor,
            &[("index-1", Some("1 hour")), ("index-2", Some("1 hour"))],
        );

        retention_policy_executor.refresh_index_configs().await;
        assert_eq!(retention_policy_executor.counters.num_refresh_passes, 2);
        assert_scheduled_indexes(
            &retention_policy_executor,
            &[
                ("index-1", Some("1 hour")),
                ("index-2", Some("2 hour")),
                ("index-3", Some("1 hour")),
            ],
        );

        retention_policy_executor.refresh_index_configs().await;
        assert_eq!(retention_policy_executor.counters.num_refresh_passes, 3);
        assert_scheduled_indexes(
            &retention_policy_executor,
            &[("index-2", Some("1 hour")), ("index-4", Some("1 hour"))],
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_retention_executor_ignores_executions_from_previous_incarnation()
    -> anyhow::Result<()> {
        let previous_index_uid = IndexUid::for_test("index-1", 0);
        let recreated_index_uid = IndexUid::for_test("index-1", 1);

        let mut mock_metastore = MockMetastoreService::new();
        let mut sequence = Sequence::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .in_sequence(&mut sequence)
            .returning({
                let previous_index_uid = previous_index_uid.clone();
                move |_list_indexes_request| {
                    let index_metadata = IndexMetadata::new_with_index_uid(
                        previous_index_uid.clone(),
                        make_index("index-1", Some("3 hours")),
                    );
                    Ok(ListIndexesMetadataResponse::for_test(vec![index_metadata]))
                }
            });
        mock_metastore
            .expect_list_indexes_metadata()
            .times(1)
            .in_sequence(&mut sequence)
            .returning({
                let recreated_index_uid = recreated_index_uid.clone();
                move |_list_indexes_request| {
                    let index_metadata = IndexMetadata::new_with_index_uid(
                        recreated_index_uid.clone(),
                        make_index("index-1", Some("3 hours")),
                    );
                    Ok(ListIndexesMetadataResponse::for_test(vec![index_metadata]))
                }
            });
        // An execution queued before the index was recreated must not reach the metastore.
        mock_metastore.expect_list_splits().times(0);
        mock_metastore.expect_mark_splits_for_deletion().times(0);

        let mut retention_policy_executor =
            RetentionPolicyExecutor::new(MetastoreServiceClient::from_mock(mock_metastore));

        // First refresh: the index exists with its original UID.
        retention_policy_executor.refresh_index_configs().await;
        assert_eq!(retention_policy_executor.counters.num_refresh_passes, 1);

        // The index is deleted and recreated under the same ID, so the next refresh sees a new UID.
        retention_policy_executor.refresh_index_configs().await;
        assert_eq!(retention_policy_executor.counters.num_refresh_passes, 2);

        // An execution targeting the previous UID is ignored.
        retention_policy_executor
            .execute_retention_policy(&previous_index_uid)
            .await;
        assert_eq!(retention_policy_executor.counters.num_execution_passes, 1);
        assert_eq!(retention_policy_executor.counters.num_expired_splits, 0);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn test_retention_policy_execution_calls_dependencies() -> anyhow::Result<()> {
        let mut mock_metastore = MockMetastoreService::new();
        mock_metastore
            .expect_list_indexes_metadata()
            .times(..)
            .returning(|_list_indexes_request| {
                let indexes_metadata = make_indexes(&[
                    ("index-1", Some("2 hour")),
                    ("index-2", Some("1 hour")),
                    ("index-3", None),
                ]);
                Ok(ListIndexesMetadataResponse::for_test(indexes_metadata))
            });

        mock_metastore
            .expect_list_splits()
            .times(2..=4)
            .returning(|list_splits_request| {
                let query = list_splits_request.deserialize_list_splits_query().unwrap();
                assert_eq!(query.split_states, &[SplitState::Published]);
                let splits = match query.index_uids.unwrap()[0].index_id.as_ref() {
                    "index-1" => {
                        vec![
                            make_split("split-1", Some(1000..=5000)),
                            make_split("split-2", Some(2000..=6000)),
                            make_split("split-3", None),
                        ]
                    }
                    "index-2" => Vec::new(),
                    unknown => panic!("Unknown index: `{unknown}`."),
                };
                let splits_response = ListSplitsResponse::try_from_splits(splits).unwrap();
                Ok(ServiceStream::from(vec![Ok(splits_response)]))
            });

        mock_metastore
            .expect_mark_splits_for_deletion()
            .times(1..=3)
            .returning(|mark_splits_for_deletion_request| {
                let index_uid: IndexUid = mark_splits_for_deletion_request.index_uid().clone();
                assert_eq!(index_uid.index_id, "index-1");
                assert_eq!(
                    mark_splits_for_deletion_request.split_ids,
                    ["split-1", "split-2"]
                );
                Ok(EmptyResponse {})
            });

        let retention_policy_executor =
            RetentionPolicyExecutor::new(MetastoreServiceClient::from_mock(mock_metastore));
        let mut counters_rx = retention_policy_executor.subscribe_counters();
        let shutdown = CancellationToken::new();
        let handle = retention_policy_executor.spawn(shutdown.clone());

        let counters = counters_rx
            .wait_for(|counters| counters.num_refresh_passes >= 1)
            .await
            .unwrap()
            .clone();
        assert_eq!(counters.num_execution_passes, 0);
        assert_eq!(counters.num_expired_splits, 0);

        tokio::time::advance(shift_time_by()).await;
        let counters = counters_rx
            .wait_for(|counters| counters.num_execution_passes >= 2)
            .await
            .unwrap()
            .clone();
        assert_eq!(counters.num_execution_passes, 2);
        assert_eq!(counters.num_expired_splits, 2);
        shutdown.cancel();
        handle.await.unwrap();

        Ok(())
    }
}
