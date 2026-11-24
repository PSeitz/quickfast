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

use std::time::Duration;

use async_trait::async_trait;
use fail::fail_point;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_proto::metastore::{
    MetastoreError, MetastoreResult, MetastoreService, MetastoreServiceClient, PublishSplitsRequest,
};
use serde::Serialize;
use tracing::{info, instrument, warn};

use crate::actors::MergePlanner;
use crate::metrics::record_published_split;
use crate::models::{NewSplits, SharedPublishToken, SplitsUpdate};

#[derive(Clone, Debug, Default, Serialize)]
pub struct PublisherCounters {
    pub num_published_splits: u64,
    pub num_replace_operations: u64,
    pub num_empty_splits: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum PublisherType {
    MergePublisher,
}

impl PublisherType {
    pub fn actor_name(&self) -> &'static str {
        match self {
            PublisherType::MergePublisher => "MergePublisher",
        }
    }
}

/// Disconnects the merge planner loop back to let the merge pipeline terminate.
#[derive(Debug)]
pub(crate) struct DisconnectMergePlanner;

#[derive(Clone)]
pub struct Publisher {
    publisher_type: PublisherType,
    metastore: MetastoreServiceClient,
    merge_planner_mailbox_opt: Option<Mailbox<MergePlanner>>,
    publish_token: SharedPublishToken,
    counters: PublisherCounters,
}

impl Publisher {
    pub fn new(
        publisher_type: PublisherType,
        metastore: MetastoreServiceClient,
        merge_planner_mailbox_opt: Option<Mailbox<MergePlanner>>,
        publish_token: SharedPublishToken,
    ) -> Publisher {
        Publisher {
            publisher_type,
            metastore,
            merge_planner_mailbox_opt,
            publish_token,
            counters: PublisherCounters::default(),
        }
    }
}

async fn publish_with_retry<T, F, Fut>(
    ctx: &ActorContext<Publisher>,
    operation_name: &str,
    mut publish: F,
) -> Result<(), ActorExitStatus>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = MetastoreResult<T>>,
{
    for retry_delay in [
        Some(Duration::from_secs(1)),
        Some(Duration::from_secs(3)),
        None,
    ] {
        let Err(error) = ctx.protect_future(publish()).await else {
            return Ok(());
        };
        let retryable = matches!(error, MetastoreError::InvalidPublishToken { .. });
        match retry_delay {
            Some(retry_delay) if retryable => {
                warn!(%error, operation = operation_name, "metastore publish failed, retrying");
                ctx.protect_future(ctx.sleep(retry_delay)).await;
            }
            _ => {
                warn!(%error, operation = operation_name, retryable, "metastore publish failed, giving up after 3 tries");
                return Err(anyhow::Error::from(error)
                    .context(format!("failed to {operation_name}"))
                    .into());
            }
        }
    }
    unreachable!();
}

#[async_trait]
impl Actor for Publisher {
    type ObservableState = PublisherCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn name(&self) -> String {
        self.publisher_type.actor_name().to_string()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        match self.publisher_type {
            PublisherType::MergePublisher => QueueCapacity::Unbounded,
        }
    }
}

#[async_trait]
impl Handler<DisconnectMergePlanner> for Publisher {
    type Reply = ();

    async fn handle(
        &mut self,
        _: DisconnectMergePlanner,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.merge_planner_mailbox_opt = None;
        Ok(())
    }
}

#[async_trait]
impl Handler<SplitsUpdate> for Publisher {
    type Reply = ();

    #[instrument(name="publisher", parent=split_update.parent_span.id(), skip(self, ctx))]
    async fn handle(
        &mut self,
        split_update: SplitsUpdate,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        fail_point!("publisher:before");

        let SplitsUpdate {
            index_uid,
            new_splits,
            replaced_split_ids,
            publish_lock,
            ..
        } = split_update;

        let index_id = index_uid.index_id.clone();
        let split_ids: Vec<String> = new_splits
            .iter()
            .map(|split| split.split_id.to_string())
            .collect();
        let Some(guard) = publish_lock.acquire().await else {
            info!(
                split_ids=?split_ids,
                "Splits' publish lock is dead."
            );
            return Ok(());
        };
        publish_with_retry(ctx, "publish splits", || {
            let metastore = self.metastore.clone();
            let publish_splits_request = PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids: split_ids.clone(),
                replaced_split_ids: replaced_split_ids.iter().map(String::from).collect(),
                index_checkpoint_delta_json_opt: None,
                publish_token_opt: self
                    .publish_token
                    .load()
                    .as_deref()
                    .map(|publish_token| publish_token.to_string()),
            };
            async move { metastore.publish_splits(publish_splits_request).await }
        })
        .await?;
        drop(guard);

        for split in &new_splits {
            record_published_split(&index_id, split);
        }
        let num_docs: usize = new_splits.iter().map(|split| split.num_docs).sum();
        let split_size_bytes: u64 = new_splits
            .iter()
            .map(|split| split.footer_offsets.end)
            .sum();
        info!(
            num_splits = new_splits.len(),
            num_docs, split_size_bytes, "publish-new-splits"
        );

        if !new_splits.is_empty() {
            if let Some(merge_planner_mailbox) = self.merge_planner_mailbox_opt.as_ref() {
                let _ = ctx
                    .send_message(merge_planner_mailbox, NewSplits { new_splits })
                    .await;
            }

            if replaced_split_ids.is_empty() {
                self.counters.num_published_splits += 1;
            } else {
                self.counters.num_replace_operations += 1;
            }
        } else {
            self.counters.num_empty_splits += 1;
        }
        fail_point!("publisher:after");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::Universe;
    use quickwit_metastore::SplitMetadata;
    use quickwit_proto::metastore::{EmptyResponse, MetastoreServiceClient, MockMetastoreService};
    use quickwit_proto::types::{IndexUid, SplitId};

    use super::{Publisher, PublisherType};
    use crate::models::{NewSplits, PublishLock, SharedPublishToken, SplitsUpdate};

    #[tokio::test]
    async fn test_publisher_publish_operation() {
        let universe = Universe::with_accelerated_time();
        let ref_index_uid: IndexUid = IndexUid::for_test("index", 1);
        let mut mock_metastore = MockMetastoreService::new();
        let ref_index_uid_clone = ref_index_uid.clone();
        mock_metastore
            .expect_publish_splits()
            .withf(move |req| {
                req.index_uid() == &ref_index_uid_clone
                    && req.staged_split_ids[..] == ["split"]
                    && req.replaced_split_ids[..] == ["old_split"]
                    && req.index_checkpoint_delta_json_opt.is_none()
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));
        let (merge_planner_mailbox, merge_planner_inbox) = universe.create_test_mailbox();

        let publisher = Publisher::new(
            PublisherType::MergePublisher,
            MetastoreServiceClient::from_mock(mock_metastore),
            Some(merge_planner_mailbox),
            SharedPublishToken::default(),
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);

        publisher_mailbox
            .send_message(SplitsUpdate {
                index_uid: ref_index_uid.clone(),
                new_splits: vec![SplitMetadata {
                    split_id: "split".into(),
                    ..Default::default()
                }],
                replaced_split_ids: vec![SplitId::from("old_split")],
                publish_lock: PublishLock::default(),
                merge_task: None,
                parent_span: tracing::Span::none(),
            })
            .await
            .unwrap();

        let publisher_observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(publisher_observation.num_replace_operations, 1);

        let merger_msgs: Vec<NewSplits> = merge_planner_inbox.drain_for_test_typed::<NewSplits>();
        assert_eq!(merger_msgs.len(), 1);
        assert_eq!(merger_msgs[0].new_splits.len(), 1);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_publisher_ignores_dead_publish_lock() {
        let universe = Universe::with_accelerated_time();
        let mock_metastore = MockMetastoreService::new();
        let publisher = Publisher::new(
            PublisherType::MergePublisher,
            MetastoreServiceClient::from_mock(mock_metastore),
            None,
            SharedPublishToken::default(),
        );
        let (publisher_mailbox, publisher_handle) = universe.spawn_builder().spawn(publisher);
        let publish_lock = PublishLock::default();
        publish_lock.kill().await;

        publisher_mailbox
            .send_message(SplitsUpdate {
                index_uid: IndexUid::for_test("index", 1),
                new_splits: vec![SplitMetadata {
                    split_id: "split".into(),
                    ..Default::default()
                }],
                replaced_split_ids: Vec::new(),
                publish_lock,
                merge_task: None,
                parent_span: tracing::Span::none(),
            })
            .await
            .unwrap();

        let obs = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(obs.num_published_splits, 0);
        universe.assert_quit().await;
    }
}
