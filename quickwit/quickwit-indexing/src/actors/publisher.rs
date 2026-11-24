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

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, QueueCapacity};
use quickwit_proto::metastore::{MetastoreService, MetastoreServiceClient, PublishSplitsRequest};
use serde::Serialize;
use tracing::{info, instrument};

use crate::metrics::record_published_split;
use crate::models::SplitsUpdate;

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

#[derive(Clone)]
pub struct Publisher {
    publisher_type: PublisherType,
    metastore: MetastoreServiceClient,
    counters: PublisherCounters,
}

impl Publisher {
    pub fn new(publisher_type: PublisherType, metastore: MetastoreServiceClient) -> Publisher {
        Publisher {
            publisher_type,
            metastore,
            counters: PublisherCounters::default(),
        }
    }
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
impl Handler<SplitsUpdate> for Publisher {
    type Reply = ();

    #[instrument(name="publisher", parent=split_update.parent_span.id(), skip(self, ctx))]
    async fn handle(
        &mut self,
        split_update: SplitsUpdate,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let SplitsUpdate {
            index_uid,
            new_splits,
            replaced_split_ids,
            ..
        } = split_update;

        let index_id = index_uid.index_id.clone();
        let split_ids: Vec<String> = new_splits
            .iter()
            .map(|split| split.split_id.to_string())
            .collect();

        let publish_splits_request = PublishSplitsRequest {
            index_uid: Some(index_uid.clone()),
            staged_split_ids: split_ids.clone(),
            replaced_split_ids: replaced_split_ids.iter().map(String::from).collect(),
        };
        ctx.protect_future(self.metastore.publish_splits(publish_splits_request))
            .await
            .map_err(|error| anyhow::Error::from(error).context("failed to publish splits"))?;

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
            if replaced_split_ids.is_empty() {
                self.counters.num_published_splits += 1;
            } else {
                self.counters.num_replace_operations += 1;
            }
        } else {
            self.counters.num_empty_splits += 1;
        }
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
    use crate::models::SplitsUpdate;

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
            })
            .times(1)
            .returning(|_| Ok(EmptyResponse {}));

        let publisher = Publisher::new(
            PublisherType::MergePublisher,
            MetastoreServiceClient::from_mock(mock_metastore),
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
                merge_task: None,
                parent_span: tracing::Span::none(),
            })
            .await
            .unwrap();

        let publisher_observation = publisher_handle.process_pending_and_observe().await.state;
        assert_eq!(publisher_observation.num_replace_operations, 1);
        universe.assert_quit().await;
    }
}
