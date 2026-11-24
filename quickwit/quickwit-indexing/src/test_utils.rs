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

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use bytes::Bytes;
use quickwit_actors::{Mailbox, Universe};
use quickwit_common::Progress;
use quickwit_common::io::IoControls;
use quickwit_common::pubsub::EventBroker;
use quickwit_common::rand::append_random_suffix;
use quickwit_common::temp_dir::TempDirectory;
use quickwit_common::uri::Uri;
use quickwit_config::{
    ConfigFormat, INGEST_V2_SOURCE_ID, IndexConfig, MetastoreConfigs, SourceConfig,
    SourceInputFormat, build_doc_mapper,
};
use quickwit_doc_mapper::DocMapper;
use quickwit_metastore::{
    CreateIndexRequestExt, MetastoreResolver, Split, SplitMetadata, SplitState,
    StageSplitsRequestExt,
};
use quickwit_proto::indexing::{IndexingPipelineId, MergePipelineId};
use quickwit_proto::metastore::{
    CreateIndexRequest, LastDeleteOpstampRequest, MetastoreService, MetastoreServiceClient,
    PublishSplitsRequest, StageSplitsRequest,
};
use quickwit_proto::types::{IndexUid, NodeId, PipelineUid, SourceId};
use quickwit_storage::{Storage, StorageResolver};
use serde_json::Value as JsonValue;

use crate::actors::{
    DocumentConverter, MergePipeline, MergePipelineParams, MergePlanner, MergeSchedulerService,
    create_packaged_split, prepare_split_for_upload,
};
use crate::merge_policy::{MergePolicy, merge_policy_from_settings};
use crate::models::{IndexedSplitBuilder, IndexingStatistics, NewSplits};
use crate::split_store::{IndexingSplitCache, IndexingSplitStore};

/// Creates a test environment backed by RAM storage and a local scratch directory.
pub struct TestSandbox {
    node_id: NodeId,
    index_uid: IndexUid,
    source_id: SourceId,
    index_config: IndexConfig,
    doc_mapper: Arc<DocMapper>,
    merge_policy: Arc<dyn MergePolicy>,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    storage: Arc<dyn Storage>,
    split_store: IndexingSplitStore,
    merge_planner_mailbox: Mailbox<MergePlanner>,
    universe: Universe,
    _temp_dir: tempfile::TempDir,
}

const METASTORE_URI: &str = "ram://quickwit-test-indexes";

fn index_uri(index_id: &str) -> Uri {
    Uri::from_str(&format!("{METASTORE_URI}/{index_id}")).unwrap()
}

impl TestSandbox {
    /// Creates a new test environment.
    pub async fn create(
        index_id: &str,
        doc_mapping_yaml: &str,
        indexing_settings_yaml: &str,
        search_fields: &[&str],
    ) -> anyhow::Result<TestSandbox> {
        let node_id = NodeId::from_str(&append_random_suffix("test-node"));
        let index_uri = index_uri(index_id);
        let mut index_config = IndexConfig::for_test(index_id, index_uri.as_str());
        index_config.doc_mapping = ConfigFormat::Yaml.parse(doc_mapping_yaml.as_bytes())?;
        index_config.indexing_settings =
            ConfigFormat::Yaml.parse(indexing_settings_yaml.as_bytes())?;
        index_config.search_settings.default_search_fields = search_fields
            .iter()
            .map(|search_field| search_field.to_string())
            .collect();
        let source_config = SourceConfig::ingest_v2();
        let storage_resolver = StorageResolver::for_test();
        let metastore_resolver =
            MetastoreResolver::configured(storage_resolver.clone(), &MetastoreConfigs::default());
        let metastore = metastore_resolver
            .resolve(&Uri::for_test(METASTORE_URI))
            .await?;
        let create_index_request = CreateIndexRequest::try_from_index_and_source_configs(
            &index_config,
            std::slice::from_ref(&source_config),
        )?;
        let index_uid: IndexUid = metastore
            .create_index(create_index_request)
            .await?
            .index_uid()
            .clone();
        let doc_mapper =
            build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)?;
        let merge_policy = merge_policy_from_settings(&index_config.indexing_settings);
        let temp_dir = tempfile::tempdir()?;
        let storage = storage_resolver.resolve(&index_uri).await?;
        let split_store =
            IndexingSplitStore::new(storage.clone(), Arc::new(IndexingSplitCache::no_caching()));
        let universe = Universe::with_accelerated_time();
        let merge_scheduler_mailbox: Mailbox<MergeSchedulerService> = universe.get_or_spawn_one();
        let merge_scratch_dir = TempDirectory::for_test();
        let merge_pipeline = MergePipeline::new(
            MergePipelineParams {
                pipeline_id: MergePipelineId {
                    node_id: node_id.clone(),
                    index_uid: index_uid.clone(),
                    source_id: INGEST_V2_SOURCE_ID.to_string(),
                },
                doc_mapper: doc_mapper.clone(),
                indexing_directory: merge_scratch_dir,
                metastore: metastore.clone(),
                merge_scheduler_service: merge_scheduler_mailbox,
                split_store: split_store.clone(),
                merge_policy: merge_policy.clone(),
                retention_policy: index_config.retention_policy_opt.clone(),
                max_concurrent_split_uploads: 4,
                merge_io_throughput_limiter_opt: None,
                event_broker: EventBroker::default(),
            },
            Some(Vec::new()),
            universe.spawn_ctx(),
        );
        let merge_planner_mailbox = merge_pipeline.merge_planner_mailbox().clone();
        universe.spawn_builder().spawn(merge_pipeline);
        Ok(TestSandbox {
            node_id,
            index_uid,
            source_id: INGEST_V2_SOURCE_ID.to_string(),
            index_config,
            doc_mapper,
            merge_policy,
            metastore,
            storage_resolver,
            storage,
            split_store,
            merge_planner_mailbox,
            universe,
            _temp_dir: temp_dir,
        })
    }

    /// Indexes, uploads, and publishes `json_docs` directly as new split(s).
    pub async fn add_documents<I>(&self, json_docs: I) -> anyhow::Result<IndexingStatistics>
    where
        I: IntoIterator<Item = JsonValue> + 'static,
        I::IntoIter: Send,
    {
        let mut converter = DocumentConverter::try_new(
            self.index_uid.index_id.clone(),
            self.source_id.clone(),
            self.doc_mapper.clone(),
            None,
            SourceInputFormat::Json,
        )?;
        let mut processed_docs = Vec::new();
        for json_doc in json_docs {
            converter.process_raw_doc(Bytes::from(json_doc.to_string()), &mut processed_docs);
        }

        let delete_opstamp = self
            .metastore
            .last_delete_opstamp(LastDeleteOpstampRequest {
                index_uid: Some(self.index_uid.clone()),
            })
            .await?
            .last_delete_opstamp;

        let pipeline_id = IndexingPipelineId {
            node_id: self.node_id.clone(),
            index_uid: self.index_uid.clone(),
            source_id: self.source_id.clone(),
            pipeline_uid: PipelineUid::for_test(0u128),
        };
        let mut builders: HashMap<u64, IndexedSplitBuilder> = HashMap::new();
        for doc in processed_docs {
            let partition_id = doc.partition;
            let builder = match builders.entry(partition_id) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let scratch_dir = TempDirectory::for_test();
                    entry.insert(IndexedSplitBuilder::from_doc_mapper(
                        pipeline_id.clone(),
                        partition_id,
                        delete_opstamp,
                        &self.doc_mapper,
                        &self.index_config.indexing_settings,
                        scratch_dir,
                        IoControls::default(),
                        None,
                    )?)
                }
            };
            builder.add_document(doc)?;
        }

        let tag_fields = self.doc_mapper.tag_named_fields()?;
        let progress = Progress::default();
        let mut new_splits = Vec::with_capacity(builders.len());
        let mut total_size_splits = 0u64;
        for (_, builder) in builders {
            let indexed_split = builder.finalize()?;
            let packaged_split = create_packaged_split(indexed_split, &tag_fields, &progress)?;
            let (split_metadata, split_payload) = prepare_split_for_upload(
                &packaged_split,
                &self.merge_policy,
                self.index_config.retention_policy_opt.as_ref(),
            )?;
            total_size_splits += split_metadata.footer_offsets.end;
            self.metastore
                .stage_splits(StageSplitsRequest::try_from_split_metadata(
                    self.index_uid.clone(),
                    &split_metadata,
                )?)
                .await?;
            self.split_store
                .store_split(
                    &split_metadata,
                    packaged_split.split_scratch_directory.path(),
                    Box::new(split_payload),
                )
                .await?;
            new_splits.push(split_metadata);
        }

        if !new_splits.is_empty() {
            let staged_split_ids = new_splits
                .iter()
                .map(|split| split.split_id.to_string())
                .collect();
            self.metastore
                .publish_splits(PublishSplitsRequest {
                    index_uid: Some(self.index_uid.clone()),
                    staged_split_ids,
                    replaced_split_ids: Vec::new(),
                    index_checkpoint_delta_json_opt: None,
                    publish_token_opt: None,
                })
                .await?;
            let _ = self
                .merge_planner_mailbox
                .send_message(NewSplits {
                    new_splits: new_splits.clone(),
                })
                .await;
        }

        let counters = converter.counters();
        let num_splits = new_splits.len() as u64;
        Ok(IndexingStatistics {
            num_docs: counters.num_processed_docs(),
            num_invalid_docs: counters.num_invalid_docs(),
            num_local_splits: num_splits,
            num_staged_splits: num_splits,
            num_uploaded_splits: num_splits,
            num_published_splits: num_splits,
            total_bytes_processed: counters
                .num_bytes_total
                .load(std::sync::atomic::Ordering::Relaxed),
            total_size_splits,
            ..Default::default()
        })
    }

    pub fn metastore(&self) -> MetastoreServiceClient {
        self.metastore.clone()
    }

    pub fn storage(&self) -> Arc<dyn Storage> {
        self.storage.clone()
    }

    pub fn storage_resolver(&self) -> StorageResolver {
        self.storage_resolver.clone()
    }

    pub fn doc_mapper(&self) -> Arc<DocMapper> {
        self.doc_mapper.clone()
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    pub fn index_uid(&self) -> IndexUid {
        self.index_uid.clone()
    }

    pub fn source_id(&self) -> SourceId {
        self.source_id.clone()
    }

    pub fn universe(&self) -> &Universe {
        &self.universe
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub async fn assert_quit(self) {
        self.universe.assert_quit().await
    }
}

/// Mock split builder.
pub struct MockSplitBuilder {
    split_metadata: SplitMetadata,
}

impl MockSplitBuilder {
    pub fn new(split_id: &str) -> Self {
        Self {
            split_metadata: mock_split_meta(split_id, &IndexUid::for_test("test-index", 0)),
        }
    }

    pub fn with_index_uid(mut self, index_uid: &IndexUid) -> Self {
        self.split_metadata.index_uid = index_uid.clone();
        self
    }

    pub fn build(self) -> Split {
        Split {
            split_state: SplitState::Published,
            split_metadata: self.split_metadata,
            update_timestamp: 0,
            publish_timestamp: None,
        }
    }
}

/// Mock split helper.
pub fn mock_split(split_id: &str) -> Split {
    MockSplitBuilder::new(split_id).build()
}

/// Mock split meta helper.
pub fn mock_split_meta(split_id: &str, index_uid: &IndexUid) -> SplitMetadata {
    SplitMetadata {
        index_uid: index_uid.clone(),
        split_id: split_id.into(),
        partition_id: 13u64,
        num_docs: if split_id == "split1" { 1_000_000 } else { 10 },
        uncompressed_docs_size_in_bytes: 256,
        time_range: Some(121000..=130198),
        create_timestamp: 0,
        footer_offsets: 700..800,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use quickwit_metastore::{ListSplitsRequestExt, MetastoreServiceStreamSplitsExt};
    use quickwit_proto::metastore::{ListSplitsRequest, MetastoreService};

    use super::TestSandbox;

    #[tokio::test]
    async fn test_test_sandbox() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let doc_mapping_yaml = r#"
            field_mappings:
              - name: title
                type: text
              - name: body
                type: text
              - name: url
                type: text
        "#;
        let test_sandbox =
            TestSandbox::create("test_index", doc_mapping_yaml, "{}", &["body"]).await?;
        let statistics = test_sandbox
            .add_documents(vec![
                serde_json::json!({"title": "Hurricane Fay", "body": "...", "url": "http://hurricane-fay"}),
                serde_json::json!({"title": "Ganimede", "body": "...", "url": "http://ganimede"}),
            ])
            .await?;
        assert_eq!(statistics.num_uploaded_splits, 1);
        let metastore = test_sandbox.metastore();
        {
            let splits = metastore
                .list_splits(
                    ListSplitsRequest::try_from_index_uid(test_sandbox.index_uid()).unwrap(),
                )
                .await?
                .collect_splits()
                .await?;
            assert_eq!(splits.len(), 1);
            test_sandbox
                .add_documents(vec![serde_json::json!({"title": "Byzantine-Ottoman wars", "body": "...", "url": "http://biz-ottoman"})])
                .await?;
        }
        {
            let splits = metastore
                .list_splits(
                    ListSplitsRequest::try_from_index_uid(test_sandbox.index_uid()).unwrap(),
                )
                .await?
                .collect_splits()
                .await?;
            assert_eq!(splits.len(), 2);
        }
        test_sandbox.assert_quit().await;
        Ok(())
    }
}
