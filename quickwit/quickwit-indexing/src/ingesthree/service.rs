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

use anyhow::{Context, ensure};
use async_trait::async_trait;
use bytesize::ByteSize;
use quickwit_actors::{ActorContext, ActorExitStatus, Handler};
use quickwit_common::temp_dir;
use quickwit_config::INGEST_V2_SOURCE_ID;

use super::IndexingService;
use crate::ingesthree::{S3Indexer, S3IndexerSupervisor, StopS3Indexers};

/// Reads the target total input bytes for one indexer batch.
fn indexer_batch_bytes() -> anyhow::Result<u64> {
    const ENV_KEY: &str = "QW_INGESTHREE_INDEXER_BATCH_SIZE";
    let num_bytes = match std::env::var(ENV_KEY) {
        Ok(value) => value
            .parse::<ByteSize>()
            .map_err(|error| anyhow::anyhow!("{ENV_KEY}: {error}"))?
            .as_u64(),
        Err(std::env::VarError::NotPresent) => 100_000_000,
        Err(error) => return Err(error).with_context(|| format!("failed to read {ENV_KEY}")),
    };
    ensure!(
        num_bytes > 0 && num_bytes <= i64::MAX as u64,
        "{ENV_KEY} must be between 1 and {} bytes",
        i64::MAX,
    );
    Ok(num_bytes)
}

/// Reads how long an underfilled indexer batch waits, in seconds since registration.
fn indexer_max_wait_secs() -> anyhow::Result<u32> {
    const ENV_KEY: &str = "QW_INGESTHREE_INDEXER_MAX_WAIT_SECS";
    match std::env::var(ENV_KEY) {
        Ok(value) => value.parse::<u32>().with_context(|| {
            format!(
                "{ENV_KEY} must be an integer between 0 and {} seconds",
                u32::MAX
            )
        }),
        Err(std::env::VarError::NotPresent) => Ok(15),
        Err(error) => Err(error).with_context(|| format!("failed to read {ENV_KEY}")),
    }
}

impl IndexingService {
    pub(crate) fn enable_ingesthree(&mut self) -> anyhow::Result<()> {
        self.ingesthree_enabled = true;
        Ok(())
    }

    pub(super) fn start_s3_indexers(&mut self) -> anyhow::Result<()> {
        if !self.ingesthree_enabled {
            return Ok(());
        }
        let max_num_bytes = indexer_batch_bytes()?;
        let max_wait_secs = indexer_max_wait_secs()?;
        let num_workers = quickwit_common::num_cpus();
        ensure!(num_workers > 0, "S3 indexing requires at least one CPU");
        let scratch_directory = temp_dir::Builder::default()
            .join("ingesthree")
            .tempdir_in(&self.indexing_root_directory)?;
        let worker = S3Indexer {
            node_id: self.node_id.clone(),
            source_id: INGEST_V2_SOURCE_ID.to_string(),
            metastore: self.metastore.clone(),
            storage_resolver: self.storage_resolver.clone(),
            scratch_directory,
            split_cache: self.split_cache.clone(),
            max_concurrent_split_uploads: self.max_concurrent_split_uploads,
            indexing_io_throughput_limiter: self.indexing_io_throughput_limiter_opt.clone(),
            clustering_config: self.clustering_config.clone(),
        };
        self.s3_indexers = (0..num_workers)
            .map(|_| S3IndexerSupervisor::spawn(worker.clone(), max_num_bytes, max_wait_secs))
            .collect();
        Ok(())
    }

    pub(super) async fn stop_s3_indexers(&mut self) {
        self.ingesthree_enabled = false;
        futures::future::join_all(self.s3_indexers.drain(..).map(S3IndexerSupervisor::stop)).await;
    }
}

#[async_trait]
impl Handler<StopS3Indexers> for IndexingService {
    type Reply = ();

    async fn handle(
        &mut self,
        _: StopS3Indexers,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        ctx.protect_future(async {
            self.stop_source_forwarders().await;
            self.stop_s3_indexers().await;
        })
        .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use bytes::Bytes;
    use quickwit_actors::{ActorHandle, Mailbox, Universe};
    use quickwit_common::temp_dir::TempDirectory;
    use quickwit_common::uri::Uri;
    use quickwit_config::merge_policy_config::{MergePolicyConfig, StableLogMergePolicyConfig};
    use quickwit_config::{INGEST_V2_SOURCE_ID, IndexConfig};
    use quickwit_ingest::ingesthree::Ingester;
    use quickwit_metastore::{
        CreateIndexRequestExt, FileBackedMetastore, ListSplitsQuery, ListSplitsRequestExt,
        MetastoreServiceStreamSplitsExt, SplitMetadata, SplitState,
    };
    use quickwit_proto::metastore::{
        AssignFilesForIndexing, CreateIndexRequest, ListSplitsRequest, MetastoreService,
        MockMetastoreService, RegisterFileForIndexing,
    };
    use quickwit_proto::types::IndexUid;
    use quickwit_storage::RamStorage;

    use super::super::*;
    use crate::ingesthree::{S3Indexer, StopS3Indexers};

    const DOC: &[u8] = br#"{"timestamp":"2024-01-01T00:00:00Z","body":"hello","owner":"alice"}"#;

    async fn fixture() -> (S3Indexer, IndexUid) {
        let mut config = IndexConfig::for_test("file-service", "ram:///splits");
        config.indexing_settings.resources.heap_size = ByteSize::mb(100);
        config.indexing_settings.merge_policy =
            MergePolicyConfig::StableLog(StableLogMergePolicyConfig {
                merge_factor: 2,
                max_merge_factor: 2,
                ..Default::default()
            });
        let metastore = MetastoreServiceClient::new(FileBackedMetastore::for_test(Arc::new(
            RamStorage::default(),
        )));
        let index_uid = metastore
            .create_index(CreateIndexRequest::try_from_index_config(&config).unwrap())
            .await
            .unwrap()
            .index_uid()
            .clone();
        let worker = S3Indexer {
            node_id: NodeId::from_str("test-node"),
            source_id: INGEST_V2_SOURCE_ID.to_string(),
            metastore,
            storage_resolver: StorageResolver::for_test(),
            scratch_directory: TempDirectory::for_test(),
            split_cache: Arc::new(IndexingSplitCache::no_caching()),
            max_concurrent_split_uploads: 2,
            indexing_io_throughput_limiter: None,
            clustering_config: None,
        };
        (worker, index_uid)
    }

    async fn register(worker: &mut S3Indexer, index_uid: &IndexUid) {
        let directory = TempDirectory::for_test();
        let mut ingester = Ingester::open(directory.path(), 1_000_000).unwrap();
        ingester
            .append(index_uid, &[Bytes::from_static(DOC)])
            .unwrap();
        let file = ingester.finish().unwrap().pop().unwrap();
        let storage = worker
            .storage_resolver
            .resolve(&Uri::for_test("ram:///inputs"))
            .await
            .unwrap();
        storage
            .put(
                Path::new(&file.file_id.to_string()),
                Box::new(std::fs::read(&file.path).unwrap()),
            )
            .await
            .unwrap();
        worker
            .metastore
            .register_file_for_indexing(RegisterFileForIndexing {
                index_uid: Some(index_uid.clone()),
                file_id: file.file_id.to_string(),
                file_uri: storage
                    .uri()
                    .join(file.file_id.to_string())
                    .unwrap()
                    .to_string(),
                num_bytes: file.num_bytes,
            })
            .await
            .unwrap();
    }

    async fn new_service(directory: &Path, worker: &S3Indexer) -> IndexingService {
        IndexingService::new(
            worker.node_id.clone(),
            directory.to_path_buf(),
            IndexerConfig::for_test().unwrap(),
            worker.metastore.clone(),
            worker.storage_resolver.clone(),
            worker.split_cache.clone(),
            None,
            None,
        )
        .await
        .unwrap()
    }

    async fn spawn_service(
        directory: &Path,
        universe: &Universe,
        worker: &S3Indexer,
        enabled: bool,
    ) -> (Mailbox<IndexingService>, ActorHandle<IndexingService>) {
        let mut service = new_service(directory, worker).await;
        if enabled {
            service.enable_ingesthree().unwrap();
        }
        universe.spawn_builder().spawn(service)
    }

    #[derive(Debug)]
    struct InspectS3Indexers;

    #[async_trait]
    impl Handler<InspectS3Indexers> for IndexingService {
        type Reply = usize;

        async fn handle(
            &mut self,
            _: InspectS3Indexers,
            _: &ActorContext<Self>,
        ) -> Result<Self::Reply, ActorExitStatus> {
            Ok(self.s3_indexers.len())
        }
    }

    async fn published_splits(worker: &mut S3Indexer, index_uid: &IndexUid) -> Vec<SplitMetadata> {
        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
        worker
            .metastore
            .list_splits(ListSplitsRequest::try_from_list_splits_query(&query).unwrap())
            .await
            .unwrap()
            .collect_splits_metadata()
            .await
            .unwrap()
    }

    async fn wait_for_publications(
        worker: &mut S3Indexer,
        index_uid: &IndexUid,
        num_splits: usize,
    ) -> Vec<SplitMetadata> {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let splits = published_splits(worker, index_uid).await;
                if splits.len() == num_splits {
                    return splits;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn file_indexing_is_opt_in_and_stop_is_idempotent() {
        let (mut worker, index_uid) = fixture().await;
        register(&mut worker, &index_uid).await;
        let directory = tempfile::tempdir().unwrap();
        let universe = Universe::new();
        let (service, _) = spawn_service(directory.path(), &universe, &worker, false).await;
        assert_eq!(service.ask(InspectS3Indexers).await.unwrap(), 0);
        assert!(published_splits(&mut worker, &index_uid).await.is_empty());
        service.ask(StopS3Indexers).await.unwrap();
        service.ask(StopS3Indexers).await.unwrap();
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn file_indexing_publishes_splits_and_stops() {
        let (mut worker, index_uid) = fixture().await;
        register(&mut worker, &index_uid).await;
        let directory = tempfile::tempdir().unwrap();
        let universe = Universe::new();
        let (service, _) = spawn_service(directory.path(), &universe, &worker, true).await;
        assert!(service.ask(InspectS3Indexers).await.unwrap() >= 1);
        let splits = wait_for_publications(&mut worker, &index_uid, 1).await;
        assert_eq!(splits[0].num_docs, 1);
        service.ask(StopS3Indexers).await.unwrap();
        assert_eq!(service.ask(InspectS3Indexers).await.unwrap(), 0);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn stop_s3_indexers_blocks_until_worker_exits() {
        let (mut worker, _) = fixture().await;
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let entered_tx = std::sync::Mutex::new(Some(entered_tx));
        let mut mock = MockMetastoreService::new();
        mock.expect_assign_files_for_indexing()
            .returning(move |_req: AssignFilesForIndexing| {
                if let Some(tx) = entered_tx.lock().unwrap().take() {
                    let _ = tx.send(());
                }
                Ok(Default::default())
            });
        worker.metastore = MetastoreServiceClient::from_mock(mock);
        let directory = tempfile::tempdir().unwrap();
        let universe = Universe::new();
        let (service, _) = spawn_service(directory.path(), &universe, &worker, true).await;
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        service.ask(StopS3Indexers).await.unwrap();
        assert_eq!(service.ask(InspectS3Indexers).await.unwrap(), 0);
        universe.assert_quit().await;
    }
}
