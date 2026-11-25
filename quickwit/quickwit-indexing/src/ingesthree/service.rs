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
use bytesize::ByteSize;
use quickwit_common::temp_dir;
use quickwit_config::INGEST_V2_SOURCE_ID;

use super::IndexingService;
use crate::ingesthree::{S3Indexer, S3IndexerSupervisor};

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
    pub(super) fn spawn_s3_indexers(&self) -> anyhow::Result<Vec<S3IndexerSupervisor>> {
        let max_num_bytes = indexer_batch_bytes()?;
        let max_wait_secs = indexer_max_wait_secs()?;
        let num_workers = quickwit_common::num_cpus();
        ensure!(num_workers > 0, "S3 indexing requires at least one CPU");
        let scratch_directory = temp_dir::Builder::default()
            .join("ingesthree")
            .tempdir_in(&self.inner.indexing_root_directory)?;
        let worker = S3Indexer {
            node_id: self.inner.node_id.clone(),
            source_id: INGEST_V2_SOURCE_ID.to_string(),
            metastore: self.inner.metastore.clone(),
            storage_resolver: self.inner.storage_resolver.clone(),
            scratch_directory,
            max_concurrent_split_uploads: self.inner.max_concurrent_split_uploads,
            indexing_io_throughput_limiter: self.inner.indexing_io_throughput_limiter_opt.clone(),
            clustering_config: self.inner.clustering_config.clone(),
        };
        Ok((0..num_workers)
            .map(|_| S3IndexerSupervisor::spawn(worker.clone(), max_num_bytes, max_wait_secs))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use bytes::Bytes;
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
    use crate::ingesthree::S3Indexer;

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

    async fn start_service(directory: &Path, worker: &S3Indexer) -> IndexingService {
        IndexingService::start(
            worker.node_id.clone(),
            directory.to_path_buf(),
            IndexerConfig::for_test().unwrap(),
            worker.metastore.clone(),
            worker.storage_resolver.clone(),
            None,
            None,
        )
        .await
        .unwrap()
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
    async fn file_indexing_publishes_splits_and_stop_is_idempotent() {
        let (mut worker, index_uid) = fixture().await;
        register(&mut worker, &index_uid).await;
        let directory = tempfile::tempdir().unwrap();
        let service = start_service(directory.path(), &worker).await;
        assert!(service.counters().await.num_running_s3_indexers >= 1);
        let splits = wait_for_publications(&mut worker, &index_uid, 1).await;
        assert_eq!(splits[0].num_docs, 1);
        service.stop().await;
        service.stop().await;
        assert_eq!(service.counters().await.num_running_s3_indexers, 0);
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
        let service = start_service(directory.path(), &worker).await;
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        service.stop().await;
        assert_eq!(service.counters().await.num_running_s3_indexers, 0);
    }
}
