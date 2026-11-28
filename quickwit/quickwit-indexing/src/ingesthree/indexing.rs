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
use std::io::Cursor;
use std::sync::Arc;

use anyhow::ensure;
use bytes::Bytes;
use quickwit_common::io::IoControls;
use quickwit_common::metrics::{IN_FLIGHT_INDEX_WRITER, index_label};
use quickwit_common::temp_dir::TempDirectory;
use quickwit_config::{
    DocsClusteringConfig, IndexConfig, IndexingSettings, RetentionPolicy, SourceInputFormat,
    build_doc_mapper,
};
use quickwit_doc_mapper::{DocMapper, QuickwitDocData};
use quickwit_ingest::ingesthree::DocumentFileReader;
use quickwit_metastore::SplitMetadata;
use quickwit_metrics::{Counter, Gauge, GaugeGuard, counter};
use quickwit_proto::indexing::IndexingPipelineId;
use quickwit_storage::SplitPayload;

use super::downloads::DownloadedFile;
use super::metrics::MEMORY_FLUSHES;
use crate::merge_policy::MergePolicy;
use crate::models::{IndexedSplit, IndexedSplitBuilder, ProcessedDoc};
use crate::{
    DocumentConverter, OTHER_PARTITION_ID, create_packaged_split, prepare_split_for_upload,
};

/// Owns the scratch directory so payload files survive until upload and retries finish.
pub(super) struct PreparedSplit {
    pub scratch_directory: TempDirectory,
    pub metadata: SplitMetadata,
    pub payload: SplitPayload,
}

/// A split writer whose memory is counted until serialization finishes.
/// Field order releases the builder before its memory guard on error or cancellation.
pub(super) struct SplitWriter {
    pub builder: IndexedSplitBuilder,
    memory_usage: GaugeGuard,
}

impl SplitWriter {
    fn new(builder: IndexedSplitBuilder, memory_gauge: &Gauge) -> Self {
        let memory_usage = GaugeGuard::new(memory_gauge, builder.mem_usage() as f64);
        Self {
            builder,
            memory_usage,
        }
    }

    fn add_document(
        &mut self,
        document: ProcessedDoc,
        doc_data: &QuickwitDocData,
    ) -> anyhow::Result<()> {
        let result = self.builder.add_document(document, doc_data);
        self.memory_usage
            .increment(self.builder.mem_usage() as f64 - self.memory_usage.delta());
        result
    }

    pub fn finalize(self, doc_data: Arc<QuickwitDocData>) -> anyhow::Result<IndexedSplit> {
        let Self {
            builder,
            memory_usage,
        } = self;
        let result = builder.finalize(doc_data);
        drop(memory_usage);
        result
    }
}

/// Converts documents and accumulates splits until their memory target is reached.
pub(super) struct SplitBuilders {
    converter: DocumentConverter,
    mapper: Arc<DocMapper>,
    pipeline_id: IndexingPipelineId,
    settings: IndexingSettings,
    delete_opstamp: u64,
    directory: TempDirectory,
    pub io_controls: IoControls,
    clustering_config: Option<DocsClusteringConfig>,
    splits: HashMap<u64, SplitWriter>,
    overflow: Option<SplitWriter>,
    doc_data_memory: Option<GaugeGuard>,
    writer_memory: Gauge,
    memory_flushes: Counter,
}

impl SplitBuilders {
    pub fn new(
        config: IndexConfig,
        pipeline_id: IndexingPipelineId,
        delete_opstamp: u64,
        directory: TempDirectory,
        io_controls: IoControls,
        clustering_config: Option<DocsClusteringConfig>,
    ) -> anyhow::Result<Self> {
        let mapper = build_doc_mapper(&config.doc_mapping, &config.search_settings)?;
        // Ingesthree files contain JSON documents. Source decoding/transforms belong before intake.
        let converter = DocumentConverter::try_new(
            pipeline_id.index_uid.index_id.clone(),
            pipeline_id.source_id.clone(),
            mapper.clone(),
            None,
            SourceInputFormat::Json,
        )?;
        Ok(Self {
            converter,
            mapper,
            memory_flushes: counter!(
                parent: MEMORY_FLUSHES,
                "index" => index_label(&pipeline_id.index_uid.index_id).to_string(),
                "source" => pipeline_id.source_id.clone(),
            ),
            pipeline_id,
            settings: config.indexing_settings,
            delete_opstamp,
            directory,
            io_controls,
            clustering_config,
            splits: HashMap::new(),
            overflow: None,
            doc_data_memory: None,
            writer_memory: IN_FLIGHT_INDEX_WRITER.clone(),
        })
    }

    fn create_split(&self, partition: u64) -> anyhow::Result<SplitWriter> {
        let builder = IndexedSplitBuilder::from_doc_mapper(
            self.pipeline_id.clone(),
            partition,
            self.delete_opstamp,
            &self.mapper,
            &self.settings,
            self.directory.clone(),
            self.io_controls.clone().set_component("indexer"),
            self.clustering_config.as_ref(),
        )?;
        Ok(SplitWriter::new(builder, &self.writer_memory))
    }

    fn sync_doc_data_memory(&mut self) {
        let bytes = self.converter.doc_data().mem_usage() as f64;
        match &mut self.doc_data_memory {
            Some(guard) => guard.increment(bytes - guard.delta()),
            None => {
                self.doc_data_memory = Some(GaugeGuard::new(&self.writer_memory, bytes));
            }
        }
    }

    fn add_processed_docs(&mut self, processed_docs: Vec<ProcessedDoc>) -> anyhow::Result<()> {
        self.sync_doc_data_memory();
        let doc_data = self.converter.doc_data();
        let max_partitions = self.mapper.max_num_partitions().get() as usize;
        for document in processed_docs {
            self.io_controls.check_if_alive()?;
            let partition = document.partition;
            if let Some(split) = self.splits.get_mut(&partition) {
                split.add_document(document, doc_data)?;
            } else if self.splits.len() < max_partitions {
                let mut split = self.create_split(partition)?;
                split.add_document(document, doc_data)?;
                self.splits.insert(partition, split);
            } else {
                let overflow = match &mut self.overflow {
                    Some(split) => split,
                    None => self.overflow.insert(self.create_split(OTHER_PARTITION_ID)?),
                };
                overflow.add_document(document, doc_data)?;
            }
        }
        Ok(())
    }

    pub fn add_document(&mut self, document: Bytes) -> anyhow::Result<()> {
        self.io_controls.check_if_alive()?;
        let processed_docs = self.converter.process_doc(document);
        self.add_processed_docs(processed_docs)
    }

    #[cfg(test)]
    pub fn add_documents(&mut self, documents: Vec<Bytes>) -> anyhow::Result<()> {
        self.io_controls.check_if_alive()?;
        let processed_docs = self.converter.process_docs(documents);
        self.add_processed_docs(processed_docs)
    }

    /// Processes the assignment on the blocking runtime, yielding only while waiting for input.
    /// Owns downloaded input buffers and releases each one after processing it.
    /// Flushes serialize to scratch here. Uploads happen after the entire assignment is consumed.
    pub async fn index_files(
        &mut self,
        mut files: tokio::sync::mpsc::UnboundedReceiver<anyhow::Result<DownloadedFile>>,
        merge_policy: &Arc<dyn MergePolicy>,
        retention: Option<&RetentionPolicy>,
    ) -> anyhow::Result<Vec<PreparedSplit>> {
        let mut prepared = Vec::new();
        while let Some(file) = files.recv().await {
            self.io_controls.check_if_alive()?;
            let contents = file?.into_contents();
            let num_bytes = contents.len() as u64;
            let mut reader = DocumentFileReader::new(
                Cursor::new(contents),
                &self.pipeline_id.index_uid,
                num_bytes,
            )?;
            while let Some(document) = reader.read_document()? {
                self.io_controls.check_if_alive()?;
                self.add_document(document)?;
                if self.should_flush() {
                    self.memory_flushes.inc();
                    self.prepare_splits(merge_policy, retention, &mut prepared)?;
                }
            }
        }
        self.prepare_splits(merge_policy, retention, &mut prepared)?;
        Ok(prepared)
    }

    fn prepare_splits(
        &mut self,
        merge_policy: &Arc<dyn MergePolicy>,
        retention: Option<&RetentionPolicy>,
        prepared: &mut Vec<PreparedSplit>,
    ) -> anyhow::Result<()> {
        let (doc_data, _doc_data_memory, splits) = self.take_splits();
        ensure!(
            prepared.len() + splits.len() <= 1000,
            "assigned group exceeds the publication split limit"
        );
        let tags = self.mapper.tag_named_fields()?;
        for split in splits {
            let io_controls = self.io_controls.clone().set_component("index_serializer");
            io_controls.check_if_alive()?;
            split
                .builder
                .controlled_directory
                .set_io_controls(io_controls.clone());
            let packaged = create_packaged_split(split.finalize(doc_data.clone())?, &tags)?;
            io_controls.check_if_alive()?;
            let (metadata, payload) = prepare_split_for_upload(&packaged, merge_policy, retention)?;
            prepared.push(PreparedSplit {
                scratch_directory: packaged.split_scratch_directory,
                metadata,
                payload,
            });
        }
        Ok(())
    }

    pub fn should_flush(&self) -> bool {
        if self.splits.is_empty() && self.overflow.is_none() {
            return false;
        }
        let mut num_bytes = self.converter.doc_data().mem_usage() as u64;
        for split in self.splits.values().chain(self.overflow.iter()) {
            num_bytes += split.builder.mem_usage() as u64;
        }
        num_bytes >= self.settings.resources.heap_size.as_u64()
    }

    pub fn take_splits(&mut self) -> (Arc<QuickwitDocData>, Option<GaugeGuard>, Vec<SplitWriter>) {
        let doc_data = self.converter.take_frozen_doc_data();
        let mut doc_data_memory = self.doc_data_memory.take();
        if let Some(guard) = &mut doc_data_memory {
            guard.increment(doc_data.mem_usage() as f64 - guard.delta());
        }
        let splits = self
            .splits
            .drain()
            .map(|(_, split)| split)
            .chain(self.overflow.take())
            .collect();
        (doc_data, doc_data_memory, splits)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::time::Duration;

    use quickwit_config::DocsClusteringConfig;
    use quickwit_proto::types::{IndexUid, NodeId};
    use quickwit_storage::PutPayload;
    use tantivy::schema::Value;
    use tantivy::{DocAddress, TantivyDocument};

    use super::*;
    use crate::ingesthree::run_indexing;

    #[test]
    fn test_partition_overflow_clustering_and_memory_flush() {
        let mut config = IndexConfig::for_test("partitions", "ram:///splits");
        config.doc_mapping.partition_key = Some("owner".into());
        config.doc_mapping.max_num_partitions = NonZeroU32::new(1).unwrap();
        config.indexing_settings.resources.heap_size = bytesize::ByteSize(1);
        let clustering: DocsClusteringConfig = serde_json::from_value(serde_json::json!([
            {"kind": "raw", "path": "owner"}
        ]))
        .unwrap();
        let mut builders = SplitBuilders::new(
            config,
            IndexingPipelineId {
                node_id: NodeId::from_str("test-node"),
                index_uid: IndexUid::for_test("partitions", 1),
                source_id: "ingesthree".into(),
            },
            7,
            TempDirectory::for_test(),
            IoControls::default(),
            Some(clustering),
        )
        .unwrap();
        let writer_memory = Gauge::local();
        builders.writer_memory = writer_memory.clone();
        // Document IDs must continue across batches within each split.
        for owners in [["alice", "bob"], ["charlie", "alice"]] {
            let docs = owners
                .into_iter()
                .map(|owner| {
                    Bytes::from(
                        serde_json::json!({
                            "timestamp": "2024-01-01T00:00:00Z", "owner": owner,
                        })
                        .to_string(),
                    )
                })
                .collect();
            builders.add_documents(docs).unwrap();
        }
        assert!(builders.should_flush());
        assert_eq!(builders.splits.len(), 1);
        assert_eq!(
            builders
                .overflow
                .as_ref()
                .unwrap()
                .builder
                .split_attrs
                .partition_id,
            OTHER_PARTITION_ID
        );
        assert_eq!(
            builders
                .overflow
                .as_ref()
                .unwrap()
                .builder
                .split_attrs
                .num_docs,
            2
        );
        let split_bytes_total: usize = builders
            .splits
            .values()
            .chain(builders.overflow.iter())
            .map(|split| split.builder.mem_usage())
            .sum();
        let expected_bytes = split_bytes_total + builders.converter.doc_data().mem_usage();
        assert!(expected_bytes > 0);
        assert_eq!(writer_memory.get(), expected_bytes as f64);
        let (doc_data, doc_data_memory, splits) = builders.take_splits();
        assert!(!builders.should_flush());
        assert_eq!(
            writer_memory.get(),
            (split_bytes_total + doc_data.mem_usage()) as f64
        );
        for split in splits {
            assert_eq!(split.builder.split_attrs.num_docs, 2);
            let is_overflow = split.builder.split_attrs.partition_id == OTHER_PARTITION_ID;
            let expected_groups = if is_overflow { vec![1, 1] } else { vec![2] };
            assert_eq!(
                split
                    .builder
                    .doc_clusterer_opt
                    .as_ref()
                    .map(|clusterer| clusterer.group_sizes()),
                Some(expected_groups)
            );
            let memory_before = writer_memory.get();
            let split_bytes = split.memory_usage.delta();
            let split = split.finalize(doc_data.clone()).unwrap();
            assert_eq!(writer_memory.get(), memory_before - split_bytes);
            let reader = split.index.reader().unwrap();
            let searcher = reader.searcher();
            assert_eq!(searcher.num_docs(), 2);
            let owner_field = split.index.schema().get_field("owner").unwrap();
            let owners: Vec<String> = (0..2)
                .map(|doc_id| {
                    let doc: TantivyDocument = searcher.doc(DocAddress::new(0, doc_id)).unwrap();
                    doc.get_first(owner_field)
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_string()
                })
                .collect();
            let expected_owners = if is_overflow {
                ["bob", "charlie"]
            } else {
                ["alice", "alice"]
            };
            assert_eq!(owners, expected_owners);
        }
        drop(doc_data_memory);
        drop(doc_data);
        assert_eq!(writer_memory.get(), 0.0);
    }

    fn builders_for_test(directory: TempDirectory, writer_memory: Gauge) -> SplitBuilders {
        let config = IndexConfig::for_test("writer-memory", "ram:///splits");
        let mut builders = SplitBuilders::new(
            config,
            IndexingPipelineId {
                node_id: NodeId::from_str("test-node"),
                index_uid: IndexUid::for_test("writer-memory", 1),
                source_id: "ingesthree".into(),
            },
            0,
            directory,
            IoControls::default(),
            None,
        )
        .unwrap();
        builders.writer_memory = writer_memory;
        builders
            .add_documents(vec![Bytes::from_static(
                br#"{"timestamp":"2024-01-01T00:00:00Z","body":"hello"}"#,
            )])
            .unwrap();
        builders
    }

    #[tokio::test]
    async fn test_prepared_split_keeps_payload_files_alive() {
        let mut builders = builders_for_test(TempDirectory::for_test(), Gauge::local());
        let merge_policy = crate::merge_policy::merge_policy_from_settings(&builders.settings);
        let mut prepared = Vec::new();
        builders
            .prepare_splits(&merge_policy, None, &mut prepared)
            .unwrap();
        drop(builders);
        let prepared = prepared.pop().unwrap();
        let path = prepared.scratch_directory.path().to_path_buf();
        assert!(path.try_exists().unwrap());
        let contents = prepared.payload.read_all().await.unwrap();
        // Upload retries clone the payload and reopen its files.
        let retry_payload = prepared.payload.clone();
        let retry_contents = retry_payload.read_all().await.unwrap();
        assert_eq!(contents.as_slice(), retry_contents.as_slice());
        drop(prepared);
        assert!(!path.try_exists().unwrap());
    }

    #[tokio::test]
    async fn test_indexing_yields_for_input_and_preserves_download_error() {
        let writer_memory = Gauge::local();
        let mut builders = builders_for_test(TempDirectory::for_test(), writer_memory.clone());
        let merge_policy = crate::merge_policy::merge_policy_from_settings(&builders.settings);
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        {
            let indexing = builders.index_files(receiver, &merge_policy, None);
            tokio::pin!(indexing);
            assert!(futures::poll!(&mut indexing).is_pending());
            // Existing writer data must not become successful output when a later GET fails.
            assert!(writer_memory.get() > 0.0);
            assert!(
                sender
                    .send(Err(anyhow::anyhow!("injected download error")))
                    .is_ok()
            );
            assert!(
                matches!(indexing.await, Err(error) if error.to_string() == "injected download error")
            );
        }
        drop(builders);
        assert_eq!(writer_memory.get(), 0.0);
        assert!(sender.is_closed());
    }

    #[test]
    fn test_writer_memory_released_on_serialization_error() {
        let writer_memory = Gauge::local();
        let mut builders = builders_for_test(TempDirectory::for_test(), writer_memory.clone());
        let (doc_data, doc_data_memory, mut splits) = builders.take_splits();
        let writer = splits.pop().unwrap();
        assert_eq!(
            writer_memory.get(),
            (writer.builder.mem_usage() + doc_data.mem_usage()) as f64
        );
        assert!(writer_memory.get() > 0.0);
        builders.io_controls.kill();
        assert!(writer.finalize(doc_data).is_err());
        drop(doc_data_memory);
        assert_eq!(writer_memory.get(), 0.0);
    }

    #[tokio::test]
    async fn test_writer_memory_and_assignment_released_on_blocking_error_or_panic() {
        for panic in [false, true] {
            let writer_memory = Gauge::local();
            let active_assignments = Gauge::local();
            let directory = TempDirectory::for_test();
            let builders = builders_for_test(directory.clone(), writer_memory.clone());
            assert!(writer_memory.get() > 0.0);
            let assignment = Arc::new(GaugeGuard::new(&active_assignments, 1.0));
            let result: anyhow::Result<()> = run_indexing(directory, assignment, async move {
                let _builders = builders;
                if panic {
                    panic!("injected blocking panic");
                }
                anyhow::bail!("injected blocking error");
            })
            .await;
            assert!(result.is_err());
            assert_eq!(writer_memory.get(), 0.0);
            assert_eq!(active_assignments.get(), 0.0);
        }
    }

    #[tokio::test]
    async fn test_cancelled_indexing_keeps_scratch_memory_and_assignment_until_work_exits() {
        let directory = TempDirectory::for_test();
        let path = directory.path().to_path_buf();
        let (started, wait_started) = tokio::sync::oneshot::channel();
        let (release, wait_release) = std::sync::mpsc::channel();
        let (finished, wait_finished) = tokio::sync::oneshot::channel();
        let work_path = path.clone();
        let writer_memory = Gauge::local();
        let mut builders = builders_for_test(directory.clone(), writer_memory.clone());
        let (doc_data, doc_data_memory, mut splits) = builders.take_splits();
        let writer = splits.pop().unwrap();
        drop(builders);
        let expected_bytes = (writer.builder.mem_usage() + doc_data.mem_usage()) as f64;
        assert!(expected_bytes > 0.0);
        let active_assignments = Gauge::local();
        let assignment = Arc::new(GaugeGuard::new(&active_assignments, 1.0));
        let task = tokio::spawn(run_indexing(directory, assignment, async move {
            started.send(()).unwrap();
            wait_release.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(work_path.try_exists().unwrap());
            drop(writer.finalize(doc_data).unwrap());
            drop(doc_data_memory);
            finished.send(()).unwrap();
            Ok(())
        }));
        wait_started.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(active_assignments.get(), 1.0);
        assert_eq!(writer_memory.get(), expected_bytes);
        assert!(
            path.try_exists().unwrap(),
            "scratch deleted while synchronous work was running"
        );
        release.send(()).unwrap();
        wait_finished.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while path.try_exists().unwrap() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(active_assignments.get(), 0.0);
        assert_eq!(writer_memory.get(), 0.0);
    }
}
