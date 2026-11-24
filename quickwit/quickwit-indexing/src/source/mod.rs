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

//! Thin source-to-ingest forwarders.
//!
//! Supported pull sources (Kafka, SQS, GCP Pub/Sub) rely on native broker coordination
//! (consumer groups, competing consumers / visibility timeouts, subscriptions) and forward
//! document batches directly to the ingest API (`IngestRouterService`), committing offsets or
//! acknowledging messages only after `ingesthree` accepts each batch.

pub mod doc_file_reader;
#[cfg(feature = "kafka")]
mod kafka;
#[cfg(feature = "gcp-pubsub")]
mod pubsub;
#[cfg(feature = "sqs")]
mod sqs;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use async_trait::async_trait;
use bytes::Bytes;
use quickwit_common::rate_limited_warn;
use quickwit_config::{
    FileSourceNotification, FileSourceParams, INGEST_V2_SOURCE_ID, SourceConfig, SourceParams,
};
use quickwit_ingest::IngestServiceError;
use quickwit_ingest::router_client::{IngestFailure, IngestRouterClient, IngestRouterError};
use quickwit_metastore::ListIndexesMetadataResponseExt;
use quickwit_proto::ingest::router::{IngestRequestV2, IngestSubrequest};
use quickwit_proto::ingest::{CommitTypeV2, DocBatchV2, IngestV2Error};
use quickwit_proto::metastore::{
    ListIndexesMetadataRequest, MetastoreService, MetastoreServiceClient,
};
use quickwit_proto::types::{DocUidGenerator, IndexUid, SourceId};
use quickwit_storage::StorageResolver;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{info, warn};

/// Maximum payload bytes per forwarded batch.
pub const BATCH_NUM_BYTES_LIMIT: usize = 4 * 1024 * 1024;
/// Maximum wait before flushing a partial batch.
pub const BATCH_MAX_WAIT: Duration = Duration::from_millis(500);

const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
const INITIAL_RETRY_BACKOFF: Duration = Duration::from_millis(200);
const MAX_RETRY_BACKOFF: Duration = Duration::from_secs(5);
const SUPERVISOR_RESTART_BACKOFF: Duration = Duration::from_secs(2);

/// Target sink receiving batches forwarded by pull sources.
#[async_trait]
pub trait IngestSink: Send + Sync + 'static {
    async fn ingest(&self, index_id: &str, docs: Vec<Bytes>) -> Result<(), IngestV2Error>;

    /// Retries transient ingest errors until accepted or `shutdown` fires.
    async fn ingest_with_retry(
        &self,
        index_id: &str,
        docs: Vec<Bytes>,
        shutdown: &mut watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let mut backoff = INITIAL_RETRY_BACKOFF;
        loop {
            match self.ingest(index_id, docs.clone()).await {
                Ok(()) => return Ok(()),
                Err(error)
                    if matches!(
                        error,
                        IngestV2Error::Unavailable(_)
                            | IngestV2Error::TooManyRequests(_)
                            | IngestV2Error::Timeout(_)
                    ) =>
                {
                    rate_limited_warn!(
                        limit_per_min = 6,
                        index_id = %index_id,
                        error = %error,
                        "transient ingest error while forwarding source batch; retrying"
                    );
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                bail!("source forwarder shutting down while retrying ingest");
                            }
                        }
                        _ = time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(MAX_RETRY_BACKOFF);
                }
                Err(error) => {
                    return Err(anyhow::Error::from(error))
                        .with_context(|| format!("ingest rejected batch for index `{index_id}`"));
                }
            }
        }
    }
}

/// Forwards batches directly to [`IngestRouterClient`] (`ingesthree`).
#[derive(Clone)]
pub struct IngestRouterSink {
    router: IngestRouterClient,
}

impl IngestRouterSink {
    pub fn new(router: IngestRouterClient) -> Self {
        Self { router }
    }
}

fn router_error_to_ingest_error(error: IngestRouterError) -> IngestV2Error {
    match error {
        IngestRouterError::V2(err) => err,
        IngestRouterError::Ingesthree(err) => ingesthree_error_to_ingest_error(err),
    }
}

fn ingesthree_error_to_ingest_error(error: IngestServiceError) -> IngestV2Error {
    match error {
        IngestServiceError::Unavailable(msg) => IngestV2Error::Unavailable(msg),
        IngestServiceError::RateLimited(cause) => IngestV2Error::TooManyRequests(cause),
        other => IngestV2Error::Internal(other.to_string()),
    }
}

#[async_trait]
impl IngestSink for IngestRouterSink {
    async fn ingest(&self, index_id: &str, docs: Vec<Bytes>) -> Result<(), IngestV2Error> {
        let mut doc_uid_generator = DocUidGenerator::default();
        let mut doc_uids = Vec::with_capacity(docs.len());
        let mut doc_lengths = Vec::with_capacity(docs.len());
        let total_len: usize = docs.iter().map(Bytes::len).sum();
        let mut doc_buffer = Vec::with_capacity(total_len);
        for doc in docs {
            doc_uids.push(doc_uid_generator.next_doc_uid());
            doc_lengths.push(doc.len() as u32);
            doc_buffer.extend_from_slice(&doc);
        }
        let doc_batch = DocBatchV2 {
            doc_uids,
            doc_buffer: Bytes::from(doc_buffer),
            doc_lengths,
        };
        let request = IngestRequestV2 {
            commit_type: CommitTypeV2::Auto as i32,
            subrequests: vec![IngestSubrequest {
                subrequest_id: 0,
                index_id: index_id.to_string(),
                source_id: INGEST_V2_SOURCE_ID.to_string(),
                doc_batch: Some(doc_batch),
            }],
        };
        let response = self
            .router
            .ingest(request)
            .await
            .map_err(router_error_to_ingest_error)?;
        if let Some(failure) = response.failures.into_iter().next() {
            return Err(match failure {
                IngestFailure::Ingesthree { error, .. } => ingesthree_error_to_ingest_error(error),
                IngestFailure::V2(failure) => IngestV2Error::Internal(format!(
                    "ingest subrequest failed for index `{}`: {:?}",
                    failure.index_id,
                    failure.reason()
                )),
            });
        }
        Ok(())
    }
}

/// Validates connectivity for a `SourceConfig`.
pub async fn check_source_connectivity(
    storage_resolver: &StorageResolver,
    source_config: &SourceConfig,
) -> anyhow::Result<()> {
    match &source_config.source_params {
        SourceParams::File(FileSourceParams::Filepath(uri)) => {
            let (dir_uri, file_name) = doc_file_reader::dir_and_filename(uri)?;
            let storage = storage_resolver.resolve(&dir_uri).await?;
            if !storage.exists(file_name).await? {
                bail!("file `{uri}` does not exist");
            }
            Ok(())
        }
        SourceParams::File(FileSourceParams::Notifications(FileSourceNotification::Sqs(
            sqs_params,
        ))) => {
            #[cfg(feature = "sqs")]
            {
                sqs::check_sqs_connectivity(&sqs_params.queue_url).await
            }
            #[cfg(not(feature = "sqs"))]
            {
                let _ = sqs_params;
                bail!("SQS source requires the `sqs` cargo feature")
            }
        }
        SourceParams::Kafka(kafka_params) => {
            #[cfg(feature = "kafka")]
            {
                kafka::check_kafka_connectivity(kafka_params).await
            }
            #[cfg(not(feature = "kafka"))]
            {
                let _ = kafka_params;
                bail!("Kafka source requires the `kafka` cargo feature")
            }
        }
        SourceParams::PubSub(pubsub_params) => {
            #[cfg(feature = "gcp-pubsub")]
            {
                pubsub::check_pubsub_connectivity(pubsub_params).await
            }
            #[cfg(not(feature = "gcp-pubsub"))]
            {
                let _ = pubsub_params;
                bail!("GCP PubSub source requires the `gcp-pubsub` cargo feature")
            }
        }
        SourceParams::Ingest
        | SourceParams::IngestApi
        | SourceParams::IngestCli
        | SourceParams::Stdin
        | SourceParams::Vec(_)
        | SourceParams::Void(_) => Ok(()),
        SourceParams::Kinesis(_) | SourceParams::Pulsar(_) => {
            bail!(
                "source type `{}` is not supported; use Kafka, SQS, GCP PubSub, or push directly \
                 to the ingest API",
                source_config.source_type().as_str_name()
            )
        }
    }
}

fn is_forwardable_source(source_config: &SourceConfig) -> bool {
    source_config.enabled
        && matches!(
            source_config.source_params,
            SourceParams::Kafka(_)
                | SourceParams::PubSub(_)
                | SourceParams::File(FileSourceParams::Notifications(
                    FileSourceNotification::Sqs(_)
                ))
        )
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct ForwarderKey {
    index_uid: IndexUid,
    source_id: SourceId,
}

struct RunningForwarder {
    config: SourceConfig,
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

/// Polls index metadata from the metastore and runs one supervised forwarder task per enabled
/// pull source (Kafka, SQS, GCP Pub/Sub).
pub struct SourceForwarderManager {
    shutdown_tx: watch::Sender<bool>,
    reconciler_handle: JoinHandle<()>,
}

impl SourceForwarderManager {
    pub fn spawn(
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        sink: Arc<dyn IngestSink>,
    ) -> Self {
        Self::spawn_with_interval(
            metastore,
            storage_resolver,
            sink,
            DEFAULT_RECONCILE_INTERVAL,
        )
    }

    pub fn spawn_with_interval(
        metastore: MetastoreServiceClient,
        storage_resolver: StorageResolver,
        sink: Arc<dyn IngestSink>,
        reconcile_interval: Duration,
    ) -> Self {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let reconciler_handle = tokio::spawn(async move {
            let mut running: HashMap<ForwarderKey, RunningForwarder> = HashMap::new();
            let mut interval = time::interval(reconcile_interval);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        reconcile_forwarders(
                            &metastore,
                            &storage_resolver,
                            &sink,
                            &mut running,
                        )
                        .await;
                    }
                }
            }
            for (_, worker) in running.drain() {
                let _ = worker.shutdown_tx.send(true);
                let _ = worker.handle.await;
            }
        });
        Self {
            shutdown_tx,
            reconciler_handle,
        }
    }

    /// Stops all active source forwarders and waits for them to flush and commit.
    pub async fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.reconciler_handle.await;
    }
}

async fn reconcile_forwarders(
    metastore: &MetastoreServiceClient,
    storage_resolver: &StorageResolver,
    sink: &Arc<dyn IngestSink>,
    running: &mut HashMap<ForwarderKey, RunningForwarder>,
) {
    let response = match metastore
        .list_indexes_metadata(ListIndexesMetadataRequest::all())
        .await
    {
        Ok(response) => response,
        Err(error) => {
            rate_limited_warn!(
                limit_per_min = 6,
                error = %error,
                "failed to list indexes while reconciling source forwarders"
            );
            return;
        }
    };
    let indexes = match response.deserialize_indexes_metadata().await {
        Ok(indexes) => indexes,
        Err(error) => {
            rate_limited_warn!(
                limit_per_min = 6,
                error = %error,
                "failed to deserialize indexes while reconciling source forwarders"
            );
            return;
        }
    };

    let mut desired: HashMap<ForwarderKey, SourceConfig> = HashMap::new();
    for index in indexes {
        for source_config in index.sources.into_values() {
            if is_forwardable_source(&source_config) {
                desired.insert(
                    ForwarderKey {
                        index_uid: index.index_uid.clone(),
                        source_id: source_config.source_id.clone(),
                    },
                    source_config,
                );
            }
        }
    }

    let keys_to_stop: Vec<ForwarderKey> = running
        .iter()
        .filter(|(key, worker)| desired.get(key) != Some(&worker.config))
        .map(|(key, _)| key.clone())
        .collect();
    for key in keys_to_stop {
        if let Some(worker) = running.remove(&key) {
            info!(
                index_id = %key.index_uid.index_id,
                source_id = %key.source_id,
                "stopping source forwarder"
            );
            let _ = worker.shutdown_tx.send(true);
            let _ = worker.handle.await;
        }
    }

    let existing_keys: HashSet<ForwarderKey> = running.keys().cloned().collect();
    for (key, config) in desired {
        if existing_keys.contains(&key) {
            continue;
        }
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_supervised_forwarder(
            key.index_uid.index_id.clone(),
            config.clone(),
            storage_resolver.clone(),
            sink.clone(),
            shutdown_rx,
        );
        running.insert(
            key,
            RunningForwarder {
                config,
                shutdown_tx,
                handle,
            },
        );
    }
}

fn spawn_supervised_forwarder(
    index_id: String,
    source_config: SourceConfig,
    storage_resolver: StorageResolver,
    sink: Arc<dyn IngestSink>,
    mut shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let source_id = source_config.source_id.clone();
        loop {
            if *shutdown.borrow() {
                break;
            }
            let result = run_source_once(
                &index_id,
                &source_config,
                &storage_resolver,
                &sink,
                shutdown.clone(),
            )
            .await;
            if *shutdown.borrow() {
                break;
            }
            match result {
                Ok(()) => break,
                Err(error) => {
                    warn!(
                        index_id = %index_id,
                        source_id = %source_id,
                        error = %error,
                        "source forwarder failed; restarting after backoff"
                    );
                    tokio::select! {
                        biased;
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                break;
                            }
                        }
                        _ = time::sleep(SUPERVISOR_RESTART_BACKOFF) => {}
                    }
                }
            }
        }
    })
}

#[cfg_attr(
    not(any(feature = "kafka", feature = "sqs", feature = "gcp-pubsub")),
    allow(unused_variables)
)]
async fn run_source_once(
    index_id: &str,
    source_config: &SourceConfig,
    storage_resolver: &StorageResolver,
    sink: &Arc<dyn IngestSink>,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let source_id = source_config.source_id.clone();
    match &source_config.source_params {
        SourceParams::Kafka(params) => {
            #[cfg(feature = "kafka")]
            {
                kafka::run_kafka_forwarder(
                    index_id.to_string(),
                    source_id,
                    params.clone(),
                    sink.clone(),
                    shutdown,
                )
                .await
            }
            #[cfg(not(feature = "kafka"))]
            {
                let _ = (params, storage_resolver, sink, shutdown);
                bail!("Kafka source requires the `kafka` cargo feature")
            }
        }
        SourceParams::File(FileSourceParams::Notifications(FileSourceNotification::Sqs(
            params,
        ))) => {
            #[cfg(feature = "sqs")]
            {
                sqs::run_sqs_forwarder(
                    index_id.to_string(),
                    source_id,
                    params.clone(),
                    storage_resolver.clone(),
                    sink.clone(),
                    shutdown,
                )
                .await
            }
            #[cfg(not(feature = "sqs"))]
            {
                let _ = (params, storage_resolver, sink, shutdown);
                bail!("SQS source requires the `sqs` cargo feature")
            }
        }
        SourceParams::PubSub(params) => {
            #[cfg(feature = "gcp-pubsub")]
            {
                pubsub::run_pubsub_forwarder(
                    index_id.to_string(),
                    source_id,
                    params.clone(),
                    sink.clone(),
                    shutdown,
                )
                .await
            }
            #[cfg(not(feature = "gcp-pubsub"))]
            {
                let _ = (params, storage_resolver, sink, shutdown);
                bail!("GCP PubSub source requires the `gcp-pubsub` cargo feature")
            }
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    pub struct RecordingSink {
        batches: Mutex<Vec<(String, Vec<Bytes>)>>,
        transient_failures_left: AtomicUsize,
    }

    impl RecordingSink {
        pub fn with_transient_failures(count: usize) -> Self {
            Self {
                batches: Mutex::new(Vec::new()),
                transient_failures_left: AtomicUsize::new(count),
            }
        }

        pub fn batches(&self) -> Vec<(String, Vec<Bytes>)> {
            self.batches.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl IngestSink for RecordingSink {
        async fn ingest(&self, index_id: &str, docs: Vec<Bytes>) -> Result<(), IngestV2Error> {
            let remaining = self.transient_failures_left.load(Ordering::SeqCst);
            if remaining > 0
                && self
                    .transient_failures_left
                    .compare_exchange(remaining, remaining - 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                return Err(IngestV2Error::Unavailable("try again".to_string()));
            }
            self.batches
                .lock()
                .unwrap()
                .push((index_id.to_string(), docs));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_ingest_sink_retries_transient_errors() {
        let sink = RecordingSink::with_transient_failures(2);
        let (_tx, mut rx) = watch::channel(false);
        sink.ingest_with_retry("logs", vec![Bytes::from_static(br#"{"a":1}"#)], &mut rx)
            .await
            .unwrap();
        let batches = sink.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, "logs");
    }
}
