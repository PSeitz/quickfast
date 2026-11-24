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
use std::collections::hash_map::Entry;

use bytesize::ByteSize;
use futures::StreamExt;
use quickwit_common::metrics::IN_FLIGHT_MULTI_FETCH_STREAM;
use quickwit_common::retry::RetryParams;
use quickwit_common::spawn_named_task;
use quickwit_common::stream_utils::InFlightValue;
use quickwit_proto::ingest::ingester::{
    FetchMessage, IngesterService, OpenFetchStreamRequest, fetch_message,
};
use quickwit_proto::ingest::{IngestV2Error, IngestV2Result};
use quickwit_proto::types::{IndexUid, NodeId, Position, QueueId, ShardId, SourceId, queue_id};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, warn};

use crate::{ClientId, IngesterPool};

#[derive(Debug)]
pub struct FetchStreamError {
    pub shard_id: ShardId,
    pub ingest_error: IngestV2Error,
}

/// Combines multiple fetch streams originating from different ingesters into a single stream.
pub struct MultiFetchStream {
    client_id: ClientId,
    ingester_pool: IngesterPool,
    retry_params: RetryParams,
    fetch_task_handles: HashMap<QueueId, JoinHandle<()>>,
    fetch_message_rx: mpsc::Receiver<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
    fetch_message_tx: mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
}

impl MultiFetchStream {
    pub fn new(
        client_id: ClientId,
        ingester_pool: IngesterPool,
        retry_params: RetryParams,
    ) -> Self {
        let (fetch_message_tx, fetch_message_rx) = mpsc::channel(3);
        Self {
            client_id,
            ingester_pool,
            retry_params,
            fetch_task_handles: HashMap::new(),
            fetch_message_rx,
            fetch_message_tx,
        }
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn fetch_message_tx(
        &self,
    ) -> mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>> {
        self.fetch_message_tx.clone()
    }

    /// Subscribes to a shard.
    #[allow(clippy::too_many_arguments)]
    pub async fn subscribe(
        &mut self,
        ingester_id: NodeId,
        index_uid: IndexUid,
        source_id: SourceId,
        shard_id: ShardId,
        from_position_exclusive: Position,
    ) -> IngestV2Result<()> {
        let queue_id = queue_id(&index_uid, &source_id, &shard_id);
        let entry = self.fetch_task_handles.entry(queue_id.clone());

        if let Entry::Occupied(_) = entry {
            return Err(IngestV2Error::Internal(format!(
                "stream has already subscribed to shard `{queue_id}`"
            )));
        }
        let fetch_stream_future = retrying_fetch_stream(
            self.client_id.clone(),
            index_uid,
            source_id,
            shard_id,
            from_position_exclusive,
            ingester_id,
            self.ingester_pool.clone(),
            self.retry_params,
            self.fetch_message_tx.clone(),
        );
        let fetch_task_handle = spawn_named_task(fetch_stream_future, "fetch_stream");
        self.fetch_task_handles.insert(queue_id, fetch_task_handle);
        Ok(())
    }

    pub fn unsubscribe(
        &mut self,
        index_uid: &IndexUid,
        source_id: &str,
        shard_id: ShardId,
    ) -> IngestV2Result<()> {
        let queue_id = queue_id(index_uid, source_id, &shard_id);

        if let Some(fetch_stream_handle) = self.fetch_task_handles.remove(&queue_id) {
            fetch_stream_handle.abort();
        }
        Ok(())
    }

    /// Returns the next fetch response. This method blocks until a response is available.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe.
    pub async fn next(&mut self) -> Result<FetchMessage, FetchStreamError> {
        // Because we always hold a sender and never call `close()` on the receiver, the channel is
        // always open.
        self.fetch_message_rx
            .recv()
            .await
            .expect("channel should be open")
            .map(|value: InFlightValue<FetchMessage>| value.into_inner())
    }

    /// Resets the stream by aborting all the active fetch tasks and dropping all queued responses.
    ///
    /// The borrow checker guarantees that both `next()` and `reset()` cannot be called
    /// simultaneously because they are both `&mut self` methods.
    pub fn reset(&mut self) {
        for (_queue_id, fetch_stream_handle) in self.fetch_task_handles.drain() {
            fetch_stream_handle.abort();
        }
        let (fetch_message_tx, fetch_message_rx) = mpsc::channel(3);
        self.fetch_message_tx = fetch_message_tx;
        self.fetch_message_rx = fetch_message_rx;
    }
}

impl Drop for MultiFetchStream {
    fn drop(&mut self) {
        self.reset();
    }
}

/// Performs multiple fault-tolerant fetch stream attempts until the stream reaches
/// the end of the shard.
#[allow(clippy::too_many_arguments)]
async fn retrying_fetch_stream(
    client_id: String,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    mut from_position_exclusive: Position,
    ingester_id: NodeId,
    ingester_pool: IngesterPool,
    retry_params: RetryParams,
    fetch_message_tx: mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
) {
    for num_attempts in 1..=retry_params.max_attempts {
        fetch_stream_once(
            client_id.clone(),
            index_uid.clone(),
            source_id.clone(),
            shard_id.clone(),
            &mut from_position_exclusive,
            &ingester_id,
            ingester_pool.clone(),
            fetch_message_tx.clone(),
        )
        .await;

        if from_position_exclusive.is_eof() {
            break;
        }
        let delay = retry_params.compute_delay(num_attempts);
        tokio::time::sleep(delay).await;
    }
}

/// Streams records from an ingester until the stream ends or fails.
#[allow(clippy::too_many_arguments)]
async fn fetch_stream_once(
    client_id: String,
    index_uid: IndexUid,
    source_id: SourceId,
    shard_id: ShardId,
    from_position_exclusive: &mut Position,
    ingester_id: &NodeId,
    ingester_pool: IngesterPool,
    fetch_message_tx: mpsc::Sender<Result<InFlightValue<FetchMessage>, FetchStreamError>>,
) {
    let Some(ingester) = ingester_pool.get(ingester_id) else {
        error!(
            client_id=%client_id,
            index_uid=%index_uid,
            source_id=%source_id,
            shard_id=%shard_id,
            "ingester `{ingester_id}` is unavailable: closing fetch stream"
        );
        let ingest_error = IngestV2Error::Unavailable(format!(
            "ingester `{ingester_id}` is unavailable: closing fetch stream"
        ));
        let fetch_stream_error = FetchStreamError {
            shard_id,
            ingest_error,
        };
        let _ = fetch_message_tx.send(Err(fetch_stream_error)).await;
        return;
    };
    let open_fetch_stream_request = OpenFetchStreamRequest {
        client_id: client_id.clone(),
        index_uid: index_uid.clone().into(),
        source_id: source_id.clone(),
        shard_id: Some(shard_id.clone()),
        from_position_exclusive: Some(from_position_exclusive.clone()),
    };
    let mut fetch_stream = match ingester
        .client
        .open_fetch_stream(open_fetch_stream_request)
        .await
    {
        Ok(fetch_stream) => fetch_stream,
        Err(ingest_error) => {
            let is_shard_not_found = matches!(&ingest_error, IngestV2Error::ShardNotFound { .. });

            if is_shard_not_found {
                error!(
                    client_id=%client_id,
                    index_uid=%index_uid,
                    source_id=%source_id,
                    shard_id=%shard_id,
                    "failed to open fetch stream from ingester `{ingester_id}`: shard not found"
                );
            } else {
                error!(
                    client_id=%client_id,
                    index_uid=%index_uid,
                    source_id=%source_id,
                    shard_id=%shard_id,
                    error=%ingest_error,
                    "failed to open fetch stream from ingester `{ingester_id}`: closing fetch stream"
                );
            }
            let fetch_stream_error = FetchStreamError {
                shard_id,
                ingest_error,
            };
            let _ = fetch_message_tx.send(Err(fetch_stream_error)).await;

            if is_shard_not_found {
                from_position_exclusive.to_eof();
            }
            return;
        }
    };
    while let Some(fetch_message_result) = fetch_stream.next().await {
        match fetch_message_result {
            Ok(fetch_message) => match &fetch_message.message {
                Some(fetch_message::Message::Payload(fetch_payload)) => {
                    let batch_size = fetch_payload.estimate_size();
                    let to_position_inclusive = fetch_payload.to_position_inclusive();
                    let in_flight_value = InFlightValue::new(
                        fetch_message,
                        batch_size,
                        &IN_FLIGHT_MULTI_FETCH_STREAM,
                    );
                    if fetch_message_tx.send(Ok(in_flight_value)).await.is_err() {
                        // The consumer was dropped.
                        return;
                    }
                    *from_position_exclusive = to_position_inclusive;
                }
                Some(fetch_message::Message::Eof(fetch_eof)) => {
                    let eof_position = fetch_eof.eof_position();
                    let in_flight_value = InFlightValue::new(
                        fetch_message,
                        ByteSize(0),
                        &IN_FLIGHT_MULTI_FETCH_STREAM,
                    );
                    // We ignore the send error if the consumer was dropped because we're going
                    // to return anyway.
                    let _ = fetch_message_tx.send(Ok(in_flight_value)).await;

                    *from_position_exclusive = eof_position;
                    return;
                }
                None => {
                    warn!("received empty fetch message");
                    continue;
                }
            },
            Err(ingest_error) => {
                error!(
                    client_id=%client_id,
                    index_uid=%index_uid,
                    source_id=%source_id,
                    shard_id=%shard_id,
                    error=%ingest_error,
                    "failed to fetch records from ingester `{ingester_id}`: closing fetch stream"
                );
                let fetch_stream_error = FetchStreamError {
                    shard_id,
                    ingest_error,
                };
                let _ = fetch_message_tx.send(Err(fetch_stream_error)).await;
                return;
            }
        }
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::time::Duration;

    use quickwit_common::ServiceStream;
    use quickwit_proto::ingest::MRecordBatch;
    use quickwit_proto::ingest::ingester::{
        FetchPayload, IngesterServiceClient, MockIngesterService,
    };
    use tokio::time::timeout;

    use super::*;
    use crate::IngesterPoolEntry;

    pub fn into_fetch_payload(fetch_message: FetchMessage) -> FetchPayload {
        match fetch_message.message.unwrap() {
            fetch_message::Message::Payload(fetch_payload) => fetch_payload,
            other => panic!("expected fetch payload, got `{other:?}`"),
        }
    }

    #[tokio::test]
    async fn test_fetch_stream_once_shard_not_found() {
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let shard_id = ShardId::from(1);
        let mut from_position_exclusive = Position::offset(0u64);

        let ingester_id = NodeId::from_str("test-ingester-0");
        let ingester_pool = IngesterPool::default();

        let (fetch_message_tx, mut fetch_stream) = ServiceStream::new_bounded(5);

        let mut mock_ingester_0 = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester_0
            .expect_open_fetch_stream()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Err(IngestV2Error::ShardNotFound {
                    shard_id: ShardId::from(1),
                })
            });
        let ingester_0 =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester_0));
        ingester_pool.insert(NodeId::from_str("test-ingester-0"), ingester_0);

        fetch_stream_once(
            client_id,
            index_uid,
            source_id,
            shard_id,
            &mut from_position_exclusive,
            &ingester_id,
            ingester_pool,
            fetch_message_tx,
        )
        .await;

        let fetch_stream_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();

        assert!(matches!(
            fetch_stream_error.ingest_error,
            IngestV2Error::ShardNotFound { shard_id } if shard_id == ShardId::from(1)
        ));
        assert!(from_position_exclusive.is_eof());
    }

    #[tokio::test]
    async fn test_retrying_fetch_stream() {
        let client_id = "test-client".to_string();
        let index_uid: IndexUid = IndexUid::for_test("test-index", 0);
        let source_id: SourceId = "test-source".into();
        let shard_id = ShardId::from(1);
        let from_position_exclusive = Position::offset(0u64);

        let ingester_id = NodeId::from_str("test-ingester");
        let ingester_pool = IngesterPool::default();

        let (fetch_message_tx, mut fetch_stream) = ServiceStream::new_bounded(5);
        let (service_stream_tx_1, service_stream_1) = ServiceStream::new_unbounded();
        let (service_stream_tx_2, service_stream_2) = ServiceStream::new_unbounded();

        let mut retry_params = RetryParams::for_test();
        retry_params.max_attempts = 3;

        let mut mock_ingester = MockIngesterService::new();
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_open_fetch_stream()
            .once()
            .returning(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Err(IngestV2Error::Internal(
                    "open fetch stream error".to_string(),
                ))
            });
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_open_fetch_stream()
            .once()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(0u64));

                Ok(service_stream_1)
            });
        let index_uid_clone = index_uid.clone();
        mock_ingester
            .expect_open_fetch_stream()
            .once()
            .return_once(move |request| {
                assert_eq!(request.client_id, "test-client");
                assert_eq!(request.index_uid(), &index_uid_clone);
                assert_eq!(request.source_id, "test-source");
                assert_eq!(request.shard_id(), ShardId::from(1));
                assert_eq!(request.from_position_exclusive(), Position::offset(1u64));

                Ok(service_stream_2)
            });
        let ingester =
            IngesterPoolEntry::ready_with_client(IngesterServiceClient::from_mock(mock_ingester));

        ingester_pool.insert(NodeId::from_str("test-ingester"), ingester);

        let fetch_payload = FetchPayload {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            mrecord_batch: MRecordBatch::for_test(["\0\0test-doc-foo"]),
            from_position_exclusive: Some(Position::offset(0u64)),
            to_position_inclusive: Some(Position::offset(1u64)),
        };
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        service_stream_tx_1.send(Ok(fetch_message)).unwrap();

        let ingest_error = IngestV2Error::Internal("fetch stream error #1".into());
        service_stream_tx_1.send(Err(ingest_error)).unwrap();

        let fetch_payload = FetchPayload {
            index_uid: Some(index_uid.clone()),
            source_id: source_id.clone(),
            shard_id: Some(shard_id.clone()),
            mrecord_batch: MRecordBatch::for_test(["\0\0test-doc-bar"]),
            from_position_exclusive: Some(Position::offset(1u64)),
            to_position_inclusive: Some(Position::offset(2u64)),
        };
        let fetch_message = FetchMessage::new_payload(fetch_payload);
        service_stream_tx_2.send(Ok(fetch_message)).unwrap();

        let ingest_error = IngestV2Error::Internal("fetch stream error #2".into());
        service_stream_tx_2.send(Err(ingest_error)).unwrap();

        retrying_fetch_stream(
            client_id,
            index_uid,
            source_id,
            shard_id,
            from_position_exclusive,
            ingester_id,
            ingester_pool,
            retry_params,
            fetch_message_tx,
        )
        .await;

        let ingest_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .ingest_error;
        assert!(
            matches!(ingest_error, IngestV2Error::Internal(message) if message == "open fetch stream error")
        );

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(0u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(1u64)
        );

        let fetch_stream_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(fetch_stream_error.ingest_error, IngestV2Error::Internal(message) if message == "fetch stream error #1")
        );

        let fetch_message = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_inner();
        let fetch_payload = into_fetch_payload(fetch_message);

        assert_eq!(
            fetch_payload.from_position_exclusive(),
            Position::offset(1u64)
        );
        assert_eq!(
            fetch_payload.to_position_inclusive(),
            Position::offset(2u64)
        );

        let fetch_stream_error = timeout(Duration::from_millis(100), fetch_stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(fetch_stream_error.ingest_error, IngestV2Error::Internal(message) if message == "fetch stream error #2")
        );

        assert!(
            timeout(Duration::from_millis(100), fetch_stream.next())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_multi_fetch_stream() {
        let client_id = "test-client".to_string();
        let ingester_pool = IngesterPool::default();
        let retry_params = RetryParams::for_test();
        let _multi_fetch_stream = MultiFetchStream::new(client_id, ingester_pool, retry_params);
        // TODO: Backport from original branch.
    }
}
