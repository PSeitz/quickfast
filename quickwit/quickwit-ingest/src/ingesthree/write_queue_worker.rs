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

//! Validates and compresses queued batches, then drives local writes and upload handoff.

use std::sync::{Arc, Mutex};

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::{FuturesOrdered, StreamExt};
use quickwit_common::thread_pool::{Panicked, run_cpu_intensive};
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::ingest::{DocBatchV2, ParseFailure};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use super::{
    Admission, AppendRequest, AppendResponse, CompressedBatch, DocumentsStagedForUpload,
    FileWriter, IngestError, PreparedRequest, ValidationMetrics, WriterRequest,
};
use crate::doc_mapper::{is_document_validation_enabled, validate_doc_batch_impl};
use crate::ingesthree::handoff::FileHandoffs;
use crate::ingesthree::{OpenFileLimit, Uploader};

type PrepareFuture = BoxFuture<'static, Result<PreparedRequest, Panicked>>;

enum PendingStop {
    Finish(oneshot::Sender<Result<Vec<DocumentsStagedForUpload>, IngestError>>),
    Drain(oneshot::Sender<Result<(), IngestError>>),
    Closed,
}

fn prepare_concurrency() -> usize {
    (quickwit_common::num_cpus() / 2).max(4)
}

impl AppendRequest {
    pub(super) fn prepare(self, metrics: &ValidationMetrics) -> PreparedRequest {
        let Self {
            index_uid,
            batch,
            mapper,
            reply,
            reservation,
        } = self;
        // Validation and compression own their buffers inside this call, so they
        // drop before the reservation on errors and panics as well as success.
        let preparation =
            validate_documents(batch, mapper, metrics).and_then(|(batch, failures)| {
                let compressed = if batch.is_empty() {
                    None
                } else {
                    Some(CompressedBatch::new(
                        batch.docs().map(|(_, document)| document),
                    )?)
                };
                Ok((compressed, failures))
            });
        PreparedRequest {
            index_uid,
            reply,
            preparation,
            reservation,
        }
    }
}

/// Validates a batch and returns its valid documents and parse failures.
pub(super) fn validate_documents(
    batch: DocBatchV2,
    mapper: Option<Arc<DocMapper>>,
    metrics: &ValidationMetrics,
) -> Result<(DocBatchV2, Vec<ParseFailure>), IngestError> {
    let document_bytes = batch
        .doc_lengths
        .iter()
        .try_fold(0usize, |sum, len| sum.checked_add(*len as usize));
    if document_bytes != Some(batch.doc_buffer.len()) {
        return Err(IngestError::InvalidRequest(
            "inconsistent document lengths".into(),
        ));
    }
    let original_bytes = batch.num_bytes();
    let (batch, parse_failures) = match mapper {
        Some(mapper) if is_document_validation_enabled() => validate_doc_batch_impl(batch, &mapper),
        _ => (batch, Vec::new()),
    };
    metrics.record(&batch, parse_failures.len(), original_bytes);
    Ok((batch, parse_failures))
}

/// Writes batches and finishes files while upload and registration make progress.
pub(super) async fn write_batches(
    ingester: FileWriter,
    receiver: mpsc::UnboundedReceiver<WriterRequest>,
    admission: Arc<Mutex<Admission>>,
    uploader: Option<Uploader>,
) -> anyhow::Result<()> {
    let mut drain_reply = None;
    let result =
        write_until_stopped(ingester, receiver, &admission, uploader, &mut drain_reply).await;
    admission.lock().expect("admission mutex poisoned").close();
    // The owner and any upload future are dropped before acknowledging drain.
    if let Some(reply) = drain_reply {
        let response = result
            .as_ref()
            .map(|_| ())
            .map_err(|error| IngestError::Internal(anyhow::anyhow!("{error:#}")));
        let _ = reply.send(response);
    }
    result
}

async fn write_until_stopped(
    mut ingester: FileWriter,
    mut receiver: mpsc::UnboundedReceiver<WriterRequest>,
    admission: &Arc<Mutex<Admission>>,
    uploader: Option<Uploader>,
    drain_reply: &mut Option<oneshot::Sender<Result<(), IngestError>>>,
) -> anyhow::Result<()> {
    // Local IO errors or panics can drop the ingester on a blocking thread. Keep
    // directory ownership until handoff is shut down too (locals drop in reverse order).
    let directory_lock = ingester.directory_lock.clone();
    // Each handoff runs in its own Tokio task and completes after registration or
    // remote discard, independently of other files. Keep it tracked until local deletion.
    let mut handoffs = FileHandoffs::default();
    // Prepare multiple batches concurrently on the CPU pool while preserving FIFO write order.
    let mut preparing: FuturesOrdered<PrepareFuture> = FuturesOrdered::new();
    let mut pending_stop: Option<PendingStop> = None;
    let max_prepare_concurrency = prepare_concurrency();
    loop {
        if preparing.is_empty()
            && let Some(stop) = pending_stop.take()
        {
            match stop {
                PendingStop::Finish(reply) => {
                    handoffs.shutdown().await;
                    let result = tokio::task::spawn_blocking(move || ingester.finish()).await?;
                    drop(directory_lock);
                    let _ = reply.send(result.map_err(IngestError::Internal));
                    return Ok(());
                }
                PendingStop::Drain(reply) => {
                    *drain_reply = Some(reply);
                    receiver.close();
                    let (writer, result) = tokio::task::spawn_blocking(move || {
                        let result = ingester.finish_files();
                        (ingester, result)
                    })
                    .await?;
                    ingester = writer;
                    if let Err(error) = result {
                        handoffs.shutdown().await;
                        tokio::task::spawn_blocking(move || drop(ingester)).await?;
                        return Err(error.context("failed to finish local files"));
                    }
                    if uploader.is_none() {
                        handoffs.shutdown().await;
                        tokio::task::spawn_blocking(move || drop(ingester)).await?;
                        anyhow::bail!("handoff drain requires an uploader");
                    }
                }
                PendingStop::Closed => break,
            }
        }
        if drain_reply.is_some() && ingester.finished.is_empty() {
            break;
        }
        if let Some(uploader) = &uploader {
            handoffs.schedule_uploads(uploader, &directory_lock, &ingester.finished);
        }
        tokio::select! {
            // Cancellation and expiry take priority even when the request queue stays nonempty.
            biased;
            _ = wait_for_drain_cancel(drain_reply.as_mut(), pending_stop.as_mut()) => break,
            _ = wait_for_deadline(ingester.next_deadline()) => {
                let (writer, result) = tokio::task::spawn_blocking(move || {
                    let result = ingester.finish_expired_files();
                    (ingester, result)
                }).await?;
                ingester = writer;
                if let Err(error) = result {
                    drain_receiver_and_preparing(&mut receiver, &mut preparing).await;
                    handoffs.shutdown().await;
                    tokio::task::spawn_blocking(move || drop(ingester)).await?;
                    return Err(error.context("failed to finish local files"));
                }
            }
            result = handoffs.next_delete() => {
                let file_id = match result {
                    Ok(file_id) => file_id,
                    Err(error) => {
                        admission.lock().expect("admission mutex poisoned").close();
                        drain_receiver_and_preparing(&mut receiver, &mut preparing).await;
                        handoffs.shutdown().await;
                        // Preserve accepted writes and release the directory on the blocking pool.
                        tokio::task::spawn_blocking(move || ingester.finish()).await??;
                        return Err(error);
                    }
                };
                let (writer, result) = tokio::task::spawn_blocking(move || {
                    let result = ingester.delete_finished_file(file_id);
                    (ingester, result)
                }).await?;
                ingester = writer;
                match result {
                    Ok(_) => handoffs.deleted(file_id),
                    Err(error) => {
                        tracing::warn!(%file_id, ?error, "failed to delete finished ingesthree file; retrying");
                        handoffs.retry_delete(file_id);
                    }
                }
            }
            Some(first_result) = preparing.next(), if !preparing.is_empty() => {
                let mut ready = Vec::new();
                let mut prepare_panic = None;
                match first_result {
                    Ok(prepared) => ready.push(prepared),
                    Err(panicked) => prepare_panic = Some(panicked),
                }
                while prepare_panic.is_none() {
                    match preparing.next().now_or_never() {
                        Some(Some(Ok(prepared))) => ready.push(prepared),
                        Some(Some(Err(panicked))) => prepare_panic = Some(panicked),
                        _ => break,
                    }
                }
                if let Some(panicked) = prepare_panic {
                    admission.lock().expect("admission mutex poisoned").close();
                    drop(ready);
                    drain_receiver_and_preparing(&mut receiver, &mut preparing).await;
                    handoffs.shutdown().await;
                    tokio::task::spawn_blocking(move || ingester.finish()).await??;
                    return Err(panicked.into());
                }
                if drain_reply.is_none() {
                    // Refill the CPU preparation pipeline before blocking on local file IO
                    // so validation and compression keep overlapping with disk writes.
                    refill_preparing(
                        &mut receiver,
                        &mut preparing,
                        &mut pending_stop,
                        max_prepare_concurrency,
                    );
                }
                match tokio::task::spawn_blocking(move || {
                    append_prepared_batches(ingester, ready)
                })
                .await?
                {
                    Ok(writer) => ingester = writer,
                    Err(error) => {
                        drain_receiver_and_preparing(&mut receiver, &mut preparing).await;
                        handoffs.shutdown().await;
                        return Err(error);
                    }
                }
            }
            request = receiver.recv(), if drain_reply.is_none() && pending_stop.is_none() && preparing.len() < max_prepare_concurrency => {
                enqueue_request(request, &mut preparing, &mut pending_stop);
                refill_preparing(
                    &mut receiver,
                    &mut preparing,
                    &mut pending_stop,
                    max_prepare_concurrency,
                );
            }
        }
    }
    drain_receiver_and_preparing(&mut receiver, &mut preparing).await;
    handoffs.shutdown().await;
    tokio::task::spawn_blocking(move || ingester.finish()).await??;
    Ok(())
}

fn refill_preparing(
    receiver: &mut mpsc::UnboundedReceiver<WriterRequest>,
    preparing: &mut FuturesOrdered<PrepareFuture>,
    pending_stop: &mut Option<PendingStop>,
    max_prepare_concurrency: usize,
) {
    while pending_stop.is_none() && preparing.len() < max_prepare_concurrency {
        match receiver.try_recv() {
            Ok(next_request) => {
                enqueue_request(Some(next_request), preparing, pending_stop);
            }
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                *pending_stop = Some(PendingStop::Closed);
                break;
            }
        }
    }
}

fn enqueue_request(
    request: Option<WriterRequest>,
    preparing: &mut FuturesOrdered<PrepareFuture>,
    pending_stop: &mut Option<PendingStop>,
) {
    match request {
        Some(WriterRequest::Append(append)) => preparing.push_back(spawn_prepare(append)),
        Some(WriterRequest::Finish(reply)) => *pending_stop = Some(PendingStop::Finish(reply)),
        Some(WriterRequest::Drain(reply)) => *pending_stop = Some(PendingStop::Drain(reply)),
        None => *pending_stop = Some(PendingStop::Closed),
    }
}

fn spawn_prepare(append: AppendRequest) -> PrepareFuture {
    let metrics = append
        .reservation
        .admission
        .lock()
        .expect("admission mutex poisoned")
        .metrics
        .validation
        .clone();
    // Move the request into preparation. Cancellation can leave CPU work running,
    // and a discarded result must drop its buffers before its reservation.
    Box::pin(run_cpu_intensive(move || append.prepare(&metrics)))
}

/// Releases queued requests and waits for in-flight CPU preparations so all
/// buffers and byte reservations settle before the writer stops.
async fn drain_receiver_and_preparing(
    receiver: &mut mpsc::UnboundedReceiver<WriterRequest>,
    preparing: &mut FuturesOrdered<PrepareFuture>,
) {
    receiver.close();
    while receiver.try_recv().is_ok() {}
    while let Some(result) = preparing.next().await {
        drop(result);
    }
}

async fn wait_for_drain_cancel(
    drain_reply: Option<&mut oneshot::Sender<Result<(), IngestError>>>,
    pending_stop: Option<&mut PendingStop>,
) {
    if let Some(reply) = drain_reply {
        reply.closed().await;
    } else if let Some(PendingStop::Drain(reply)) = pending_stop {
        reply.closed().await;
    } else {
        std::future::pending::<()>().await;
    }
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

/// Prepares and writes one batch, settles its reservation, and replies to the caller.
#[cfg(test)]
pub(super) async fn append_batch(
    ingester: FileWriter,
    append: AppendRequest,
) -> anyhow::Result<FileWriter> {
    let prepared = spawn_prepare(append).await?;
    tokio::task::spawn_blocking(move || append_prepared(ingester, prepared)).await?
}

fn append_prepared_batches(
    mut ingester: FileWriter,
    prepared_batches: Vec<PreparedRequest>,
) -> anyhow::Result<FileWriter> {
    for prepared in prepared_batches {
        ingester = append_prepared(ingester, prepared)?;
    }
    Ok(ingester)
}

pub(super) fn append_prepared(
    mut ingester: FileWriter,
    prepared: PreparedRequest,
) -> anyhow::Result<FileWriter> {
    let PreparedRequest {
        index_uid,
        reply,
        preparation,
        reservation,
    } = prepared;
    // Declared after the reservation so the batch drops first during unwinding.
    let (batch, parse_failures) = match preparation {
        Ok(documents_and_failures) => documents_and_failures,
        Err(error) => {
            drop(reservation);
            let _ = reply.send(Err(error));
            return Ok(ingester);
        }
    };
    let previous_disk_bytes = ingester.disk_bytes;
    let num_docs = batch.as_ref().map(|batch| batch.num_docs).unwrap_or(0);
    let result = match &batch {
        Some(batch) => ingester.append_compressed(&index_uid, batch).map(Some),
        None => Ok(None),
    };
    let written_bytes = ingester.disk_bytes - previous_disk_bytes;
    drop(batch);
    match result {
        Ok(file_id) => {
            // Settle in the blocking task even if its awaiting future was cancelled.
            reservation.release_after_write(written_bytes);
            let _ = reply.send(Ok(AppendResponse {
                file_id,
                num_docs,
                parse_failures,
            }));
        }
        Err(error) if error.is::<OpenFileLimit>() => {
            let error = reservation
                .admission
                .lock()
                .expect("admission mutex poisoned")
                .metrics
                .reject(IngestError::OpenFileLimit);
            drop(reservation);
            let _ = reply.send(Err(error));
        }
        Err(error) => {
            let message = format!("{error:#}");
            {
                let mut admission = reservation
                    .admission
                    .lock()
                    .expect("admission mutex poisoned");
                admission.close();
                // A rename failure can follow a flushed append. Retain its known
                // file growth. Recovery reconciles partial writes after IO errors.
                admission.record_written(written_bytes);
            }
            // A failed BufWriter may still hold bytes. Drop it on this blocking
            // thread before releasing its reservation and directory ownership.
            drop(ingester);
            let _ = reply.send(Err(IngestError::Internal(error)));
            anyhow::bail!("local append failed: {message}");
        }
    }
    Ok(ingester)
}
