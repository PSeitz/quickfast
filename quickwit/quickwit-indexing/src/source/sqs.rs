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

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use aws_sdk_sqs::config::{Builder, Region, SharedAsyncSleep};
use aws_sdk_sqs::{Client, Config};
use quickwit_aws::retry::aws_retry;
use quickwit_aws::{DEFAULT_AWS_REGION, aws_behavior_version, get_aws_config};
use quickwit_common::rate_limited_warn;
use quickwit_common::retry::RetryParams;
use quickwit_common::uri::Uri;
use quickwit_config::{FileSourceMessageType, FileSourceSqs};
use quickwit_storage::StorageResolver;
use regex::Regex;
use serde_json::Value;
use tokio::sync::watch;
use tokio::time;
use tracing::{debug, info, warn};

use super::doc_file_reader::DocFileReader;
use super::{BATCH_NUM_BYTES_LIMIT, IngestSink};

const VISIBILITY_TIMEOUT_SECS: i32 = 60;
const VISIBILITY_EXTEND_INTERVAL: Duration = Duration::from_secs(20);
const RECEIVE_WAIT_TIME_SECS: i32 = 20;
const MAX_MESSAGES_PER_RECEIVE: i32 = 10;

#[derive(Debug, PartialEq, Eq)]
enum ParsedMessage {
    ObjectUri(Uri),
    Discard,
}

fn parse_s3_notification(body: &str) -> anyhow::Result<ParsedMessage> {
    let value: Value = serde_json::from_str(body).context("invalid JSON in SQS message")?;
    if matches!(value["Event"].as_str(), Some("s3:TestEvent")) {
        info!("discarding S3 test event");
        return Ok(ParsedMessage::Discard);
    }
    let event_name = value["Records"][0]["eventName"]
        .as_str()
        .context("invalid S3 notification: Records[0].eventName not found")?;
    if !event_name.starts_with("ObjectCreated:") {
        rate_limited_warn!(
            limit_per_min = 5,
            event = event_name,
            "ignoring non-ObjectCreated S3 event"
        );
        return Ok(ParsedMessage::Discard);
    }
    let key = value["Records"][0]["s3"]["object"]["key"]
        .as_str()
        .context("invalid S3 notification: Records[0].s3.object.key not found")?;
    let bucket = value["Records"][0]["s3"]["bucket"]["name"]
        .as_str()
        .context("invalid S3 notification: Records[0].s3.bucket.name not found")?;
    let decoded_key = percent_encoding::percent_decode(key.as_bytes())
        .decode_utf8()
        .context("invalid S3 notification: key is not valid UTF-8 after URL decoding")?;
    let uri = Uri::from_str(&format!("s3://{bucket}/{decoded_key}"))?;
    Ok(ParsedMessage::ObjectUri(uri))
}

fn parse_sqs_message(
    body: &str,
    message_type: FileSourceMessageType,
) -> anyhow::Result<ParsedMessage> {
    match message_type {
        FileSourceMessageType::S3Notification => parse_s3_notification(body),
        FileSourceMessageType::RawUri => Ok(ParsedMessage::ObjectUri(Uri::from_str(body.trim())?)),
    }
}

async fn preconfigured_builder() -> anyhow::Result<Builder> {
    let aws_config = get_aws_config().await;
    let mut sqs_config = Config::builder().behavior_version(aws_behavior_version());
    sqs_config.set_retry_config(aws_config.retry_config().cloned());
    sqs_config.set_credentials_provider(aws_config.credentials_provider());
    sqs_config.set_http_client(aws_config.http_client());
    sqs_config.set_timeout_config(aws_config.timeout_config().cloned());
    if let Some(identity_cache) = aws_config.identity_cache() {
        sqs_config.set_identity_cache(identity_cache);
    }
    sqs_config.set_sleep_impl(Some(SharedAsyncSleep::new(
        quickwit_aws::TokioSleep::default(),
    )));
    Ok(sqs_config)
}

fn queue_url_region(queue_url: &str) -> Option<Region> {
    let re = Regex::new(r"^https?://sqs\.(.*?)\.amazonaws\.com").unwrap();
    let caps = re.captures(queue_url)?;
    Some(Region::new(caps.get(1)?.as_str().to_string()))
}

fn queue_url_endpoint(queue_url: &str) -> anyhow::Result<String> {
    let re = Regex::new(r"(^https?://[^/]+)").unwrap();
    let caps = re.captures(queue_url).context("invalid SQS queue URL")?;
    Ok(caps
        .get(1)
        .context("invalid SQS queue URL")?
        .as_str()
        .to_string())
}

async fn get_sqs_client(queue_url: &str) -> anyhow::Result<Client> {
    let mut sqs_config = preconfigured_builder().await?;
    let region = queue_url_region(queue_url).unwrap_or(DEFAULT_AWS_REGION);
    let endpoint = queue_url_endpoint(queue_url)?;
    sqs_config.set_region(Some(region));
    sqs_config.set_endpoint_url(Some(endpoint));
    Ok(Client::from_conf(sqs_config.build()))
}

/// Checks connectivity and access to an SQS queue.
pub async fn check_sqs_connectivity(queue_url: &str) -> anyhow::Result<()> {
    let client = get_sqs_client(queue_url).await?;
    client
        .get_queue_attributes()
        .queue_url(queue_url)
        .send()
        .await
        .with_context(|| format!("failed to access SQS queue `{queue_url}`"))?;
    Ok(())
}

/// Streams all documents from `uri` into `sink` in batches.
async fn forward_uri(
    storage_resolver: &StorageResolver,
    uri: &Uri,
    index_id: &str,
    sink: &Arc<dyn IngestSink>,
    shutdown: &mut watch::Receiver<bool>,
) -> anyhow::Result<usize> {
    let mut reader = DocFileReader::from_uri(storage_resolver, uri).await?;
    let mut total_docs = 0usize;
    loop {
        if *shutdown.borrow() {
            anyhow::bail!("SQS forwarder shutting down");
        }
        let docs = reader.read_batch(BATCH_NUM_BYTES_LIMIT).await?;
        if docs.is_empty() {
            break;
        }
        total_docs += docs.len();
        sink.ingest_with_retry(index_id, docs, shutdown).await?;
    }
    Ok(total_docs)
}

/// Spawns a periodic visibility-timeout extender until the returned guard is dropped.
fn spawn_visibility_heartbeat(
    client: Client,
    queue_url: String,
    receipt_handle: String,
) -> tokio::task::AbortHandle {
    tokio::spawn(async move {
        let mut interval = time::interval(VISIBILITY_EXTEND_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(error) = client
                .change_message_visibility()
                .queue_url(&queue_url)
                .receipt_handle(&receipt_handle)
                .visibility_timeout(VISIBILITY_TIMEOUT_SECS)
                .send()
                .await
            {
                warn!(
                    queue_url = %queue_url,
                    error = %error,
                    "failed to extend SQS message visibility timeout"
                );
                break;
            }
        }
    })
    .abort_handle()
}

/// Receives SQS notifications, streams referenced files to `sink`, and deletes each SQS
/// message only after all of its batches have been accepted by `sink`.
pub async fn run_sqs_forwarder(
    index_id: String,
    source_id: String,
    params: FileSourceSqs,
    storage_resolver: StorageResolver,
    sink: Arc<dyn IngestSink>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let client = get_sqs_client(&params.queue_url).await?;
    let receive_retries = RetryParams::standard();

    info!(
        index_id = %index_id,
        source_id = %source_id,
        queue_url = %params.queue_url,
        "started SQS ingest forwarder"
    );

    loop {
        if *shutdown.borrow() {
            break;
        }

        let receive_fut = aws_retry(&receive_retries, || async {
            client
                .receive_message()
                .queue_url(&params.queue_url)
                .wait_time_seconds(RECEIVE_WAIT_TIME_SECS)
                .max_number_of_messages(MAX_MESSAGES_PER_RECEIVE)
                .visibility_timeout(VISIBILITY_TIMEOUT_SECS)
                .send()
                .await
        });

        let output = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
                continue;
            }
            res = receive_fut => match res {
                Ok(output) => output,
                Err(error) => {
                    rate_limited_warn!(
                        limit_per_min = 6,
                        index_id = %index_id,
                        source_id = %source_id,
                        error = %error,
                        "failed to receive messages from SQS"
                    );
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        };

        for message in output.messages.unwrap_or_default() {
            if *shutdown.borrow() {
                break;
            }
            let Some(receipt_handle) = message.receipt_handle else {
                continue;
            };
            let body = message.body.unwrap_or_default();

            let parsed = match parse_sqs_message(&body, params.message_type) {
                Ok(parsed) => parsed,
                Err(error) => {
                    warn!(
                        index_id = %index_id,
                        source_id = %source_id,
                        error = %error,
                        "failed to parse SQS message; leaving on queue for redelivery/DLQ"
                    );
                    continue;
                }
            };

            let should_delete = match parsed {
                ParsedMessage::Discard => true,
                ParsedMessage::ObjectUri(uri) => {
                    let heartbeat = spawn_visibility_heartbeat(
                        client.clone(),
                        params.queue_url.clone(),
                        receipt_handle.clone(),
                    );
                    let result =
                        forward_uri(&storage_resolver, &uri, &index_id, &sink, &mut shutdown).await;
                    heartbeat.abort();
                    match result {
                        Ok(num_docs) => {
                            debug!(
                                index_id = %index_id,
                                source_id = %source_id,
                                uri = %uri,
                                num_docs,
                                "forwarded SQS object to ingest API"
                            );
                            true
                        }
                        Err(error) => {
                            if *shutdown.borrow() {
                                break;
                            }
                            warn!(
                                index_id = %index_id,
                                source_id = %source_id,
                                uri = %uri,
                                error = %error,
                                "failed to forward SQS object; leaving message unacked"
                            );
                            false
                        }
                    }
                }
            };

            if should_delete
                && let Err(error) = client
                    .delete_message()
                    .queue_url(&params.queue_url)
                    .receipt_handle(&receipt_handle)
                    .send()
                    .await
            {
                warn!(
                    index_id = %index_id,
                    source_id = %source_id,
                    error = %error,
                    "failed to delete processed SQS message"
                );
            }
        }
    }

    info!(
        index_id = %index_id,
        source_id = %source_id,
        queue_url = %params.queue_url,
        "stopped SQS ingest forwarder"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use bytes::Bytes;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::source::tests::RecordingSink;

    #[test]
    fn test_parse_s3_notification_and_raw_uri() {
        let s3_event = r#"{
            "Records": [{
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": { "name": "my-bucket" },
                    "object": { "key": "logs/2026%2Fevents.json.gz" }
                }
            }]
        }"#;
        assert_eq!(
            parse_sqs_message(s3_event, FileSourceMessageType::S3Notification).unwrap(),
            ParsedMessage::ObjectUri(
                Uri::from_str("s3://my-bucket/logs/2026/events.json.gz").unwrap()
            )
        );

        let test_event = r#"{"Event": "s3:TestEvent"}"#;
        assert_eq!(
            parse_sqs_message(test_event, FileSourceMessageType::S3Notification).unwrap(),
            ParsedMessage::Discard
        );

        let delete_event = r#"{"Records": [{"eventName": "ObjectRemoved:Delete"}]}"#;
        assert_eq!(
            parse_sqs_message(delete_event, FileSourceMessageType::S3Notification).unwrap(),
            ParsedMessage::Discard
        );

        assert_eq!(
            parse_sqs_message("s3://bucket/file.json\n", FileSourceMessageType::RawUri).unwrap(),
            ParsedMessage::ObjectUri(Uri::from_str("s3://bucket/file.json").unwrap())
        );
    }

    #[tokio::test]
    async fn test_forward_uri_streams_to_ingest_sink() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"{{"msg":"one"}}"#).unwrap();
        writeln!(file, r#"{{"msg":"two"}}"#).unwrap();
        file.flush().unwrap();

        let uri = Uri::from_str(file.path().to_str().unwrap()).unwrap();
        let sink = Arc::new(RecordingSink::default());
        let (_tx, mut rx) = watch::channel(false);
        let dyn_sink: Arc<dyn IngestSink> = sink.clone();

        let count = forward_uri(
            &StorageResolver::unconfigured(),
            &uri,
            "my-index",
            &dyn_sink,
            &mut rx,
        )
        .await
        .unwrap();
        assert_eq!(count, 2);
        let recorded = sink.batches();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].0, "my-index");
        assert_eq!(
            recorded[0].1,
            vec![
                Bytes::from_static(br#"{"msg":"one"}"#),
                Bytes::from_static(br#"{"msg":"two"}"#),
            ]
        );
    }
}
