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
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use bytes::Bytes;
use quickwit_common::rate_limited_warn;
use quickwit_config::KafkaSourceParams;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{Message, Offset, TopicPartitionList};
use serde_json::Value as JsonValue;
use tokio::sync::watch;
use tokio::time;
use tracing::{debug, info, warn};

use super::{BATCH_MAX_WAIT, BATCH_NUM_BYTES_LIMIT, IngestSink};

const DEFAULT_KAFKA_CLIENT_LOG_LEVEL: RDKafkaLogLevel = RDKafkaLogLevel::Warning;
const CONNECTIVITY_TIMEOUT: Duration = Duration::from_secs(5);

fn parse_log_level(level: Option<&str>) -> anyhow::Result<RDKafkaLogLevel> {
    let Some(level) = level else {
        return Ok(DEFAULT_KAFKA_CLIENT_LOG_LEVEL);
    };
    match level.to_ascii_lowercase().as_str() {
        "emerg" => Ok(RDKafkaLogLevel::Emerg),
        "alert" => Ok(RDKafkaLogLevel::Alert),
        "critical" => Ok(RDKafkaLogLevel::Critical),
        "error" => Ok(RDKafkaLogLevel::Error),
        "warn" | "warning" => Ok(RDKafkaLogLevel::Warning),
        "notice" => Ok(RDKafkaLogLevel::Notice),
        "info" => Ok(RDKafkaLogLevel::Info),
        "debug" => Ok(RDKafkaLogLevel::Debug),
        other => bail!("invalid Kafka client log level `{other}`"),
    }
}

fn build_client_config(
    index_id: &str,
    source_id: &str,
    params: &KafkaSourceParams,
) -> anyhow::Result<ClientConfig> {
    let mut client_config = ClientConfig::new();
    client_config.set_log_level(parse_log_level(params.client_log_level.as_deref())?);
    client_config.set("group.id", format!("quickfast-{index_id}-{source_id}"));
    client_config.set("auto.offset.reset", "earliest");

    if let JsonValue::Object(map) = &params.client_params {
        for (key, value) in map {
            let value_str = match value {
                JsonValue::String(s) => s.clone(),
                JsonValue::Number(n) => n.to_string(),
                JsonValue::Bool(b) => b.to_string(),
                other => {
                    bail!(
                        "Kafka client_params `{key}` must be a string, number, or bool; got \
                         `{other}`"
                    )
                }
            };
            client_config.set(key, value_str);
        }
    }
    // Offsets are committed explicitly only after the ingest API accepts each batch.
    client_config.set("enable.auto.commit", "false");
    client_config.set("enable.auto.offset.store", "false");
    Ok(client_config)
}

/// Verifies connectivity to the Kafka cluster and checks that the topic exists.
pub async fn check_kafka_connectivity(params: &KafkaSourceParams) -> anyhow::Result<()> {
    let client_config = build_client_config("connectivity-check", "check", params)?;
    let consumer: StreamConsumer = client_config
        .create()
        .context("failed to create Kafka consumer")?;
    let topic = params.topic.clone();
    tokio::task::spawn_blocking(move || {
        let metadata = consumer
            .fetch_metadata(Some(&topic), CONNECTIVITY_TIMEOUT)
            .with_context(|| format!("failed to fetch Kafka metadata for topic `{topic}`"))?;
        let topic_metadata = metadata
            .topics()
            .first()
            .with_context(|| format!("Kafka topic `{topic}` not found"))?;
        if let Some(err) = topic_metadata.error() {
            bail!("Kafka topic `{topic}` metadata error: {err:?}");
        }
        if topic_metadata.partitions().is_empty() {
            bail!("Kafka topic `{topic}` has no partitions");
        }
        Ok(())
    })
    .await?
}

/// Consumes messages from Kafka using a native consumer group, forwards batches to `sink`,
/// and commits offsets only after `sink` accepts each batch.
pub async fn run_kafka_forwarder(
    index_id: String,
    source_id: String,
    params: KafkaSourceParams,
    sink: Arc<dyn IngestSink>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let client_config = build_client_config(&index_id, &source_id, &params)?;
    let consumer: StreamConsumer = client_config
        .create()
        .context("failed to create Kafka consumer")?;
    consumer
        .subscribe(&[params.topic.as_str()])
        .with_context(|| format!("failed to subscribe to Kafka topic `{}`", params.topic))?;

    info!(
        index_id = %index_id,
        source_id = %source_id,
        topic = %params.topic,
        "started Kafka ingest forwarder"
    );

    let mut batch: Vec<Bytes> = Vec::new();
    let mut batch_bytes: usize = 0;
    let mut max_offsets: HashMap<i32, i64> = HashMap::new();

    loop {
        if *shutdown.borrow() {
            break;
        }

        let flush_deadline = time::sleep(BATCH_MAX_WAIT);
        tokio::pin!(flush_deadline);

        let mut shutting_down = false;
        while batch_bytes < BATCH_NUM_BYTES_LIMIT {
            tokio::select! {
                biased;
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        shutting_down = true;
                        break;
                    }
                }
                _ = &mut flush_deadline, if !batch.is_empty() || !max_offsets.is_empty() => {
                    break;
                }
                recv = consumer.recv() => {
                    match recv {
                        Ok(message) => {
                            let partition = message.partition();
                            let offset = message.offset();
                            max_offsets
                                .entry(partition)
                                .and_modify(|cur| *cur = (*cur).max(offset))
                                .or_insert(offset);
                            if let Some(payload) = message.payload()
                                && !payload.is_empty()
                            {
                                batch_bytes += payload.len();
                                batch.push(Bytes::copy_from_slice(payload));
                            }
                        }
                        Err(error) => {
                            rate_limited_warn!(
                                limit_per_min = 6,
                                index_id = %index_id,
                                source_id = %source_id,
                                error = %error,
                                "error receiving message from Kafka"
                            );
                            time::sleep(Duration::from_millis(250)).await;
                        }
                    }
                }
            }
        }

        if !batch.is_empty() {
            let docs = std::mem::take(&mut batch);
            let num_docs = docs.len();
            let num_bytes = batch_bytes;
            batch_bytes = 0;
            sink.ingest_with_retry(&index_id, docs, &mut shutdown)
                .await?;
            debug!(
                index_id = %index_id,
                source_id = %source_id,
                num_docs,
                num_bytes,
                "forwarded Kafka batch to ingest API"
            );
        }

        if !max_offsets.is_empty() {
            let mut tpl = TopicPartitionList::new();
            for (partition, offset) in max_offsets.drain() {
                // Kafka commits the next offset to consume.
                tpl.add_partition_offset(&params.topic, partition, Offset::Offset(offset + 1))?;
            }
            let mode = if shutting_down {
                CommitMode::Sync
            } else {
                CommitMode::Async
            };
            if let Err(error) = consumer.commit(&tpl, mode) {
                warn!(
                    index_id = %index_id,
                    source_id = %source_id,
                    error = %error,
                    "failed to commit Kafka offsets"
                );
            }
        }

        if shutting_down {
            break;
        }
    }

    info!(
        index_id = %index_id,
        source_id = %source_id,
        topic = %params.topic,
        "stopped Kafka ingest forwarder"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_build_client_config_enforces_manual_commit() {
        let params = KafkaSourceParams {
            topic: "logs".to_string(),
            client_log_level: Some("info".to_string()),
            client_params: json!({
                "bootstrap.servers": "localhost:9092",
                "enable.auto.commit": true,
                "group.id": "custom-group"
            }),
            enable_backfill_mode: false,
        };
        let config = build_client_config("my-index", "kafka-src", &params).unwrap();
        assert_eq!(config.get("bootstrap.servers"), Some("localhost:9092"));
        assert_eq!(config.get("group.id"), Some("custom-group"));
        assert_eq!(config.get("enable.auto.commit"), Some("false"));
        assert_eq!(config.get("enable.auto.offset.store"), Some("false"));
    }

    #[test]
    fn test_build_client_config_default_group_id() {
        let params = KafkaSourceParams {
            topic: "logs".to_string(),
            client_log_level: None,
            client_params: json!({}),
            enable_backfill_mode: false,
        };
        let config = build_client_config("my-index", "kafka-src", &params).unwrap();
        assert_eq!(config.get("group.id"), Some("quickfast-my-index-kafka-src"));
    }
}
