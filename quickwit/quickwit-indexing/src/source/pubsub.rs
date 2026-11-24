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

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use bytes::Bytes;
use google_cloud_auth::credentials::CredentialsFile;
use google_cloud_pubsub::client::{Client, ClientConfig};
use google_cloud_pubsub::subscription::Subscription;
use quickwit_common::rate_limited_warn;
use quickwit_config::PubSubSourceParams;
use tokio::sync::watch;
use tokio::time;
use tracing::{debug, info, warn};

use super::IngestSink;

const DEFAULT_MAX_MESSAGES_PER_PULL: i32 = 1_000;

async fn open_subscription(params: &PubSubSourceParams) -> anyhow::Result<Subscription> {
    let mut client_config: ClientConfig = match &params.credentials_file {
        Some(credentials_file) => {
            let credentials = CredentialsFile::new_from_file(credentials_file.clone())
                .await
                .with_context(|| {
                    format!("failed to load GCP PubSub credentials file from `{credentials_file}`")
                })?;
            ClientConfig::default().with_credentials(credentials).await
        }
        None => ClientConfig::default().with_auth().await,
    }
    .context("failed to create GCP PubSub client config")?;

    if params.project_id.is_some() {
        client_config.project_id = params.project_id.clone();
    }

    let client = Client::new(client_config)
        .await
        .context("failed to create GCP PubSub client")?;
    let subscription = client.subscription(&params.subscription);
    if !subscription.exists(None).await? {
        bail!(
            "GCP PubSub subscription `{}` does not exist",
            params.subscription
        );
    }
    Ok(subscription)
}

/// Checks that the configured GCP Pub/Sub subscription exists and is accessible.
pub async fn check_pubsub_connectivity(params: &PubSubSourceParams) -> anyhow::Result<()> {
    open_subscription(params).await?;
    Ok(())
}

/// Pulls messages from a GCP Pub/Sub subscription, forwards each batch to `sink`,
/// and acknowledges messages only after `sink` accepts the batch.
pub async fn run_pubsub_forwarder(
    index_id: String,
    source_id: String,
    params: PubSubSourceParams,
    sink: Arc<dyn IngestSink>,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let subscription = open_subscription(&params).await?;
    let max_messages = params
        .max_messages_per_pull
        .unwrap_or(DEFAULT_MAX_MESSAGES_PER_PULL);

    info!(
        index_id = %index_id,
        source_id = %source_id,
        subscription = %params.subscription,
        max_messages,
        "started GCP PubSub ingest forwarder"
    );

    loop {
        if *shutdown.borrow() {
            break;
        }

        let messages = tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    break;
                }
                continue;
            }
            pulled = subscription.pull(max_messages, None) => match pulled {
                Ok(messages) => messages,
                Err(error) => {
                    rate_limited_warn!(
                        limit_per_min = 6,
                        index_id = %index_id,
                        source_id = %source_id,
                        error = %error,
                        "failed to pull messages from GCP PubSub"
                    );
                    time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            }
        };

        if messages.is_empty() {
            continue;
        }

        let mut docs = Vec::with_capacity(messages.len());
        for message in &messages {
            if !message.message.data.is_empty() {
                docs.push(Bytes::copy_from_slice(&message.message.data));
            }
        }

        if !docs.is_empty() {
            let num_docs = docs.len();
            sink.ingest_with_retry(&index_id, docs, &mut shutdown)
                .await?;
            debug!(
                index_id = %index_id,
                source_id = %source_id,
                num_docs,
                "forwarded GCP PubSub batch to ingest API"
            );
        }

        for message in messages {
            if let Err(error) = message.ack().await {
                warn!(
                    index_id = %index_id,
                    source_id = %source_id,
                    error = %error,
                    "failed to ack GCP PubSub message"
                );
            }
        }
    }

    info!(
        index_id = %index_id,
        source_id = %source_id,
        subscription = %params.subscription,
        "stopped GCP PubSub ingest forwarder"
    );
    Ok(())
}
