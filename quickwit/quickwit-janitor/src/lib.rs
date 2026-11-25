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

#![deny(clippy::disallowed_methods)]

use std::sync::Arc;

use quickwit_common::pubsub::EventBroker;
use quickwit_config::NodeConfig;
use quickwit_metastore::SplitInfo;
use quickwit_proto::metastore::MetastoreServiceClient;
use quickwit_search::SearchJobPlacer;
use quickwit_storage::StorageResolver;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub mod actors;
mod completed_file_cleanup;
pub mod error;
mod janitor_service;
mod metrics;
mod retention_policy_execution;

pub use janitor_service::{HealthCheckFn, JanitorService};

use crate::actors::{DeleteTaskService, GarbageCollector, RetentionPolicyExecutor};
use crate::janitor_service::monitor_task;

#[derive(utoipa::OpenApi)]
#[openapi(components(schemas(SplitInfo)))]
/// Schema used for the OpenAPI generation which are apart of this crate.
pub struct JanitorApiSchemas;

#[allow(clippy::too_many_arguments)]
pub async fn start_janitor_service(
    config: &NodeConfig,
    metastore: MetastoreServiceClient,
    search_job_placer: SearchJobPlacer,
    storage_resolver: StorageResolver,
    event_broker: EventBroker,
    run_delete_task_service: bool,
    extra_health_check: Option<HealthCheckFn>,
) -> anyhow::Result<JanitorService> {
    info!("starting janitor service");
    let shutdown = CancellationToken::new();
    let mut tasks = Vec::new();

    let garbage_collector = GarbageCollector::new(metastore.clone(), storage_resolver.clone());
    tasks.push(monitor_task(
        garbage_collector.spawn(shutdown.clone()),
        shutdown.clone(),
    ));

    let retention_policy_executor = RetentionPolicyExecutor::new(metastore.clone());
    tasks.push(monitor_task(
        retention_policy_executor.spawn(shutdown.clone()),
        shutdown.clone(),
    ));

    if run_delete_task_service {
        const DEFAULT_DELETE_CONCURRENCY: usize = 3;
        let delete_task_service = DeleteTaskService::new(
            metastore,
            search_job_placer,
            storage_resolver,
            config.data_dir_path.clone(),
            config.indexer_config.max_concurrent_split_uploads,
            Arc::new(Semaphore::new(DEFAULT_DELETE_CONCURRENCY)),
            event_broker,
        )
        .await?;
        tasks.push(monitor_task(
            delete_task_service.spawn(shutdown.clone()),
            shutdown.clone(),
        ));
    } else {
        tracing::warn!("delete task service is disabled: delete queries will not be processed");
    }

    Ok(JanitorService::new(shutdown, tasks, extra_health_check))
}
