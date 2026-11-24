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

#![recursion_limit = "256"]

mod body_limit;
mod build_info;
mod cluster_api;
mod decompression;
mod delete_task_api;
mod developer_api;
mod elasticsearch_api;
mod format;
mod grpc;
mod health_check_api;
mod index_api;
mod indexing_api;
mod ingest_api;
mod ingesthree;
mod jaeger_api;
mod load_shield;
mod mcp_api;
mod metastore;
mod metrics;
mod metrics_api;
mod node_info_handler;
mod openapi;
mod otlp_api;
mod rest;
mod rest_api_request_span;
mod rest_api_response;
mod search_api;
pub(crate) mod simple_list;
pub mod tcp_listener;
mod template_api;
mod ui_handler;

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::fs;
use std::net::SocketAddr;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use anyhow::{Context, bail};
pub(crate) use decompression::Body;
pub use format::BodyFormat;
use futures::StreamExt;
use itertools::Itertools;
use quickwit_actors::{ActorExitStatus, ActorHandle, Mailbox, Universe};
use quickwit_cluster::{
    Cluster, ClusterChange, ClusterChangeStream, ClusterNode, start_cluster_service,
};
use quickwit_common::pubsub::{EventBroker, EventSubscriptionHandle};
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::tower::{
    BalanceChannel, BoxFutureInfaillible, Change, GrpcMetricsLayer, TimeoutLayer,
};
use quickwit_common::{get_bool_from_env, spawn_named_task};
use quickwit_compaction::planner::CompactionPlanner;
use quickwit_compaction::{
    CompactorService, notify_compactor_decommission, start_compactor_service,
    wait_for_compactor_decommission,
};
use quickwit_config::NodeConfig;
use quickwit_config::service::QuickwitService;
use quickwit_index_management::{IndexService as IndexManager, IndexServiceError};
use quickwit_indexing::actors::IndexingService;
use quickwit_indexing::{IndexingSplitCache, start_indexing_service};
use quickwit_ingest::router_client::IngestRouterClient;
use quickwit_jaeger::JaegerService;
use quickwit_janitor::{JanitorService, start_janitor_service};
use quickwit_metastore::{ListIndexesMetadataResponseExt, MetastoreResolver};
use quickwit_opentelemetry::otlp::{OtlpGrpcLogsService, OtlpGrpcTracesService};
use quickwit_proto::compaction::CompactionPlannerServiceClient;
use quickwit_proto::metastore::{
    EntityKind, ListIndexesMetadataRequest, MetastoreError, MetastoreService,
    MetastoreServiceClient,
};
use quickwit_proto::search::ReportSplitsRequest;
use quickwit_search::{
    SearchJobPlacer, SearchService, SearchServiceClient, SearcherContext, SearcherNode,
    SearcherPool, create_search_client_from_channel, start_searcher_service,
};
use quickwit_storage::{SearchSplitCache, StorageResolver};
pub use quickwit_telemetry_exporters::{EnvFilterReloadFn, do_nothing_env_filter_reload_fn};
pub use quickwit_transport::reload_tls_cert;
use tcp_listener::TcpListenerResolver;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;
use tower::timeout::Timeout;
use tracing::{debug, error, info, warn};
use warp::{Filter, Rejection};

pub use crate::build_info::{BuildInfo, RuntimeInfo};
pub use crate::index_api::{ListSplitsQueryParams, ListSplitsResponse};
pub use crate::ingest_api::{RestIngestResponse, RestParseFailure};
use crate::metastore::start_metastore_service_if_needed;
#[cfg(test)]
use crate::rest::recover_fn;
pub use crate::search_api::{SearchRequestQueryString, SortBy, search_request_from_api_request};

// Shorten unit-test error paths only. Integration tests need real startup discovery while
// the remote Janitor starts concurrently, and enforce their own cluster-readiness deadline.
const COMPACTION_SERVICE_DISCOVERY_TIMEOUT: Duration = if cfg!(test) {
    Duration::from_millis(100)
} else {
    Duration::from_mins(5)
};

const DISABLE_DELETE_TASK_SERVICE_ENV_KEY: &str = "QW_DISABLE_DELETE_TASK_SERVICE";

static COMPACTION_GRPC_CLIENT_METRICS_LAYER: LazyLock<GrpcMetricsLayer> =
    LazyLock::new(|| GrpcMetricsLayer::new("compaction", "client"));
static COMPACTION_GRPC_SERVER_METRICS_LAYER: LazyLock<GrpcMetricsLayer> =
    LazyLock::new(|| GrpcMetricsLayer::new("compaction", "server"));

static GRPC_COMPACTION_PLANNER_SERVICE_TIMEOUT: Duration = Duration::from_secs(1);

struct QuickwitServices {
    pub node_config: Arc<NodeConfig>,
    pub cluster: Cluster,
    /// Locally served metastore gRPC service, either the primary metastore or a read-only replica.
    pub metastore_server_opt: Option<MetastoreServiceClient>,
    pub metastore_client: MetastoreServiceClient,
    pub index_manager: IndexManager,
    pub indexing_service_opt: Option<Mailbox<IndexingService>>,
    pub ingest_router_service: IngestRouterClient,
    ingesthree_service_opt: Option<quickwit_ingest::ingesthree::IngesthreeService>,

    pub compaction_service_client_opt: Option<CompactionPlannerServiceClient>,
    pub compactor_service_opt: Option<Mailbox<CompactorService>>,
    pub janitor_service_opt: Option<Mailbox<JanitorService>>,
    pub jaeger_service_opt: Option<JaegerService>,
    pub otlp_logs_service_opt: Option<OtlpGrpcLogsService>,

    pub otlp_traces_service_opt: Option<OtlpGrpcTracesService>,
    /// We do have a search service even on nodes that are not running `search`.
    /// It is only used to serve the rest API calls and will only execute
    /// the root requests.
    pub search_service: Arc<dyn SearchService>,

    pub env_filter_reload_fn: EnvFilterReloadFn,

    _report_splits_subscription_handle_opt: Option<EventSubscriptionHandle>,
}

async fn balance_channel_for_service(
    cluster: &Cluster,
    service: QuickwitService,
) -> BalanceChannel<SocketAddr> {
    let cluster_change_stream = cluster.change_stream();
    let service_change_stream = cluster_change_stream.filter_map(move |cluster_change| {
        Box::pin(async move {
            // The Indexer channel here serves legacy ingest. Ingesthree nodes mount a
            // separate intake RPC. Indexing-service clients use setup_indexer_pool.
            let accepts_node = |node: &ClusterNode| {
                node.is_service_enabled(service)
                    && (service != QuickwitService::Indexer || node.is_ingester())
            };
            match cluster_change {
                ClusterChange::Add(node) if accepts_node(&node) => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        remote_node_id = %chitchat_id.node_id,
                        generation_id = %chitchat_id.generation_id,
                        "adding node `{}` to {} pool",
                        chitchat_id.node_id,
                        service.as_str().replace('_', " "),
                    );
                    Some(Change::Insert(node.grpc_advertise_addr, node.channel()))
                }
                ClusterChange::Update { previous, updated }
                    if accepts_node(&previous) != accepts_node(&updated) =>
                {
                    if accepts_node(&updated) {
                        Some(Change::Insert(
                            updated.grpc_advertise_addr,
                            updated.channel(),
                        ))
                    } else {
                        Some(Change::Remove(previous.grpc_advertise_addr))
                    }
                }
                ClusterChange::Remove(node) if accepts_node(&node) => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        remote_node_id = %chitchat_id.node_id,
                        generation_id = %chitchat_id.generation_id,
                        "removing node `{}` from {} pool",
                        chitchat_id.node_id,
                        service.as_str().replace('_', " "),
                    );
                    Some(Change::Remove(node.grpc_advertise_addr))
                }
                _ => None,
            }
        })
    });
    BalanceChannel::from_stream(service_change_stream)
}

/// Builds a `CompactionPlannerServiceClient` if standalone compactors are enabled
/// and the node runs the janitor or compactor.
///
/// On janitor nodes, spawns a `CompactionPlanner` actor and builds the client from
/// its mailbox. On compactor-only nodes, connects to a remote janitor via gRPC.
///
/// The second tuple element is the local planner's `ActorHandle`, returned only
/// on janitor nodes so the caller can attach it to the janitor liveness probe.
async fn get_compaction_planner_client_if_needed(
    node_config: &NodeConfig,
    cluster: &Cluster,
    universe: &Universe,
    metastore_client: &MetastoreServiceClient,
) -> anyhow::Result<(
    Option<CompactionPlannerServiceClient>,
    Option<ActorHandle<CompactionPlanner>>,
)> {
    if !node_config.enable_standalone_compactors {
        return Ok((None, None));
    }
    let is_janitor = node_config.is_service_enabled(QuickwitService::Janitor);
    let is_compactor = node_config.is_service_enabled(QuickwitService::Compactor);
    if !is_janitor && !is_compactor {
        return Ok((None, None));
    }
    if is_janitor {
        let planner = CompactionPlanner::new(metastore_client.clone(), cluster.clone());
        let (mailbox, handle) = universe.spawn_builder().spawn(planner);
        info!("compaction planner actor started on janitor node");
        let planner_client = CompactionPlannerServiceClient::tower()
            .stack_layer(COMPACTION_GRPC_SERVER_METRICS_LAYER.clone())
            .stack_layer(TimeoutLayer::new(GRPC_COMPACTION_PLANNER_SERVICE_TIMEOUT))
            .build_from_mailbox(mailbox);
        return Ok((Some(planner_client), Some(handle)));
    }
    // Compactor-only node: connect to the planner on a remote janitor.
    let balance_channel = balance_channel_for_service(cluster, QuickwitService::Janitor).await;
    let found = balance_channel
        .wait_for(COMPACTION_SERVICE_DISCOVERY_TIMEOUT, |connections| {
            !connections.is_empty()
        })
        .await;
    if !found {
        bail!("compactor is enabled but no janitor node was found in the cluster")
    }
    info!("remote compaction planner detected on janitor node");
    let planner_client = CompactionPlannerServiceClient::tower()
        .stack_layer(COMPACTION_GRPC_CLIENT_METRICS_LAYER.clone())
        .stack_layer(TimeoutLayer::new(GRPC_COMPACTION_PLANNER_SERVICE_TIMEOUT))
        .build_from_balance_channel(
            balance_channel,
            node_config.grpc_config.max_message_size,
            None,
        );
    Ok((Some(planner_client), None))
}

/// The split cache is used when a node both indexes and merges its own splits. Split compactors
/// never do; indexers only do if split compaction is disabled. The third case is a node running
/// both services, in which case the two services share it.
async fn indexing_split_cache_for_config(
    node_config: &NodeConfig,
) -> anyhow::Result<Arc<IndexingSplitCache>> {
    let runs_indexer = node_config.is_service_enabled(QuickwitService::Indexer);
    let runs_local_compactor = node_config.is_service_enabled(QuickwitService::Compactor);
    let merges_own_splits =
        runs_indexer && (!node_config.enable_standalone_compactors || runs_local_compactor);
    if merges_own_splits {
        let cache = IndexingSplitCache::from_config(
            &node_config.indexer_config,
            &node_config.data_dir_path,
        )
        .await?;
        Ok(Arc::new(cache))
    } else {
        Ok(Arc::new(IndexingSplitCache::no_caching()))
    }
}

/// Waits for an external shutdown signal or for the server supervisor to exit, then notifies all
/// other services.
///
/// Usually called when receiving a SIGTERM signal, e.g. k8s trying to
/// decomission a pod.
#[allow(clippy::too_many_arguments)]
async fn shutdown_signal_handler(
    shutdown_signal: BoxFutureInfaillible<()>,
    shutdown_token: CancellationToken,
    universe: Universe,
    ingesthree_queue_opt: Option<quickwit_ingest::ingesthree::WriteQueue>,
    indexing_service_opt: Option<Mailbox<IndexingService>>,
    ingester_decommission_timeout: Duration,
    compactor_service_opt: Option<Mailbox<CompactorService>>,
    compactor_decommission_timeout: Duration,
    grpc_shutdown_trigger_tx: oneshot::Sender<()>,
    rest_shutdown_trigger_tx: oneshot::Sender<()>,
    health_shutdown_trigger_tx_opt: Option<oneshot::Sender<()>>,
    cluster: Cluster,
) -> HashMap<String, ActorExitStatus> {
    tokio::select! {
        _ = shutdown_signal => {
            shutdown_token.cancel();
        }
        _ = shutdown_token.cancelled() => {
            error!("server supervisor exited; initiating shutdown");
        }
    }
    if let Some(queue) = &ingesthree_queue_opt {
        cluster.set_self_node_readiness(false).await;
        let indexing_service = indexing_service_opt
            .as_ref()
            .expect("file intake starts only alongside the indexing service");
        ingesthree::shutdown(queue, indexing_service, ingester_decommission_timeout).await;
    }
    let compactor_status_rx_opt = notify_compactor_decommission(compactor_service_opt.as_ref())
        .await
        .unwrap_or_else(|error| {
            error!("failed to initiate compactor decommission: {:?}", error);
            None
        });
    if let Err(error) =
        wait_for_compactor_decommission(compactor_status_rx_opt, compactor_decommission_timeout)
            .await
    {
        error!("failed to decommission compactor gracefully: {:?}", error);
    }
    let actor_exit_statuses = universe.quit().await;

    if grpc_shutdown_trigger_tx.send(()).is_err() {
        debug!("gRPC server shutdown signal receiver was dropped");
    }
    if rest_shutdown_trigger_tx.send(()).is_err() {
        debug!("REST server shutdown signal receiver was dropped");
    }
    if let Some(health_shutdown_trigger_tx) = health_shutdown_trigger_tx_opt
        && health_shutdown_trigger_tx.send(()).is_err()
    {
        debug!("health check server shutdown signal receiver was dropped");
    }
    if let Err(err) = cluster.initiate_shutdown().await {
        debug!("{err}");
    }
    actor_exit_statuses
}

pub async fn serve_quickwit(
    node_config: NodeConfig,
    _runtimes_config: RuntimesConfig,
    metastore_resolver: MetastoreResolver,
    storage_resolver: StorageResolver,
    tcp_listener_resolver: impl TcpListenerResolver,
    mut shutdown_signal: BoxFutureInfaillible<()>,
    env_filter_reload_fn: EnvFilterReloadFn,
) -> anyhow::Result<HashMap<String, ActorExitStatus>> {
    ingesthree::validate_config(&node_config)?;

    let cluster = start_cluster_service(&node_config)
        .await
        .context("failed to start cluster service")?;

    let event_broker = EventBroker::default();
    let _ingesthree_metadata_subscriptions =
        ingesthree::publish_metadata_changes(&cluster, &event_broker);
    let universe = Universe::new();
    let grpc_config = node_config.grpc_config.clone();

    // Instantiate the local metastore gRPC server for this node (the primary metastore, a
    // read-only replica, or none; the `metastore` and `metastore_read_replica` roles are mutually
    // exclusive).
    let local_metastore_server =
        start_metastore_service_if_needed(&node_config, &metastore_resolver, &event_broker)
            .await
            .context("failed to start metastore service")?;

    let primary_metastore_client = local_metastore_server
        .resolve_primary_client(&cluster, &node_config)
        .await?;

    check_cluster_configuration(
        &node_config.enabled_services,
        &node_config.peer_seeds,
        primary_metastore_client.clone(),
    )
    .await?;

    let (compaction_service_client_opt, compaction_planner_handle_opt) =
        get_compaction_planner_client_if_needed(
            &node_config,
            &cluster,
            &universe,
            &primary_metastore_client,
        )
        .await
        .context("failed to initialize compaction service client")?;

    let indexing_split_cache = indexing_split_cache_for_config(&node_config).await?;

    let router = quickwit_ingest::ingesthree::IngesthreeRouter::new(
        cluster.clone(),
        node_config.availability_zone.clone(),
        node_config.grpc_config.max_message_size,
    );
    let ingest_router_service = IngestRouterClient::from(router);

    let indexing_service_opt = if node_config.is_service_enabled(QuickwitService::Indexer) {
        let split_cache = indexing_split_cache.clone();
        let indexing_service = start_indexing_service(
            &universe,
            &node_config,
            primary_metastore_client.clone(),
            storage_resolver.clone(),
            split_cache,
            ingest_router_service.clone(),
        )
        .await
        .context("failed to start indexing service")?;
        Some(indexing_service)
    } else {
        None
    };

    let mut ingesthree_queue_opt = None;
    if node_config.is_service_enabled(QuickwitService::Indexer) {
        // Recovery may wait on storage or metastore. Shutdown must also be able to
        // cancel startup before the public servers and shutdown handler exist.
        let queue = tokio::select! {
            result = ingesthree::open(
                &node_config,
                primary_metastore_client.clone(),
                &storage_resolver,
            ) => result?,
            _ = &mut shutdown_signal => {
                cluster.initiate_shutdown().await?;
                return Ok(universe.quit().await);
            }
        };
        queue.publish_initial_capacity(&cluster).await;
        ingesthree_queue_opt = Some(queue);
        cluster.set_self_ingesthree_enabled(true).await;
    }

    // Any node can serve index management requests (create/update/delete index, etc.),
    // so we always instantiate an index manager.
    let mut index_manager =
        IndexManager::new(primary_metastore_client.clone(), storage_resolver.clone());

    if node_config.is_service_enabled(QuickwitService::Indexer)
        && node_config.indexer_config.enable_otlp_endpoint
    {
        {
            let otel_logs_index_config =
                OtlpGrpcLogsService::index_config(&node_config.default_index_root_uri)
                    .context("failed to load OTEL logs index config")?;
            let otel_traces_index_config =
                OtlpGrpcTracesService::index_config(&node_config.default_index_root_uri)
                    .context("failed to load OTEL traces index config")?;
            for (index_name, index_config) in [
                ("OTEL logs", otel_logs_index_config),
                ("OTEL traces", otel_traces_index_config),
            ] {
                match index_manager.create_index(index_config, false).await {
                    Ok(_)
                    | Err(IndexServiceError::Metastore(MetastoreError::AlreadyExists(
                        EntityKind::Index { .. },
                    ))) => {}
                    Err(error) => bail!("failed to create {index_name} index: {error}",),
                };
            }
        }
    }

    let search_split_cache_opt: Option<Arc<SearchSplitCache>> =
        if let Some(split_cache_limits) = node_config.searcher_config.split_cache {
            let search_split_cache = SearchSplitCache::with_root_path(
                node_config.data_dir_path.join("searcher-split-cache"),
                storage_resolver.clone(),
                split_cache_limits,
            )
            .context("failed to load searcher split cache")?;
            Some(search_split_cache)
        } else {
            None
        };

    // Initialize Lambda invoker if enabled and searcher service is running
    let searcher_context = if node_config.is_service_enabled(QuickwitService::Searcher) {
        if let Some(lambda_config) = &node_config.searcher_config.lambda {
            #[cfg(feature = "lambda")]
            {
                info!("initializing AWS Lambda invoker for search");
                warn!("offloading to lambda is EXPERIMENTAL. Use at your own risk");
                let invoker =
                    quickwit_lambda_client::try_get_or_deploy_invoker(lambda_config).await?;
                Arc::new(SearcherContext::new(
                    node_config.searcher_config.clone(),
                    search_split_cache_opt,
                    Some(invoker),
                ))
            }
            #[cfg(not(feature = "lambda"))]
            {
                let _ = lambda_config;
                bail!("lambda support is statically disabled, but enabled in configuration");
            }
        } else {
            Arc::new(SearcherContext::new_without_invoker(
                node_config.searcher_config.clone(),
                search_split_cache_opt,
            ))
        }
    } else {
        Arc::new(SearcherContext::new_without_invoker(
            node_config.searcher_config.clone(),
            search_split_cache_opt,
        ))
    };

    let read_replica_metastore_client_opt = local_metastore_server
        .resolve_read_only_client(&cluster, &node_config)
        .await?;

    // Search uses the read replica when configured, and the primary otherwise.
    let search_metastore_kind = if read_replica_metastore_client_opt.is_some() {
        "read_replica"
    } else {
        "primary"
    };
    let search_metastore_client = read_replica_metastore_client_opt
        .clone()
        .unwrap_or_else(|| primary_metastore_client.clone());
    info!(
        metastore_kind = search_metastore_kind,
        "configured search metastore client"
    );

    let (search_job_placer, search_service) = setup_searcher(
        &node_config,
        cluster.change_stream(),
        search_metastore_client.clone(),
        storage_resolver.clone(),
        searcher_context,
    )
    .await
    .context("failed to start searcher service")?;

    let report_splits_subscription_handle_opt =
        // DISCLAIMER: This is quirky here: We base our decision to forward the split report depending
        // on the current searcher configuration.
        if node_config.searcher_config.split_cache.is_some() {
            // The searcher receive hints about new splits to populate their index.
            Some(event_broker.subscribe::<ReportSplitsRequest>(search_job_placer.clone()))
        } else {
            None
        };

    let janitor_service_opt = if node_config.is_service_enabled(QuickwitService::Janitor) {
        let janitor_service = start_janitor_service(
            &universe,
            &node_config,
            primary_metastore_client.clone(),
            search_job_placer,
            storage_resolver.clone(),
            event_broker.clone(),
            !get_bool_from_env(DISABLE_DELETE_TASK_SERVICE_ENV_KEY, false),
            compaction_planner_handle_opt,
        )
        .await
        .context("failed to start janitor service")?;
        Some(janitor_service)
    } else {
        None
    };

    let compactor_service_opt = if node_config.is_service_enabled(QuickwitService::Compactor)
        && node_config.enable_standalone_compactors
    {
        let compaction_dir = node_config.data_dir_path.join("compaction");
        fs::create_dir_all(&compaction_dir)?;
        let compaction_root_directory = quickwit_common::temp_dir::Builder::default()
            .tempdir_in(&compaction_dir)
            .context("failed to create compaction temp directory")?;
        let compaction_client = compaction_service_client_opt
            .clone()
            .expect("compactor service enabled but no compaction client available");
        let split_cache = indexing_split_cache.clone();
        let compactor_mailbox = start_compactor_service(
            &universe,
            cluster.self_node_id(),
            compaction_client,
            &node_config.compactor_config,
            primary_metastore_client.clone(),
            storage_resolver.clone(),
            split_cache,
            event_broker.clone(),
            compaction_root_directory,
        )
        .await
        .context("failed to start compactor service")?;
        Some(compactor_mailbox)
    } else {
        None
    };

    let jaeger_service_opt = if node_config.jaeger_config.enable_endpoint
        && node_config.is_service_enabled(QuickwitService::Searcher)
    {
        let search_service = search_service.clone();
        Some(JaegerService::new(
            node_config.jaeger_config.clone(),
            search_service,
        ))
    } else {
        None
    };

    let otlp_logs_service_opt = if node_config.is_service_enabled(QuickwitService::Indexer)
        && node_config.indexer_config.enable_otlp_endpoint
    {
        Some(OtlpGrpcLogsService::new(ingest_router_service.clone()))
    } else {
        None
    };

    let otlp_traces_service_opt = if node_config.is_service_enabled(QuickwitService::Indexer)
        && node_config.indexer_config.enable_otlp_endpoint
    {
        Some(OtlpGrpcTracesService::new(
            ingest_router_service.clone(),
            None,
        ))
    } else {
        None
    };

    let grpc_listen_addr = node_config.grpc_listen_addr;
    let rest_listen_addr = node_config.rest_config.listen_addr;
    let ingester_decommission_timeout = node_config.ingest_api_config.decommission_timeout();
    let compactor_decommission_timeout = node_config.compactor_config.decommission_timeout();
    let quickwit_services: Arc<QuickwitServices> = Arc::new(QuickwitServices {
        node_config: Arc::new(node_config),
        cluster: cluster.clone(),
        metastore_server_opt: local_metastore_server.client().cloned(),
        metastore_client: primary_metastore_client.clone(),
        _report_splits_subscription_handle_opt: report_splits_subscription_handle_opt,
        index_manager,
        indexing_service_opt: indexing_service_opt.clone(),
        ingest_router_service,
        ingesthree_service_opt: match ingesthree_queue_opt.as_ref() {
            Some(queue) => Some(
                quickwit_ingest::ingesthree::IngesthreeService::new(
                    primary_metastore_client.clone(),
                    queue.clone(),
                )
                .with_metadata_updates(&cluster)
                .await,
            ),
            None => None,
        },
        compaction_service_client_opt,
        compactor_service_opt: compactor_service_opt.clone(),
        janitor_service_opt,
        jaeger_service_opt,
        otlp_logs_service_opt,
        otlp_traces_service_opt,
        search_service,
        env_filter_reload_fn,
    });
    // Setup and start gRPC server.
    let (grpc_readiness_trigger_tx, grpc_readiness_signal_rx) = oneshot::channel::<()>();
    let grpc_readiness_trigger = Box::pin(async move {
        if grpc_readiness_trigger_tx.send(()).is_err() {
            debug!("gRPC server readiness signal receiver was dropped");
        }
    });
    let (grpc_shutdown_trigger_tx, grpc_shutdown_signal_rx) = oneshot::channel::<()>();
    let grpc_shutdown_signal = Box::pin(async move {
        if grpc_shutdown_signal_rx.await.is_err() {
            debug!("gRPC server shutdown trigger sender was dropped");
        }
    });
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    let grpc_server = grpc::start_grpc_server(
        tcp_listener_resolver.resolve(grpc_listen_addr).await?,
        grpc_config,
        quickwit_services.clone(),
        grpc_readiness_trigger,
        grpc_shutdown_signal,
        health_service,
    );
    // Setup and start REST server.
    let (rest_readiness_trigger_tx, rest_readiness_signal_rx) = oneshot::channel::<()>();
    let rest_readiness_trigger = Box::pin(async move {
        if rest_readiness_trigger_tx.send(()).is_err() {
            debug!("REST server readiness signal receiver was dropped");
        }
    });
    let (rest_shutdown_trigger_tx, rest_shutdown_signal_rx) = oneshot::channel::<()>();
    let rest_shutdown_signal = Box::pin(async move {
        if rest_shutdown_signal_rx.await.is_err() {
            debug!("REST server shutdown trigger sender was dropped");
        }
    });

    let rest_server = rest::start_rest_server(
        tcp_listener_resolver.resolve(rest_listen_addr).await?,
        quickwit_services.clone(),
        rest_readiness_trigger,
        rest_shutdown_signal,
    );

    // Setup and start the optional plaintext health-check server. It only runs when a health
    // listen port is configured (`health.listen_port` or `QW_HEALTH_LISTEN_PORT`).
    let (health_shutdown_trigger_tx_opt, health_server_fut) =
        if let Some(health_config) = quickwit_services.node_config.health_config.clone() {
            let health_listener = tcp_listener_resolver
                .resolve(health_config.listen_addr)
                .await?;
            let health_readiness_trigger = Box::pin(async {});
            let (health_shutdown_trigger_tx, health_shutdown_signal_rx) = oneshot::channel::<()>();
            let health_shutdown_signal = Box::pin(async move {
                if health_shutdown_signal_rx.await.is_err() {
                    debug!("health check server shutdown trigger sender was dropped");
                }
            });
            let health_server = rest::start_health_check_server(
                health_listener,
                quickwit_services.clone(),
                health_readiness_trigger,
                health_shutdown_signal,
            );
            (
                Some(health_shutdown_trigger_tx),
                futures::future::Either::Left(health_server),
            )
        } else {
            (
                None,
                futures::future::Either::Right(futures::future::ready(anyhow::Ok(()))),
            )
        };

    let readiness_reporting_fut = advertise_node_readiness(
        cluster.clone(),
        grpc_readiness_signal_rx,
        rest_readiness_signal_rx,
        health_reporter,
    );

    let shutdown_token = CancellationToken::new();
    let intake_health = ingesthree::watch_writer(
        &cluster,
        ingesthree_queue_opt.clone(),
        shutdown_token.clone(),
    );
    let shutdown_handle = tokio::spawn(shutdown_signal_handler(
        shutdown_signal,
        shutdown_token.clone(),
        universe,
        ingesthree_queue_opt,
        indexing_service_opt,
        ingester_decommission_timeout,
        compactor_service_opt,
        compactor_decommission_timeout,
        grpc_shutdown_trigger_tx,
        rest_shutdown_trigger_tx,
        health_shutdown_trigger_tx_opt,
        cluster.clone(),
    ));
    let grpc_join_handle = async move {
        spawn_named_task(grpc_server, "grpc_server")
            .await
            .context("gRPC server task failed")?
            .context("gRPC server failed")
    };

    let rest_join_handle = async move {
        spawn_named_task(rest_server, "rest_server")
            .await
            .context("REST server task failed")?
            .context("REST server failed")
    };

    let health_join_handle = async move {
        spawn_named_task(health_server_fut, "health_server")
            .await
            .context("health check server task failed")?
            .context("health check server failed")
    };

    let chitchat_server_handle = cluster.chitchat_server_termination_watcher().await;

    if let Err(error) = tokio::try_join!(
        grpc_join_handle,
        rest_join_handle,
        health_join_handle,
        chitchat_server_handle,
        readiness_reporting_fut,
        intake_health
    ) {
        error!(?error, "server failed");
        shutdown_token.cancel();
        if let Err(shutdown_error) = shutdown_handle.await {
            error!(
                ?shutdown_error,
                "failed to gracefully shut down services after server failure"
            );
        }
        return Err(error);
    }

    let actor_exit_statuses = shutdown_handle
        .await
        .context("failed to gracefully shutdown services")?;
    Ok(actor_exit_statuses)
}

async fn setup_searcher(
    node_config: &NodeConfig,
    cluster_change_stream: ClusterChangeStream,
    metastore: MetastoreServiceClient,
    storage_resolver: StorageResolver,
    searcher_context: Arc<SearcherContext>,
) -> anyhow::Result<(SearchJobPlacer, Arc<dyn SearchService>)> {
    let searcher_pool = SearcherPool::default();
    let search_job_placer = SearchJobPlacer::new(searcher_pool.clone());

    let search_service = start_searcher_service(
        metastore,
        storage_resolver,
        search_job_placer.clone(),
        searcher_context,
    )
    .await?;
    let search_service_clone = search_service.clone();
    let max_message_size = node_config.grpc_config.max_message_size;
    let request_timeout = node_config.searcher_config.request_timeout();
    let searcher_change_stream = cluster_change_stream.filter_map(move |cluster_change| {
        let search_service_clone = search_service_clone.clone();
        Box::pin(async move {
            match cluster_change {
                ClusterChange::Add(node) if node.is_searcher() => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        remote_node_id = %chitchat_id.node_id,
                        generation_id = %chitchat_id.generation_id,
                        "adding node `{}` to searcher pool",
                        chitchat_id.node_id,
                    );
                    let grpc_addr = node.grpc_advertise_addr;
                    let client = if node.is_self_node() {
                        SearchServiceClient::from_service(search_service_clone, grpc_addr)
                    } else {
                        let timeout_channel = Timeout::new(node.channel(), request_timeout);
                        create_search_client_from_channel(
                            grpc_addr,
                            timeout_channel,
                            max_message_size,
                        )
                    };
                    Some(Change::Insert(
                        grpc_addr,
                        SearcherNode {
                            node_id: node.node_id.clone(),
                            client,
                        },
                    ))
                }
                ClusterChange::Remove(node) if node.is_searcher() => {
                    let chitchat_id = node.chitchat_id();
                    info!(
                        remote_node_id = %chitchat_id.node_id,
                        generation_id = %chitchat_id.generation_id,
                        "removing node `{}` from searcher pool",
                        chitchat_id.node_id,
                    );
                    Some(Change::Remove(node.grpc_advertise_addr))
                }
                _ => None,
            }
        })
    });
    searcher_pool.listen_for_changes(searcher_change_stream);
    Ok((search_job_placer, search_service))
}

fn require<T: Clone + Send>(
    val_opt: Option<T>,
) -> impl Filter<Extract = (T,), Error = Rejection> + Clone {
    warp::any().and_then(move || {
        let val_opt_clone = val_opt.clone();
        async move {
            if let Some(val) = val_opt_clone {
                Ok(val)
            } else {
                Err(warp::reject())
            }
        }
    })
}

fn with_arg<T: Clone + Send>(arg: T) -> impl Filter<Extract = (T,), Error = Infallible> + Clone {
    warp::any().map(move || arg.clone())
}

/// Latches node readiness once both the gRPC and REST servers are accepting connections.
async fn advertise_node_readiness(
    cluster: Cluster,
    grpc_readiness_signal_rx: oneshot::Receiver<()>,
    rest_readiness_signal_rx: oneshot::Receiver<()>,
    health_reporter: HealthReporter,
) -> anyhow::Result<()> {
    cluster.set_self_node_readiness(false).await;
    // Set the initial health status to `NotServing` with "" meaning all services, as per
    // https://github.com/grpc/grpc/blob/master/doc/health-checking.md
    health_reporter
        .set_service_status("", ServingStatus::NotServing)
        .await;

    grpc_readiness_signal_rx
        .await
        .context("gRPC readiness trigger was dropped before firing")?;
    info!("gRPC server is ready");

    rest_readiness_signal_rx
        .await
        .context("REST readiness trigger was dropped before firing")?;
    info!("REST server is ready");

    cluster.set_self_node_readiness(true).await;
    health_reporter
        .set_service_status("", ServingStatus::Serving)
        .await;
    info!("node is ready");
    Ok(())
}

/// Displays some warnings if the cluster runs a file-backed metastore or serves file-backed
/// indexes.
async fn check_cluster_configuration(
    services: &HashSet<QuickwitService>,
    peer_seeds: &[String],
    metastore: MetastoreServiceClient,
) -> anyhow::Result<()> {
    if !services.contains(&QuickwitService::Metastore) || peer_seeds.is_empty() {
        return Ok(());
    }
    if metastore
        .endpoints()
        .iter()
        .any(|uri| !uri.protocol().is_database())
    {
        warn!(
            metastore_endpoints=?metastore.endpoints(),
            "Using a file-backed metastore in cluster mode is not recommended for production use.
            Running multiple file-backed metastores simultaneously can lead to data loss.");
    }
    let file_backed_indexes = metastore
        .list_indexes_metadata(ListIndexesMetadataRequest::all())
        .await?
        .deserialize_indexes_metadata()
        .await?
        .into_iter()
        .filter(|index_metadata| index_metadata.index_uri().protocol().is_file_storage())
        .collect::<Vec<_>>();
    if !file_backed_indexes.is_empty() {
        let index_ids = file_backed_indexes
            .iter()
            .map(|index_metadata| index_metadata.index_id())
            .join(", ");
        let index_uris = file_backed_indexes
            .iter()
            .map(|index_metadata| index_metadata.index_uri())
            .join(", ");
        warn!(
            index_ids=%index_ids,
            index_uris=%index_uris,
            "Found some file-backed indexes in the metastore. Some nodes in the cluster may not have access to all index files."
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use quickwit_cluster::{ChitchatTransport, ClusterNode, create_cluster_for_test};
    use quickwit_common::uri::Uri;
    use quickwit_config::SearcherConfig;
    use quickwit_metastore::{IndexMetadata, metastore_for_test};
    use quickwit_proto::metastore::{ListIndexesMetadataResponse, MockMetastoreService};
    use quickwit_search::Job;
    use tonic::transport::{Channel, Server};
    use tonic_health::pb::HealthCheckRequest;
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::server::health_reporter;

    use super::*;

    #[tokio::test]
    async fn test_check_cluster_configuration() {
        let services = HashSet::from_iter([QuickwitService::Metastore]);
        let peer_seeds = ["192.168.0.12:7280".to_string()];
        let mut mock_metastore = MockMetastoreService::new();

        mock_metastore
            .expect_endpoints()
            .return_const(vec![Uri::for_test("file:///qwdata/indexes")]);
        mock_metastore
            .expect_list_indexes_metadata()
            .return_once(|_| {
                Ok(ListIndexesMetadataResponse::for_test(vec![
                    IndexMetadata::for_test("test-index", "file:///qwdata/indexes/test-index"),
                ]))
            });

        check_cluster_configuration(
            &services,
            &peer_seeds,
            MetastoreServiceClient::from_mock(mock_metastore),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_readiness_updates() {
        let transport = ChitchatTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap();
        let (grpc_readiness_trigger_tx, grpc_readiness_signal_rx) = oneshot::channel();
        let (rest_readiness_trigger_tx, rest_readiness_signal_rx) = oneshot::channel();

        let (health_reporter, health_service) = health_reporter();
        let (client, server) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            Server::builder()
                .add_service(health_service)
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
                .await
                .unwrap();
        });
        let mut client_opt = Some(client);
        let connector = tower::service_fn(move |_: http::Uri| {
            let client = client_opt.take().unwrap();
            async move { Ok::<_, Infallible>(hyper_util::rt::TokioIo::new(client)) }
        });
        let channel = Channel::builder("http://[::]:50051".parse().unwrap())
            .connect_with_connector(connector)
            .await
            .unwrap();

        let mut health_client = HealthClient::new(channel);

        let readiness_handle = tokio::spawn(advertise_node_readiness(
            cluster.clone(),
            grpc_readiness_signal_rx,
            rest_readiness_signal_rx,
            health_reporter,
        ));
        assert!(!cluster.is_self_node_ready().await);

        let request = tonic::Request::new(HealthCheckRequest::default());
        let response = health_client.check(request).await.unwrap().into_inner();
        assert_eq!(response.status(), ServingStatus::NotServing.into());

        // A single server reaching its accept loop is not enough: both are required.
        grpc_readiness_trigger_tx.send(()).unwrap();
        assert!(!cluster.is_self_node_ready().await);

        rest_readiness_trigger_tx.send(()).unwrap();
        readiness_handle.await.unwrap().unwrap();
        assert!(cluster.is_self_node_ready().await);

        let request = tonic::Request::new(HealthCheckRequest::default());
        let response = health_client.check(request).await.unwrap().into_inner();
        assert_eq!(response.status(), ServingStatus::Serving.into());

        // A server that dies before signalling readiness drops its trigger, which must surface as
        // an error rather than leaving the node silently stuck in a not-ready state.
        let stuck_cluster = create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap();
        let (dropped_grpc_readiness_trigger_tx, grpc_readiness_signal_rx) =
            oneshot::channel::<()>();
        let (_rest_readiness_trigger_tx, rest_readiness_signal_rx) = oneshot::channel::<()>();
        let (stuck_health_reporter, _stuck_health_service) =
            tonic_health::server::health_reporter();
        drop(dropped_grpc_readiness_trigger_tx);

        let error = advertise_node_readiness(
            stuck_cluster.clone(),
            grpc_readiness_signal_rx,
            rest_readiness_signal_rx,
            stuck_health_reporter,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("gRPC readiness trigger"));
        assert!(!stuck_cluster.is_self_node_ready().await);
    }

    #[tokio::test]
    async fn test_setup_searcher() {
        let node_config = NodeConfig::for_test();
        let searcher_context = Arc::new(SearcherContext::new_without_invoker(
            SearcherConfig::default(),
            None,
        ));
        let metastore = metastore_for_test();
        let (change_stream, change_stream_tx) = ClusterChangeStream::new_unbounded();
        let storage_resolver = StorageResolver::unconfigured();
        let (search_job_placer, _searcher_service) = setup_searcher(
            &node_config,
            change_stream,
            metastore,
            storage_resolver,
            searcher_context,
        )
        .await
        .unwrap();

        struct DummyJob(String);

        impl Job for DummyJob {
            fn split_id(&self) -> &str {
                &self.0
            }

            fn cost(&self) -> usize {
                1
            }
        }
        search_job_placer
            .assign_job(DummyJob("job-1".to_string()), &HashSet::new())
            .await
            .unwrap_err();

        let self_node = ClusterNode::for_test("node-1", 1337, true, &["searcher"]).await;
        change_stream_tx
            .send(ClusterChange::Add(self_node.clone()))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        let searcher_client = search_job_placer
            .assign_job(DummyJob("job-1".to_string()), &HashSet::new())
            .await
            .unwrap();
        assert!(searcher_client.is_local());

        change_stream_tx
            .send(ClusterChange::Remove(self_node))
            .unwrap();

        let node = ClusterNode::for_test("node-1", 1337, false, &["searcher"]).await;
        change_stream_tx.send(ClusterChange::Add(node)).unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;

        let searcher_client = search_job_placer
            .assign_job(DummyJob("job-1".to_string()), &HashSet::new())
            .await
            .unwrap();
        assert!(!searcher_client.is_local());
    }

    #[tokio::test]
    async fn test_compaction_service_on_janitor_node() {
        let transport = ChitchatTransport::default();
        let cluster =
            create_cluster_for_test(Vec::new(), &["janitor", "indexer"], &transport, true)
                .await
                .unwrap();
        let universe = Universe::new();
        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());

        // Janitor + indexer with standalone compactors enabled: planner client is returned.
        let mut node_config = NodeConfig::for_test_with_standalone_compactors();
        node_config.enabled_services =
            HashSet::from([QuickwitService::Janitor, QuickwitService::Indexer]);
        let (client_opt, handle_opt) =
            get_compaction_planner_client_if_needed(&node_config, &cluster, &universe, &metastore)
                .await
                .unwrap();
        assert!(client_opt.is_some());
        assert!(handle_opt.is_some());

        // With compactor + janitor enabled, planner client is also returned.
        node_config.enabled_services = HashSet::from([
            QuickwitService::Janitor,
            QuickwitService::Indexer,
            QuickwitService::Compactor,
        ]);
        let (client_opt, handle_opt) =
            get_compaction_planner_client_if_needed(&node_config, &cluster, &universe, &metastore)
                .await
                .unwrap();
        assert!(client_opt.is_some());
        assert!(handle_opt.is_some());

        // Neither janitor nor compactor: no client, no handle.
        node_config.enabled_services = HashSet::from([QuickwitService::Indexer]);
        let (client_opt, handle_opt) =
            get_compaction_planner_client_if_needed(&node_config, &cluster, &universe, &metastore)
                .await
                .unwrap();
        assert!(client_opt.is_none());
        assert!(handle_opt.is_none());

        // Standalone compactors disabled: short-circuit returns (None, None) regardless of
        // which services are enabled.
        node_config.enable_standalone_compactors = false;
        node_config.enabled_services =
            HashSet::from([QuickwitService::Janitor, QuickwitService::Indexer]);
        let (client_opt, handle_opt) =
            get_compaction_planner_client_if_needed(&node_config, &cluster, &universe, &metastore)
                .await
                .unwrap();
        assert!(client_opt.is_none());
        assert!(handle_opt.is_none());

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexing_split_cache_for_config() {
        async fn cache_created(services: &[QuickwitService], standalone_compactors: bool) -> bool {
            let temp_dir = tempfile::tempdir().unwrap();
            let mut node_config = NodeConfig::for_test();
            node_config.data_dir_path = temp_dir.path().to_path_buf();
            node_config.enabled_services = services.iter().copied().collect();
            node_config.enable_standalone_compactors = standalone_compactors;
            indexing_split_cache_for_config(&node_config).await.unwrap();
            temp_dir
                .path()
                .join("indexer-split-cache")
                .join("splits")
                .is_dir()
        }

        // Indexer merging its own splits in-pipeline → real cache.
        assert!(cache_created(&[QuickwitService::Indexer], false).await);
        // Indexer co-located with a compactor → shared real cache.
        assert!(
            cache_created(
                &[QuickwitService::Indexer, QuickwitService::Compactor],
                true
            )
            .await
        );
        // Indexer offloading merges to remote compactors → no cache.
        assert!(!cache_created(&[QuickwitService::Indexer], true).await);
        // Standalone compactor → no cache by default.
        assert!(!cache_created(&[QuickwitService::Compactor], true).await);
        // Node that neither produces nor merges splits → no cache.
        assert!(!cache_created(&[QuickwitService::Searcher], false).await);
    }

    #[tokio::test]
    async fn test_planner_client_times_out_when_planner_never_responds() {
        use std::time::Instant;

        use quickwit_proto::compaction::{
            CompactionError, CompactionPlannerService, ReportStatusRequest,
        };

        let universe = Universe::new();
        let (planner_mailbox, _planner_inbox) = universe.create_test_mailbox::<CompactionPlanner>();
        let planner_client = CompactionPlannerServiceClient::tower()
            .stack_layer(TimeoutLayer::new(GRPC_COMPACTION_PLANNER_SERVICE_TIMEOUT))
            .build_from_mailbox(planner_mailbox);

        let request = ReportStatusRequest {
            node_id: "test-node".to_string(),
            available_slots: 1,
            ..Default::default()
        };
        let start = Instant::now();
        let error = planner_client.report_status(request).await.unwrap_err();
        let elapsed = start.elapsed();

        assert!(matches!(error, CompactionError::Timeout(_)));
        assert!(elapsed >= GRPC_COMPACTION_PLANNER_SERVICE_TIMEOUT);
        assert!(elapsed < GRPC_COMPACTION_PLANNER_SERVICE_TIMEOUT * 5);

        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_compaction_service_returns_error_when_no_janitor() {
        let transport = ChitchatTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, false)
            .await
            .unwrap();
        let universe = Universe::new();
        let metastore = MetastoreServiceClient::from_mock(MockMetastoreService::new());

        let mut node_config = NodeConfig::for_test_with_standalone_compactors();
        node_config.enabled_services =
            HashSet::from([QuickwitService::Indexer, QuickwitService::Compactor]);
        let result =
            get_compaction_planner_client_if_needed(&node_config, &cluster, &universe, &metastore)
                .await;
        assert!(result.is_err());

        universe.assert_quit().await;
    }
}
