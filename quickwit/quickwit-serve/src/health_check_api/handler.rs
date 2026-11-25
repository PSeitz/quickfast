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

use quickwit_cluster::Cluster;
use quickwit_compaction::CompactorServiceHandle;
use quickwit_indexing::IndexingService;
use quickwit_janitor::JanitorService;
use tracing::error;
use warp::hyper::StatusCode;
use warp::reply::with_status;
use warp::{Filter, Rejection};

use crate::rest::{BoxedRoute, boxed_route, combine_routes, recover_fn};
use crate::with_arg;

#[derive(utoipa::OpenApi)]
#[openapi(paths(get_liveness, get_startup))]
pub struct HealthCheckApi;

/// Health check handlers.
pub(crate) fn health_check_handlers(
    cluster: Cluster,
    indexer_service_opt: Option<IndexingService>,
    janitor_service_opt: Option<JanitorService>,
    compactor_service_opt: Option<CompactorServiceHandle>,
) -> BoxedRoute {
    combine_routes([
        boxed_route(liveness_handler(
            indexer_service_opt,
            janitor_service_opt,
            compactor_service_opt,
        )),
        boxed_route(startup_handler(cluster)),
    ])
}

fn liveness_handler(
    indexer_service_opt: Option<IndexingService>,
    janitor_service_opt: Option<JanitorService>,
    compactor_service_opt: Option<CompactorServiceHandle>,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("health" / "livez")
        .and(warp::get())
        .and(with_arg(indexer_service_opt))
        .and(with_arg(janitor_service_opt))
        .and(with_arg(compactor_service_opt))
        .then(get_liveness)
        .recover(recover_fn)
}

fn startup_handler(
    cluster: Cluster,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("health" / "startupz")
        .or(warp::path!("health" / "readyz"))
        .unify()
        .and(warp::get())
        .and(with_arg(cluster))
        .then(get_startup)
        .recover(recover_fn)
}

#[utoipa::path(
    get,
    tag = "Node Health",
    path = "/livez",
    responses(
        (status = 200, description = "The service is live.", body = bool),
        (status = 503, description = "The service is not live.", body = bool),
    ),
)]
/// Get Node Liveliness
async fn get_liveness(
    indexer_service_opt: Option<IndexingService>,
    janitor_service_opt: Option<JanitorService>,
    compactor_service_opt: Option<CompactorServiceHandle>,
) -> impl warp::Reply {
    let mut is_live = true;

    if let Some(service) = indexer_service_opt.as_ref()
        && !service.is_healthy()
    {
        error!("indexer service is unhealthy");
        is_live = false;
    }
    if let Some(service) = janitor_service_opt.as_ref()
        && !service.is_healthy()
    {
        error!("janitor service is unhealthy");
        is_live = false;
    }
    if let Some(service) = compactor_service_opt.as_ref()
        && !service.is_healthy()
    {
        error!("compactor service is unhealthy");
        is_live = false;
    }
    let status_code = if is_live {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    with_status(warp::reply::json(&is_live), status_code)
}

#[utoipa::path(
    get,
    tag = "Node Health",
    path = "/startupz",
    responses(
        (status = 200, description = "The node has finished starting up.", body = bool),
        (status = 503, description = "The node is still starting up.", body = bool),
    ),
)]
/// Get Node Startup
async fn get_startup(cluster: Cluster) -> impl warp::Reply {
    let has_started = cluster.is_self_node_ready().await;
    let status_code = if has_started {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    with_status(warp::reply::json(&has_started), status_code)
}

#[cfg(test)]
mod tests {
    use quickwit_cluster::{ChitchatTransport, create_cluster_for_test};
    use quickwit_compaction::CompactorServiceHandle;

    #[tokio::test]
    async fn test_rest_search_api_health_checks() {
        let transport = ChitchatTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap();
        let health_check_handler = super::health_check_handlers(cluster.clone(), None, None, None);
        let resp = warp::test::request()
            .path("/health/livez")
            .reply(&health_check_handler)
            .await;
        assert_eq!(resp.status(), 200);

        for path in ["/health/startupz", "/health/readyz"] {
            let resp = warp::test::request()
                .path(path)
                .reply(&health_check_handler)
                .await;
            assert_eq!(resp.status(), 503, "`{path}` should report not started");
        }
        cluster.set_self_node_readiness(true).await;

        for path in ["/health/startupz", "/health/readyz"] {
            let resp = warp::test::request()
                .path(path)
                .reply(&health_check_handler)
                .await;
            assert_eq!(resp.status(), 200, "`{path}` should report started");
        }
    }

    #[tokio::test]
    async fn test_liveness_reports_unhealthy_compactor() {
        let transport = ChitchatTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &[], &transport, false)
            .await
            .unwrap();
        let unhealthy_compactor = CompactorServiceHandle::for_test_unhealthy();

        let health_check_handler =
            super::health_check_handlers(cluster.clone(), None, None, Some(unhealthy_compactor));
        let resp = warp::test::request()
            .path("/health/livez")
            .reply(&health_check_handler)
            .await;
        assert_eq!(resp.status(), 503);
    }
}
