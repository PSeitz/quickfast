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

use std::future::Future;
use std::time::Duration;

use quickwit_common::tower::{GrpcMetricsLayer, GrpcStatusCode};

use crate::GrpcServiceError;

pub(crate) fn make_tonic_request<T>(payload: T) -> tonic::Request<T> {
    let mut request = tonic::Request::new(payload);
    quickwit_common::tracing_utils::inject_current_context(request.metadata_mut());
    request
}

pub(crate) fn server_span(
    metadata: &tonic::metadata::MetadataMap,
    span: tracing::Span,
) -> tracing::Span {
    let parent_context = quickwit_common::tracing_utils::extract_context(metadata);
    let _ = <tracing::Span as tracing_opentelemetry::OpenTelemetrySpanExt>::set_parent(
        &span,
        parent_context,
    );
    span
}

pub(crate) async fn with_optional_metrics<T, E: GrpcStatusCode>(
    metrics: Option<&GrpcMetricsLayer>,
    rpc_name: &'static str,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    if let Some(metrics) = metrics {
        metrics.instrument_rpc(rpc_name, fut).await
    } else {
        fut.await
    }
}

pub(crate) async fn with_optional_timeout<T, E: GrpcServiceError>(
    timeout: Option<Duration>,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, fut)
            .await
            .unwrap_or_else(|_| {
                Err(E::new_timeout(format!(
                    "request timed out after {timeout:?}"
                )))
            })
    } else {
        fut.await
    }
}
