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

use bytesize::ByteSize;
use quickwit_ingest::router_client::IngestRouterClient;
use warp::{Filter, Rejection};

use super::bulk_v2::elastic_bulk_ingest_v2;
use crate::elasticsearch_api::filter::{elastic_bulk_filter, elastic_index_bulk_filter};
use crate::elasticsearch_api::make_elastic_api_response;
use crate::format::extract_format_from_qs;
use crate::rest::recover_fn;
use crate::with_arg;

/// POST `_elastic/_bulk`
pub fn es_compat_bulk_handler(
    ingest_router: impl Into<IngestRouterClient>,
    max_body_size: ByteSize,
    max_decompressed_body_size: ByteSize,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_bulk_filter(max_body_size, max_decompressed_body_size)
        .and(with_arg(ingest_router.into()))
        .then(move |body, bulk_options, ingest_router| {
            elastic_bulk_ingest_v2(None, body, bulk_options, ingest_router)
        })
        .and(extract_format_from_qs())
        .map(make_elastic_api_response)
        .recover(recover_fn)
}

/// POST `_elastic/<index>/_bulk`
pub fn es_compat_index_bulk_handler(
    ingest_router: impl Into<IngestRouterClient>,
    max_body_size: ByteSize,
    max_decompressed_body_size: ByteSize,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    elastic_index_bulk_filter(max_body_size, max_decompressed_body_size)
        .and(with_arg(ingest_router.into()))
        .then(move |index_id, body, bulk_options, ingest_router| {
            elastic_bulk_ingest_v2(Some(index_id), body, bulk_options, ingest_router)
        })
        .and(extract_format_from_qs())
        .map(make_elastic_api_response)
        .recover(recover_fn)
        .boxed()
}
