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

use quickwit_config::{INGEST_V2_SOURCE_ID, IngestApiConfig, validate_identifier};
use quickwit_ingest::router_client::IngestRouterClient;
use quickwit_ingest::{IngestServiceError, doc_batch_v2_from_ndjson};
use quickwit_proto::ingest::{CommitTypeV2, IngestRequestV2, IngestSubrequest};
use quickwit_proto::types::IndexId;
use serde::Deserialize;
use warp::{Filter, Rejection};

use super::RestIngestResponse;
use crate::decompression::get_body_bytes;
use crate::rest_api_response::into_rest_api_response;
use crate::{Body, BodyFormat, with_arg};

#[derive(utoipa::OpenApi)]
#[openapi(paths(ingest))]
pub struct IngestApi;

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct IngestOptions {
    #[serde(alias = "commit", default = "IngestOptions::default_commit_type")]
    commit_type: CommitTypeV2,
    #[serde(default)]
    use_legacy_ingest: bool,
    #[serde(default)]
    detailed_response: bool,
}

impl IngestOptions {
    // This default implementation is necessary because `CommitTypeV2::default()` is
    // `CommitTypeV2::Unspecified`.
    fn default_commit_type() -> CommitTypeV2 {
        CommitTypeV2::Auto
    }
}

pub(crate) fn ingest_api_handlers(
    ingest_router: impl Into<IngestRouterClient>,
    config: IngestApiConfig,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    ingest_handler(ingest_router.into(), config)
}

fn ingest_filter(
    config: IngestApiConfig,
) -> impl Filter<Extract = (String, Body, IngestOptions), Error = Rejection> + Clone {
    warp::path!(String / "ingest")
        .and(warp::post())
        .and(get_body_bytes(
            config.max_body_size,
            config.max_decompressed_body_size,
            "ingest",
        ))
        .and(warp::query::<IngestOptions>())
}

fn ingest_handler(
    ingest_router: IngestRouterClient,
    config: IngestApiConfig,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    ingest_filter(config)
        .and(with_arg(ingest_router))
        .then(
            move |index_id, body, ingest_options: IngestOptions, ingest_router| async move {
                if ingest_options.use_legacy_ingest
                    || ingest_options.commit_type != CommitTypeV2::Auto
                {
                    return Err(IngestServiceError::BadRequest(
                        "ingesthree supports Auto commits without legacy ingest".to_string(),
                    ));
                }
                ingest(index_id, body, ingest_options, ingest_router).await
            },
        )
        .map(|result| into_rest_api_response(result, BodyFormat::default()))
        .boxed()
}

#[utoipa::path(
    post,
    tag = "Ingest",
    path = "/{index_id}/ingest",
    request_body(content = String, description = "Documents to ingest in NDJSON format and limited to 10MB", content_type = "application/json"),
    responses(
        (status = 200, description = "Successfully ingested documents.", body = RestIngestResponse)
    ),
    params(
        ("index_id" = String, Path, description = "The index ID to add docs to."),
        ("commit" = Option<String>, Query, description = "Commit mode (auto)."),
    )
)]
/// Ingest documents
async fn ingest(
    index_id: IndexId,
    body: Body,
    ingest_options: IngestOptions,
    ingest_router: IngestRouterClient,
) -> Result<RestIngestResponse, IngestServiceError> {
    let doc_batch_opt = doc_batch_v2_from_ndjson(body.content);

    let Some(doc_batch) = doc_batch_opt else {
        return Ok(RestIngestResponse::default());
    };
    let num_docs_for_processing = doc_batch.num_docs() as u64;
    let doc_batch_clone_opt = if ingest_options.detailed_response {
        Some(doc_batch.clone())
    } else {
        None
    };

    // Validate index ID early because propagating back the right error (400)
    // from deeper ingest layers is harder
    if validate_identifier("", &index_id).is_err() {
        return Err(IngestServiceError::BadRequest(
            "invalid index ID".to_string(),
        ));
    }

    let subrequest = IngestSubrequest {
        subrequest_id: 0,
        index_id,
        source_id: INGEST_V2_SOURCE_ID.to_string(),
        doc_batch: Some(doc_batch),
    };
    let request = IngestRequestV2 {
        commit_type: ingest_options.commit_type as i32,
        subrequests: vec![subrequest],
    };
    let response = ingest_router.ingest(request).await?;
    RestIngestResponse::from_ingest_response(
        response,
        doc_batch_clone_opt.as_ref(),
        num_docs_for_processing,
    )
}

#[cfg(test)]
mod tests {
    use quickwit_config::IngestApiConfig;
    use quickwit_ingest::router_client::{IngestResponse, IngestRouterClient, MockIngestRouter};
    use quickwit_proto::ingest::IngestSuccess;
    use warp::Filter;

    use super::{RestIngestResponse, ingest_api_handlers};

    #[tokio::test]
    async fn test_ingest_api_happy_path_and_empty_payload() {
        let mut mock_router = MockIngestRouter::new();
        mock_router.expect_ingest().times(1).returning(|request| {
            assert_eq!(request.subrequests.len(), 1);
            let subrequest = &request.subrequests[0];
            assert_eq!(subrequest.index_id, "my-index");
            assert_eq!(subrequest.doc_batch.as_ref().unwrap().num_docs(), 2);
            Ok(IngestResponse {
                successes: vec![IngestSuccess {
                    subrequest_id: 0,
                    num_ingested_docs: 2,
                    ..Default::default()
                }],
                failures: Vec::new(),
            })
        });
        let ingest_router = IngestRouterClient::from_mock(mock_router);
        let handler = ingest_api_handlers(ingest_router, IngestApiConfig::default());

        let resp = warp::test::request()
            .path("/my-index/ingest")
            .method("POST")
            .body("{\"id\": 1}\n{\"id\": 2}\n")
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 200);
        let ingest_response: RestIngestResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(ingest_response.num_docs_for_processing, 2);
        assert_eq!(ingest_response.num_ingested_docs, Some(2));

        // Empty payload short-circuits without calling the router.
        let resp = warp::test::request()
            .path("/my-index/ingest")
            .method("POST")
            .body("")
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 200);
        let ingest_response: RestIngestResponse = serde_json::from_slice(resp.body()).unwrap();
        assert_eq!(ingest_response.num_docs_for_processing, 0);
    }

    #[tokio::test]
    async fn test_ingest_api_rejects_non_auto_commit() {
        let ingest_router = IngestRouterClient::mocked();
        let handler = ingest_api_handlers(ingest_router, IngestApiConfig::default());
        for query in ["commit=force", "commit=wait_for"] {
            let resp = warp::test::request()
                .path(&format!("/my-index/ingest?{query}"))
                .method("POST")
                .body(r#"{"id": 1}"#)
                .reply(&handler)
                .await;
            assert_eq!(resp.status(), 400);
        }
    }

    #[tokio::test]
    async fn test_ingest_api_returns_413_if_body_is_too_large() {
        let config: IngestApiConfig = serde_json::from_str(r#"{ "max_body_size": "1" }"#).unwrap();
        let ingest_router = IngestRouterClient::mocked();
        let handler = ingest_api_handlers(ingest_router, config).recover(crate::recover_fn);
        let resp = warp::test::request()
            .path("/my-index/ingest")
            .method("POST")
            .body(r#"{"id": 1, "message": "push"}"#)
            .reply(&handler)
            .await;
        assert_eq!(resp.status(), 413);
    }
}
