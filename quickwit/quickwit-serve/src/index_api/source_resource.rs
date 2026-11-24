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

use bytes::Bytes;
use quickwit_config::{
    ConfigFormat, FileSourceParams, SourceConfig, SourceParams,
    load_source_config_from_user_config, load_source_config_update,
};
use quickwit_index_management::{IndexService, IndexServiceError};
use quickwit_metastore::IndexMetadataResponseExt;
use quickwit_proto::metastore::{
    DeleteSourceRequest, EntityKind, IndexMetadataRequest, MetastoreError, MetastoreService,
    MetastoreServiceClient, ToggleSourceRequest as ProtoToggleSourceRequest,
};
use quickwit_proto::types::{IndexId, IndexUid, SourceId};
use serde::{Deserialize, Serialize};
use tracing::info;
use warp::{Filter, Rejection};

use super::rest_handler::{json_body, log_failure};
use crate::format::{extract_config_format, extract_format_from_qs};
use crate::rest_api_response::into_rest_api_response;
use crate::with_arg;

#[derive(Deserialize, Serialize, utoipa::ToSchema)]
pub struct ToggleSourceRequest {
    pub enable: bool,
}

fn check_source_type(source_params: &SourceParams) -> Result<(), IndexServiceError> {
    if let SourceParams::File(FileSourceParams::Filepath(_)) = source_params {
        return Err(IndexServiceError::InvalidConfig(anyhow::anyhow!(
            "path based file sources are limited to local usage; use `quickfast index ingest` or \
             `quickfast tool local-ingest`, or configure an SQS notification file source"
        )));
    }
    Ok(())
}

pub fn create_source_handler(
    index_service: IndexService,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources")
        .and(warp::post())
        .and(extract_config_format())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::filters::body::bytes())
        .and(with_arg(index_service))
        .then(create_source)
        .map(log_failure("failed to create source"))
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

#[utoipa::path(
    post,
    tag = "Sources",
    path = "/indexes/{index_id}/sources",
    request_body = VersionedSourceConfig,
    responses(
        (status = 200, description = "Successfully created source.", body = VersionedSourceConfig)
    ),
    params(
        ("index_id" = String, Path, description = "Index ID to add the source to."),
    )
)]
/// Creates a source for an index.
pub async fn create_source(
    index_id: IndexId,
    config_format: ConfigFormat,
    source_config_bytes: Bytes,
    mut index_service: IndexService,
) -> Result<SourceConfig, IndexServiceError> {
    let source_config = load_source_config_from_user_config(config_format, &source_config_bytes)
        .map_err(IndexServiceError::InvalidConfig)?;
    check_source_type(&source_config.source_params)?;
    let index_uid: IndexUid = index_service
        .metastore()
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.clone()))
        .await?
        .deserialize_index_metadata()?
        .index_uid;
    info!(index_id = %index_id, source_id = %source_config.source_id, "create-source");
    index_service.add_source(index_uid, source_config).await
}

pub fn update_source_handler(
    index_service: IndexService,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources" / String)
        .and(warp::put())
        .and(extract_config_format())
        .and(warp::body::content_length_limit(1024 * 1024))
        .and(warp::filters::body::bytes())
        .and(with_arg(index_service))
        .then(update_source)
        .map(log_failure("failed to update source"))
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

#[utoipa::path(
    put,
    tag = "Sources",
    path = "/indexes/{index_id}/sources/{source_id}",
    request_body = VersionedSourceConfig,
    responses(
        (status = 200, description = "Successfully updated source.", body = VersionedSourceConfig)
    ),
    params(
        ("index_id" = String, Path, description = "Index ID."),
        ("source_id" = String, Path, description = "Source ID."),
    )
)]
/// Updates a source for an index.
pub async fn update_source(
    index_id: IndexId,
    source_id: SourceId,
    config_format: ConfigFormat,
    source_config_bytes: Bytes,
    mut index_service: IndexService,
) -> Result<SourceConfig, IndexServiceError> {
    let index_metadata = index_service
        .metastore()
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.clone()))
        .await?
        .deserialize_index_metadata()?;
    let existing = index_metadata.sources.get(&source_id).ok_or_else(|| {
        IndexServiceError::Metastore(MetastoreError::NotFound(EntityKind::Source {
            index_id: index_id.clone(),
            source_id: source_id.clone(),
        }))
    })?;
    let source_config = load_source_config_update(config_format, &source_config_bytes, existing)
        .map_err(IndexServiceError::InvalidConfig)?;
    if source_config.source_id != source_id {
        return Err(IndexServiceError::InvalidConfig(anyhow::anyhow!(
            "source ID in path `{source_id}` does not match body `{}`",
            source_config.source_id
        )));
    }
    check_source_type(&source_config.source_params)?;
    info!(index_id = %index_id, source_id = %source_id, "update-source");
    index_service
        .update_source(index_metadata.index_uid, source_config)
        .await
}

pub fn get_source_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources" / String)
        .and(warp::get())
        .and(with_arg(metastore))
        .then(get_source)
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

#[utoipa::path(
    get,
    tag = "Sources",
    path = "/indexes/{index_id}/sources/{source_id}",
    responses(
        (status = 200, description = "Successfully fetched source.", body = VersionedSourceConfig)
    ),
    params(
        ("index_id" = String, Path, description = "Index ID."),
        ("source_id" = String, Path, description = "Source ID."),
    )
)]
/// Fetches a source configuration.
pub async fn get_source(
    index_id: IndexId,
    source_id: SourceId,
    metastore: MetastoreServiceClient,
) -> Result<SourceConfig, IndexServiceError> {
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.clone()))
        .await?
        .deserialize_index_metadata()?;
    index_metadata
        .sources
        .get(&source_id)
        .cloned()
        .ok_or_else(|| {
            IndexServiceError::Metastore(MetastoreError::NotFound(EntityKind::Source {
                index_id,
                source_id,
            }))
        })
}

pub fn toggle_source_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources" / String / "toggle")
        .and(warp::put())
        .and(json_body::<ToggleSourceRequest>())
        .and(with_arg(metastore))
        .then(toggle_source)
        .map(log_failure("failed to toggle source"))
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

#[utoipa::path(
    put,
    tag = "Sources",
    path = "/indexes/{index_id}/sources/{source_id}/toggle",
    request_body = ToggleSourceRequest,
    responses(
        (status = 200, description = "Successfully toggled source.")
    ),
    params(
        ("index_id" = String, Path, description = "Index ID."),
        ("source_id" = String, Path, description = "Source ID."),
    )
)]
/// Enables or disables a source.
pub async fn toggle_source(
    index_id: IndexId,
    source_id: SourceId,
    request: ToggleSourceRequest,
    metastore: MetastoreServiceClient,
) -> Result<(), IndexServiceError> {
    let index_uid = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id))
        .await?
        .deserialize_index_metadata()?
        .index_uid;
    metastore
        .toggle_source(ProtoToggleSourceRequest {
            index_uid: Some(index_uid),
            source_id,
            enable: request.enable,
        })
        .await?;
    Ok(())
}

pub fn delete_source_handler(
    metastore: MetastoreServiceClient,
) -> impl Filter<Extract = (impl warp::Reply,), Error = Rejection> + Clone {
    warp::path!("indexes" / String / "sources" / String)
        .and(warp::delete())
        .and(with_arg(metastore))
        .then(delete_source)
        .map(log_failure("failed to delete source"))
        .and(extract_format_from_qs())
        .map(into_rest_api_response)
        .boxed()
}

#[utoipa::path(
    delete,
    tag = "Sources",
    path = "/indexes/{index_id}/sources/{source_id}",
    responses(
        (status = 200, description = "Successfully deleted source.")
    ),
    params(
        ("index_id" = String, Path, description = "Index ID."),
        ("source_id" = String, Path, description = "Source ID."),
    )
)]
/// Deletes a source from an index.
pub async fn delete_source(
    index_id: IndexId,
    source_id: SourceId,
    metastore: MetastoreServiceClient,
) -> Result<(), IndexServiceError> {
    let index_uid = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id))
        .await?
        .deserialize_index_metadata()?
        .index_uid;
    metastore
        .delete_source(DeleteSourceRequest {
            index_uid: Some(index_uid),
            source_id,
        })
        .await?;
    Ok(())
}
