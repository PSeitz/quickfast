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

use std::num::NonZeroUsize;

use quickwit_common::rand::append_random_suffix;
use quickwit_config::{
    IndexConfig, SourceConfig, SourceInputFormat, SourceParams, TransformConfig,
};
use quickwit_proto::metastore::{
    AddSourceRequest, CreateIndexRequest, DeleteSourceRequest, EntityKind, IndexMetadataRequest,
    MetastoreError, SourceType, ToggleSourceRequest, UpdateSourceRequest,
};
use quickwit_proto::types::IndexUid;

use super::DefaultForTest;
use crate::metastore::UpdateSourceRequestExt;
use crate::tests::cleanup_index;
use crate::{
    AddSourceRequestExt, CreateIndexRequestExt, IndexMetadataResponseExt, MetastoreServiceExt,
};

pub async fn test_metastore_add_source<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-add-source");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let source_id = format!("{index_id}--source");

    let source = SourceConfig {
        source_id: source_id.to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::void(),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };

    let add_source_request =
        AddSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap();
    metastore.add_source(add_source_request).await.unwrap();

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();

    let sources = &index_metadata.sources;
    assert_eq!(sources.len(), 1);
    assert!(sources.contains_key(&source_id));
    assert_eq!(sources.get(&source_id).unwrap().source_id, source_id);
    assert_eq!(
        sources.get(&source_id).unwrap().source_type(),
        SourceType::Void
    );

    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::AlreadyExists(EntityKind::Source { .. })
    ));
    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    IndexUid::new_with_random_ulid("index-not-found"),
                    &source
                )
                .unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));
    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    IndexUid::new_with_random_ulid(&index_id),
                    &source
                )
                .unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));
    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_update_source<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-add-source");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let source_id = format!("{index_id}--source");

    let mut source = SourceConfig {
        source_id: source_id.to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::void(),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };

    let add_source_request =
        AddSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap();
    metastore.add_source(add_source_request).await.unwrap();

    source.transform_config = Some(TransformConfig::new("del(.username)".to_string(), None));

    // Update the source twice with the same value to validate indempotency
    for _ in 0..2 {
        let update_source_request =
            UpdateSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap();
        metastore
            .update_source(update_source_request)
            .await
            .unwrap();

        let index_metadata = metastore
            .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
            .await
            .unwrap()
            .deserialize_index_metadata()
            .unwrap();

        let sources = &index_metadata.sources;
        assert_eq!(sources.len(), 1);
        assert!(sources.contains_key(&source_id));
        assert_eq!(sources.get(&source_id).unwrap().source_id, source_id);
        assert_eq!(
            sources.get(&source_id).unwrap().source_type(),
            SourceType::Void
        );
        assert_eq!(
            sources.get(&source_id).unwrap().transform_config,
            Some(TransformConfig::new("del(.username)".to_string(), None))
        );
    }

    source.source_id = "unknown-src-id".to_string();
    assert!(matches!(
        metastore
            .update_source(
                UpdateSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Source { .. })
    ));
    source.source_id = source_id;
    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    IndexUid::new_with_random_ulid("index-not-found"),
                    &source
                )
                .unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_toggle_source<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-toggle-source");
    let index_uri = format!("ram:///indexes/{index_id}");
    let index_config = IndexConfig::for_test(&index_id, &index_uri);

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();

    let source_id = format!("{index_id}--source");
    let source = SourceConfig {
        source_id: source_id.to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::void(),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };
    let add_source_request =
        AddSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap();
    metastore.add_source(add_source_request).await.unwrap();
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();
    let source = index_metadata.sources.get(&source_id).unwrap();
    assert_eq!(source.enabled, true);

    // Disable source.
    metastore
        .toggle_source(ToggleSourceRequest {
            index_uid: index_uid.clone().into(),
            source_id: source.source_id.clone(),
            enable: false,
        })
        .await
        .unwrap();
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();
    let source = index_metadata.sources.get(&source_id).unwrap();
    assert_eq!(source.enabled, false);

    // Enable source.
    metastore
        .toggle_source(ToggleSourceRequest {
            index_uid: index_uid.clone().into(),
            source_id: source.source_id.clone(),
            enable: true,
        })
        .await
        .unwrap();
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap();
    let source = index_metadata.sources.get(&source_id).unwrap();
    assert_eq!(source.enabled, true);

    cleanup_index(&mut metastore, index_uid).await;
}

pub async fn test_metastore_delete_source<MetastoreToTest: MetastoreServiceExt + DefaultForTest>() {
    let mut metastore = MetastoreToTest::default_for_test().await;

    let index_id = append_random_suffix("test-delete-source");
    let index_uri = format!("ram:///indexes/{index_id}");
    let source_id = format!("{index_id}--source");

    let source = SourceConfig {
        source_id: source_id.to_string(),
        num_pipelines: NonZeroUsize::MIN,
        enabled: true,
        source_params: SourceParams::void(),
        transform_config: None,
        input_format: SourceInputFormat::Json,
    };

    let index_config = IndexConfig::for_test(&index_id, index_uri.as_str());

    let create_index_request = CreateIndexRequest::try_from_index_config(&index_config).unwrap();
    let index_uid: IndexUid = metastore
        .create_index(create_index_request)
        .await
        .unwrap()
        .index_uid()
        .clone();
    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    IndexUid::new_with_random_ulid("index-not-found"),
                    &source
                )
                .unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));
    assert!(matches!(
        metastore
            .add_source(
                AddSourceRequest::try_from_source_config(
                    IndexUid::new_with_random_ulid(&index_id),
                    &source
                )
                .unwrap()
            )
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    metastore
        .add_source(AddSourceRequest::try_from_source_config(index_uid.clone(), &source).unwrap())
        .await
        .unwrap();
    metastore
        .delete_source(DeleteSourceRequest {
            index_uid: index_uid.clone().into(),
            source_id: source_id.clone(),
        })
        .await
        .unwrap();

    let sources = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(index_id.to_string()))
        .await
        .unwrap()
        .deserialize_index_metadata()
        .unwrap()
        .sources;
    assert!(sources.is_empty());

    assert!(matches!(
        metastore
            .delete_source(DeleteSourceRequest {
                index_uid: index_uid.clone().into(),
                source_id: source_id.to_string()
            })
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Source { .. })
    ));
    assert!(matches!(
        metastore
            .delete_source(DeleteSourceRequest {
                index_uid: Some(IndexUid::new_with_random_ulid("index-not-found")),
                source_id: source_id.to_string()
            })
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));
    assert!(matches!(
        metastore
            .delete_source(DeleteSourceRequest {
                index_uid: Some(IndexUid::new_with_random_ulid(&index_id)),
                source_id: source_id.to_string()
            })
            .await
            .unwrap_err(),
        MetastoreError::NotFound(EntityKind::Index { .. })
    ));

    cleanup_index(&mut metastore, index_uid).await;
}
