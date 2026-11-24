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

use std::collections::HashSet;

use quickwit_common::rate_limited_error;
use quickwit_doc_mapper::DocMapper;
use quickwit_proto::ingest::{DocBatchV2, ParseFailure, ParseFailureReason};
use quickwit_proto::types::DocUid;
use serde_json_borrow::Value as JsonValue;

use crate::DocBatchV2Builder;

pub(crate) fn is_document_validation_enabled() -> bool {
    !quickwit_common::get_bool_from_env_cached!("QW_DISABLE_DOCUMENT_VALIDATION", false)
}

/// Validates a batch of docs.
///
/// Returns a batch of valid docs and the list of errors.
pub(crate) fn validate_doc_batch_impl(
    doc_batch: DocBatchV2,
    doc_mapper: &DocMapper,
) -> (DocBatchV2, Vec<ParseFailure>) {
    let mut parse_failures: Vec<ParseFailure> = Vec::new();
    let mut invalid_doc_ids: HashSet<DocUid> = HashSet::default();
    for (doc_uid, doc_bytes) in doc_batch.docs() {
        if let Err((reason, message)) = validate_document(doc_mapper, &doc_bytes) {
            let parse_failure = ParseFailure {
                doc_uid: Some(doc_uid),
                reason: reason as i32,
                message,
            };
            invalid_doc_ids.insert(doc_uid);
            parse_failures.push(parse_failure);
        }
    }
    if invalid_doc_ids.is_empty() {
        // All docs are valid! We don't need to build a valid doc batch.
        return (doc_batch, parse_failures);
    }
    let mut valid_doc_batch_builder = DocBatchV2Builder::default();
    for (doc_uid, doc_bytes) in doc_batch.docs() {
        if !invalid_doc_ids.contains(&doc_uid) {
            valid_doc_batch_builder.add_doc(doc_uid, &doc_bytes);
        }
    }
    let valid_doc_batch: DocBatchV2 = valid_doc_batch_builder.build().unwrap_or_default();
    assert_eq!(
        valid_doc_batch.num_docs() + parse_failures.len(),
        doc_batch.num_docs()
    );
    (valid_doc_batch, parse_failures)
}

fn validate_document(
    doc_mapper: &DocMapper,
    doc_bytes: &[u8],
) -> Result<(), (ParseFailureReason, String)> {
    let Ok(json_doc) = serde_json::from_slice::<serde_json_borrow::Value>(doc_bytes) else {
        return Err((
            ParseFailureReason::InvalidJson,
            "failed to parse JSON document".to_string(),
        ));
    };
    let JsonValue::Object(json_obj) = json_doc else {
        return Err((
            ParseFailureReason::InvalidJson,
            "JSON document is not an object".to_string(),
        ));
    };
    if let Err(error) = doc_mapper.validate_json_obj(&json_obj) {
        rate_limited_error!(
            limit_per_min = 6,
            "failed to validate JSON document: {}",
            error
        );
        return Err((ParseFailureReason::InvalidSchema, error.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use quickwit_proto::types::DocUid;

    use super::*;

    #[test]
    fn test_validate_doc_batch() {
        let doc_mapping: quickwit_config::DocMapping = serde_json::from_str(
            r#"{
            "mode": "strict",
            "field_mappings": [
                {
                    "name": "doc",
                    "type": "text"
                }
            ]
        }"#,
        )
        .unwrap();
        let doc_mapper = quickwit_config::build_doc_mapper(
            &doc_mapping,
            &quickwit_config::SearchSettings::default(),
        )
        .unwrap();
        let doc_batch = DocBatchV2::default();

        let (_, parse_failures) = validate_doc_batch_impl(doc_batch, &doc_mapper);
        assert_eq!(parse_failures.len(), 0);

        let doc_batch =
            DocBatchV2::for_test(["", "[]", r#"{"foo": "bar"}"#, r#"{"doc": "test-doc-000"}"#]);
        let (doc_batch, parse_failures) = validate_doc_batch_impl(doc_batch, &doc_mapper);
        assert_eq!(parse_failures.len(), 3);

        let parse_failure_0 = &parse_failures[0];
        assert_eq!(parse_failure_0.doc_uid(), DocUid::for_test(0));
        assert_eq!(parse_failure_0.reason(), ParseFailureReason::InvalidJson);
        assert!(parse_failure_0.message.contains("parse JSON document"));

        let parse_failure_1 = &parse_failures[1];
        assert_eq!(parse_failure_1.doc_uid(), DocUid::for_test(1));
        assert_eq!(parse_failure_1.reason(), ParseFailureReason::InvalidJson);
        assert!(parse_failure_1.message.contains("not an object"));

        let parse_failure_2 = &parse_failures[2];
        assert_eq!(parse_failure_2.doc_uid(), DocUid::for_test(2));
        assert_eq!(parse_failure_2.reason(), ParseFailureReason::InvalidSchema);
        assert!(parse_failure_2.message.contains("not declared"));

        assert_eq!(doc_batch.num_docs(), 1);
        assert_eq!(doc_batch.doc_uids[0], DocUid::for_test(3));
        let (valid_doc_uid, valid_doc_bytes) = doc_batch.docs().next().unwrap();
        assert_eq!(valid_doc_uid, DocUid::for_test(3));
        assert_eq!(&valid_doc_bytes, r#"{"doc": "test-doc-000"}"#.as_bytes());
    }
}
