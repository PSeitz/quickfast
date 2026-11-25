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
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, bail};
use bytes::Bytes;
use itertools::Either;
use quickwit_common::rate_limited_tracing::rate_limited_warn;
use quickwit_config::{SourceInputFormat, TransformConfig};
use quickwit_doc_mapper::{DocMapper, DocParsingError, QuickwitDocBatch};
use quickwit_metrics::{Counter, counter, labels};
use quickwit_opentelemetry::otlp::{
    OtlpLogsError, OtlpTracesError, parse_otlp_logs_json, parse_otlp_logs_protobuf,
    parse_otlp_spans_json, parse_otlp_spans_protobuf,
};
use quickwit_proto::types::{IndexId, SourceId};
use serde::Serialize;
use tantivy::schema::{Field, Value};
use tantivy::{DateTime, Document};
use thiserror::Error;

#[cfg(feature = "vrl")]
use super::vrl_processing::*;
use crate::metrics::{PROCESSED_BYTES, PROCESSED_DOCS_TOTAL};
use crate::models::ProcessedDoc;

#[allow(clippy::enum_variant_names)]
#[derive(Error, Debug)]
pub enum DocProcessorError {
    #[error("doc mapper parse error: {0}")]
    DocMapperParsing(DocParsingError),
    #[error("JSON parse error: {0}")]
    JsonParsing(String),
    #[error("OLTP log records parse error: {0}")]
    OltpLogsParsing(OtlpLogsError),
    #[error("OLTP traces parse error: {0}")]
    OltpTracesParsing(OtlpTracesError),
    #[cfg(feature = "vrl")]
    #[error("VRL transform error: {0}")]
    Transform(VrlTerminate),
}

impl From<OtlpLogsError> for DocProcessorError {
    fn from(error: OtlpLogsError) -> Self {
        Self::OltpLogsParsing(error)
    }
}

impl From<OtlpTracesError> for DocProcessorError {
    fn from(error: OtlpTracesError) -> Self {
        Self::OltpTracesParsing(error)
    }
}

impl From<DocParsingError> for DocProcessorError {
    fn from(error: DocParsingError) -> Self {
        Self::DocMapperParsing(error)
    }
}

impl From<serde_json::Error> for DocProcessorError {
    fn from(error: serde_json::Error) -> Self {
        Self::JsonParsing(error.to_string())
    }
}

#[derive(Debug)]
pub struct DocProcessorCounter {
    pub num_docs: AtomicU64,
    pub num_docs_metric: Counter,
    pub num_bytes_metric: Counter,
}

impl Serialize for DocProcessorCounter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_u64(self.get_num_docs())
    }
}

impl DocProcessorCounter {
    fn for_index_and_doc_processor_outcome(
        index: &str,
        outcome: &'static str,
    ) -> DocProcessorCounter {
        let labels = labels!(
            "index" => quickwit_common::metrics::index_label(index).to_string(),
            "docs_processed_status" => outcome
        );
        DocProcessorCounter {
            num_docs: Default::default(),
            num_docs_metric: counter!(parent: PROCESSED_DOCS_TOTAL, labels: [labels]),
            num_bytes_metric: counter!(parent: PROCESSED_BYTES, labels: [labels]),
        }
    }

    #[inline(always)]
    fn get_num_docs(&self) -> u64 {
        self.num_docs.load(Ordering::Relaxed)
    }

    fn record_doc(&self, num_bytes: u64) {
        self.num_docs.fetch_add(1, Ordering::Relaxed);
        self.num_docs_metric.inc();
        self.num_bytes_metric.inc_by(num_bytes);
    }
}

#[derive(Debug, Serialize)]
pub struct DocProcessorCounters {
    index_id: IndexId,
    source_id: SourceId,

    /// Overall number of documents received, partitioned
    /// into 5 categories:
    /// - valid documents
    /// - number of docs that could not be parsed.
    /// - number of docs that were not valid json.
    /// - number of docs that could not be transformed.
    /// - number of docs for which the doc mapper returned an error.
    /// - number of valid docs.
    pub valid: DocProcessorCounter,
    pub doc_mapper_errors: DocProcessorCounter,
    pub transform_errors: DocProcessorCounter,
    pub json_parse_errors: DocProcessorCounter,
    pub otlp_parse_errors: DocProcessorCounter,

    /// Number of bytes that went through the indexer
    /// during its entire lifetime.
    ///
    /// Includes both valid and invalid documents.
    pub num_bytes_total: AtomicU64,
}

impl DocProcessorCounters {
    pub fn new(index_id: IndexId, source_id: SourceId) -> Self {
        let valid_docs =
            DocProcessorCounter::for_index_and_doc_processor_outcome(&index_id, "valid");
        let doc_mapper_errors =
            DocProcessorCounter::for_index_and_doc_processor_outcome(&index_id, "doc_mapper_error");
        let transform_errors =
            DocProcessorCounter::for_index_and_doc_processor_outcome(&index_id, "transform_error");
        let json_parse_errors =
            DocProcessorCounter::for_index_and_doc_processor_outcome(&index_id, "json_parse_error");
        let otlp_parse_errors =
            DocProcessorCounter::for_index_and_doc_processor_outcome(&index_id, "otlp_parse_error");
        DocProcessorCounters {
            index_id,
            source_id,

            valid: valid_docs,
            doc_mapper_errors,
            transform_errors,
            json_parse_errors,
            otlp_parse_errors,
            num_bytes_total: Default::default(),
        }
    }

    /// Returns the overall number of docs that went through the indexer (valid or not).
    pub fn num_processed_docs(&self) -> u64 {
        self.valid.get_num_docs()
            + self.doc_mapper_errors.get_num_docs()
            + self.json_parse_errors.get_num_docs()
            + self.otlp_parse_errors.get_num_docs()
            + self.transform_errors.get_num_docs()
    }

    /// Returns the overall number of docs that were sent to the indexer but were invalid.
    /// (For instance, because they were missing a required field or because their because
    /// their format was invalid)
    pub fn num_invalid_docs(&self) -> u64 {
        self.doc_mapper_errors.get_num_docs()
            + self.json_parse_errors.get_num_docs()
            + self.otlp_parse_errors.get_num_docs()
            + self.transform_errors.get_num_docs()
    }

    pub fn record_valid(&self, num_bytes: u64) {
        self.num_bytes_total.fetch_add(num_bytes, Ordering::Relaxed);
        self.valid.record_doc(num_bytes);
    }

    pub fn record_error(&self, error: DocProcessorError, num_bytes: u64) {
        self.num_bytes_total.fetch_add(num_bytes, Ordering::Relaxed);
        match error {
            DocProcessorError::DocMapperParsing(_) => {
                self.doc_mapper_errors.record_doc(num_bytes);
            }
            DocProcessorError::JsonParsing(_) => {
                self.json_parse_errors.record_doc(num_bytes);
            }
            DocProcessorError::OltpLogsParsing(_) | DocProcessorError::OltpTracesParsing(_) => {
                self.otlp_parse_errors.record_doc(num_bytes);
            }
            #[cfg(feature = "vrl")]
            DocProcessorError::Transform(_) => {
                self.transform_errors.record_doc(num_bytes);
            }
        };
    }
}

/// Converts input documents using their format, mapping, and transform.
pub struct DocumentConverter {
    doc_mapper: Arc<DocMapper>,
    timestamp_field_opt: Option<Field>,
    counters: Arc<DocProcessorCounters>,
    #[cfg(feature = "vrl")]
    transform_opt: Option<VrlProgram>,
    input_format: SourceInputFormat,
}

impl DocumentConverter {
    pub fn try_new(
        index_id: IndexId,
        source_id: SourceId,
        doc_mapper: Arc<DocMapper>,
        transform_config_opt: Option<TransformConfig>,
        input_format: SourceInputFormat,
    ) -> anyhow::Result<Self> {
        let timestamp_field_opt = extract_timestamp_field(&doc_mapper)?;
        if cfg!(not(feature = "vrl")) && transform_config_opt.is_some() {
            bail!("VRL is not enabled: please recompile with the `vrl` feature")
        }
        Ok(Self {
            doc_mapper,
            timestamp_field_opt,
            counters: Arc::new(DocProcessorCounters::new(index_id, source_id)),
            #[cfg(feature = "vrl")]
            transform_opt: transform_config_opt
                .map(VrlProgram::try_from_transform_config)
                .transpose()?,
            input_format,
        })
    }

    pub fn counters(&self) -> &Arc<DocProcessorCounters> {
        &self.counters
    }

    fn extract_timestamp(&self, doc: &impl Document) -> Result<Option<DateTime>, DocParsingError> {
        let Some(timestamp_field) = self.timestamp_field_opt else {
            return Ok(None);
        };
        let timestamp = doc
            .iter_fields_and_values()
            .find(|(field, _)| *field == timestamp_field)
            .and_then(|(_, value)| value.as_datetime())
            .ok_or_else(|| {
                DocParsingError::RequiredField("timestamp field is required".to_string())
            })?;
        Ok(Some(timestamp))
    }

    fn parse_into_batch(
        &mut self,
        raw_doc: Bytes,
        batch: &mut QuickwitDocBatch,
        otlp_payload_lens: &mut Vec<usize>,
    ) -> Result<(), DocProcessorError> {
        #[cfg(feature = "vrl")]
        if let Some(program) = &mut self.transform_opt {
            return program.parse_into_batch(self.input_format, raw_doc, batch);
        }
        let records = match self.input_format {
            SourceInputFormat::Json => return Ok(batch.parse(&raw_doc)?),
            SourceInputFormat::PlainText => {
                let text = std::str::from_utf8(&raw_doc)
                    .map_err(|error| DocProcessorError::JsonParsing(error.to_string()))?;
                return Ok(batch.push_plain_text(text)?);
            }
            SourceInputFormat::OtlpLogsJson => Either::Left(parse_otlp_logs_json(&raw_doc)?),
            SourceInputFormat::OtlpLogsProtobuf => {
                Either::Left(parse_otlp_logs_protobuf(&raw_doc)?)
            }
            SourceInputFormat::OtlpTracesJson => Either::Right(parse_otlp_spans_json(&raw_doc)?),
            SourceInputFormat::OtlpTracesProtobuf => {
                Either::Right(parse_otlp_spans_protobuf(&raw_doc)?)
            }
        };
        // Successful records count their share of the payload. Rejections count the raw payload.
        for (record, len) in records {
            match batch.push_serialize(&record, len) {
                Ok(()) => otlp_payload_lens.push(raw_doc.len()),
                Err(error) => self.record_error(error.into(), raw_doc.len()),
            }
        }
        Ok(())
    }

    fn record_error(&self, error: DocProcessorError, num_bytes: usize) {
        rate_limited_warn!(
            limit_per_min = 10,
            index_id = self.counters.index_id,
            source_id = self.counters.source_id,
            "{error}",
        );
        self.counters.record_error(error, num_bytes as u64);
    }

    pub fn process_batch(
        &mut self,
        raw_docs: impl IntoIterator<Item = Bytes>,
    ) -> Vec<ProcessedDoc> {
        let mut batch = QuickwitDocBatch::default();
        let mut otlp_payload_lens = Vec::new();
        for raw_doc in raw_docs {
            let len = raw_doc.len();
            if let Err(error) = self.parse_into_batch(raw_doc, &mut batch, &mut otlp_payload_lens) {
                self.record_error(error, len);
            }
        }
        let mapped = batch.map(&self.doc_mapper, |_| ());
        let mut processed_docs = Vec::with_capacity(mapped.len());
        for (i, (num_bytes, result)) in mapped.enumerate() {
            let result = result.and_then(|((), partition, doc)| {
                let timestamp_opt = self.extract_timestamp(&doc)?;
                Ok(ProcessedDoc {
                    doc,
                    timestamp_opt,
                    partition,
                    num_bytes,
                })
            });
            match result {
                Ok(doc) => {
                    self.counters.record_valid(num_bytes as u64);
                    processed_docs.push(doc);
                }
                Err(error) => {
                    let len = if otlp_payload_lens.is_empty() {
                        num_bytes
                    } else {
                        otlp_payload_lens[i]
                    };
                    self.record_error(error.into(), len);
                }
            }
        }
        processed_docs
    }
}

fn extract_timestamp_field(doc_mapper: &DocMapper) -> anyhow::Result<Option<Field>> {
    let schema = doc_mapper.schema();
    let Some(timestamp_field_name) = doc_mapper.timestamp_field_name() else {
        return Ok(None);
    };
    let timestamp_field = schema
        .get_field(timestamp_field_name)
        .context("failed to find timestamp field in schema")?;
    Ok(Some(timestamp_field))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use prost::Message;
    use quickwit_common::uri::Uri;
    use quickwit_config::{SearchSettings, build_doc_mapper};
    use quickwit_doc_mapper::{DocMapper, default_doc_mapper_for_test};
    use quickwit_opentelemetry::otlp::{OtlpGrpcLogsService, OtlpGrpcTracesService};
    use quickwit_proto::opentelemetry::proto::collector::logs::v1::ExportLogsServiceRequest;
    use quickwit_proto::opentelemetry::proto::collector::trace::v1::ExportTraceServiceRequest;
    use quickwit_proto::opentelemetry::proto::common::v1::AnyValue as OtlpAnyValue;
    use quickwit_proto::opentelemetry::proto::common::v1::any_value::Value as OtlpAnyValueValue;
    use quickwit_proto::opentelemetry::proto::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use quickwit_proto::opentelemetry::proto::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use serde_json::Value as JsonValue;
    use tantivy::Document;
    use tantivy::schema::NamedFieldDocument;

    use super::*;

    #[test]
    fn test_document_converter_basic_and_errors() {
        let mut converter = DocumentConverter::try_new(
            "test-index".to_string(),
            "test-source".to_string(),
            Arc::new(default_doc_mapper_for_test()),
            None,
            SourceInputFormat::Json,
        )
        .unwrap();
        let raw_doc = Bytes::from_static(br#"{"body":"happy","timestamp":1628837062}"#);
        let second_raw_doc = Bytes::from_static(br#"{"body":"happy2","timestamp":1628837063}"#);
        let missing_timestamp = Bytes::from_static(br#"{"body":"happy"}"#);
        let invalid_json = Bytes::from_static(b"{");
        let num_bytes =
            raw_doc.len() + second_raw_doc.len() + missing_timestamp.len() + invalid_json.len();
        let processed_docs = converter.process_batch([
            raw_doc.clone(),
            second_raw_doc.clone(),
            missing_timestamp,
            invalid_json,
        ]);

        assert_eq!(processed_docs.len(), 2);
        assert!(std::ptr::eq(
            processed_docs[0].doc.dictionary(),
            processed_docs[1].doc.dictionary()
        ));
        assert_eq!(processed_docs[0].num_bytes, raw_doc.len());
        assert_eq!(
            processed_docs[0].timestamp_opt,
            Some(DateTime::from_timestamp_secs(1628837062))
        );
        let counters = converter.counters();
        assert_eq!(counters.valid.get_num_docs(), 2);
        assert_eq!(counters.doc_mapper_errors.get_num_docs(), 1);
        assert_eq!(counters.json_parse_errors.get_num_docs(), 1);
        assert_eq!(
            counters.num_bytes_total.load(Ordering::Relaxed),
            num_bytes as u64
        );
    }

    #[test]
    fn test_document_converter_otlp_logs_and_traces() {
        let logs_cfg = OtlpGrpcLogsService::index_config(&Uri::for_test("ram:///indexes")).unwrap();
        let logs_mapper =
            build_doc_mapper(&logs_cfg.doc_mapping, &SearchSettings::default()).unwrap();
        let mut logs_converter = DocumentConverter::try_new(
            "otel-logs".to_string(),
            "source".to_string(),
            logs_mapper,
            None,
            SourceInputFormat::OtlpLogsProtobuf,
        )
        .unwrap();
        let log_req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_700_000_000_000_000_000,
                        body: Some(OtlpAnyValue {
                            value: Some(OtlpAnyValueValue::StringValue("hello".to_string())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let processed = logs_converter.process_batch([Bytes::from(log_req.encode_to_vec())]);
        assert_eq!(processed.len(), 1);

        let traces_cfg =
            OtlpGrpcTracesService::index_config(&Uri::for_test("ram:///indexes")).unwrap();
        let traces_mapper =
            build_doc_mapper(&traces_cfg.doc_mapping, &SearchSettings::default()).unwrap();
        let mut traces_converter = DocumentConverter::try_new(
            "otel-traces".to_string(),
            "source".to_string(),
            traces_mapper,
            None,
            SourceInputFormat::OtlpTracesProtobuf,
        )
        .unwrap();
        let trace_req = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "op".to_string(),
                        start_time_unix_nano: 1_700_000_000_000_000_000,
                        end_time_unix_nano: 1_700_000_001_000_000_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let processed_spans =
            traces_converter.process_batch([Bytes::from(trace_req.encode_to_vec())]);
        assert_eq!(processed_spans.len(), 1);
    }

    #[test]
    fn test_document_converter_plain_text() {
        let doc_mapper: Arc<DocMapper> = Arc::new(
            serde_json::from_str(r#"{"field_mappings": [{"name": "plain_text", "type": "text"}]}"#)
                .unwrap(),
        );
        let mut converter = DocumentConverter::try_new(
            "plain-idx".to_string(),
            "plain-src".to_string(),
            doc_mapper.clone(),
            None,
            SourceInputFormat::PlainText,
        )
        .unwrap();
        let processed = converter.process_batch([Bytes::from_static(b"hello world")]);
        assert_eq!(processed.len(), 1);
        let named: NamedFieldDocument = processed[0].doc.to_named_doc(&doc_mapper.schema());
        let json: JsonValue = serde_json::to_value(named).unwrap();
        assert_eq!(json["plain_text"], serde_json::json!(["hello world"]));
    }

    #[cfg(feature = "vrl")]
    #[test]
    fn test_document_converter_vrl() {
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let mut converter = DocumentConverter::try_new(
            "vrl-idx".to_string(),
            "vrl-src".to_string(),
            doc_mapper.clone(),
            Some(TransformConfig::for_test(".body = upcase(string!(.body))")),
            SourceInputFormat::Json,
        )
        .unwrap();
        let processed = converter.process_batch([Bytes::from_static(
            br#"{"body":"hello","timestamp":1628837062}"#,
        )]);
        assert_eq!(processed.len(), 1);
        let named: NamedFieldDocument = processed[0].doc.to_named_doc(&doc_mapper.schema());
        let json: JsonValue = serde_json::to_value(named).unwrap();
        assert_eq!(json["body"], serde_json::json!(["HELLO"]));
    }
}
