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

use std::string::FromUtf8Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, bail};
use bytes::Bytes;
use quickwit_common::rate_limited_tracing::rate_limited_warn;
use quickwit_config::{SourceInputFormat, TransformConfig};
use quickwit_doc_mapper::{DocMapper, DocParsingError, JsonObject};
use quickwit_metrics::{Counter, counter, labels};
use quickwit_opentelemetry::otlp::{
    JsonLogIterator, JsonSpanIterator, OtlpLogsError, OtlpTracesError, parse_otlp_logs_json,
    parse_otlp_logs_protobuf, parse_otlp_spans_json, parse_otlp_spans_protobuf,
};
use quickwit_proto::types::{IndexId, SourceId};
use serde::Serialize;
use serde_json::Value as JsonValue;
use tantivy::schema::{Field, Value};
use tantivy::{DateTime, TantivyDocument};
use thiserror::Error;

#[cfg(feature = "vrl")]
use super::vrl_processing::*;
use crate::metrics::{PROCESSED_BYTES, PROCESSED_DOCS_TOTAL};
use crate::models::ProcessedDoc;

const PLAIN_TEXT: &str = "plain_text";
pub(super) struct JsonDoc {
    json_obj: JsonObject,
    num_bytes: usize,
}

impl JsonDoc {
    pub fn new(json_obj: JsonObject, num_bytes: usize) -> Self {
        Self {
            json_obj,
            num_bytes,
        }
    }

    pub fn try_from_json_value(
        json_value: JsonValue,
        num_bytes: usize,
    ) -> Result<Self, DocProcessorError> {
        match json_value {
            JsonValue::Object(json_obj) => Ok(Self::new(json_obj, num_bytes)),
            _ => Err(DocProcessorError::JsonParsing(
                "document is not an object".to_string(),
            )),
        }
    }

    #[cfg(feature = "vrl")]
    pub fn try_from_vrl_doc(vrl_doc: VrlDoc) -> Result<Self, DocProcessorError> {
        let json_value = serde_json::to_value(vrl_doc.vrl_value)?;
        Self::try_from_json_value(json_value, vrl_doc.num_bytes)
    }
}

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

impl From<FromUtf8Error> for DocProcessorError {
    fn from(error: FromUtf8Error) -> Self {
        Self::JsonParsing(error.to_string())
    }
}

#[cfg(feature = "vrl")]
fn try_into_vrl_doc(
    input_format: SourceInputFormat,
    raw_doc: Bytes,
    num_bytes: usize,
) -> Result<VrlDoc, DocProcessorError> {
    let vrl_value = match input_format {
        SourceInputFormat::Json => serde_json::from_slice::<VrlValue>(&raw_doc)?,
        SourceInputFormat::PlainText => {
            let mut map = std::collections::BTreeMap::new();
            let key = vrl::value::KeyString::from(PLAIN_TEXT);
            let value = VrlValue::Bytes(raw_doc);
            map.insert(key, value);
            VrlValue::Object(map)
        }
        SourceInputFormat::OtlpLogsJson
        | SourceInputFormat::OtlpLogsProtobuf
        | SourceInputFormat::OtlpTracesJson
        | SourceInputFormat::OtlpTracesProtobuf => {
            panic!("OTP logs or traces do not support VRL transforms")
        }
    };
    let vrl_doc = VrlDoc::new(vrl_value, num_bytes);
    Ok(vrl_doc)
}

fn try_into_json_docs(
    input_format: SourceInputFormat,
    raw_doc: Bytes,
    num_bytes: usize,
) -> JsonDocIterator {
    match input_format {
        SourceInputFormat::Json => {
            let json_doc_result = serde_json::from_slice::<JsonObject>(&raw_doc)
                .map(|json_obj| JsonDoc::new(json_obj, num_bytes));
            JsonDocIterator::from(json_doc_result)
        }
        SourceInputFormat::OtlpLogsJson => {
            let logs = parse_otlp_logs_json(&raw_doc);
            JsonDocIterator::from(logs)
        }
        SourceInputFormat::OtlpLogsProtobuf => {
            let logs = parse_otlp_logs_protobuf(&raw_doc);
            JsonDocIterator::from(logs)
        }
        SourceInputFormat::OtlpTracesJson => {
            let spans = parse_otlp_spans_json(&raw_doc);
            JsonDocIterator::from(spans)
        }
        SourceInputFormat::OtlpTracesProtobuf => {
            let spans = parse_otlp_spans_protobuf(&raw_doc);
            JsonDocIterator::from(spans)
        }
        SourceInputFormat::PlainText => {
            let json_doc_result = String::from_utf8(raw_doc.to_vec()).map(|value| {
                let mut json_obj = serde_json::Map::with_capacity(1);
                let key = PLAIN_TEXT.to_string();
                json_obj.insert(key, JsonValue::String(value));
                JsonDoc::new(json_obj, num_bytes)
            });
            JsonDocIterator::from(json_doc_result)
        }
    }
}

#[cfg(feature = "vrl")]
fn parse_raw_doc(
    input_format: SourceInputFormat,
    raw_doc: Bytes,
    num_bytes: usize,
    vrl_program_opt: Option<&mut VrlProgram>,
) -> JsonDocIterator {
    let Some(vrl_program) = vrl_program_opt else {
        return try_into_json_docs(input_format, raw_doc, num_bytes);
    };
    let json_doc_result = try_into_vrl_doc(input_format, raw_doc, num_bytes)
        .and_then(|vrl_doc| vrl_program.transform_doc(vrl_doc))
        .and_then(JsonDoc::try_from_vrl_doc);

    JsonDocIterator::from(json_doc_result)
}

#[cfg(not(feature = "vrl"))]
fn parse_raw_doc(
    input_format: SourceInputFormat,
    raw_doc: Bytes,
    num_bytes: usize,
    _vrl_program_opt: Option<&mut VrlProgram>,
) -> JsonDocIterator {
    try_into_json_docs(input_format, raw_doc, num_bytes)
}

enum JsonDocIterator {
    One(Option<Result<JsonDoc, DocProcessorError>>),
    Logs(JsonLogIterator),
    Spans(JsonSpanIterator),
}

impl Iterator for JsonDocIterator {
    type Item = Result<JsonDoc, DocProcessorError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::One(opt) => opt.take(),
            Self::Logs(logs) => logs
                .next()
                .map(|(json_value, num_bytes)| JsonDoc::try_from_json_value(json_value, num_bytes)),
            Self::Spans(spans) => spans
                .next()
                .map(|(json_value, num_bytes)| JsonDoc::try_from_json_value(json_value, num_bytes)),
        }
    }
}

impl<E> From<Result<JsonDoc, E>> for JsonDocIterator
where E: Into<DocProcessorError>
{
    fn from(result: Result<JsonDoc, E>) -> Self {
        match result {
            Ok(json_doc) => Self::One(Some(Ok(json_doc))),
            Err(error) => Self::One(Some(Err(error.into()))),
        }
    }
}

impl From<Result<JsonLogIterator, OtlpLogsError>> for JsonDocIterator {
    fn from(result: Result<JsonLogIterator, OtlpLogsError>) -> Self {
        match result {
            Ok(logs) => Self::Logs(logs),
            Err(error) => Self::One(Some(Err(DocProcessorError::from(error)))),
        }
    }
}

impl From<Result<JsonSpanIterator, OtlpTracesError>> for JsonDocIterator {
    fn from(result: Result<JsonSpanIterator, OtlpTracesError>) -> Self {
        match result {
            Ok(spans) => Self::Spans(spans),
            Err(error) => Self::One(Some(Err(DocProcessorError::from(error)))),
        }
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

    // Extract a timestamp from a tantivy document.
    //
    // If the timestamp is set up in the docmapper and the timestamp is missing,
    // returns an PrepareDocumentError::MissingField error.
    fn extract_timestamp(
        &self,
        doc: &TantivyDocument,
    ) -> Result<Option<DateTime>, DocProcessorError> {
        let Some(timestamp_field) = self.timestamp_field_opt else {
            return Ok(None);
        };
        let timestamp = doc
            .get_first(timestamp_field)
            .and_then(|val| val.as_datetime())
            .ok_or(DocProcessorError::from(DocParsingError::RequiredField(
                "timestamp field is required".to_string(),
            )))?;
        Ok(Some(timestamp))
    }

    pub fn process_raw_doc(&mut self, raw_doc: Bytes, processed_docs: &mut Vec<ProcessedDoc>) {
        let num_bytes = raw_doc.len();

        #[cfg(feature = "vrl")]
        let transform_opt = self.transform_opt.as_mut();
        #[cfg(not(feature = "vrl"))]
        let transform_opt: Option<&mut VrlProgram> = None;

        for json_doc_result in parse_raw_doc(self.input_format, raw_doc, num_bytes, transform_opt) {
            let processed_doc_result =
                json_doc_result.and_then(|json_doc| self.process_json_doc(json_doc));

            match processed_doc_result {
                Ok(processed_doc) => {
                    self.counters.record_valid(processed_doc.num_bytes as u64);
                    processed_docs.push(processed_doc);
                }
                Err(error) => {
                    rate_limited_warn!(
                        limit_per_min = 10,
                        index_id = self.counters.index_id,
                        source_id = self.counters.source_id,
                        "{error}",
                    );
                    self.counters.record_error(error, num_bytes as u64);
                }
            }
        }
    }

    fn process_json_doc(&self, json_doc: JsonDoc) -> Result<ProcessedDoc, DocProcessorError> {
        let num_bytes = json_doc.num_bytes;
        let (partition, doc) = self
            .doc_mapper
            .doc_from_json_obj(json_doc.json_obj, num_bytes as u64)?;
        let timestamp_opt = self.extract_timestamp(&doc)?;
        Ok(ProcessedDoc {
            doc,
            timestamp_opt,
            partition,
            num_bytes,
        })
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

#[cfg(not(feature = "vrl"))]
struct VrlProgram {}

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
        let missing_timestamp = Bytes::from_static(br#"{"body":"happy"}"#);
        let invalid_json = Bytes::from_static(b"{");
        let num_bytes = raw_doc.len() + missing_timestamp.len() + invalid_json.len();
        let mut processed_docs = Vec::new();
        converter.process_raw_doc(raw_doc.clone(), &mut processed_docs);
        converter.process_raw_doc(missing_timestamp, &mut processed_docs);
        converter.process_raw_doc(invalid_json, &mut processed_docs);

        assert_eq!(processed_docs.len(), 1);
        assert_eq!(processed_docs[0].num_bytes, raw_doc.len());
        assert_eq!(
            processed_docs[0].timestamp_opt,
            Some(DateTime::from_timestamp_secs(1628837062))
        );
        let counters = converter.counters();
        assert_eq!(counters.valid.get_num_docs(), 1);
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
        let mut processed = Vec::new();
        logs_converter.process_raw_doc(Bytes::from(log_req.encode_to_vec()), &mut processed);
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
        let mut processed_spans = Vec::new();
        traces_converter
            .process_raw_doc(Bytes::from(trace_req.encode_to_vec()), &mut processed_spans);
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
        let mut processed = Vec::new();
        converter.process_raw_doc(Bytes::from_static(b"hello world"), &mut processed);
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
        let mut processed = Vec::new();
        converter.process_raw_doc(
            Bytes::from_static(br#"{"body":"hello","timestamp":1628837062}"#),
            &mut processed,
        );
        assert_eq!(processed.len(), 1);
        let named: NamedFieldDocument = processed[0].doc.to_named_doc(&doc_mapper.schema());
        let json: JsonValue = serde_json::to_value(named).unwrap();
        assert_eq!(json["body"], serde_json::json!(["HELLO"]));
    }
}
