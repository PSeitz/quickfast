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

pub(crate) mod serialize;

use std::borrow::Cow;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use anyhow::ensure;
use quickwit_common::is_false;
use quickwit_common::uri::Uri;
use quickwit_proto::metastore::SourceType;
use quickwit_proto::types::SourceId;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
// For backward compatibility.
use serialize::VersionedSourceConfig;
pub use serialize::{load_source_config_from_user_config, load_source_config_update};
use siphasher::sip::SipHasher;

/// Reserved source ID for the `quickwit index ingest` CLI command.
pub const CLI_SOURCE_ID: &str = "_ingest-cli-source";

/// Reserved source ID used for native Quickwit ingest.
pub const INGEST_V2_SOURCE_ID: &str = "_ingest-source";

pub const RESERVED_SOURCE_IDS: &[&str] = &[CLI_SOURCE_ID, INGEST_V2_SOURCE_ID];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(into = "VersionedSourceConfig")]
#[serde(try_from = "VersionedSourceConfig")]
pub struct SourceConfig {
    pub source_id: SourceId,

    // Denotes if this source is enabled.
    pub enabled: bool,

    pub source_params: SourceParams,

    pub transform_config: Option<TransformConfig>,

    // Denotes the input data format.
    #[serde(default)]
    pub input_format: SourceInputFormat,
}

impl SourceConfig {
    pub fn source_type(&self) -> SourceType {
        self.source_params.source_type()
    }

    /// Checks that the VRL transform, if any, compiles.
    ///
    /// Deserialization does not compile it, so that a source stored with an older VRL stays
    /// readable. This must be called wherever a new or updated source config is accepted.
    pub fn validate_transform(&self) -> anyhow::Result<()> {
        let Some(transform_config) = &self.transform_config else {
            return Ok(());
        };
        transform_config.validate_vrl_script()
    }

    // TODO: Remove after source factory refactor.
    pub fn params(&self) -> JsonValue {
        match &self.source_params {
            SourceParams::File(params) => serde_json::to_value(params),
            SourceParams::PubSub(params) => serde_json::to_value(params),
            SourceParams::Ingest => serde_json::to_value(()),
            SourceParams::IngestCli => serde_json::to_value(()),
            SourceParams::Kafka(params) => serde_json::to_value(params),
            SourceParams::Stdin => serde_json::to_value(()),
            SourceParams::Void(params) => serde_json::to_value(params),
        }
        .expect("`SourceParams` should be JSON serializable")
    }

    /// Creates the default CLI source config. The CLI source ingests data from stdin.
    pub fn cli() -> Self {
        Self {
            source_id: CLI_SOURCE_ID.to_string(),
            enabled: true,
            source_params: SourceParams::IngestCli,
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }
    }

    /// Creates a native Quickwit ingest source.
    pub fn ingest_v2() -> Self {
        Self {
            source_id: INGEST_V2_SOURCE_ID.to_string(),
            enabled: true,
            source_params: SourceParams::Ingest,
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }
    }

    /// Returns a fingerprint of parameters relevant for indexers.
    ///
    /// This should remain private to this crate to avoid confusion with the
    /// full indexing pipeline fingerprint that also includes the index config's
    /// fingerprint.
    pub(crate) fn indexing_params_fingerprint(&self) -> u64 {
        let mut hasher = SipHasher::new();
        self.input_format.hash(&mut hasher);
        self.source_params.hash(&mut hasher);
        self.transform_config.hash(&mut hasher);
        hasher.finish()
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(source_id: &str, source_params: SourceParams) -> Self {
        Self {
            source_id: source_id.to_string(),
            enabled: true,
            source_params,
            transform_config: None,
            input_format: SourceInputFormat::Json,
        }
    }
}

#[cfg(any(test, feature = "testsuite"))]
impl crate::TestableForRegression for SourceConfig {
    fn sample_for_regression() -> Self {
        SourceConfig {
            source_id: "kafka-source".to_string(),
            enabled: true,
            source_params: SourceParams::Kafka(KafkaSourceParams {
                topic: "kafka-topic".to_string(),
                client_log_level: None,
                client_params: serde_json::json!({}),
                enable_backfill_mode: false,
            }),
            transform_config: Some(TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: default_timezone(),
            }),
            input_format: SourceInputFormat::Json,
        }
    }

    fn assert_equality(&self, other: &Self) {
        assert_eq!(self, other);
    }
}

#[derive(
    Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash, utoipa::ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SourceInputFormat {
    #[default]
    Json,
    OtlpLogsJson,
    #[serde(alias = "otlp_logs_proto")]
    OtlpLogsProtobuf,
    #[serde(alias = "otlp_trace_json")]
    OtlpTracesJson,
    #[serde(
        alias = "otlp_trace_proto",
        alias = "otlp_trace_protobuf",
        alias = "otlp_traces_proto"
    )]
    OtlpTracesProtobuf,
    #[serde(alias = "plain")]
    PlainText,
}

impl FromStr for SourceInputFormat {
    type Err = String;

    fn from_str(format_str: &str) -> Result<Self, String> {
        match format_str {
            "json" => Ok(Self::Json),
            "plain" => Ok(Self::PlainText),
            unknown => Err(format!("unknown source input format: `{unknown}`")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, utoipa::ToSchema)]
#[serde(tag = "source_type", content = "params", rename_all = "snake_case")]
pub enum SourceParams {
    #[schema(value_type = FileSourceParamsForSerde)]
    File(FileSourceParams),
    Ingest,
    #[serde(rename = "ingest-cli")]
    IngestCli,
    Kafka(KafkaSourceParams),
    #[serde(rename = "pubsub")]
    PubSub(PubSubSourceParams),
    Stdin,
    Void(VoidSourceParams),
}

impl SourceParams {
    pub fn file_from_uri(uri: Uri) -> Self {
        Self::File(FileSourceParams::Filepath(uri))
    }

    pub fn file_from_str<P: AsRef<str>>(filepath: P) -> anyhow::Result<Self> {
        Uri::from_str(filepath.as_ref()).map(Self::file_from_uri)
    }

    pub fn stdin() -> Self {
        Self::Stdin
    }

    pub fn void() -> Self {
        Self::Void(VoidSourceParams)
    }

    fn source_type(&self) -> SourceType {
        match self {
            SourceParams::File(_) => SourceType::File,
            SourceParams::Ingest => SourceType::IngestV2,
            SourceParams::IngestCli => SourceType::Cli,
            SourceParams::Kafka(_) => SourceType::Kafka,
            SourceParams::PubSub(_) => SourceType::PubSub,
            SourceParams::Stdin => SourceType::Stdin,
            SourceParams::Void(_) => SourceType::Void,
        }
    }

    fn validate_update(&self, new_source_params: &SourceParams) -> anyhow::Result<()> {
        match (self, new_source_params) {
            (
                SourceParams::File(FileSourceParams::Notifications(current)),
                SourceParams::File(FileSourceParams::Notifications(new)),
            ) => current.validate_update(new),
            (SourceParams::Kafka(current), SourceParams::Kafka(new)) => {
                current.validate_update(new)
            }
            (SourceParams::PubSub(current), SourceParams::PubSub(new)) => {
                current.validate_update(new)
            }
            (current, new) if current.source_type() != new.source_type() => Err(anyhow::anyhow!(
                "source type cannot be changed, current type {}",
                current.source_type(),
            )),
            _ => Err(anyhow::anyhow!(
                "source type {} cannot be updated",
                self.source_type(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileSourceMessageType {
    /// See <https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html>
    S3Notification,
    /// A string with the URI of the file (e.g `s3://bucket/key`)
    RawUri,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FileSourceSqs {
    pub queue_url: String,
    pub message_type: FileSourceMessageType,
    #[serde(default = "default_deduplication_window_duration_secs")]
    pub deduplication_window_duration_secs: u32,
    #[serde(default = "default_deduplication_window_max_messages")]
    pub deduplication_window_max_messages: u32,
    #[serde(default = "default_deduplication_cleanup_interval_secs")]
    pub deduplication_cleanup_interval_secs: u32,
}

fn default_deduplication_window_duration_secs() -> u32 {
    3600
}

fn default_deduplication_window_max_messages() -> u32 {
    100_000
}

fn default_deduplication_cleanup_interval_secs() -> u32 {
    60
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FileSourceNotification {
    Sqs(FileSourceSqs),
}

impl FileSourceNotification {
    fn validate_update(&self, other: &Self) -> anyhow::Result<()> {
        match (self, other) {
            (Self::Sqs(_), Self::Sqs(_)) => {
                // changing the queue or the deduplication settings should be fine
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct FileSourceParamsForSerde {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    notifications: Vec<FileSourceNotification>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    filepath: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(
    try_from = "FileSourceParamsForSerde",
    into = "FileSourceParamsForSerde"
)]
pub enum FileSourceParams {
    Notifications(FileSourceNotification),
    Filepath(Uri),
}

impl TryFrom<FileSourceParamsForSerde> for FileSourceParams {
    type Error = Cow<'static, str>;

    fn try_from(mut value: FileSourceParamsForSerde) -> Result<Self, Self::Error> {
        if value.filepath.is_some() && !value.notifications.is_empty() {
            return Err(
                "File source parameters `notifications` and `filepath` are mutually exclusive"
                    .into(),
            );
        }
        if let Some(filepath) = value.filepath {
            let uri = Uri::from_str(&filepath).map_err(|err| err.to_string())?;
            Ok(FileSourceParams::Filepath(uri))
        } else if value.notifications.len() == 1 {
            Ok(FileSourceParams::Notifications(
                value.notifications.remove(0),
            ))
        } else if value.notifications.len() > 1 {
            Err("Only one notification can be specified for now".into())
        } else {
            Err(
                "Either `notifications` or `filepath` must be specified as file source parameters"
                    .into(),
            )
        }
    }
}

impl From<FileSourceParams> for FileSourceParamsForSerde {
    fn from(value: FileSourceParams) -> Self {
        match value {
            FileSourceParams::Filepath(uri) => Self {
                filepath: Some(uri.to_string()),
                notifications: vec![],
            },
            FileSourceParams::Notifications(notification) => Self {
                filepath: None,
                notifications: vec![notification],
            },
        }
    }
}

impl FileSourceParams {
    pub fn from_filepath<P: AsRef<str>>(filepath: P) -> anyhow::Result<Self> {
        Uri::from_str(filepath.as_ref()).map(Self::Filepath)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct KafkaSourceParams {
    /// Name of the topic that the source consumes.
    pub topic: String,
    /// Kafka client log level. Possible values are `debug`, `info`, `warn`, and `error`.
    #[schema(value_type = String)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_log_level: Option<String>,
    /// Kafka client configuration parameters.
    #[schema(value_type = Object)]
    #[serde(default = "serde_json::Value::default")]
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub client_params: JsonValue,
    /// When backfill mode is enabled, the source exits after reaching the end of the topic.
    #[serde(default)]
    #[serde(skip_serializing_if = "is_false")]
    pub enable_backfill_mode: bool,
}

impl KafkaSourceParams {
    fn validate_update(&self, other: &Self) -> anyhow::Result<()> {
        // Updating the topic would likely mess up the checkpoints because the
        // Kafka partition IDs are used as metastore checkpoint PartitionId
        // and there uniqueness is not guaranteed across topics.
        ensure!(self.topic == other.topic, "Kafka topic cannot be updated");
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PubSubSourceParams {
    /// Name of the subscription that the source consumes.
    pub subscription: String,
    /// When backfill mode is enabled, the source exits after reaching the end of the topic.
    #[serde(default)]
    #[serde(skip_serializing_if = "is_false")]
    pub enable_backfill_mode: bool,
    /// GCP service account credentials (`None` will use default via
    /// GOOGLE_APPLICATION_CREDENTIALS)
    /// Path to a google_cloud_auth::credentials::CredentialsFile serialized in JSON. See also
    /// `<https://cloud.google.com/docs/authentication/application-default-credentials>` and
    /// `<https://github.com/yoshidan/google-cloud-rust/tree/main/pubsub#automatically>` and
    /// `<https://docs.rs/google-cloud-auth/0.12.0/google_cloud_auth/credentials/struct.CredentialsFile.html>`.
    pub credentials_file: Option<String>,
    /// GCP project ID (Defaults to credentials file project ID).
    pub project_id: Option<String>,
    /// Maximum number of messages returned by a pull request (default 1,000)
    pub max_messages_per_pull: Option<i32>,
}

impl PubSubSourceParams {
    fn validate_update(&self, _other: &Self) -> anyhow::Result<()> {
        // experimental source, no validation is performed
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct VoidSourceParams;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TransformConfig {
    /// [VRL] source code of the transform compiled to a VRL [`Program`](vrl::compiler::Program).
    ///
    /// [VRL]: https://vector.dev/docs/reference/vrl/
    #[serde(rename = "script")]
    vrl_script: String,

    /// Timezone used in the VRL [`Program`](vrl::compiler::Program) for date and time
    /// manipulations. Defaults to `UTC` if not timezone is specified.
    #[serde(default = "default_timezone")]
    timezone: String,
}

fn default_timezone() -> String {
    "UTC".to_string()
}

impl TransformConfig {
    /// Creates a new [`TransformConfig`] instance from the provided VRL script and optional
    /// timezone.
    pub fn new(vrl_script: String, timezone_opt: Option<String>) -> Self {
        Self {
            vrl_script,
            timezone: timezone_opt.unwrap_or_else(default_timezone),
        }
    }

    #[cfg(feature = "vrl")]
    pub(crate) fn validate_vrl_script(&self) -> anyhow::Result<()> {
        self.compile_vrl_script()?;
        Ok(())
    }

    #[cfg(not(feature = "vrl"))]
    pub(crate) fn validate_vrl_script(&self) -> anyhow::Result<()> {
        // If we are missing the VRL feature we do not return an error here,
        // to avoid breaking unit tests.
        //
        // We do return an explicit error on instantiation of the program however.
        Ok(())
    }

    #[cfg(feature = "vrl")]
    /// Compiles the VRL script to a VRL [`Program`](vrl::compiler::Program) and returns it along
    /// with the timezone.
    pub fn compile_vrl_script(
        &self,
    ) -> anyhow::Result<(vrl::compiler::Program, vrl::compiler::TimeZone)> {
        use anyhow::Context;
        let timezone = vrl::compiler::TimeZone::parse(&self.timezone).with_context(|| {
            format!(
                "failed to parse timezone: `{}`. timezone must be a valid name \
            in the TZ database: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones",
                self.timezone,
            )
        })?;
        // Append "\n." to the script to return the entire document and not only the modified
        // fields.
        let vrl_script = self.vrl_script.clone() + "\n.";
        let functions = vrl::stdlib::all();

        let compilation_res = match vrl::compiler::compile(&vrl_script, &functions) {
            Ok(compilation_res) => compilation_res,
            Err(diagnostics) => {
                let mut formatter = vrl::diagnostic::Formatter::new(&vrl_script, diagnostics);
                formatter.enable_colors(!quickwit_common::no_color());
                anyhow::bail!("failed to compile VRL script:\n {formatter}")
            }
        };

        let vrl::compiler::CompilationResult {
            program, warnings, ..
        } = compilation_res;

        if !warnings.is_empty() {
            let mut formatter = vrl::diagnostic::Formatter::new(&vrl_script, warnings);
            formatter.enable_colors(!quickwit_common::no_color());
            tracing::warn!("VRL program compiled with some warnings: {formatter}");
        }
        Ok((program, timezone))
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(vrl_script: &str) -> Self {
        Self {
            vrl_script: vrl_script.to_string(),
            timezone: default_timezone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use quickwit_common::uri::Uri;
    use serde_json::json;

    use super::*;
    use crate::{ConfigFormat, FileSourceParams};

    fn get_source_config_filepath(source_config_filename: &str) -> String {
        format!(
            "{}/resources/tests/source_config/{}",
            env!("CARGO_MANIFEST_DIR"),
            source_config_filename
        )
    }

    #[tokio::test]
    async fn test_load_kafka_source_config() {
        let source_config_filepath = get_source_config_filepath("kafka-source.json");
        let file_content = std::fs::read_to_string(&source_config_filepath).unwrap();
        let source_config_uri = Uri::from_str(&source_config_filepath).unwrap();
        let config_format = ConfigFormat::sniff_from_uri(&source_config_uri).unwrap();
        let source_config =
            load_source_config_from_user_config(config_format, file_content.as_bytes()).unwrap();
        let expected_source_config = SourceConfig {
            source_id: "hdfs-logs-kafka-source".to_string(),
            enabled: true,
            source_params: SourceParams::Kafka(KafkaSourceParams {
                topic: "cloudera-cluster-logs".to_string(),
                client_log_level: None,
                client_params: json! {{"bootstrap.servers": "localhost:9092"}},
                enable_backfill_mode: false,
            }),
            transform_config: Some(TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: "local".to_string(),
            }),
            input_format: SourceInputFormat::Json,
        };
        assert_eq!(source_config, expected_source_config);
    }

    #[test]
    fn test_kafka_source_params_serialization() {
        {
            let params = KafkaSourceParams {
                topic: "my-topic".to_string(),
                client_log_level: None,
                client_params: json!(null),
                enable_backfill_mode: false,
            };
            let params_yaml = serde_yaml::to_string(&params).unwrap();

            assert_eq!(
                serde_yaml::from_str::<KafkaSourceParams>(&params_yaml).unwrap(),
                params,
            )
        }
        {
            let params = KafkaSourceParams {
                topic: "my-topic".to_string(),
                client_log_level: Some("info".to_string()),
                client_params: json! {{"bootstrap.servers": "localhost:9092"}},
                enable_backfill_mode: false,
            };
            let params_yaml = serde_yaml::to_string(&params).unwrap();

            assert_eq!(
                serde_yaml::from_str::<KafkaSourceParams>(&params_yaml).unwrap(),
                params,
            )
        }
    }

    #[test]
    fn test_kafka_source_params_deserialization() {
        {
            let yaml = r#"
                    topic: my-topic
                "#;
            assert_eq!(
                serde_yaml::from_str::<KafkaSourceParams>(yaml).unwrap(),
                KafkaSourceParams {
                    topic: "my-topic".to_string(),
                    client_log_level: None,
                    client_params: json!(null),
                    enable_backfill_mode: false,
                }
            );
        }
        {
            let yaml = r#"
                    topic: my-topic
                    client_log_level: info
                    client_params:
                        bootstrap.servers: localhost:9092
                    enable_backfill_mode: true
                "#;
            assert_eq!(
                serde_yaml::from_str::<KafkaSourceParams>(yaml).unwrap(),
                KafkaSourceParams {
                    topic: "my-topic".to_string(),
                    client_log_level: Some("info".to_string()),
                    client_params: json! {{"bootstrap.servers": "localhost:9092"}},
                    enable_backfill_mode: true,
                }
            );
        }
    }

    #[test]
    fn test_file_source_params_serde() {
        {
            let yaml = r#"
                filepath: source-path.json
            "#;
            let file_params_deserialized = serde_yaml::from_str::<FileSourceParams>(yaml).unwrap();
            let uri = Uri::from_str("source-path.json").unwrap();
            assert_eq!(file_params_deserialized, FileSourceParams::Filepath(uri));
            let file_params_reserialized = serde_json::to_value(file_params_deserialized).unwrap();
            file_params_reserialized
                .get("filepath")
                .unwrap()
                .as_str()
                .unwrap()
                .contains("source-path.json");
        }
        {
            let yaml = r#"
                notifications:
                  - type: sqs
                    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/queue-name
                    message_type: s3_notification
            "#;
            let file_params_deserialized = serde_yaml::from_str::<FileSourceParams>(yaml).unwrap();
            assert_eq!(
                file_params_deserialized,
                FileSourceParams::Notifications(FileSourceNotification::Sqs(FileSourceSqs {
                    queue_url: "https://sqs.us-east-1.amazonaws.com/123456789012/queue-name"
                        .to_string(),
                    message_type: FileSourceMessageType::S3Notification,
                    deduplication_window_duration_secs: default_deduplication_window_duration_secs(
                    ),
                    deduplication_window_max_messages: default_deduplication_window_max_messages(),
                    deduplication_cleanup_interval_secs:
                        default_deduplication_cleanup_interval_secs()
                })),
            );
            let file_params_reserialized = serde_json::to_value(&file_params_deserialized).unwrap();
            assert_eq!(
                file_params_reserialized,
                json!({"notifications": [{
                    "type": "sqs",
                    "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/queue-name",
                    "message_type": "s3_notification",
                    "deduplication_window_duration_secs": default_deduplication_window_duration_secs(),
                    "deduplication_window_max_messages": default_deduplication_window_max_messages(),
                    "deduplication_cleanup_interval_secs": default_deduplication_cleanup_interval_secs(),
                }]})
            );
        }
        {
            let yaml = r#"
                filepath: source-path.json
                notifications:
                  - type: sqs
                    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/queue-name
                    message_type: s3_notification
            "#;
            let error = serde_yaml::from_str::<FileSourceParams>(yaml).unwrap_err();
            assert_eq!(
                error.to_string(),
                "File source parameters `notifications` and `filepath` are mutually exclusive"
            );
        }
        {
            let yaml = r#"
                notifications:
                  - type: sqs
                    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/queue1
                    message_type: s3_notification
                  - type: sqs
                    queue_url: https://sqs.us-east-1.amazonaws.com/123456789012/queue2
                    message_type: s3_notification
            "#;
            let error = serde_yaml::from_str::<FileSourceParams>(yaml).unwrap_err();
            assert_eq!(
                error.to_string(),
                "Only one notification can be specified for now"
            );
        }
        {
            let json = r#"
            {
                "notifications": [
                    {
                        "queue_url": "https://sqs.us-east-1.amazonaws.com/123456789012/queue",
                        "message_type": "s3_notification"
                    }
                ]
            }
            "#;
            let error = serde_json::from_str::<FileSourceParams>(json).unwrap_err();
            assert!(error.to_string().contains("missing field `type`"));
        }
    }

    #[test]
    fn test_transform_config_serialization() {
        {
            let transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: "local".to_string(),
            };
            let transform_config_yaml = serde_yaml::to_string(&transform_config).unwrap();
            assert_eq!(
                serde_yaml::from_str::<TransformConfig>(&transform_config_yaml).unwrap(),
                transform_config,
            );
        }
        {
            let transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: default_timezone(),
            };
            let transform_config_yaml = serde_yaml::to_string(&transform_config).unwrap();
            assert_eq!(
                serde_yaml::from_str::<TransformConfig>(&transform_config_yaml).unwrap(),
                transform_config,
            );
        }
    }

    #[test]
    fn test_transform_config_deserialization() {
        {
            let transform_config_yaml = r#"
                script: .message = downcase(string!(.message))
            "#;
            let transform_config =
                serde_yaml::from_str::<TransformConfig>(transform_config_yaml).unwrap();

            let expected_transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: default_timezone(),
            };
            assert_eq!(transform_config, expected_transform_config);
        }
        {
            let transform_config_yaml = r#"
                script: .message = downcase(string!(.message))
                timezone: Turkey
            "#;
            let transform_config =
                serde_yaml::from_str::<TransformConfig>(transform_config_yaml).unwrap();

            let expected_transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: "Turkey".to_string(),
            };
            assert_eq!(transform_config, expected_transform_config);
        }
    }

    #[cfg(feature = "vrl")]
    #[test]
    fn test_transform_config_compile_vrl_script() {
        {
            let transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: "Turkey".to_string(),
            };
            transform_config.compile_vrl_script().unwrap();
        }
        {
            let transform_config = TransformConfig {
                vrl_script: r#"
                . = parse_json!(string!(.message))
                .timestamp = to_unix_timestamp(timestamp!(.timestamp))
                del(.username)
                .message = downcase(string!(.message))
                "#
                .to_string(),
                timezone: default_timezone(),
            };
            transform_config.compile_vrl_script().unwrap();
        }
        {
            let transform_config = TransformConfig {
                vrl_script: ".message = downcase(string!(.message))".to_string(),
                timezone: "foo".to_string(),
            };
            let error = transform_config.compile_vrl_script().unwrap_err();
            assert!(error.to_string().starts_with("failed to parse timezone"));
        }
        {
            let transform_config = TransformConfig {
                vrl_script: "foo".to_string(),
                timezone: "Turkey".to_string(),
            };
            let error = transform_config.compile_vrl_script().unwrap_err();
            assert!(error.to_string().starts_with("failed to compile"));
        }
    }

    #[cfg(feature = "vrl")]
    #[test]
    fn test_source_config_transform_compiled_on_load_not_on_deserialize() {
        let current_source_config = SourceConfig::for_test("void-source", SourceParams::void());
        let mut source_config = current_source_config.clone();
        source_config.transform_config = Some(TransformConfig::for_test("foo"));
        let source_config_json = serde_json::to_string(&source_config).unwrap();

        let deserialized_source_config: SourceConfig =
            serde_json::from_str(&source_config_json).unwrap();
        assert_eq!(deserialized_source_config, source_config);

        let error =
            load_source_config_from_user_config(ConfigFormat::Json, source_config_json.as_bytes())
                .unwrap_err();
        assert!(error.to_string().starts_with("failed to compile"));

        let error = load_source_config_update(
            ConfigFormat::Json,
            source_config_json.as_bytes(),
            &current_source_config,
        )
        .unwrap_err();
        assert!(error.to_string().starts_with("failed to compile"));
    }

    #[tokio::test]
    async fn test_source_config_plain_text_input_format() {
        let file_content = r#"{
            "version": "0.7",
            "source_id": "logs-file-source",
            "desired_num_pipelines": 1,
            "max_num_pipelines_per_indexer": 1,
            "source_type": "file",
            "params": {
              "filepath": "s3://mybucket/test_non_json_corpus.txt"
            },
            "input_format": "plain_text"
        }"#;
        let source_config =
            load_source_config_from_user_config(ConfigFormat::Json, file_content.as_bytes())
                .unwrap();
        assert_eq!(source_config.input_format, SourceInputFormat::PlainText);
    }

    #[tokio::test]
    async fn test_update_kafka_source_config() {
        let source_config_filepath = get_source_config_filepath("kafka-source.json");
        let file_content = std::fs::read(&source_config_filepath).unwrap();
        let source_config_uri = Uri::from_str(&source_config_filepath).unwrap();
        let config_format = ConfigFormat::sniff_from_uri(&source_config_uri).unwrap();
        {
            let mut existing_source_config =
                load_source_config_from_user_config(config_format, &file_content).unwrap();
            existing_source_config.enabled = false;
            let new_source_config =
                load_source_config_update(config_format, &file_content, &existing_source_config)
                    .unwrap();

            let expected_source_config = SourceConfig {
                source_id: "hdfs-logs-kafka-source".to_string(),
                enabled: true,
                source_params: SourceParams::Kafka(KafkaSourceParams {
                    topic: "cloudera-cluster-logs".to_string(),
                    client_log_level: None,
                    client_params: json! {{"bootstrap.servers": "localhost:9092"}},
                    enable_backfill_mode: false,
                }),
                transform_config: Some(TransformConfig {
                    vrl_script: ".message = downcase(string!(.message))".to_string(),
                    timezone: "local".to_string(),
                }),
                input_format: SourceInputFormat::Json,
            };
            assert_eq!(new_source_config, expected_source_config);
        }
        {
            // the source type cannot be updated
            let mut existing_source_config =
                load_source_config_from_user_config(config_format, &file_content).unwrap();
            existing_source_config.source_params = SourceParams::void();
            load_source_config_update(config_format, &file_content, &existing_source_config)
                .unwrap_err();
        }
        {
            // the topic cannot be updated
            let mut existing_source_config =
                load_source_config_from_user_config(config_format, &file_content).unwrap();
            let SourceParams::Kafka(kafka_params) = &mut existing_source_config.source_params
            else {
                panic!("expected Kafka source params");
            };
            kafka_params.topic = "other_topic_name".to_string();
            load_source_config_update(config_format, &file_content, &existing_source_config)
                .unwrap_err();
        }
    }
}
