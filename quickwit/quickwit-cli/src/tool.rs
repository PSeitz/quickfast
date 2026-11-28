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

use std::collections::{HashMap, HashSet};
use std::io::IsTerminal;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::{Duration, Instant};
use std::{env, io};

use anyhow::{Context, bail};
use clap::{ArgMatches, Command, arg};
use colored::Colorize;
use humantime::format_duration;
use quickwit_common::io::IoControls;
use quickwit_common::runtimes::RuntimesConfig;
use quickwit_common::uri::Uri;
use quickwit_config::service::QuickwitService;
use quickwit_config::{
    CLI_SOURCE_ID, SourceConfig, SourceInputFormat, SourceParams, TransformConfig, build_doc_mapper,
};
use quickwit_index_management::{IndexService, clear_cache_directory};
use quickwit_indexing::merge_policy::merge_policy_from_settings;
use quickwit_indexing::models::IndexedSplitBuilder;
use quickwit_indexing::source::BATCH_NUM_BYTES_LIMIT;
use quickwit_indexing::source::doc_file_reader::DocFileReader;
use quickwit_indexing::{
    DocumentConverter, IndexingSplitStore, create_packaged_split, prepare_split_for_upload,
    run_local_merges,
};
use quickwit_metastore::{IndexMetadataResponseExt, StageSplitsRequestExt};
use quickwit_proto::indexing::{IndexingPipelineId, MergePipelineId};
use quickwit_proto::metastore::{
    IndexMetadataRequest, LastDeleteOpstampRequest, MetastoreService, MetastoreServiceClient,
    PublishSplitsRequest, StageSplitsRequest,
};
use quickwit_proto::search::{CountHits, SearchResponse};
use quickwit_proto::types::{IndexId, SourceId, SplitId};
use quickwit_search::{SearchResponseRest, single_node_search};
use quickwit_serve::{
    BodyFormat, SearchRequestQueryString, SortBy, search_request_from_api_request,
};
use quickwit_storage::{BundleStorage, Storage};
use thousands::Separable;
use tracing::debug;

use crate::checklist::{GREEN_COLOR, RED_COLOR};
use crate::{
    config_cli_arg, get_resolvers, load_node_config, run_index_checklist, start_actor_runtimes,
};

pub fn build_tool_command() -> Command {
    Command::new("tool")
        .about("Performs utility operations. Requires a node config.")
        .arg(config_cli_arg())
        .subcommand(
            Command::new("local-ingest")
                .display_order(10)
                .about("Indexes NDJSON documents locally.")
                .long_about("Local ingest indexes locally NDJSON documents from a file or from stdin and uploads splits on the configured storage.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--"input-path" <INPUT_PATH> "Location of the input file.")
                        .required(false),
                    arg!(--"input-format" <INPUT_FORMAT> "Format of the input data.")
                        .default_value("json")
                        .required(false),
                    arg!(--overwrite "Overwrites pre-existing index.")
                        .required(false),
                    arg!(--"transform-script" <SCRIPT> "VRL program to transform docs before ingesting.")
                        .required(false),
                    arg!(--"keep-cache" "Does not clear local cache directory upon completion.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("local-search")
                .display_order(10)
                .about("Searches an index locally.")
                .long_about("Searchers an index directly on the configured storage without using a server.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--query <QUERY> "Query expressed in natural query language ((barack AND obama) OR \"president of united states\"). Learn more on https://quickwit.io/docs/reference/search-language.")
                        .display_order(2)
                        .required(true),
                    arg!(--aggregation <AGG> "JSON serialized aggregation request in tantivy/elasticsearch format.")
                        .required(false),
                    arg!(--"max-hits" <MAX_HITS> "Maximum number of hits returned.")
                        .default_value("20")
                        .required(false),
                    arg!(--"start-offset" <OFFSET> "Offset in the global result set of the first hit returned.")
                        .default_value("0")
                        .required(false),
                    arg!(--"search-fields" <FIELD_NAME> "List of fields that Quickwit will search into if the user query does not explicitly target a field in the query. It overrides the default search fields defined in the index config. Space-separated list, e.g. \"field1 field2\". ")
                        .num_args(1..)
                        .required(false),
                    arg!(--"snippet-fields" <FIELD_NAME> "List of fields that Quickwit will return snippet highlight on. Space-separated list, e.g. \"field1 field2\". ")
                        .num_args(1..)
                        .required(false),
                    arg!(--"start-timestamp" <TIMESTAMP> "Filters out documents before that timestamp (time-series indexes only).")
                        .required(false),
                    arg!(--"end-timestamp" <TIMESTAMP> "Filters out documents after that timestamp (time-series indexes only).")
                        .required(false),
                    arg!(--"sort-by-field" <SORT_BY_FIELD> "Sort by field.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("extract-split")
                .about("Downloads and extracts a split to a directory.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--split <SPLIT> "ID of the target split")
                        .display_order(2)
                        .required(true),
                    arg!(--"target-dir" <TARGET_DIR> "Directory to extract the split to."),
                ])
            )
        .subcommand(
            Command::new("gc")
                .display_order(10)
                .about("Garbage collects stale staged splits and splits marked for deletion.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index")
                        .display_order(1)
                        .required(true),
                    arg!(--"grace-period" <GRACE_PERIOD> "Threshold period after which stale staged splits are garbage collected.")
                        .default_value("1h")
                        .required(false),
                    arg!(--"dry-run" "Executes the command in dry run mode and only displays the list of splits candidates for garbage collection.")
                        .required(false),
                ])
            )
        .subcommand(
            Command::new("merge")
                .display_order(10)
                .about("Merges all the splits for a given Node ID, index ID, source ID.")
                .args(&[
                    arg!(--index <INDEX> "ID of the target index.")
                        .display_order(1)
                        .required(true),
                    arg!(--source <SOURCE_ID> "ID of the target source.")
                        .display_order(2)
                        .required(true),
                ])
            )
        .arg_required_else_help(true)
}

#[derive(Debug, Eq, PartialEq)]
pub struct LocalIngestDocsArgs {
    pub config_uri: Uri,
    pub index_id: IndexId,
    pub input_path_opt: Option<Uri>,
    pub input_format: SourceInputFormat,
    pub overwrite: bool,
    pub vrl_script: Option<String>,
    pub clear_cache: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct LocalSearchArgs {
    pub config_uri: Uri,
    pub index_id: IndexId,
    pub query: String,
    pub aggregation: Option<String>,
    pub max_hits: usize,
    pub start_offset: usize,
    pub search_fields: Option<Vec<String>>,
    pub snippet_fields: Option<Vec<String>>,
    pub start_timestamp: Option<i64>,
    pub end_timestamp: Option<i64>,
    pub sort_by_field: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct GarbageCollectIndexArgs {
    pub config_uri: Uri,
    pub index_id: IndexId,
    pub grace_period: Duration,
    pub dry_run: bool,
}

#[derive(Debug, Eq, PartialEq)]
pub struct MergeArgs {
    pub config_uri: Uri,
    pub index_id: IndexId,
    pub source_id: SourceId,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ExtractSplitArgs {
    pub config_uri: Uri,
    pub index_id: IndexId,
    pub split_id: SplitId,
    pub target_dir: PathBuf,
}

#[derive(Debug, Eq, PartialEq)]
pub enum ToolCliCommand {
    GarbageCollect(GarbageCollectIndexArgs),
    LocalIngest(LocalIngestDocsArgs),
    LocalSearch(LocalSearchArgs),
    ExtractSplit(ExtractSplitArgs),
    Merge(MergeArgs),
}

impl ToolCliCommand {
    pub fn parse_cli_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let (subcommand, submatches) = matches
            .remove_subcommand()
            .context("failed to parse tool subcommand")?;
        match subcommand.as_str() {
            "gc" => Self::parse_garbage_collect_args(submatches),
            "local-ingest" => Self::parse_local_ingest_args(submatches),
            "local-search" => Self::parse_local_search_args(submatches),
            "merge" => Self::parse_merge_args(submatches),
            "extract-split" => Self::parse_extract_split_args(submatches),
            _ => bail!("unknown tool subcommand `{subcommand}`"),
        }
    }

    fn parse_local_ingest_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .remove_one::<String>("config")
            .map(|uri_str| Uri::from_str(&uri_str))
            .expect("`config` should be a required arg.")?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let input_path_opt = if let Some(input_path) = matches.remove_one::<String>("input-path") {
            Some(Uri::from_str(&input_path)?)
        } else {
            None
        };

        let input_format = matches
            .remove_one::<String>("input-format")
            .map(|input_format| SourceInputFormat::from_str(&input_format))
            .expect("`input-format` should have a default value.")
            .map_err(|err| anyhow::anyhow!(err))?;
        let overwrite = matches.get_flag("overwrite");
        let vrl_script = matches.remove_one::<String>("transform-script");
        let clear_cache = !matches.get_flag("keep-cache");

        Ok(Self::LocalIngest(LocalIngestDocsArgs {
            config_uri,
            index_id,
            input_path_opt,
            input_format,
            overwrite,
            vrl_script,
            clear_cache,
        }))
    }

    fn parse_local_search_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .remove_one::<String>("config")
            .map(|uri_str| Uri::from_str(&uri_str))
            .expect("`config` should be a required arg.")?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let query = matches
            .remove_one::<String>("query")
            .context("`query` should be a required arg")?;
        let aggregation = matches.remove_one::<String>("aggregation");
        let max_hits = matches
            .remove_one::<String>("max-hits")
            .expect("`max-hits` should have a default value.")
            .parse()?;
        let start_offset = matches
            .remove_one::<String>("start-offset")
            .expect("`start-offset` should have a default value.")
            .parse()?;
        let search_fields = matches
            .remove_many::<String>("search-fields")
            .map(|values| values.collect());
        let snippet_fields = matches
            .remove_many::<String>("snippet-fields")
            .map(|values| values.collect());
        let sort_by_field = matches.remove_one::<String>("sort-by-field");
        let start_timestamp = matches
            .remove_one::<String>("start-timestamp")
            .map(|ts| ts.parse())
            .transpose()?;
        let end_timestamp = matches
            .remove_one::<String>("end-timestamp")
            .map(|ts| ts.parse())
            .transpose()?;
        Ok(Self::LocalSearch(LocalSearchArgs {
            config_uri,
            index_id,
            query,
            aggregation,
            max_hits,
            start_offset,
            search_fields,
            snippet_fields,
            start_timestamp,
            end_timestamp,
            sort_by_field,
        }))
    }

    fn parse_merge_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .remove_one::<String>("config")
            .map(|uri_str| Uri::from_str(&uri_str))
            .expect("`config` should be a required arg.")?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("'index-id' should be a required arg.");
        let source_id = matches
            .remove_one::<String>("source")
            .expect("'source-id' should be a required arg.");
        Ok(Self::Merge(MergeArgs {
            index_id,
            source_id,
            config_uri,
        }))
    }

    fn parse_garbage_collect_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let config_uri = matches
            .get_one("config")
            .map(|uri_str: &String| Uri::from_str(uri_str))
            .expect("`config` should be a required arg.")?;
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let grace_period = matches
            .get_one("grace-period")
            .map(|duration_str: &String| humantime::parse_duration(duration_str))
            .expect("`grace-period` should have a default value.")?;
        let dry_run = matches.get_flag("dry-run");
        Ok(Self::GarbageCollect(GarbageCollectIndexArgs {
            index_id,
            grace_period,
            dry_run,
            config_uri,
        }))
    }

    fn parse_extract_split_args(mut matches: ArgMatches) -> anyhow::Result<Self> {
        let index_id = matches
            .remove_one::<String>("index")
            .expect("`index` should be a required arg.");
        let split_id: SplitId = matches
            .remove_one::<String>("split")
            .expect("`split` should be a required arg.")
            .into();
        let config_uri = matches
            .remove_one::<String>("config")
            .map(|uri_str| Uri::from_str(&uri_str))
            .expect("`config` should be a required arg.")?;
        let target_dir = matches
            .remove_one::<String>("target-dir")
            .map(PathBuf::from)
            .expect("`target-dir` should be a required arg.");
        Ok(Self::ExtractSplit(ExtractSplitArgs {
            config_uri,
            index_id,
            split_id,
            target_dir,
        }))
    }

    pub async fn execute(self) -> anyhow::Result<()> {
        match self {
            Self::GarbageCollect(args) => garbage_collect_index_cli(args).await,
            Self::LocalIngest(args) => local_ingest_docs_cli(args).await,
            Self::LocalSearch(args) => local_search_cli(args).await,
            Self::Merge(args) => merge_cli(args).await,
            Self::ExtractSplit(args) => extract_split_cli(args).await,
        }
    }
}

pub async fn local_ingest_docs_cli(args: LocalIngestDocsArgs) -> anyhow::Result<()> {
    debug!(args=?args, "local-ingest-docs");
    println!("❯ Ingesting documents locally...");

    let config = load_node_config(&args.config_uri, None).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let mut metastore = metastore_resolver.resolve(&config.metastore_uri).await?;

    let source_params = if let Some(uri) = args.input_path_opt.as_ref() {
        SourceParams::file_from_uri(uri.clone())
    } else {
        SourceParams::stdin()
    };
    let transform_config = args
        .vrl_script
        .map(|vrl_script| TransformConfig::new(vrl_script, None));
    let source_config = SourceConfig {
        source_id: CLI_SOURCE_ID.to_string(),
        enabled: true,
        source_params,
        transform_config,
        input_format: args.input_format,
    };
    run_index_checklist(
        &mut metastore,
        &storage_resolver,
        &args.index_id,
        Some(&source_config),
    )
    .await?;

    if args.overwrite {
        let mut index_service = IndexService::new(metastore.clone(), storage_resolver.clone());
        index_service.clear_index(&args.index_id).await?;
    }

    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(args.index_id.clone()))
        .await?
        .deserialize_index_metadata()?;
    let index_uid = index_metadata.index_uid.clone();
    let index_config = index_metadata.index_config;
    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)?;
    let merge_policy = merge_policy_from_settings(&index_config.indexing_settings);
    let storage = storage_resolver.resolve(&index_config.index_uri).await?;
    let split_store = IndexingSplitStore::new(storage);

    if args.input_path_opt.is_none() && io::stdin().is_terminal() {
        let eof_shortcut = match env::consts::OS {
            "windows" => "CTRL+Z",
            _ => "CTRL+D",
        };
        println!(
            "Please, enter JSON documents one line at a time.\nEnd your input using \
             {eof_shortcut}."
        );
    }

    let start_time = Instant::now();
    let mut reader = if let Some(uri) = args.input_path_opt.as_ref() {
        DocFileReader::from_uri(&storage_resolver, uri).await?
    } else {
        DocFileReader::from_reader(Box::new(tokio::io::stdin()))
    };
    let mut converter = DocumentConverter::try_new(
        index_uid.index_id.clone(),
        CLI_SOURCE_ID.to_string(),
        doc_mapper.clone(),
        source_config.transform_config,
        source_config.input_format,
    )?;
    let delete_opstamp = metastore
        .last_delete_opstamp(LastDeleteOpstampRequest {
            index_uid: Some(index_uid.clone()),
        })
        .await?
        .last_delete_opstamp;
    let pipeline_id = IndexingPipelineId {
        node_id: config.node_id.clone(),
        index_uid: index_uid.clone(),
        source_id: CLI_SOURCE_ID.to_string(),
    };
    let scratch_root = tempfile::tempdir()?;
    let mut builders: HashMap<u64, IndexedSplitBuilder> = HashMap::new();
    loop {
        let batch = reader.read_batch(BATCH_NUM_BYTES_LIMIT).await?;
        if batch.is_empty() {
            break;
        }
        for doc in converter.process_docs(batch) {
            let partition_id = doc.partition;
            let builder = match builders.entry(partition_id) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let scratch_dir = quickwit_common::temp_dir::Builder::default()
                        .join("local-ingest")
                        .tempdir_in(scratch_root.path())?;
                    entry.insert(IndexedSplitBuilder::from_doc_mapper(
                        pipeline_id.clone(),
                        partition_id,
                        delete_opstamp,
                        &doc_mapper,
                        &index_config.indexing_settings,
                        scratch_dir,
                        IoControls::default(),
                        config.docs_clustering_config.as_ref(),
                    )?)
                }
            };
            builder.add_document(doc, converter.doc_data())?;
        }
    }

    let doc_data = converter.take_frozen_doc_data();
    let tag_fields = doc_mapper.tag_named_fields()?;
    let mut staged_split_ids = Vec::with_capacity(builders.len());
    for (_, builder) in builders {
        let indexed_split = builder.finalize(doc_data.clone())?;
        let packaged_split = create_packaged_split(indexed_split, &tag_fields)?;
        let (split_metadata, split_payload) = prepare_split_for_upload(
            &packaged_split,
            &merge_policy,
            index_config.retention_policy_opt.as_ref(),
        )?;
        metastore
            .stage_splits(StageSplitsRequest::try_from_split_metadata(
                index_uid.clone(),
                &split_metadata,
            )?)
            .await?;
        split_store
            .store_split(
                &split_metadata,
                packaged_split.split_scratch_directory.path(),
                Box::new(split_payload),
            )
            .await?;
        staged_split_ids.push(split_metadata.split_id.to_string());
    }
    let num_published_splits = staged_split_ids.len();
    if !staged_split_ids.is_empty() {
        metastore
            .publish_splits(PublishSplitsRequest {
                index_uid: Some(index_uid.clone()),
                staged_split_ids,
                replaced_split_ids: Vec::new(),
            })
            .await?;
    }

    let counters = converter.counters();
    let num_docs = counters.num_processed_docs();
    let num_invalid_docs = counters.num_invalid_docs();
    if num_docs > 0 {
        let secs = Duration::from_secs(start_time.elapsed().as_secs());
        println!(
            "Indexed {} documents ({} invalid) into {} split(s) in {}.",
            num_docs.separate_with_commas(),
            num_invalid_docs.separate_with_commas(),
            num_published_splits,
            format_duration(secs)
        );
    }
    if num_published_splits > 0 {
        println!(
            "Now, you can query the index with the following command:\nquickfast index search \
             --index {} --config ./config/quickwit.yaml --query \"my query\"",
            args.index_id
        );
    }

    if args.clear_cache {
        println!("Clearing local cache directory...");
        clear_cache_directory(&config.data_dir_path).await?;
        println!("{} Local cache directory cleared.", "✔".color(GREEN_COLOR));
    }

    match num_invalid_docs {
        0 => {
            println!("{} Documents successfully indexed.", "✔".color(GREEN_COLOR));
            Ok(())
        }
        _ => bail!("failed to ingest all the documents"),
    }
}

pub async fn local_search_cli(args: LocalSearchArgs) -> anyhow::Result<()> {
    debug!(args=?args, "local-search");
    println!("❯ Searching directly on the index storage (without calling REST API)...");
    let config = load_node_config(&args.config_uri, None).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let metastore: MetastoreServiceClient =
        metastore_resolver.resolve(&config.metastore_uri).await?;
    let aggs = args
        .aggregation
        .map(|agg_string| serde_json::from_str(&agg_string))
        .transpose()?;
    let sort_by: SortBy = args.sort_by_field.map(SortBy::from).unwrap_or_default();
    let search_request_query_string = SearchRequestQueryString {
        query: args.query,
        start_offset: args.start_offset as u64,
        max_hits: args.max_hits as u64,
        search_fields: args.search_fields,
        snippet_fields: args.snippet_fields,
        start_timestamp: args.start_timestamp,
        end_timestamp: args.end_timestamp,
        aggs,
        format: BodyFormat::Json,
        sort_by,
        count_all: CountHits::CountAll,
        allow_failed_splits: false,
    };
    let search_request =
        search_request_from_api_request(vec![args.index_id], search_request_query_string)?;
    debug!(search_request=?search_request, "search-request");
    let search_response: SearchResponse =
        single_node_search(search_request, metastore, storage_resolver).await?;
    let search_response_rest = SearchResponseRest::try_from(search_response)?;
    let search_response_json = serde_json::to_string_pretty(&search_response_rest)?;
    println!("{search_response_json}");
    Ok(())
}

pub async fn merge_cli(args: MergeArgs) -> anyhow::Result<()> {
    debug!(args=?args, "run-merge-operations");
    println!("❯ Merging splits locally...");
    let config = load_node_config(&args.config_uri, None).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let mut metastore = metastore_resolver.resolve(&config.metastore_uri).await?;
    run_index_checklist(&mut metastore, &storage_resolver, &args.index_id, None).await?;
    let runtimes_config = RuntimesConfig::get();
    start_actor_runtimes(
        runtimes_config,
        &HashSet::from_iter([QuickwitService::Indexer]),
    )?;
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(args.index_id.clone()))
        .await?
        .deserialize_index_metadata()?;
    let index_config = index_metadata.index_config;
    let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)?;
    let storage = storage_resolver.resolve(&index_config.index_uri).await?;
    let split_store = IndexingSplitStore::new(storage);
    let merge_policy = merge_policy_from_settings(&index_config.indexing_settings);
    let scratch_root = tempfile::tempdir()?;
    let indexing_directory = quickwit_common::temp_dir::Builder::default()
        .join("merge")
        .tempdir_in(scratch_root.path())?;

    run_local_merges(
        MergePipelineId {
            node_id: config.node_id,
            index_uid: index_metadata.index_uid,
            source_id: args.source_id,
        },
        doc_mapper,
        indexing_directory,
        metastore,
        split_store,
        merge_policy,
        index_config.retention_policy_opt,
        config.indexer_config.max_concurrent_split_uploads,
    )
    .await?;
    println!("{} Merge successful.", "✔".color(GREEN_COLOR));
    Ok(())
}

pub async fn garbage_collect_index_cli(args: GarbageCollectIndexArgs) -> anyhow::Result<()> {
    debug!(args=?args, "garbage-collect-index");
    println!("❯ Garbage collecting index...");

    let config = load_node_config(&args.config_uri, None).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let metastore = metastore_resolver.resolve(&config.metastore_uri).await?;
    let mut index_service = IndexService::new(metastore, storage_resolver);

    let removal_info = index_service
        .garbage_collect_index(&args.index_id, args.grace_period, args.dry_run)
        .await?;
    print_tantivy_gc_result(args.dry_run, removal_info)
}

fn print_tantivy_gc_result(
    dry_run: bool,
    removal_info: quickwit_index_management::SplitRemovalInfo,
) -> anyhow::Result<()> {
    if removal_info.removed_split_entries.is_empty() && removal_info.failed_splits.is_empty() {
        println!("No dangling files to garbage collect.");
        return Ok(());
    }

    if dry_run {
        println!("The following files will be garbage collected.");
        for entry in &removal_info.removed_split_entries {
            println!(" - {}", entry.file_name.display());
        }
        return Ok(());
    }

    if !removal_info.failed_splits.is_empty() {
        println!("The following splits were attempted to be removed, but failed.");
        for split in &removal_info.failed_splits {
            println!(" - {}", split.split_id);
        }
        println!(
            "{} Splits were unable to be removed.",
            removal_info.failed_splits.len()
        );
    }

    let deleted_bytes: u64 = removal_info
        .removed_split_entries
        .iter()
        .map(|s| s.file_size_bytes.as_u64())
        .sum();
    println!(
        "{}MB of storage garbage collected.",
        deleted_bytes / 1_000_000
    );

    if removal_info.failed_splits.is_empty() {
        println!(
            "{} Index successfully garbage collected.",
            "✔".color(GREEN_COLOR)
        );
    } else if removal_info.removed_split_entries.is_empty() {
        println!("{} Failed to garbage collect index.", "✘".color(RED_COLOR));
    } else {
        println!(
            "{} Index partially garbage collected.",
            "✘".color(RED_COLOR)
        );
    }

    Ok(())
}

async fn extract_split_cli(args: ExtractSplitArgs) -> anyhow::Result<()> {
    debug!(args=?args, "extract-split");
    println!("❯ Extracting split...");

    let config = load_node_config(&args.config_uri, None).await?;
    let (storage_resolver, metastore_resolver) =
        get_resolvers(&config.storage_configs, &config.metastore_configs);
    let metastore = metastore_resolver.resolve(&config.metastore_uri).await?;
    let index_metadata = metastore
        .index_metadata(IndexMetadataRequest::for_index_id(args.index_id))
        .await?
        .deserialize_index_metadata()?;
    let index_storage = storage_resolver.resolve(index_metadata.index_uri()).await?;
    let split_file = PathBuf::from(format!("{}.split", args.split_id));
    let split_bytes = index_storage.get_all(split_file.as_path()).await?;
    let (bundle_storage, _hotcache_bytes) =
        BundleStorage::open_from_split_bytes(index_storage, split_file, split_bytes)?;
    std::fs::create_dir_all(&args.target_dir)?;
    for path in bundle_storage.iter_files() {
        let mut out_path = args.target_dir.to_owned();
        out_path.push(path);
        println!("Copying {out_path:?}");
        bundle_storage.copy_to_file(path, &out_path).await?;
    }

    println!("{} Split successfully extracted.", "✔".color(GREEN_COLOR));
    Ok(())
}
