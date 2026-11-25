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

mod doc_processor;
mod indexing_service;
mod merge_executor;
mod packager;
mod uploader;
#[cfg(feature = "vrl")]
mod vrl_processing;

pub use doc_processor::{DocProcessorCounters, DocumentConverter};
pub use indexing_service::{INDEXING_DIR_NAME, IndexingService, IndexingServiceCounters};
pub use merge_executor::{
    MergeExecutionContext, combine_partition_ids, execute_merge_operation, merge_split_attrs,
};
pub use packager::create_packaged_split;
pub(crate) use uploader::acquire_indexing_upload_permit;
pub use uploader::prepare_split_for_upload;

/// Partition ID assigned to overflow splits when the number of partitions exceeds the split limit.
pub(crate) const OTHER_PARTITION_ID: u64 = 3264326757911759461u64;
