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

use std::collections::HashMap;

use itertools::Itertools;
use quickwit_proto::metastore::DocumentsStagedForUpload;
use serde::{Deserialize, Serialize};

use crate::file_backed::file_backed_index::FileBackedIndex;
use crate::metastore::DeleteTask;
use crate::{IndexMetadata, Split};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "version")]
pub(crate) enum VersionedFileBackedIndex {
    #[serde(rename = "0.9")]
    V0_9(FileBackedIndexV0_8),
    // Retro compatibility.
    #[serde(alias = "0.8")]
    #[serde(alias = "0.7")]
    V0_8(FileBackedIndexV0_8),
}

impl From<FileBackedIndex> for VersionedFileBackedIndex {
    fn from(index: FileBackedIndex) -> Self {
        VersionedFileBackedIndex::V0_9(index.into())
    }
}

impl From<VersionedFileBackedIndex> for FileBackedIndex {
    fn from(index: VersionedFileBackedIndex) -> Self {
        match index {
            VersionedFileBackedIndex::V0_8(v0_8) | VersionedFileBackedIndex::V0_9(v0_8) => {
                v0_8.into()
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FileBackedIndexV0_8 {
    #[serde(rename = "index")]
    metadata: IndexMetadata,
    splits: Vec<Split>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    staged_files: HashMap<String, DocumentsStagedForUpload>,
    #[serde(default)]
    delete_tasks: Vec<DeleteTask>,
}

impl From<FileBackedIndex> for FileBackedIndexV0_8 {
    fn from(index: FileBackedIndex) -> Self {
        let splits = index
            .splits
            .into_values()
            .sorted_by_key(|split| split.update_timestamp)
            .collect();
        let delete_tasks = index
            .delete_tasks
            .into_iter()
            .sorted_by_key(|delete_task| delete_task.opstamp)
            .collect();
        Self {
            metadata: index.metadata,
            splits,
            staged_files: index.staged_files,
            delete_tasks,
        }
    }
}

impl From<FileBackedIndexV0_8> for FileBackedIndex {
    fn from(index: FileBackedIndexV0_8) -> Self {
        let mut file_backed_index = Self::new(index.metadata, index.splits, index.delete_tasks);
        file_backed_index.staged_files = index.staged_files;
        file_backed_index
    }
}
