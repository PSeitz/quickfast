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

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use quickwit_common::temp_dir::TempDirectory;
use quickwit_proto::types::IndexUid;

use crate::models::SplitAttrs;

pub struct PackagedSplit {
    pub serialized_split_fields: Vec<u8>,
    pub split_attrs: SplitAttrs,
    pub split_scratch_directory: TempDirectory,
    pub tags: BTreeSet<String>,
    pub split_files: Vec<PathBuf>,
    pub hotcache_bytes: Vec<u8>,
}

impl PackagedSplit {
    pub fn index_uid(&self) -> &IndexUid {
        &self.split_attrs.index_uid
    }

    pub fn split_id(&self) -> &str {
        self.split_attrs.split_id.as_str()
    }
}

impl fmt::Debug for PackagedSplit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PackagedSplit")
            .field("split_attrs", &self.split_attrs)
            .field("split_scratch_directory", &self.split_scratch_directory)
            .field("tags", &self.tags)
            .field("split_files", &self.split_files)
            .finish()
    }
}
