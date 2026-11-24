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

use std::fmt;

use itertools::Itertools;
use quickwit_metastore::SplitMetadata;
use quickwit_proto::types::{IndexUid, SplitId};
use tracing::Span;

use crate::merge_policy::MergeTask;

pub struct SplitsUpdate {
    pub index_uid: IndexUid,
    pub new_splits: Vec<SplitMetadata>,
    pub replaced_split_ids: Vec<SplitId>,
    /// A [`MergeTask`] tracked by the `DeleteTaskPlanner` in `DeleteTaskPipeline`.
    pub merge_task: Option<MergeTask>,
    pub parent_span: Span,
}

impl fmt::Debug for SplitsUpdate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let new_split_ids: String = self
            .new_splits
            .iter()
            .map(|split| split.split_id())
            .join(",");
        f.debug_struct("SplitsUpdate")
            .field("index_id", &self.index_uid.index_id)
            .field("new_splits", &new_split_ids)
            .finish()
    }
}
