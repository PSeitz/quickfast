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
use std::fmt::{Display, Formatter};
use std::hash::Hash;

use crate::types::{IndexUid, NodeId, SourceId};

/// Uniquely identifies an indexing pipeline.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct IndexingPipelineId {
    pub node_id: NodeId,
    pub index_uid: IndexUid,
    pub source_id: SourceId,
}

impl Display for IndexingPipelineId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.index_uid, self.source_id)
    }
}

/// Uniquely identifies a merge / compaction pipeline.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct MergePipelineId {
    pub node_id: NodeId,
    pub index_uid: IndexUid,
    pub source_id: SourceId,
}

impl Display for MergePipelineId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "merge:{}:{}", self.index_uid, self.source_id)
    }
}
