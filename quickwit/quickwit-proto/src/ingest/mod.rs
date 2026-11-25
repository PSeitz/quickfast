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

use std::iter::zip;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::types::DocUid;

pub mod ingesthree {
    include!("../codegen/quickwit/quickwit.ingest.ingesthree.rs");

    pub const INGESTHREE_FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!("../codegen/quickwit/ingesthree_descriptor.bin");
}

pub use ingesthree::*;

impl IngestRequestV2 {
    pub fn num_bytes(&self) -> usize {
        self.subrequests
            .iter()
            .map(|subrequest| subrequest.num_bytes())
            .sum()
    }
}

impl IngestSubrequest {
    pub fn num_bytes(&self) -> usize {
        self.doc_batch
            .as_ref()
            .map(|doc_batch| doc_batch.doc_buffer.len())
            .unwrap_or(0)
    }
}

#[derive(Debug, Copy, Clone, thiserror::Error, Eq, PartialEq, Serialize, Deserialize)]
pub enum RateLimitingCause {
    #[error("load shedding")]
    LoadShedding,
    #[error("unknown")]
    Unknown,
}

impl DocBatchV2 {
    pub fn docs(&self) -> impl Iterator<Item = (DocUid, Bytes)> + Clone + '_ {
        zip(&self.doc_uids, &self.doc_lengths).scan(
            self.doc_buffer.clone(),
            |doc_buffer, (doc_uid, doc_len)| {
                let doc = doc_buffer.split_to(*doc_len as usize);
                Some((*doc_uid, doc))
            },
        )
    }

    pub fn is_empty(&self) -> bool {
        self.doc_lengths.is_empty()
    }

    pub fn num_bytes(&self) -> usize {
        self.doc_buffer.len() + self.doc_lengths.len() * 4
    }

    pub fn num_docs(&self) -> usize {
        self.doc_lengths.len()
    }

    #[cfg(any(test, feature = "testsuite"))]
    pub fn for_test(docs: impl IntoIterator<Item = &'static str>) -> Self {
        let mut doc_uids = Vec::new();
        let mut doc_buffer = Vec::new();
        let mut doc_lengths = Vec::new();

        for (doc_uid, doc) in docs.into_iter().enumerate() {
            doc_uids.push(DocUid::for_test(doc_uid as u128));
            doc_buffer.extend(doc.as_bytes());
            doc_lengths.push(doc.len() as u32);
        }
        Self {
            doc_uids,
            doc_buffer: Bytes::from(doc_buffer),
            doc_lengths,
        }
    }
}
