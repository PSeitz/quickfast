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

use crate::ingest::*;
use crate::metastore::*;
use crate::types::*;

macro_rules! generate_getters {
    (impl fn $field:ident() -> $type:ty {} for $($struct:ty),+) => {
        $(
        impl $struct {
            // we track caller so the reported line isn't the macro invocation below
            #[track_caller]
            pub fn $field(&self) -> $type {
                self.$field
                    .as_ref()
                    .expect(concat!("`",
                    stringify!($field), "` should be a required field"))
            }
        }
        )*
    }
}

macro_rules! generate_copy_getters {
    (impl fn $field:ident() -> $type:ty {} for $($struct:ty),+) => {
        $(
        impl $struct {
            // we track caller so the reported line isn't the macro invocation below
            #[track_caller]
            pub fn $field(&self) -> $type {
                self.$field
                    .expect(concat!("`",
                    stringify!($field), "` should be a required field"))
            }
        }
        )*
    }
}

// [`DocUid`] getters
generate_copy_getters! {
    impl fn doc_uid() -> DocUid {} for

    ParseFailure
}

// [`IndexUid`] getters
generate_getters! {
    impl fn index_uid() -> &IndexUid {} for
    // Ingest API
    IngestSuccess,

    // Metastore API
    AddSourceRequest,
    CreateIndexResponse,
    DeleteIndexRequest,
    DeleteQuery,
    DeleteSourceRequest,
    DeleteSplitsRequest,
    DocumentsStagedForUpload,
    LastDeleteOpstampRequest,
    ListDeleteTasksRequest,
    ListStaleSplitsRequest,
    MarkSplitsForDeletionRequest,
    PublishFileSplits,
    PublishSplitsRequest,
    RegisterFileForIndexing,
    StageSplitsRequest,
    ToggleSourceRequest,
    UpdateIndexRequest,
    UpdateSourceRequest,
    UpdateSplitsDeleteOpstampRequest
}
