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

use quickwit_metrics::{LazyCounter, lazy_counter};

static SKIPPED_MRECORDS_TOTAL: LazyCounter = lazy_counter!(
        name: "skipped_mrecords_total",
        description: "Number of mrecords skipped during decoding, by reason (e.g. an unknown \
                      header version or record type written by a newer binary).",
        subsystem: "ingest",
);

pub(super) static SKIPPED_MRECORDS_UNKNOWN_HEADER_VERSION: LazyCounter =
    lazy_counter!(parent: SKIPPED_MRECORDS_TOTAL, "reason" => "unknown_header_version");

pub(super) static SKIPPED_MRECORDS_UNKNOWN_RECORD_TYPE: LazyCounter =
    lazy_counter!(parent: SKIPPED_MRECORDS_TOTAL, "reason" => "unknown_record_type");

pub(super) static SKIPPED_MRECORDS_MALFORMED: LazyCounter =
    lazy_counter!(parent: SKIPPED_MRECORDS_TOTAL, "reason" => "malformed");

static WAL_BYTES_WRITTEN_TOTAL: LazyCounter = lazy_counter!(
    name: "wal_bytes_written_total",
    description: "Total number of bytes written to the WAL by write operations (create_queue, append_records, truncate_queue, delete_queue), including frame headers and end-of-block padding.",
    subsystem: "ingest",
);

pub(crate) static WAL_BYTES_WRITTEN_CREATE_QUEUE: LazyCounter =
    lazy_counter!(parent: WAL_BYTES_WRITTEN_TOTAL, "operation" => "create_queue");

pub(crate) static WAL_BYTES_WRITTEN_DELETE_QUEUE: LazyCounter =
    lazy_counter!(parent: WAL_BYTES_WRITTEN_TOTAL, "operation" => "delete_queue");

pub(crate) static WAL_BYTES_WRITTEN_APPEND: LazyCounter =
    lazy_counter!(parent: WAL_BYTES_WRITTEN_TOTAL, "operation" => "append");

pub(crate) static WAL_BYTES_WRITTEN_TRUNCATE: LazyCounter =
    lazy_counter!(parent: WAL_BYTES_WRITTEN_TOTAL, "operation" => "truncate");
