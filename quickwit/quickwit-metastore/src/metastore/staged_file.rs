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

use quickwit_proto::metastore::{AssignFilesForIndexing, DocumentsStagedForUpload};

pub(super) fn is_eligible(file: &DocumentsStagedForUpload, retry_before: i64) -> bool {
    file.completed_timestamp.is_none()
        && file
            .processing_started_timestamp
            .map(|started| started < retry_before)
            .unwrap_or(true)
}

/// Candidates are sorted, count-capped, and belong to one index. Include the file
/// that reaches or crosses the soft byte target.
pub(super) fn assignment_file_count(
    file_sizes: impl Iterator<Item = u64>,
    max_num_bytes: u64,
) -> usize {
    let mut remaining = max_num_bytes;
    let mut count = 0;
    for num_bytes in file_sizes {
        count += 1;
        remaining = remaining.saturating_sub(num_bytes);
        if remaining == 0 {
            break;
        }
    }
    count
}

/// Check only the selected prefix: completed, in-flight and SKIP LOCKED files cannot
/// contribute bytes or age. The count cap alone never releases an underfilled batch.
pub(super) fn assignment_is_ready<'a>(
    mut files: impl Iterator<Item = &'a DocumentsStagedForUpload>,
    request: &AssignFilesForIndexing,
    now: i64,
) -> bool {
    let Some(oldest) = files.next() else {
        return false;
    };
    if request.max_wait_secs == 0
        || oldest.create_timestamp <= now - i64::from(request.max_wait_secs)
    {
        return true;
    }
    let num_bytes = files.fold(oldest.num_bytes, |total, file| {
        total.saturating_add(file.num_bytes)
    });
    num_bytes >= request.max_num_bytes
}

/// The backend must first verify that every requested input exists in the requested
/// index. Publication IDs identify immutable input/output sets, never assignments.
/// Retained completion evidence makes retries independent of output split lifetime.
pub(super) fn publication_outcome(
    files: &[impl std::borrow::Borrow<DocumentsStagedForUpload>],
    publication_id: &str,
) -> Option<bool> {
    if !files
        .iter()
        .any(|file| file.borrow().completed_timestamp.is_some())
    {
        return None;
    }
    Some(files.iter().all(|file| {
        let file = file.borrow();
        file.completed_timestamp.is_some() && file.publication_id.as_deref() == Some(publication_id)
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_assignment_age_boundary() {
        let file = DocumentsStagedForUpload {
            create_timestamp: 100,
            num_bytes: 40,
            ..Default::default()
        };
        let request = AssignFilesForIndexing {
            max_num_files: 1,
            max_num_bytes: 100,
            retry_after_secs: 60,
            max_wait_secs: 15,
        };
        assert!(!assignment_is_ready(std::iter::once(&file), &request, 114));
        assert!(assignment_is_ready(std::iter::once(&file), &request, 115));
        assert!(assignment_is_ready(std::iter::once(&file), &request, 116));
        assert!(assignment_is_ready(
            std::iter::once(&file),
            &AssignFilesForIndexing {
                max_wait_secs: 0,
                ..request
            },
            100,
        ));
        assert!(!assignment_is_ready(std::iter::empty(), &request, 115));
    }

    #[test]
    fn test_assignment_prefix_includes_crossing_file() {
        assert_eq!(assignment_file_count([40, 70, 10].into_iter(), 100), 2);
        assert_eq!(assignment_file_count([40, 60, 10].into_iter(), 100), 2);
        assert_eq!(assignment_file_count([150, 10].into_iter(), 100), 1);
        assert_eq!(assignment_file_count([40, 20].into_iter(), 100), 2);
        assert_eq!(assignment_file_count(std::iter::empty(), 100), 0);
        assert_eq!(
            assignment_file_count([u64::MAX - 1, 2].into_iter(), u64::MAX),
            2
        );
    }
}
