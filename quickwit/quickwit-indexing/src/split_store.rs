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

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use quickwit_common::io::{IoControls, IoControlsAccess};
use quickwit_common::uri::Uri;
use quickwit_directories::BundleDirectory;
use quickwit_metastore::SplitMetadata;
use quickwit_proto::types::SplitId;
use quickwit_storage::{PutPayload, Storage, StorageResult};
use tantivy::Directory;
use tantivy::directory::{Advice, MmapDirectory};
use time::OffsetDateTime;
use tracing::{Instrument, debug, info_span, instrument};

pub fn get_tantivy_directory_from_split_bundle(
    split_file: &Path,
) -> StorageResult<Box<dyn Directory>> {
    let mmap_directory = MmapDirectory::open_with_madvice(
        split_file.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("couldn't find parent for {}", split_file.display()),
            )
        })?,
        Advice::Sequential,
    )?;
    let split_fileslice = mmap_directory.open_read(Path::new(&split_file))?;
    Ok(Box::new(BundleDirectory::open_split(split_fileslice)?))
}

/// Thin wrapper around [`Storage`] for uploading and downloading splits.
#[derive(Clone)]
pub struct IndexingSplitStore {
    remote_storage: Arc<dyn Storage>,
}

impl IndexingSplitStore {
    pub fn new(remote_storage: Arc<dyn Storage>) -> Self {
        Self { remote_storage }
    }

    pub fn remote_uri(&self) -> &Uri {
        self.remote_storage.uri()
    }

    fn split_path(&self, split_id: &SplitId) -> PathBuf {
        PathBuf::from(quickwit_common::split_file(split_id))
    }

    /// Uploads a packaged split to remote storage and removes the local scratch folder.
    #[instrument("store_split", skip_all)]
    pub async fn store_split(
        &self,
        split: &SplitMetadata,
        split_folder_path: &Path,
        put_payload: Box<dyn PutPayload>,
    ) -> anyhow::Result<()> {
        let start = Instant::now();
        let split_num_bytes = put_payload.len();
        let key = self.split_path(split.split_id());
        let is_mature = split.is_mature(OffsetDateTime::now_utc());

        self.remote_storage
            .put(&key, put_payload)
            .instrument(info_span!(
                "store_split_in_remote_storage",
                split = ?split.split_id(),
                is_mature = is_mature,
                num_bytes = split_num_bytes
            ))
            .await
            .with_context(|| {
                format!(
                    "failed uploading key {} in bucket {}",
                    key.display(),
                    self.remote_storage.uri()
                )
            })?;

        let elapsed_secs = start.elapsed().as_secs_f32();
        let split_size_in_megabytes = split_num_bytes as f32 / 1_000_000f32;
        let throughput_mb_s = split_size_in_megabytes / elapsed_secs;

        debug!(
            split_size_in_megabytes = %split_size_in_megabytes,
            num_docs = %split.num_docs,
            elapsed_secs = %elapsed_secs,
            throughput_mb_s = %throughput_mb_s,
            is_mature = is_mature,
            "store-split-remote-success"
        );

        tokio::fs::remove_dir_all(split_folder_path).await?;
        Ok(())
    }

    /// Downloads a split bundle from remote storage into `output_dir_path` and opens it.
    #[instrument(skip(self, output_dir_path, io_controls))]
    pub async fn fetch_and_open_split(
        &self,
        split_id: SplitId,
        output_dir_path: &Path,
        io_controls: &IoControls,
    ) -> StorageResult<Box<dyn Directory>> {
        let path = self.split_path(&split_id);
        let dest_filepath = output_dir_path.join(&path);
        let dest_file = tokio::fs::File::create(&dest_filepath).await?;
        let mut dest_file_with_write_limit = io_controls.clone().wrap_write(dest_file);
        self.remote_storage
            .copy_to(&path, &mut dest_file_with_write_limit)
            .instrument(info_span!("fetch_split_from_remote_storage", path = ?path))
            .await?;
        get_tantivy_directory_from_split_bundle(&dest_filepath)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_common::io::IoControls;
    use quickwit_metastore::{SplitMaturity, SplitMetadata};
    use quickwit_proto::types::SplitId;
    use quickwit_storage::{RamStorage, SplitPayloadBuilder};
    use time::OffsetDateTime;
    use tokio::fs;

    use super::IndexingSplitStore;

    #[tokio::test]
    async fn test_store_and_fetch_split() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let remote_storage = Arc::new(RamStorage::default());
        let split_store = IndexingSplitStore::new(remote_storage);

        let split_id = SplitId::new();
        let split_dir = temp_dir.path().join(split_id.as_str());
        fs::create_dir_all(&split_dir).await?;
        let file_path = split_dir.join("splitfile");
        fs::write(&file_path, b"hello").await?;
        let payload = SplitPayloadBuilder::get_split_payload(&[file_path], &[], None, &[])?;
        let split_metadata = SplitMetadata {
            split_id: split_id.clone(),
            create_timestamp: OffsetDateTime::now_utc().unix_timestamp(),
            maturity: SplitMaturity::Immature {
                maturation_period: Duration::from_hours(1),
            },
            ..Default::default()
        };

        split_store
            .store_split(&split_metadata, &split_dir, Box::new(payload))
            .await?;
        assert!(!split_dir.try_exists()?);

        let output = tempfile::tempdir()?;
        let directory = split_store
            .fetch_and_open_split(split_id, output.path(), &IoControls::default())
            .await?;
        let contents = directory.atomic_read(std::path::Path::new("splitfile"))?;
        assert_eq!(contents, b"hello");
        Ok(())
    }
}
