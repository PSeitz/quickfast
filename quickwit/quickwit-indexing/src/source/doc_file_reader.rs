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
use std::path::Path;

use anyhow::Context;
use async_compression::tokio::bufread::GzipDecoder;
use bytes::Bytes;
use quickwit_common::uri::Uri;
use quickwit_storage::StorageResolver;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};

/// Streams newline-delimited documents from local or object storage, decoding `.gz` files on the
/// fly.
pub struct DocFileReader {
    reader: BufReader<Box<dyn AsyncRead + Send + Unpin>>,
    is_eof: bool,
}

impl DocFileReader {
    /// Opens a file at `uri` through `storage_resolver`.
    pub async fn from_uri(storage_resolver: &StorageResolver, uri: &Uri) -> anyhow::Result<Self> {
        let (dir_uri, file_name) = dir_and_filename(uri)?;
        let storage = storage_resolver.resolve(&dir_uri).await?;
        let file_num_bytes = storage.file_num_bytes(file_name).await? as usize;
        let raw_reader = storage
            .get_slice_stream(file_name, 0..file_num_bytes)
            .await?;
        let is_gzip = file_name
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("gz"))
            .unwrap_or(false);
        let reader: Box<dyn AsyncRead + Send + Unpin> = if is_gzip {
            let mut decoder = GzipDecoder::new(BufReader::new(raw_reader));
            decoder.multiple_members(true);
            Box::new(decoder)
        } else {
            raw_reader
        };
        Ok(Self::from_reader(reader))
    }

    /// Wraps any async reader (for stdin or tests).
    pub fn from_reader(reader: Box<dyn AsyncRead + Send + Unpin>) -> Self {
        Self {
            reader: BufReader::new(reader),
            is_eof: false,
        }
    }

    /// Reads the next non-empty line as bytes, returning `None` at EOF.
    pub async fn next_doc(&mut self) -> io::Result<Option<Bytes>> {
        if self.is_eof {
            return Ok(None);
        }
        let mut line = String::new();
        loop {
            line.clear();
            let num_bytes = self.reader.read_line(&mut line).await?;
            if num_bytes == 0 {
                self.is_eof = true;
                return Ok(None);
            }
            let trimmed = line.trim();
            if !trimmed.is_empty() {
                return Ok(Some(Bytes::copy_from_slice(trimmed.as_bytes())));
            }
        }
    }

    /// Reads a batch of documents up to `max_bytes` total payload bytes.
    /// Returns an empty `Vec` once EOF has been reached.
    pub async fn read_batch(&mut self, max_bytes: usize) -> io::Result<Vec<Bytes>> {
        let mut docs = Vec::new();
        let mut total_bytes = 0usize;
        while total_bytes < max_bytes {
            match self.next_doc().await? {
                Some(doc) => {
                    total_bytes += doc.len();
                    docs.push(doc);
                }
                None => break,
            }
        }
        Ok(docs)
    }

    pub fn is_eof(&self) -> bool {
        self.is_eof
    }
}

pub(crate) fn dir_and_filename(filepath: &Uri) -> anyhow::Result<(Uri, &Path)> {
    let dir_uri: Uri = filepath
        .parent()
        .context("parent directory could not be resolved")?;
    let file_name = filepath
        .file_name()
        .context("path does not appear to be a file")?;
    Ok((dir_uri, file_name))
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::str::FromStr;

    use async_compression::tokio::write::GzipEncoder;
    use tempfile::NamedTempFile;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn test_doc_file_reader_plain_and_batches() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"{{"id": 1}}"#).unwrap();
        writeln!(file).unwrap();
        writeln!(file, r#"{{"id": 2}}"#).unwrap();
        writeln!(file, r#"{{"id": 3}}"#).unwrap();
        file.flush().unwrap();

        let uri = Uri::from_str(file.path().to_str().unwrap()).unwrap();
        let mut reader = DocFileReader::from_uri(&StorageResolver::unconfigured(), &uri)
            .await
            .unwrap();

        let first = reader.read_batch(15).await.unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(first[0], Bytes::from_static(br#"{"id": 1}"#));
        assert_eq!(first[1], Bytes::from_static(br#"{"id": 2}"#));

        let second = reader.read_batch(1024).await.unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0], Bytes::from_static(br#"{"id": 3}"#));
        assert!(reader.is_eof());
        assert!(reader.read_batch(1024).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_doc_file_reader_gzip() {
        let mut encoder = GzipEncoder::new(Vec::new());
        encoder
            .write_all(b"{\"k\":\"a\"}\n{\"k\":\"b\"}\n")
            .await
            .unwrap();
        encoder.shutdown().await.unwrap();
        let gz_bytes = encoder.into_inner();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.json.gz");
        std::fs::write(&path, &gz_bytes).unwrap();

        let uri = Uri::from_str(path.to_str().unwrap()).unwrap();
        let mut reader = DocFileReader::from_uri(&StorageResolver::unconfigured(), &uri)
            .await
            .unwrap();
        let docs = reader.read_batch(1024).await.unwrap();
        assert_eq!(
            docs,
            vec![
                Bytes::from_static(br#"{"k":"a"}"#),
                Bytes::from_static(br#"{"k":"b"}"#),
            ]
        );
    }
}
