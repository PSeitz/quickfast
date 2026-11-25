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

use std::any::TypeId;
use std::fmt;
use std::pin::Pin;

use futures::{Stream, StreamExt, TryStreamExt, stream};

pub type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send + Unpin + 'static>>;

/// A stream impl for code-generated services with streaming endpoints.
pub struct ServiceStream<T> {
    inner: BoxStream<T>,
}

impl<T> ServiceStream<T>
where T: Send + 'static
{
    pub fn new(inner: BoxStream<T>) -> Self {
        Self { inner }
    }

    pub fn empty() -> Self {
        Self {
            inner: Box::pin(stream::empty()),
        }
    }

    pub fn map<F, U>(self, f: F) -> ServiceStream<U>
    where
        F: FnMut(T) -> U + Send + 'static,
        U: Send + 'static,
    {
        ServiceStream {
            inner: Box::pin(self.inner.map(f)),
        }
    }
}

impl<T> fmt::Debug for ServiceStream<T>
where T: 'static
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServiceStream<{:?}>", TypeId::of::<T>())
    }
}

impl<T> Unpin for ServiceStream<T> {}

impl<T, E> ServiceStream<Result<T, E>>
where
    T: Send + 'static,
    E: Send + 'static,
{
    pub fn map_err<F, U>(self, f: F) -> ServiceStream<Result<T, U>>
    where
        F: FnMut(E) -> U + Send + 'static,
        U: Send + 'static,
    {
        ServiceStream {
            inner: Box::pin(self.inner.map_err(f)),
        }
    }
}

impl<T> Stream for ServiceStream<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// Adapts a tonic::Streaming into a ServiceStream of `Result<T, tonic::Status>`. Once
/// an error is encountered, the stream will be closed and subsequent calls to `poll_next` will
/// return `None`.
impl<T> From<tonic::Streaming<T>> for ServiceStream<Result<T, tonic::Status>>
where T: Send + 'static
{
    fn from(streaming: tonic::Streaming<T>) -> Self {
        Self {
            inner: Box::pin(streaming),
        }
    }
}

#[cfg(any(test, feature = "testsuite"))]
impl<T> From<Vec<T>> for ServiceStream<T>
where T: Send + 'static
{
    fn from(values: Vec<T>) -> Self {
        Self {
            inner: Box::pin(stream::iter(values)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_service_stream_map() {
        let mapped_values = ServiceStream::from(vec![0, 1, 2, 3])
            .map(|x| x * 2)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(mapped_values, vec![0, 2, 4, 6]);
    }
}
