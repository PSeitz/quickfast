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

mod change;
mod metrics;
mod pool;
mod transport;

use std::error;
use std::pin::Pin;

pub use change::Change;
use futures::Future;
pub use metrics::{GrpcMetrics, GrpcMetricsLayer, GrpcStatusCode, RpcName};
pub use pool::Pool;
pub use transport::{BalanceChannel, warmup_channel};

pub type BoxError = Box<dyn error::Error + Send + Sync + 'static>;

pub type BoxFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

pub type BoxFutureInfaillible<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
