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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub type HealthCheckFn = Arc<dyn Fn() -> bool + Send + Sync>;

struct JanitorServiceInner {
    shutdown: CancellationToken,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    task_health_flags: Vec<Arc<AtomicBool>>,
    extra_health_check: Option<HealthCheckFn>,
}

/// Manages background janitor tasks (GC, retention policy, delete task service)
/// and reports combined liveness.
#[derive(Clone)]
pub struct JanitorService {
    inner: Arc<JanitorServiceInner>,
}

impl JanitorService {
    pub fn new(
        shutdown: CancellationToken,
        tasks: Vec<(JoinHandle<()>, Arc<AtomicBool>)>,
        extra_health_check: Option<HealthCheckFn>,
    ) -> Self {
        let mut join_handles = Vec::with_capacity(tasks.len());
        let mut task_health_flags = Vec::with_capacity(tasks.len());
        for (handle, healthy_flag) in tasks {
            join_handles.push(handle);
            task_health_flags.push(healthy_flag);
        }
        Self {
            inner: Arc::new(JanitorServiceInner {
                shutdown,
                tasks: Mutex::new(join_handles),
                task_health_flags,
                extra_health_check,
            }),
        }
    }

    pub fn is_healthy(&self) -> bool {
        let tasks_healthy = self
            .inner
            .task_health_flags
            .iter()
            .all(|flag| flag.load(Ordering::Relaxed));
        let extra_healthy = self
            .inner
            .extra_health_check
            .as_ref()
            .map(|check| check())
            .unwrap_or(true);
        tasks_healthy && extra_healthy
    }

    /// Cancels all janitor background tasks and waits for them to exit. Idempotent.
    pub async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        let handles = std::mem::take(&mut *self.inner.tasks.lock().await);
        for handle in handles {
            let _ = handle.await;
        }
    }
}

/// Wraps a background task handle so any early exit or panic before `shutdown` is
/// cancelled flips the returned health flag to `false`.
pub(crate) fn monitor_task(
    handle: JoinHandle<()>,
    shutdown: CancellationToken,
) -> (JoinHandle<()>, Arc<AtomicBool>) {
    let healthy = Arc::new(AtomicBool::new(true));
    let healthy_clone = healthy.clone();
    let monitor = tokio::spawn(async move {
        let res = handle.await;
        if res.is_err() || !shutdown.is_cancelled() {
            healthy_clone.store(false, Ordering::Relaxed);
        }
    });
    (monitor, healthy)
}
