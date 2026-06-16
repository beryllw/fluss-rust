// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Sync<->async bridge for DataFusion's synchronous catalog callbacks.
//!
//! `CatalogProvider::schema_names`/`schema` and `SchemaProvider::table_names`/
//! `table_exist` are synchronous, but every Fluss metadata call is async. These
//! helpers run an async future to completion from those callbacks. A single
//! lazily-built global runtime backs the fallback, so we never spin up a fresh
//! runtime per call.

use std::future::Future;
use std::sync::OnceLock;

use tokio::runtime::{Handle, Runtime};

/// Panic message shared by the blocking catalog bridges when their helper thread
/// dies. Lives here next to [`block_on_with_runtime`], the only place it is used.
pub(crate) const ACCESS_PANIC: &str = "fluss catalog access thread panicked";

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn global_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Runtime::new()
            .expect("failed to build global tokio runtime for fluss datafusion integration")
    })
}

/// Blocking bridge for synchronous DataFusion catalog callbacks
/// (`schema_names`/`table_names`/`table_exist`) that cannot `.await`.
///
/// If already on a tokio runtime, spawn the future onto the persistent global
/// runtime and block the caller on the result channel — this avoids the "cannot
/// block the current thread from within a runtime" panic while reusing the
/// runtime's worker pool instead of spawning a fresh OS thread per call.
/// Concurrent callers each spawn independently onto the multi-thread runtime, so
/// they still run concurrently. If the spawned task panics it drops the sender
/// without sending, so `rx.recv()` returns `Err` and `.expect(panic_error)`
/// surfaces the same panic message to the caller. Otherwise (not on a runtime)
/// drive the global runtime directly.
pub(crate) fn block_on_with_runtime<F>(future: F, panic_error: &'static str) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if Handle::try_current().is_ok() {
        let (tx, rx) = std::sync::mpsc::channel();
        global_runtime().spawn(async move {
            let out = future.await;
            let _ = tx.send(out);
        });
        rx.recv().expect(panic_error)
    } else {
        global_runtime().block_on(future)
    }
}

/// Lighter bridge for the async `table()` path: already inside a runtime, so just
/// await; otherwise drive the global runtime.
///
/// Kept as the async counterpart to [`block_on_with_runtime`] for symmetry and
/// for callbacks that gain an out-of-runtime async path later; the current async
/// `table()` is always driven by DataFusion's runtime, so it is not yet called.
#[allow(dead_code)]
pub(crate) async fn await_with_runtime<F>(future: F) -> F::Output
where
    F: Future,
{
    if Handle::try_current().is_ok() {
        future.await
    } else {
        global_runtime().block_on(future)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn returns_value_inside_and_outside_runtime() {
        // No ambient runtime: drives the not-in-runtime `block_on` branch.
        assert!(Handle::try_current().is_err());
        let out = block_on_with_runtime(async { 42_u32 }, ACCESS_PANIC);
        assert_eq!(out, 42);

        // On a tokio runtime: `spawn_blocking` gives us a thread that is still
        // associated with the runtime, so `Handle::try_current().is_ok()` holds
        // inside the closure and the spawn+channel branch executes.
        let rt = Runtime::new().expect("build test runtime");
        let out = rt.block_on(async {
            tokio::task::spawn_blocking(|| {
                assert!(Handle::try_current().is_ok());
                block_on_with_runtime(async { 7_u64 }, ACCESS_PANIC)
            })
            .await
            .expect("spawn_blocking join failed")
        });
        assert_eq!(out, 7);
    }

    #[test]
    fn concurrent_calls_all_return_and_run_concurrently() {
        // From outside any runtime, issue N calls from N std threads. Each future
        // sleeps, so if they were serialized the wall clock would be ~N * sleep.
        // We assert correctness for every call, plus a generous timing margin.
        const N: u64 = 8;
        const SLEEP_MS: u64 = 100;

        let start = Instant::now();
        let (tx, rx) = mpsc::channel();
        let mut handles = Vec::new();
        for i in 0..N {
            let tx = tx.clone();
            handles.push(std::thread::spawn(move || {
                let out = block_on_with_runtime(
                    async move {
                        tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
                        i * 2
                    },
                    ACCESS_PANIC,
                );
                tx.send((i, out)).unwrap();
            }));
        }
        drop(tx);
        for h in handles {
            h.join().unwrap();
        }

        let mut results: Vec<(u64, u64)> = rx.iter().collect();
        results.sort_unstable();
        let expected: Vec<(u64, u64)> = (0..N).map(|i| (i, i * 2)).collect();
        assert_eq!(results, expected);

        // Concurrency check with a wide margin to avoid flakiness: serial would be
        // N * SLEEP_MS; we only require it to be comfortably below half of that.
        let serial = Duration::from_millis(N * SLEEP_MS);
        assert!(
            start.elapsed() < serial / 2,
            "calls appear serialized: elapsed {:?} >= {:?}",
            start.elapsed(),
            serial / 2
        );
    }

    #[test]
    fn panicking_future_propagates_panic_error() {
        // A bridged future that panics drops `tx` without sending, so `rx.recv()`
        // errors and `.expect(panic_error)` panics on the caller thread. We drive
        // the in-runtime branch via spawn_blocking on a small runtime so the panic
        // surfaces through our channel rather than being swallowed by tokio's
        // JoinHandle.
        let rt = Runtime::new().expect("build test runtime");
        let result = rt.block_on(async {
            tokio::task::spawn_blocking(|| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    block_on_with_runtime::<_>(
                        async {
                            panic!("boom inside bridged future");
                            #[allow(unreachable_code)]
                            0_u32
                        },
                        ACCESS_PANIC,
                    )
                }))
            })
            .await
            .expect("spawn_blocking join failed")
        });

        let err = result.expect_err("expected the bridged panic to unwind the caller");
        // `.expect(panic_error)` formats the payload as `"{panic_error}: {RecvError}"`,
        // matching the old `.join().expect(panic_error)` behavior, so assert the
        // caller-side panic message carries `panic_error` as its prefix.
        let msg = err
            .downcast_ref::<&'static str>()
            .copied()
            .map(str::to_owned)
            .or_else(|| err.downcast_ref::<String>().cloned())
            .expect("panic payload was neither &str nor String");
        assert!(
            msg.starts_with(ACCESS_PANIC),
            "panic message {msg:?} does not start with {ACCESS_PANIC:?}"
        );
    }
}
