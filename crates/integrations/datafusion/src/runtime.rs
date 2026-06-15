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
/// If already on a tokio runtime, spawn a fresh OS thread that `block_on`s the
/// global runtime handle and join it — this avoids the "cannot block the current
/// thread from within a runtime" panic. Otherwise drive the global runtime
/// directly.
pub(crate) fn block_on_with_runtime<F>(future: F, panic_error: &'static str) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if Handle::try_current().is_ok() {
        let handle = global_runtime().handle().clone();
        std::thread::spawn(move || handle.block_on(future))
            .join()
            .expect(panic_error)
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
