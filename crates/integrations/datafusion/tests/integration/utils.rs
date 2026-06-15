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

//! Shared helpers for fluss-datafusion integration tests.
//!
//! Two backends, one contract:
//! - fake: replays committed fixtures (no cluster). Default for fast tests.
//! - real: capture path only, gated by `integration_tests` (needs Docker).
//!
//! The same fixture data drives both the capture (write) and replay (read)
//! sides, so the fake never drifts from real Fluss.

#![allow(dead_code)]

use std::path::PathBuf;

/// The single committed fixture file the fake replays.
pub const FIXTURE_FILE: &str = "phase1.json";

/// Known capture identifiers. Keep capture and replay assertions in sync via
/// these constants instead of stringly-typed names.
pub mod names {
    pub const DATABASE: &str = "fluss";
    /// KV table with a single-column primary key.
    pub const KV_SIMPLE: &str = "df_kv_simple";
    /// KV table with a composite primary key.
    pub const KV_COMPOSITE: &str = "df_kv_composite";
    /// Log (append-only) table for bounded scans.
    pub const LOG_BASIC: &str = "df_log_basic";
}

/// Absolute path to the committed fixtures directory.
pub fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

/// Absolute path to the committed Phase 1 fixture file.
pub fn fixture_path() -> PathBuf {
    fixtures_dir().join(FIXTURE_FILE)
}

/// Builds a fixture-backed fake source. Cluster-free; opens zero sockets.
#[cfg(feature = "test-fake")]
pub fn fake_source() -> std::sync::Arc<dyn fluss_datafusion::testing::FlussSource> {
    let path = fixture_path();
    let fake = fluss_datafusion::testing::FakeFlussSource::from_fixture_file(&path)
        .unwrap_or_else(|e| panic!("failed to load fixture {}: {e}", path.display()));
    std::sync::Arc::new(fake)
}

/// True when committed fixtures exist (so replay tests can run).
#[cfg(feature = "test-fake")]
pub fn fixtures_present() -> bool {
    fixture_path().exists()
}

/// Returns `true` when fixtures are present; otherwise logs a skip message and
/// returns `false` so a test can early-return.
///
/// Fixtures are produced by the `integration_tests` capture path against a real
/// cluster. Without them there is nothing to replay; failing loudly here would
/// punish environments that simply have not run capture yet.
#[cfg(feature = "test-fake")]
pub fn fixtures_ready() -> bool {
    if fixtures_present() {
        return true;
    }
    eprintln!(
        "skipping: no committed fixtures at {} (run capture with --features integration_tests)",
        fixture_path().display()
    );
    false
}

/// Shared SQL-path helpers for the integration tests.
///
/// Backend-agnostic helpers (row counting, column extraction, error capture,
/// `EXPLAIN` rendering) are available to both the cluster-free (`test-fake`) and
/// real-cluster (`integration_tests`) suites. The fake-only `ctx_with_catalog`
/// stays gated to `test-fake`; the real suite builds its context from a live
/// connection instead.
#[cfg(any(feature = "test-fake", feature = "integration_tests"))]
pub mod helpers {
    use arrow::array::{Array, Int32Array, RecordBatch, StringArray};
    use datafusion::execution::context::SessionContext;

    use fluss_datafusion::FlussDatafusionOptions;

    /// Catalog name every SQL test registers the Fluss catalog under.
    pub const CATALOG: &str = "fluss";

    /// Default installer options for the tests.
    pub fn options() -> FlussDatafusionOptions {
        FlussDatafusionOptions::default()
    }

    /// Builds a context with the Fluss catalog registered through the fake.
    #[cfg(feature = "test-fake")]
    pub async fn ctx_with_catalog() -> SessionContext {
        use fluss_datafusion::{FlussDatafusion, RegisterCatalogOptions};

        use super::fake_source;

        let fd = FlussDatafusion::new_with_source(fake_source(), options());
        let ctx = SessionContext::new();
        fd.register_catalog(&ctx, CATALOG, RegisterCatalogOptions::default())
            .await
            .expect("register_catalog");
        ctx
    }

    /// Total row count across all batches.
    pub fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Flattens the `i32` values of column `col` across all batches, in order.
    pub fn collect_i32(batches: &[RecordBatch], col: usize) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|b| {
                b.column(col)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("expected int32 column")
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// Concatenates every string-column cell across all batches, one per line.
    ///
    /// `EXPLAIN` renders its plan into string columns; flattening them this way
    /// gives a single haystack a test can assert a custom plan name appears in.
    pub fn render_explain(batches: &[RecordBatch]) -> String {
        let mut rendered = String::new();
        for b in batches {
            for col in b.columns() {
                if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                    for i in 0..arr.len() {
                        if arr.is_valid(i) {
                            rendered.push_str(arr.value(i));
                            rendered.push('\n');
                        }
                    }
                }
            }
        }
        rendered
    }

    /// Runs a query expected to be rejected and returns the rendered error.
    ///
    /// Failure may surface at planning (`sql`) or at execution (`collect`); accept
    /// either so the conservative-failure contract is fully covered.
    pub async fn expect_query_error(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(e) => e.to_string(),
            Ok(df) => match df.collect().await {
                Err(e) => e.to_string(),
                Ok(_) => panic!("expected query to fail but it succeeded: {sql}"),
            },
        }
    }
}
