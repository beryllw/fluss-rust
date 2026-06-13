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
