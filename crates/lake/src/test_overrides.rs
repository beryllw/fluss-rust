// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations under
// the License.

//! Test-only seam overrides for `fluss-lake` integration tests.
//!
//! The stock Fluss test harness has no tiering service, so a lake-enabled table
//! cannot obtain a real `LakeSnapshot` from the cluster. Gated tests therefore
//! seed a Paimon warehouse directly and inject the matching seam (`LakeSeam`)
//! here. Production code never sees this module: it is compiled and exported only
//! under the `integration_tests` feature.

#![cfg(feature = "integration_tests")]

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use fluss::metadata::{LakeSnapshot, TablePath};

use crate::snapshot::LakeSeam;

static TEST_LAKE_SEAMS: LazyLock<Mutex<HashMap<String, LakeSeam>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn key(table_path: &TablePath) -> String {
    table_path.to_string()
}

/// Installs a test-only seam override for one table.
pub fn set_test_lake_seam_override(table_path: &TablePath, seam: LakeSeam) {
    TEST_LAKE_SEAMS
        .lock()
        .expect("lake seam override mutex poisoned")
        .insert(key(table_path), seam);
}

/// Convenience helper for tests that naturally start from a synthetic
/// `LakeSnapshot` rather than from the internal `LakeSeam` type.
pub fn set_test_lake_snapshot_override(table_path: &TablePath, snapshot: LakeSnapshot) {
    set_test_lake_seam_override(table_path, LakeSeam::from_lake_snapshot(&snapshot));
}

/// Removes a previously-installed seam override for one table.
pub fn clear_test_lake_seam_override(table_path: &TablePath) {
    TEST_LAKE_SEAMS
        .lock()
        .expect("lake seam override mutex poisoned")
        .remove(&key(table_path));
}

/// Looks up the seam override for a table, if one is installed.
///
/// Public only under `integration_tests`, so the thin connector can delegate
/// test seam resolution back into `fluss-lake` instead of maintaining its own
/// duplicate override registry.
pub fn get_test_lake_seam_override(table_path: &TablePath) -> Option<LakeSeam> {
    TEST_LAKE_SEAMS
        .lock()
        .expect("lake seam override mutex poisoned")
        .get(&key(table_path))
        .cloned()
}
