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

//! Test-only overrides for the real-cluster integration suite.
//!
//! The stock Fluss test harness has no tiering service, so a lake-enabled table
//! cannot obtain a real `LakeSnapshot` from the cluster. The SQL end-to-end
//! tests therefore seed a Paimon warehouse directly and inject the matching seam
//! (`LakeSeam`) here. Production code never sees this module: it is compiled and
//! exported only under the `integration_tests` feature.

#![cfg(feature = "integration_tests")]

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use fluss::metadata::TablePath;
use fluss_lake::LakeSeam;

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

/// Removes a previously-installed seam override for one table.
pub fn clear_test_lake_seam_override(table_path: &TablePath) {
    TEST_LAKE_SEAMS
        .lock()
        .expect("lake seam override mutex poisoned")
        .remove(&key(table_path));
}

/// Looks up the seam override for a table, if one is installed.
pub(crate) fn get_test_lake_seam_override(table_path: &TablePath) -> Option<LakeSeam> {
    TEST_LAKE_SEAMS
        .lock()
        .expect("lake seam override mutex poisoned")
        .get(&key(table_path))
        .cloned()
}
