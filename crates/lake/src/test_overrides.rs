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

//! Test-only overrides for `fluss-lake` integration tests.
//!
//! Two kinds of override live here, both compiled and exported only under the
//! `integration_tests` feature so production code never sees them:
//!
//! 1. **Seam override** — for tests that seed a Paimon warehouse directly (no
//!    tiering service) and inject the matching seam (`LakeSeam`). When set, it
//!    short-circuits the real `get_latest_lake_snapshot` call.
//! 2. **S3 endpoint override** — for real-tiering tests where the Fluss cluster,
//!    the Flink tiering job, and the lake warehouse all live behind an
//!    S3-compatible store (RustFS) on a container network. The server-derived
//!    lake catalog props carry the *container-internal* endpoint
//!    (`http://rustfs-<name>:9000`), which the host test process cannot resolve.
//!    This override rewrites the S3 endpoint to the host-mapped address so the
//!    in-process Paimon reader reaches the same store. It is global (one store
//!    per test run), so it needs no per-table key.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use fluss::metadata::{LakeSnapshot, TablePath};

use crate::snapshot::LakeSeam;

static TEST_LAKE_SEAMS: LazyLock<Mutex<HashMap<String, LakeSeam>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Host-reachable S3 connection info that lake catalog config should use instead
/// of the values in the server-derived table properties. The server carries the
/// container-internal endpoint and (for security) omits the static credentials,
/// so the host test process must supply both.
#[derive(Clone)]
struct TestLakeS3 {
    endpoint: String,
    access_key: String,
    secret_key: String,
}

static TEST_LAKE_S3: LazyLock<Mutex<Option<TestLakeS3>>> = LazyLock::new(|| Mutex::new(None));

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

/// Installs the host-reachable S3 endpoint and credentials that override the
/// container-internal endpoint and the (server-omitted) credentials in every
/// lake catalog config built during the test.
pub fn set_test_lake_s3_endpoint_override(
    endpoint: impl Into<String>,
    access_key: impl Into<String>,
    secret_key: impl Into<String>,
) {
    *TEST_LAKE_S3
        .lock()
        .expect("lake s3 override mutex poisoned") = Some(TestLakeS3 {
        endpoint: endpoint.into(),
        access_key: access_key.into(),
        secret_key: secret_key.into(),
    });
}

/// Removes the S3 override.
pub fn clear_test_lake_s3_endpoint_override() {
    *TEST_LAKE_S3
        .lock()
        .expect("lake s3 override mutex poisoned") = None;
}

/// Applies the S3 override (if installed) to a lake catalog property map, in
/// place, before it is turned into a Paimon catalog config.
///
/// Rewrites `s3.endpoint` to the host-mapped address and injects the static
/// credentials (the server omits them from table properties, so without this the
/// S3 client would fall back to the EC2 metadata credential chain and hang).
/// Also forces `s3.path-style-access=true` (custom IP-addressable endpoint) and
/// defaults `s3.region` when unset, both required for request signing.
pub(crate) fn apply_s3_endpoint_override(props: &mut HashMap<String, String>) {
    let Some(s3) = TEST_LAKE_S3
        .lock()
        .expect("lake s3 override mutex poisoned")
        .clone()
    else {
        return;
    };
    // Only rewrite when the props already describe an S3 warehouse; otherwise a
    // filesystem-warehouse test would gain spurious S3 keys.
    let is_s3 = props
        .get("warehouse")
        .map(|w| w.starts_with("s3://") || w.starts_with("s3a://"))
        .unwrap_or(false);
    if !is_s3 {
        return;
    }
    props.insert("s3.endpoint".to_string(), s3.endpoint);
    props.insert("s3.access-key".to_string(), s3.access_key);
    props.insert("s3.secret-key".to_string(), s3.secret_key);
    props.insert("s3.path-style-access".to_string(), "true".to_string());
    props
        .entry("s3.region".to_string())
        .or_insert_with(|| "us-east-1".to_string());
}
