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

//! Smoke test for the package-local `datafusion_demo` example.
//!
//! The goal is intentionally narrow: prove the documented user-facing demo path
//! still works end to end against a real Fluss cluster, without duplicating the
//! full correctness matrix from `e2e.rs`.

#![cfg(feature = "integration_tests")]

use std::sync::Arc;

#[path = "../../examples/support/demo_runner.rs"]
mod demo_runner;

use crate::integration::setup;

const CLUSTER_NAME: &str = "df-example-smoke";
const CLUSTER_PORT: u16 = 9135;

#[tokio::test]
async fn package_local_demo_runs_against_real_cluster() {
    let cluster = setup::start_cluster(CLUSTER_NAME, CLUSTER_PORT).await;
    let connection = Arc::new(cluster.get_fluss_connection().await);

    let summary = demo_runner::run_demo(connection)
        .await
        .expect("demo path should succeed");

    assert_eq!(summary.kv_name, "Noco");
    assert_eq!(summary.kv_age, 25);
    assert_eq!(summary.log_ids, vec![2, 3, 4]);
    assert_eq!(summary.log_actions, vec!["click", "scroll", "close"]);
    assert!(
        summary.kv_explain.contains("FlussKvLookupExec"),
        "expected KV explain to show FlussKvLookupExec, got:\n{}",
        summary.kv_explain
    );
    assert!(
        summary.log_explain.contains("FlussLogScanExec"),
        "expected log explain to show FlussLogScanExec, got:\n{}",
        summary.log_explain
    );

    cluster.stop();
}
