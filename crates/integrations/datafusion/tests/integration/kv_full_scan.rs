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

//! Real-cluster proof that a NON-lake KV table does NOT support an unbounded
//! full-table scan (`SELECT * FROM kv` with no filter and no `LIMIT`).
//!
//! The supported read shapes are:
//! - complete PK equality => point lookup,
//! - complete bucket-key prefix equality => prefix lookup,
//! - any other predicate shape + LIMIT => bounded scan.
//!
//! With no `LIMIT` and no complete lookup key, the provider must fail
//! conservatively with `unsupported query pattern` instead of attempting to read
//! the KV table's current state from its changelog.
//!
//! Gated by `integration_tests` (needs a container runtime). Run with:
//!   cargo test -p fluss-datafusion --features integration_tests -- kv_full_scan

#![cfg(feature = "integration_tests")]

use std::sync::Arc;

use datafusion::execution::context::SessionContext;

use fluss_datafusion::{FlussDatafusion, RegisterCatalogOptions};

use crate::integration::setup;
use crate::integration::utils::helpers::{expect_query_error, options, CATALOG};
use crate::integration::utils::names;

/// Dedicated name/port so this suite never collides with the other clusters in
/// the same `integration_tests` binary (e2e=9133, live_metadata=9134,
/// example_smoke=9135, kv_bounded=9134-range, kv_prefix_lookup=9137-range).
const CLUSTER_NAME: &str = "df-kv-full-scan";
const CLUSTER_PORT: u16 = 9145;

#[tokio::test]
async fn kv_full_scan_without_limit_is_rejected_through_real_backend() {
    let cluster = setup::start_cluster(CLUSTER_NAME, CLUSTER_PORT).await;
    let connection = Arc::new(cluster.get_fluss_connection().await);

    setup::create_kv_full_scan(&connection).await;

    let fd = FlussDatafusion::new(connection.clone(), options())
        .await
        .expect("FlussDatafusion::new over real connection");
    let ctx = SessionContext::new();
    fd.register_catalog(&ctx, CATALOG, RegisterCatalogOptions::default())
        .await
        .expect("register_catalog");

    let err = expect_query_error(&ctx, &format!("SELECT * FROM {}", table())).await;
    assert!(
        err.contains("unsupported query pattern"),
        "expected unsupported-query error, got: {err}"
    );

    setup::drop_table_named(&connection, names::KV_FULL_SCAN).await;
    cluster.stop();
}

fn table() -> String {
    format!("{CATALOG}.{}.{}", names::DATABASE, names::KV_FULL_SCAN)
}
