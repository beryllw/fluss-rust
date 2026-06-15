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

//! End-to-end path: drives real `ctx.sql(...)` through the production public API
//! (`FlussDatafusion::new` -> `RealFlussSource`) against a live Fluss cluster.
//!
//! This is the one path the cluster-free `test-fake` suite structurally cannot
//! cover: the replay tests inject a fake source, and `capture` talks to the
//! low-level client directly. Here the real backend, the sync/async metadata
//! bridge, and the custom execution plans all run against actual Fluss.
//!
//! Gated by `integration_tests` (needs a container runtime). Run with:
//!   cargo test -p fluss-datafusion --features integration_tests -- e2e
//!
//! Assertions mirror the fake-backed `kv_lookup` / `log_scan` / `catalog` suites
//! so the real backend is held to the same contract the fixtures encode. The
//! whole flow runs in a single test against one cluster: bringing up a Fluss
//! cluster is expensive, and the assertions are read-only and independent.

#![cfg(feature = "integration_tests")]

use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use datafusion::execution::context::SessionContext;

use fluss_datafusion::{FlussDatafusion, RegisterCatalogOptions};

use crate::integration::setup;
use crate::integration::utils::helpers::{
    CATALOG, collect_i32, expect_query_error, options, render_explain, total_rows,
};
use crate::integration::utils::names;

/// Distinct from `capture`'s `df-capture`/9123 so both real-cluster tests can run
/// concurrently in the same `integration_tests` binary without colliding.
const CLUSTER_NAME: &str = "df-e2e";
const CLUSTER_PORT: u16 = 9133;

#[tokio::test]
async fn end_to_end_sql_through_real_backend() {
    let cluster = setup::start_cluster(CLUSTER_NAME, CLUSTER_PORT).await;
    let connection = Arc::new(cluster.get_fluss_connection().await);

    setup::create_kv_simple(&connection).await;
    setup::create_kv_composite(&connection).await;
    setup::create_log_basic(&connection).await;

    // The production constructor over the real connection: the path under test.
    let fd = FlussDatafusion::new(connection.clone(), options())
        .await
        .expect("FlussDatafusion::new over real connection");
    let ctx = SessionContext::new();
    fd.register_catalog(&ctx, CATALOG, RegisterCatalogOptions::default())
        .await
        .expect("register_catalog");

    catalog_lists_phase1_tables(&ctx);
    kv_single_pk_equality_returns_row(&ctx).await;
    kv_absent_key_returns_no_rows(&ctx).await;
    kv_composite_pk_equality_returns_row(&ctx).await;
    kv_non_primary_key_predicate_fails(&ctx).await;
    log_bounded_scan_with_limit_returns_rows(&ctx).await;
    log_projection_keeps_only_projected_column(&ctx).await;
    log_missing_limit_fails(&ctx).await;
    explain_shows_custom_plans(&ctx).await;

    // Best-effort cleanup so reruns start clean; the cluster also stops on drop.
    setup::drop_phase1_tables(&connection).await;
    cluster.stop();
}

fn catalog_lists_phase1_tables(ctx: &SessionContext) {
    let schema = ctx
        .catalog(CATALOG)
        .expect("catalog registered")
        .schema(names::DATABASE)
        .expect("database schema present");
    let tables = schema.table_names();
    for expected in [names::KV_SIMPLE, names::KV_COMPOSITE, names::LOG_BASIC] {
        assert!(
            tables.iter().any(|t| t == expected),
            "expected table {expected} in {tables:?}"
        );
    }
}

async fn kv_single_pk_equality_returns_row(ctx: &SessionContext) {
    let batches = ctx
        .sql(&format!(
            "SELECT id, name, age FROM {CATALOG}.{}.{} WHERE id = 2",
            names::DATABASE,
            names::KV_SIMPLE
        ))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");

    assert_eq!(total_rows(&batches), 1, "id=2 should match exactly one row");
    let batch = &batches[0];
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id int32");
    assert_eq!(ids.value(0), 2);
    let names_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name string");
    assert_eq!(names_col.value(0), "Noco");
    let ages = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("age int64");
    assert_eq!(ages.value(0), 25);
}

async fn kv_absent_key_returns_no_rows(ctx: &SessionContext) {
    let batches = ctx
        .sql(&format!(
            "SELECT * FROM {CATALOG}.{}.{} WHERE id = 999",
            names::DATABASE,
            names::KV_SIMPLE
        ))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");

    assert_eq!(
        total_rows(&batches),
        0,
        "absent key yields zero rows, no error"
    );
}

async fn kv_composite_pk_equality_returns_row(ctx: &SessionContext) {
    let batches = ctx
        .sql(&format!(
            "SELECT region, id, score FROM {CATALOG}.{}.{} WHERE region = 'us' AND id = 2",
            names::DATABASE,
            names::KV_COMPOSITE
        ))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");

    assert_eq!(total_rows(&batches), 1);
    let scores = batches[0]
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("score int64");
    assert_eq!(scores.value(0), 200);
}

async fn kv_non_primary_key_predicate_fails(ctx: &SessionContext) {
    let err = expect_query_error(
        ctx,
        &format!(
            "SELECT * FROM {CATALOG}.{}.{} WHERE name = 'x'",
            names::DATABASE,
            names::KV_SIMPLE
        ),
    )
    .await;
    assert!(
        err.contains("unsupported query pattern"),
        "expected unsupported-query error, got: {err}"
    );
}

async fn log_bounded_scan_with_limit_returns_rows(ctx: &SessionContext) {
    let batches = ctx
        .sql(&format!(
            "SELECT * FROM {CATALOG}.{}.{} LIMIT 4",
            names::DATABASE,
            names::LOG_BASIC
        ))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");

    assert_eq!(total_rows(&batches), 4, "limit 4 should yield four rows");
    // Fluss's LimitBatchScanner keeps the LAST `limit` rows: ids [1..6] -> [3,4,5,6].
    assert_eq!(collect_i32(&batches, 0), vec![3, 4, 5, 6]);
}

async fn log_projection_keeps_only_projected_column(ctx: &SessionContext) {
    let batches = ctx
        .sql(&format!(
            "SELECT id FROM {CATALOG}.{}.{} LIMIT 4",
            names::DATABASE,
            names::LOG_BASIC
        ))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");

    assert_eq!(total_rows(&batches), 4);
    assert_eq!(
        batches[0].num_columns(),
        1,
        "projection to id should keep exactly one column"
    );
    assert_eq!(batches[0].schema().field(0).name(), "id");
    assert_eq!(collect_i32(&batches, 0), vec![3, 4, 5, 6]);
}

async fn log_missing_limit_fails(ctx: &SessionContext) {
    let err = expect_query_error(
        ctx,
        &format!(
            "SELECT * FROM {CATALOG}.{}.{}",
            names::DATABASE,
            names::LOG_BASIC
        ),
    )
    .await;
    assert!(
        err.contains("LIMIT required"),
        "expected LIMIT-required error, got: {err}"
    );
}

async fn explain_shows_custom_plans(ctx: &SessionContext) {
    let kv = ctx
        .sql(&format!(
            "EXPLAIN SELECT * FROM {CATALOG}.{}.{} WHERE id = 2",
            names::DATABASE,
            names::KV_SIMPLE
        ))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let kv_rendered = render_explain(&kv);
    assert!(
        kv_rendered.contains("FlussKvLookupExec"),
        "EXPLAIN should show the custom lookup plan, got:\n{kv_rendered}"
    );

    let log = ctx
        .sql(&format!(
            "EXPLAIN SELECT * FROM {CATALOG}.{}.{} LIMIT 4",
            names::DATABASE,
            names::LOG_BASIC
        ))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    let log_rendered = render_explain(&log);
    assert!(
        log_rendered.contains("FlussLogScanExec"),
        "EXPLAIN should show the custom log-scan plan, got:\n{log_rendered}"
    );
}
