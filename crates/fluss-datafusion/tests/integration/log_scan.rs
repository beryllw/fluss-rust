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

//! Cluster-free log bounded-scan tests: drive `ctx.sql(...)` through the
//! fixture-backed fake. Proves the required-`LIMIT` contract, projection
//! pushdown, conservative failure when `LIMIT` is absent, and that `EXPLAIN`
//! shows the custom log-scan plan.
//!
//! Available only under `test-fake`. Opens zero sockets.

#![cfg(feature = "test-fake")]

use std::time::Duration;

use arrow::array::{Array, Int32Array, RecordBatch, StringArray};
use datafusion::execution::context::SessionContext;

use fluss_datafusion::{FlussDatafusion, FlussDatafusionOptions, RegisterCatalogOptions};

use crate::integration::utils::{fake_source, fixtures_present, names};

/// Skips when fixtures are not present (mirrors `kv_lookup.rs`/`replay.rs`).
macro_rules! require_fixtures {
    () => {
        if !fixtures_present() {
            eprintln!(
                "skipping: no committed fixtures at {} (run capture with --features integration_tests)",
                crate::integration::utils::fixture_path().display()
            );
            return;
        }
    };
}

fn options() -> FlussDatafusionOptions {
    FlussDatafusionOptions {
        metadata_cache_ttl: Duration::from_secs(300),
        table_cache_capacity: 64,
    }
}

const CATALOG: &str = "fluss";

/// Builds a context with the Fluss catalog registered through the fake.
async fn ctx_with_catalog() -> SessionContext {
    let fd = FlussDatafusion::new_with_source(fake_source(), options());
    let ctx = SessionContext::new();
    fd.register_catalog(&ctx, CATALOG, RegisterCatalogOptions::default())
        .await
        .expect("register_catalog");
    ctx
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Flattens the `i32` values of column `col` across all batches, in order.
fn collect_i32(batches: &[RecordBatch], col: usize) -> Vec<i32> {
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

#[tokio::test]
async fn bounded_scan_with_limit_returns_rows() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;

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

#[tokio::test]
async fn projection_pushdown_keeps_only_projected_column() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;

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

/// Runs a query expected to be rejected and returns the rendered error.
async fn expect_query_error(ctx: &SessionContext, sql: &str) -> String {
    // Failure may surface at planning (`sql`) or at execution (`collect`);
    // accept either so the conservative-failure contract is fully covered.
    match ctx.sql(sql).await {
        Err(e) => e.to_string(),
        Ok(df) => match df.collect().await {
            Err(e) => e.to_string(),
            Ok(_) => panic!("expected query to fail but it succeeded: {sql}"),
        },
    }
}

#[tokio::test]
async fn missing_limit_fails_clearly() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;
    let err = expect_query_error(
        &ctx,
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

#[tokio::test]
async fn explain_shows_custom_log_scan_plan() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;

    let batches = ctx
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

    let mut rendered = String::new();
    for b in &batches {
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

    assert!(
        rendered.contains("FlussLogScanExec"),
        "EXPLAIN should show the custom log-scan plan, got:\n{rendered}"
    );
}
