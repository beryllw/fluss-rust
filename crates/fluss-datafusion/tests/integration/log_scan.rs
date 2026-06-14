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

use arrow::array::{Array, StringArray};

use crate::integration::utils::fixtures_ready;
use crate::integration::utils::helpers::{
    CATALOG, collect_i32, ctx_with_catalog, expect_query_error, total_rows,
};
use crate::integration::utils::names;

#[tokio::test]
async fn bounded_scan_with_limit_returns_rows() {
    if !fixtures_ready() {
        return;
    }
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
    if !fixtures_ready() {
        return;
    }
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

#[tokio::test]
async fn missing_limit_fails_clearly() {
    if !fixtures_ready() {
        return;
    }
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
    if !fixtures_ready() {
        return;
    }
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
