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

//! Cluster-free KV point-lookup tests: drive `ctx.sql(...)` through the
//! fixture-backed fake. Proves full-primary-key equality pushdown (single +
//! composite), absent-key behaviour, conservative failure for unsupported
//! predicates, and that `EXPLAIN` shows the custom lookup plan.
//!
//! Available only under `test-fake`. Opens zero sockets.

#![cfg(feature = "test-fake")]

use std::time::Duration;

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::execution::context::SessionContext;

use fluss_datafusion::{FlussDatafusion, FlussDatafusionOptions, RegisterCatalogOptions};

use crate::integration::utils::{fake_source, fixtures_present, names};

/// Skips when fixtures are not present (mirrors `catalog.rs`/`replay.rs`).
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

#[tokio::test]
async fn single_pk_equality_returns_matching_row() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;

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

#[tokio::test]
async fn single_pk_absent_key_returns_no_rows() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;

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

    assert_eq!(total_rows(&batches), 0, "absent key yields zero rows, no error");
}

#[tokio::test]
async fn composite_pk_equality_returns_matching_row() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;

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
async fn non_primary_key_predicate_fails_clearly() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;
    let err = expect_query_error(
        &ctx,
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

#[tokio::test]
async fn partial_composite_key_fails_clearly() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;
    let err = expect_query_error(
        &ctx,
        &format!(
            "SELECT * FROM {CATALOG}.{}.{} WHERE region = 'us'",
            names::DATABASE,
            names::KV_COMPOSITE
        ),
    )
    .await;
    assert!(
        err.contains("unsupported query pattern"),
        "expected unsupported-query error, got: {err}"
    );
}

#[tokio::test]
async fn full_scan_without_filter_fails_clearly() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;
    let err = expect_query_error(
        &ctx,
        &format!(
            "SELECT * FROM {CATALOG}.{}.{}",
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

#[tokio::test]
async fn in_list_predicate_fails_clearly() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;
    let err = expect_query_error(
        &ctx,
        &format!(
            "SELECT * FROM {CATALOG}.{}.{} WHERE id IN (1, 2)",
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

#[tokio::test]
async fn explain_shows_custom_lookup_plan() {
    require_fixtures!();
    let ctx = ctx_with_catalog().await;

    let batches = ctx
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
        rendered.contains("FlussKvLookupExec"),
        "EXPLAIN should show the custom lookup plan, got:\n{rendered}"
    );
}
