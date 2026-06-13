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

//! Cluster-free replay tests: drive the [`FlussSource`] seam through the
//! fixture-backed fake. This is the fast green gate for Task 2 and proves the
//! seam surface (metadata + KV lookup + bounded log scan) replays correctly.
//!
//! Available only under `test-fake`. Opens zero sockets.

#![cfg(feature = "test-fake")]

use arrow::array::{Int32Array, Int64Array};

use fluss_datafusion::testing::{KeyValue, TableRef};

use crate::integration::utils::{fake_source, fixtures_present, names};

/// Skips a replay test (with a clear message) when fixtures are not present.
///
/// Fixtures are produced by the `integration_tests` capture path against a real
/// cluster. Without them there is nothing to replay; failing loudly here would
/// punish environments that simply have not run capture yet.
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

/// Flattens the `i32` values of column `col` across all batches, in order.
fn collect_i32(batches: &[arrow::array::RecordBatch], col: usize) -> Vec<i32> {
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
async fn lists_databases_and_tables() {
    require_fixtures!();
    let source = fake_source();

    let databases = source.list_databases().await.expect("list_databases");
    assert!(
        databases.iter().any(|d| d == names::DATABASE),
        "expected database {} in {databases:?}",
        names::DATABASE
    );

    let tables = source
        .list_tables(names::DATABASE)
        .await
        .expect("list_tables");
    for expected in [names::KV_SIMPLE, names::KV_COMPOSITE, names::LOG_BASIC] {
        assert!(
            tables.iter().any(|t| t == expected),
            "expected table {expected} in {tables:?}"
        );
    }
}

#[tokio::test]
async fn unknown_database_and_table_fail_clearly() {
    require_fixtures!();
    let source = fake_source();

    let err = source.list_tables("does_not_exist").await.unwrap_err();
    assert!(
        matches!(
            err,
            fluss_datafusion::FlussDatafusionError::DatabaseNotFound(_)
        ),
        "expected DatabaseNotFound, got {err:?}"
    );

    let missing = TableRef::new(names::DATABASE, "no_such_table");
    let err = source.get_table_meta(&missing).await.unwrap_err();
    assert!(
        matches!(
            err,
            fluss_datafusion::FlussDatafusionError::TableNotFound(_)
        ),
        "expected TableNotFound, got {err:?}"
    );
}

#[tokio::test]
async fn table_meta_reflects_primary_key() {
    require_fixtures!();
    let source = fake_source();

    let kv = source
        .get_table_meta(&TableRef::new(names::DATABASE, names::KV_SIMPLE))
        .await
        .expect("kv meta");
    assert!(kv.has_primary_key(), "kv table should have a primary key");
    assert_eq!(kv.primary_keys, vec!["id".to_string()]);

    let composite = source
        .get_table_meta(&TableRef::new(names::DATABASE, names::KV_COMPOSITE))
        .await
        .expect("composite meta");
    assert_eq!(
        composite.primary_keys,
        vec!["region".to_string(), "id".to_string()]
    );

    let log = source
        .get_table_meta(&TableRef::new(names::DATABASE, names::LOG_BASIC))
        .await
        .expect("log meta");
    assert!(
        !log.has_primary_key(),
        "log table should not have a primary key"
    );
}

#[tokio::test]
async fn kv_lookup_present_key_returns_row() {
    require_fixtures!();
    let source = fake_source();

    let batch = source
        .lookup(
            &TableRef::new(names::DATABASE, names::KV_SIMPLE),
            &vec![KeyValue::Int32(2)],
        )
        .await
        .expect("lookup id=2");

    assert_eq!(batch.num_rows(), 1, "present key should yield one row");
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("id column int32");
    assert_eq!(ids.value(0), 2);
}

#[tokio::test]
async fn kv_lookup_absent_key_returns_empty_batch() {
    require_fixtures!();
    let source = fake_source();

    let batch = source
        .lookup(
            &TableRef::new(names::DATABASE, names::KV_SIMPLE),
            &vec![KeyValue::Int32(999)],
        )
        .await
        .expect("lookup absent");

    assert_eq!(batch.num_rows(), 0, "absent key should yield zero rows");
}

#[tokio::test]
async fn kv_composite_lookup_returns_row() {
    require_fixtures!();
    let source = fake_source();

    let batch = source
        .lookup(
            &TableRef::new(names::DATABASE, names::KV_COMPOSITE),
            &vec![KeyValue::String("us".to_string()), KeyValue::Int32(2)],
        )
        .await
        .expect("composite lookup");

    assert_eq!(batch.num_rows(), 1);
    let scores = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("score column int64");
    assert_eq!(scores.value(0), 200);
}

#[tokio::test]
async fn log_bounded_scan_returns_rows() {
    require_fixtures!();
    let source = fake_source();

    let batches = source
        .log_scan(&TableRef::new(names::DATABASE, names::LOG_BASIC), None, 4)
        .await
        .expect("bounded scan");

    // The capture wrote ids [1,2,3,4,5,6] into a single bucket. Fluss's
    // LimitBatchScanner keeps the LAST `limit` rows, so limit 4 yields [3,4,5,6]
    // (not [1,2,3,4]). Asserting the exact ids locks that semantics in place.
    assert_eq!(collect_i32(&batches, 0), vec![3, 4, 5, 6]);
    assert_eq!(batches[0].num_columns(), 2, "full scan keeps both columns");
}

#[tokio::test]
async fn log_projected_scan_keeps_only_projected_columns() {
    require_fixtures!();
    let source = fake_source();

    let batches = source
        .log_scan(
            &TableRef::new(names::DATABASE, names::LOG_BASIC),
            Some(&[0]),
            4,
        )
        .await
        .expect("projected scan");

    assert_eq!(
        batches[0].num_columns(),
        1,
        "projection to [0] should keep one column"
    );
    // Same last-N rows as the full scan, projected to the id column only.
    assert_eq!(collect_i32(&batches, 0), vec![3, 4, 5, 6]);
}
