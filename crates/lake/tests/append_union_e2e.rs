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

//! End-to-end test of the `fluss-lake` public façade for an append/log table.
//!
//! It exercises the full public read path against a real Fluss cluster:
//!
//! ```text
//! FlussLakeTable -> new_scan() -> plan() / new_read() -> read_split(split)
//! ```
//!
//! The harness has no tiering service, so the lake side is seeded by writing a
//! Paimon warehouse directly and the per-bucket seam is injected through the
//! test-only override. The union of `lake(snapshot)` and `log_tail(seam..latest)`
//! must reconstruct the full row set exactly once.
//!
//! Run with: `cargo test -p fluss-lake --features integration_tests`
//! (requires a container runtime).
#![cfg(feature = "integration_tests")]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;

use fluss::metadata::{DataTypes, LakeSnapshot, Schema, TableBucket, TableDescriptor, TablePath};
use fluss_lake::{FlussLakeTable, set_test_lake_snapshot_override};
use fluss_test_cluster::FlussTestingClusterBuilder;
use paimon::catalog::Identifier;
use paimon::spec::{DataType, IntType, Schema as PaimonSchema, VarCharType};
use paimon::{CatalogFactory, CatalogOptions, Options};

const DATABASE: &str = "lake_e2e_db";
const TABLE: &str = "append_union";

/// Six Fluss log rows at offsets 0..6 (ids 1..=6).
fn fluss_log_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    let ids = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
    let names = StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
}

/// The first two rows (ids 1,2) as already tiered into the Paimon snapshot.
fn paimon_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    let ids = Int32Array::from(vec![1, 2]);
    let names = StringArray::from(vec!["a", "b"]);
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
}

async fn seed_paimon_warehouse(warehouse: &str) {
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = CatalogFactory::create(options)
        .await
        .expect("create paimon catalog");
    catalog
        .create_database(DATABASE, true, HashMap::new())
        .await
        .expect("create paimon db");
    let schema = PaimonSchema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .option("bucket", "1")
        .option("bucket-key", "id")
        .build()
        .unwrap();
    let id = Identifier::new(DATABASE, TABLE);
    catalog.create_table(&id, schema, false).await.unwrap();
    let table = catalog.get_table(&id).await.unwrap();
    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().unwrap();
    writer.write_arrow_batch(&paimon_batch()).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    drop(writer);
    write_builder.new_commit().commit(messages).await.unwrap();
}

async fn wait_for_offsets(connection: &fluss::client::FlussConnection, table_path: &TablePath) {
    let admin = connection.get_admin().unwrap();
    let start = std::time::Instant::now();
    loop {
        if admin
            .list_offsets(table_path, &[0], fluss::rpc::message::OffsetSpec::Latest)
            .await
            .is_ok()
        {
            return;
        }
        if start.elapsed() >= std::time::Duration::from_secs(30) {
            panic!("table {table_path} bucket not ready in 30s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

fn collect_ids(batches: &[RecordBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

#[tokio::test]
async fn facade_reads_lake_plus_log_tail() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());
    seed_paimon_warehouse(&warehouse).await;

    let cluster = FlussTestingClusterBuilder::new("lake-append-union-e2e")
        .with_port(9148)
        .build()
        .await;
    let connection = Arc::new(cluster.get_fluss_connection().await);

    // Create a lake-enabled append/log table and append 6 rows at offsets 0..6.
    let table_path = TablePath::new(DATABASE, TABLE);
    let admin = connection.get_admin().unwrap();
    admin.create_database(DATABASE, None, true).await.unwrap();
    // The table carries no datalake properties; the lake catalog config is
    // supplied explicitly to `FlussLakeTable::new` below, keeping the test
    // independent of whether the cluster image accepts `table.datalake.*` props.
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .build()
                .unwrap(),
        )
        .distributed_by(Some(1), vec![])
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, true)
        .await
        .unwrap();
    let fluss_table = connection.get_table(&table_path).await.unwrap();
    let writer = fluss_table.new_append().unwrap().create_writer().unwrap();
    writer.append_arrow_batch(fluss_log_batch()).unwrap();
    writer.flush().await.unwrap();
    wait_for_offsets(&connection, &table_path).await;

    // Inject the seam: the lake snapshot covers offsets [0, 2); the log tail
    // therefore must cover [2, latest) = ids 3,4,5,6.
    let table_info = admin.get_table_info(&table_path).await.unwrap();
    set_test_lake_snapshot_override(
        &table_path,
        LakeSnapshot::new(
            1,
            HashMap::from([(TableBucket::new(table_info.get_table_id(), 0), 2)]),
        ),
    );

    // Read the table through the public façade. The lake catalog config
    // (warehouse) is supplied explicitly rather than read from table props.
    let lake_table = FlussLakeTable::new(
        connection.clone(),
        table_path.clone(),
        table_info.get_schema().clone(),
        table_info.get_num_buckets(),
        table_info.get_partition_keys().to_vec(),
        HashMap::from([("warehouse".to_string(), warehouse.clone())]),
    );
    let scan = lake_table.new_scan();
    let plan = scan.plan().await.unwrap();
    assert_eq!(
        plan.split_count(),
        1,
        "single-bucket table => one logical split"
    );

    let read = scan.new_read().unwrap();
    let mut all = Vec::new();
    for split in plan.splits() {
        let batches: Vec<RecordBatch> = read
            .read_split(split)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        all.extend(batches);
    }

    let mut ids = collect_ids(&all);
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1, 2, 3, 4, 5, 6],
        "lake(1,2) ++ log_tail(3,4,5,6)"
    );

    cluster.stop();
}
