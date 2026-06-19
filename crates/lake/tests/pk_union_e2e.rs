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

//! End-to-end test of the `fluss-lake` public façade for a primary-key table.
//!
//! Models a PK union read: the Paimon lake holds the deduplicated current state
//! tiered up to a seam, and the Fluss changelog tail `[seam, latest)` applies the
//! later upserts/deletes on top. The merge must override updated keys, drop
//! deleted keys, and add inserted keys, leaving untouched keys from the lake.
//!
//! Determinism: the seam offset is captured from the actual latest offset after
//! the "tiered" writes, so the test does not depend on how many changelog records
//! each upsert/delete produces.
//!
//! Run with: `cargo test -p fluss-lake --features integration_tests`
//! (requires a container runtime).
#![cfg(feature = "integration_tests")]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use futures::TryStreamExt;

use fluss::metadata::{DataTypes, LakeSnapshot, Schema, TableBucket, TableDescriptor, TablePath};
use fluss::row::GenericRow;
use fluss_lake::{FlussLakeTable, set_test_lake_snapshot_override};
use fluss_test_cluster::FlussTestingClusterBuilder;
use paimon::catalog::Identifier;
use paimon::spec::{DataType, IntType, Schema as PaimonSchema, VarCharType};
use paimon::{CatalogFactory, CatalogOptions, Options};

const DATABASE: &str = "lake_pk_db";
const TABLE: &str = "pk_union";

/// The "already tiered" lake current state: id 1,2,3.
fn paimon_state_batch() -> RecordBatch {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
        arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, true),
    ]));
    let ids = Int32Array::from(vec![1, 2, 3]);
    let names = StringArray::from(vec!["v1", "v2", "v3"]);
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
}

async fn seed_paimon_pk_warehouse(warehouse: &str) {
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = CatalogFactory::create(options)
        .await
        .expect("create paimon catalog");
    catalog
        .create_database(DATABASE, true, HashMap::new())
        .await
        .expect("create paimon db");
    // Primary-key table, default merge engine (deduplicate), single bucket.
    let schema = PaimonSchema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .primary_key(["id"])
        .option("bucket", "1")
        .build()
        .unwrap();
    let id = Identifier::new(DATABASE, TABLE);
    catalog.create_table(&id, schema, false).await.unwrap();
    let table = catalog.get_table(&id).await.unwrap();
    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().unwrap();
    writer
        .write_arrow_batch(&paimon_state_batch())
        .await
        .unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    drop(writer);
    write_builder.new_commit().commit(messages).await.unwrap();
}

fn row(id: i32, name: &str) -> GenericRow<'static> {
    let mut r = GenericRow::new(2);
    r.set_field(0, id);
    r.set_field(1, name.to_string());
    r
}

async fn latest_offset(connection: &fluss::client::FlussConnection, table_path: &TablePath) -> i64 {
    let admin = connection.get_admin().unwrap();
    let start = std::time::Instant::now();
    loop {
        if let Ok(map) = admin
            .list_offsets(table_path, &[0], fluss::rpc::message::OffsetSpec::Latest)
            .await
        {
            if let Some(&off) = map.get(&0) {
                return off;
            }
        }
        if start.elapsed() >= std::time::Duration::from_secs(30) {
            panic!("bucket 0 of {table_path} not ready in 30s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

fn collect_pairs(batches: &[RecordBatch]) -> Vec<(i32, String)> {
    let mut out = Vec::new();
    for b in batches {
        let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let names = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..b.num_rows() {
            out.push((ids.value(i), names.value(i).to_string()));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn facade_merges_pk_lake_state_with_changelog_tail() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());

    let cluster = FlussTestingClusterBuilder::new("lake-pk-union-e2e")
        .with_port(9149)
        .build()
        .await;
    let connection = Arc::new(cluster.get_fluss_connection().await);

    let table_path = TablePath::new(DATABASE, TABLE);
    let admin = connection.get_admin().unwrap();
    admin.create_database(DATABASE, None, true).await.unwrap();
    // The table itself carries no datalake properties; the lake catalog config is
    // supplied explicitly to `FlussLakeTable::new` below. This keeps the test
    // independent of whether the cluster image accepts `table.datalake.*` props.
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .primary_key(vec!["id".to_string()])
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
    let upsert = fluss_table.new_upsert().unwrap().create_writer().unwrap();

    // Phase 1: the writes that built the "tiered" lake state (id 1,2,3).
    upsert.upsert(&row(1, "v1")).unwrap();
    upsert.upsert(&row(2, "v2")).unwrap();
    upsert.upsert(&row(3, "v3")).unwrap();
    upsert.flush().await.unwrap();
    // Seam: lake covers the changelog up to here.
    let seam = latest_offset(&connection, &table_path).await;

    // Seed the lake current state as of the seam.
    seed_paimon_pk_warehouse(&warehouse).await;

    // Phase 2: post-seam changes — update id2, delete id3, insert id4.
    upsert.upsert(&row(2, "v2b")).unwrap();
    upsert.delete(&row(3, "v3")).unwrap();
    upsert.upsert(&row(4, "v4")).unwrap();
    upsert.flush().await.unwrap();

    let table_info = admin.get_table_info(&table_path).await.unwrap();
    set_test_lake_snapshot_override(
        &table_path,
        LakeSnapshot::new(
            1,
            HashMap::from([(TableBucket::new(table_info.get_table_id(), 0), seam)]),
        ),
    );

    // Read through the public façade. The lake catalog config (warehouse) is
    // supplied explicitly rather than read from table props.
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
    assert_eq!(plan.split_count(), 1, "single-bucket PK table => one split");
    let read = scan.new_read().unwrap();

    let mut batches = Vec::new();
    for split in plan.splits() {
        let part: Vec<RecordBatch> = read
            .read_split(split)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        batches.extend(part);
    }

    assert_eq!(
        collect_pairs(&batches),
        vec![
            (1, "v1".into()),  // untouched (from lake)
            (2, "v2b".into()), // updated by changelog tail
            (4, "v4".into()),  // inserted by changelog tail
        ],
        "id3 deleted; id2 updated; id4 inserted; id1 from lake"
    );

    cluster.stop();
}
