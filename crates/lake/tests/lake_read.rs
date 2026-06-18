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

//! End-to-end test of the lake read half: seed a Paimon warehouse in a temp dir
//! using Paimon's own write API (no Spark, no Fluss cluster), then read it back
//! through `fluss-lake`'s catalog + lake reader. This mirrors how paimon-rust's
//! own tests seed a warehouse, and validates snapshot pinning + the
//! catalog/ReadBuilder/to_arrow wiring the union read depends on.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::spec::{DataType, IntType, Schema, VarCharType};
use paimon::{CatalogFactory, CatalogOptions, Options};

use fluss_lake::catalog::{get_table_at_snapshot, open_catalog};
use fluss_lake::config::LakeCatalogConfig;
use fluss_lake::reader::lake::read_lake_table;

const DB: &str = "lake_db";
const TABLE: &str = "events";

fn seed_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "carol"])),
        ],
    )
    .unwrap()
}

/// Seeds a single-snapshot Paimon append table with 3 rows.
async fn seed_warehouse(warehouse: &str) {
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = CatalogFactory::create(options).await.expect("create catalog");

    catalog
        .create_database(DB, true, HashMap::new())
        .await
        .expect("create database");

    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .option("bucket", "1")
        .option("bucket-key", "id")
        .build()
        .expect("build schema");
    let identifier = Identifier::new(DB, TABLE);
    catalog
        .create_table(&identifier, schema, false)
        .await
        .expect("create table");

    let table = catalog.get_table(&identifier).await.expect("get table");
    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().expect("table write");
    writer
        .write_arrow_batch(&seed_batch())
        .await
        .expect("write batch");
    let messages = writer.prepare_commit().await.expect("prepare commit");
    drop(writer);
    write_builder
        .new_commit()
        .commit(messages)
        .await
        .expect("commit");
}

#[tokio::test]
async fn reads_seeded_paimon_snapshot() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());
    seed_warehouse(&warehouse).await;

    // Read it back the way the union kernel will: build the catalog config from
    // (prefix-stripped) lake table properties, open the catalog, pin snapshot 1.
    let config = LakeCatalogConfig::from_catalog_properties(&HashMap::from([(
        "warehouse".to_string(),
        warehouse.clone(),
    )]))
    .unwrap();
    let catalog = open_catalog(&config).await.unwrap();
    let table = get_table_at_snapshot(&catalog, DB, TABLE, 1).await.unwrap();

    let stream = read_lake_table(&table, None, None).await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3, "expected the 3 seeded rows");
    // Schema carries exactly the table columns (no projection requested).
    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(1).name(), "name");
}

#[tokio::test]
async fn projection_selects_named_columns() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());
    seed_warehouse(&warehouse).await;

    let config = LakeCatalogConfig::from_catalog_properties(&HashMap::from([(
        "warehouse".to_string(),
        warehouse.clone(),
    )]))
    .unwrap();
    let catalog = open_catalog(&config).await.unwrap();
    let table = get_table_at_snapshot(&catalog, DB, TABLE, 1).await.unwrap();

    let projection = vec!["id".to_string()];
    let stream = read_lake_table(&table, Some(&projection), None).await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

    assert_eq!(batches[0].num_columns(), 1);
    assert_eq!(batches[0].schema().field(0).name(), "id");
}

const MULTI_DB: &str = "lake_db_mb";
const MULTI_TABLE: &str = "events_mb";
const MULTI_BUCKETS: i32 = 4;

/// Seeds a multi-bucket Paimon append table with `ids` (one row per id).
async fn seed_multi_bucket_warehouse(warehouse: &str, ids: &[i32]) {
    let mut options = Options::new();
    options.set(CatalogOptions::WAREHOUSE, warehouse);
    let catalog = CatalogFactory::create(options).await.expect("create catalog");
    catalog
        .create_database(MULTI_DB, true, HashMap::new())
        .await
        .expect("create database");
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("name", DataType::VarChar(VarCharType::string_type()))
        .option("bucket", &MULTI_BUCKETS.to_string())
        .option("bucket-key", "id")
        .build()
        .expect("build schema");
    let identifier = Identifier::new(MULTI_DB, MULTI_TABLE);
    catalog.create_table(&identifier, schema, false).await.unwrap();

    let batch = {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let names: Vec<String> = ids.iter().map(|i| format!("n{i}")).collect();
        RecordBatch::try_new(
            arrow_schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    };

    let table = catalog.get_table(&identifier).await.unwrap();
    let write_builder = table.new_write_builder();
    let mut writer = write_builder.new_write().unwrap();
    writer.write_arrow_batch(&batch).await.unwrap();
    let messages = writer.prepare_commit().await.unwrap();
    drop(writer);
    write_builder.new_commit().commit(messages).await.unwrap();
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

/// The per-bucket lake read must partition the snapshot: the union of every
/// bucket's rows equals the full read exactly once (no duplication, no loss).
/// This is the regression guard for the bug where each union execution partition
/// re-read the whole snapshot.
#[tokio::test]
async fn per_bucket_read_partitions_snapshot_without_overlap() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());
    let ids: Vec<i32> = (1..=20).collect();
    seed_multi_bucket_warehouse(&warehouse, &ids).await;

    let config = LakeCatalogConfig::from_catalog_properties(&HashMap::from([(
        "warehouse".to_string(),
        warehouse.clone(),
    )]))
    .unwrap();
    let catalog = open_catalog(&config).await.unwrap();
    let table = get_table_at_snapshot(&catalog, MULTI_DB, MULTI_TABLE, 1).await.unwrap();

    // Full read (no bucket filter) is the ground truth.
    let full: Vec<RecordBatch> = read_lake_table(&table, None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let mut full_ids = collect_ids(&full);
    full_ids.sort_unstable();
    assert_eq!(full_ids, ids, "full read must return every seeded row once");

    // Per-bucket reads must tile the snapshot with no overlap and no loss.
    let mut union_ids = Vec::new();
    let mut nonempty_buckets = 0;
    for bucket in 0..MULTI_BUCKETS {
        let table = get_table_at_snapshot(&catalog, MULTI_DB, MULTI_TABLE, 1).await.unwrap();
        let batches: Vec<RecordBatch> = read_lake_table(&table, None, Some(bucket))
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let bucket_ids = collect_ids(&batches);
        if !bucket_ids.is_empty() {
            nonempty_buckets += 1;
        }
        union_ids.extend(bucket_ids);
    }
    union_ids.sort_unstable();

    assert_eq!(
        union_ids, ids,
        "per-bucket reads must reproduce the full snapshot exactly once (no dup, no loss)"
    );
    assert!(
        nonempty_buckets > 1,
        "expected rows to actually spread across buckets, got {nonempty_buckets}"
    );
}
