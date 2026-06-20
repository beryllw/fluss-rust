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

//! Real-tiering end-to-end test of the `fluss-lake` public façade.
//!
//! Unlike `append_union_e2e` / `pk_union_e2e` (which seed a Paimon warehouse
//! directly and inject the seam), this test drives the *production* discovery
//! path against a real lakehouse:
//!
//! 1. A real Fluss cluster is started with cluster-level Paimon datalake config
//!    behind a RustFS (S3) store, plus a Flink cluster.
//! 2. `table.datalake.enabled = true` tables are created; the server derives the
//!    lake catalog properties (warehouse, S3 endpoint) into the tables.
//! 3. Batch 1 is written, then the real Flink tiering job tiers it into Paimon.
//! 4. The tiering job is **stopped** to freeze the lake seam; batch 2 is then
//!    written and stays in the Fluss log.
//! 5. The tables are read through the façade with no seam injection — the seam
//!    comes from the real `get_latest_lake_snapshot`, and the lake side is the
//!    real tiering output read by paimon-rust over S3.
//!
//! Both an append/log table and a primary-key table are covered in a single test
//! sharing one cluster and one tiering cycle (Flink is heavy; one job tiers all
//! datalake-enabled tables). The only test-only hook is the S3 endpoint/cred
//! override: the server-derived props carry the container-internal endpoint and
//! omit credentials, so the host process supplies host-reachable values.
//!
//! Run with: `cargo test -p fluss-lake --features integration_tests tiering`
//! (requires a container runtime with network access to pull the Flink/RustFS
//! images and download the `paimon-s3` plugin jar).
#![cfg(feature = "integration_tests")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use futures::TryStreamExt;

use fluss::client::FlussConnection;
use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TableInfo, TablePath};
use fluss::row::GenericRow;
use fluss_lake::{
    FlussLakeTable, clear_test_lake_s3_endpoint_override, set_test_lake_s3_endpoint_override,
};
use fluss_test_cluster::{FlussTestingClusterBuilder, PaimonLakeConfig};

const DATABASE: &str = "lake_tiering_db";
const APPEND_TABLE: &str = "append_tiering";
const PK_TABLE: &str = "pk_tiering";

const SEAM_TIMEOUT: Duration = Duration::from_secs(240);

fn append_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(names.to_vec())),
        ],
    )
    .unwrap()
}

fn pk_row(id: i32, name: &str) -> GenericRow<'static> {
    let mut r = GenericRow::new(2);
    r.set_field(0, id);
    r.set_field(1, name.to_string());
    r
}

fn lake_descriptor(schema: Schema) -> TableDescriptor {
    TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(1), vec![])
        .property("table.datalake.enabled", "true")
        .property("table.datalake.freshness", "30s")
        .build()
        .unwrap()
}

async fn write_append(connection: &FlussConnection, table_path: &TablePath, batch: RecordBatch) {
    let table = connection.get_table(table_path).await.unwrap();
    let writer = table.new_append().unwrap().create_writer().unwrap();
    writer.append_arrow_batch(batch).unwrap();
    writer.flush().await.unwrap();
}

/// The latest log offset for bucket 0 — used as the PK seam target, independent
/// of how many changelog records each upsert/delete produces.
async fn latest_offset(connection: &FlussConnection, table_path: &TablePath) -> i64 {
    let admin = connection.get_admin().unwrap();
    let start = Instant::now();
    loop {
        let offset = admin
            .list_offsets(table_path, &[0], fluss::rpc::message::OffsetSpec::Latest)
            .await
            .ok()
            .and_then(|map| map.get(&0).copied());
        if let Some(off) = offset {
            return off;
        }
        if start.elapsed() >= Duration::from_secs(30) {
            panic!("bucket 0 of {table_path} not ready in 30s");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Polls the real lake snapshot until bucket 0's seam offset reaches
/// `want_offset` (i.e. batch 1 is fully tiered), or panics after `SEAM_TIMEOUT`.
async fn wait_for_lake_seam(
    connection: &FlussConnection,
    table_path: &TablePath,
    table_id: i64,
    want_offset: i64,
) {
    let admin = connection.get_admin().unwrap();
    let bucket = TableBucket::new(table_id, 0);
    let start = Instant::now();
    let mut last;
    loop {
        match admin.get_latest_lake_snapshot(table_path).await {
            Ok(snapshot) => {
                let offset = snapshot.table_buckets_offset().get(&bucket).copied();
                last = format!(
                    "snapshot {} bucket0 offset {:?}",
                    snapshot.snapshot_id(),
                    offset
                );
                if offset == Some(want_offset) {
                    return;
                }
            }
            Err(e) => last = format!("error: {e}"),
        }
        if start.elapsed() >= SEAM_TIMEOUT {
            panic!(
                "{table_path} lake seam did not reach offset {want_offset} within {SEAM_TIMEOUT:?}; last = {last}"
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn facade_batches(
    connection: Arc<FlussConnection>,
    table_info: &TableInfo,
) -> Vec<RecordBatch> {
    let lake_table = FlussLakeTable::try_from_table_info(connection, table_info).unwrap();
    let scan = lake_table.new_scan();
    let plan = scan.plan().await.unwrap();
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
    all
}

fn collect_ids(batches: &[RecordBatch]) -> Vec<i32> {
    let mut ids: Vec<i32> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
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
async fn facade_reads_real_tiered_lake_plus_log_tail() {
    let cluster = FlussTestingClusterBuilder::new("lake-tiering-e2e")
        .with_port(9150)
        .with_paimon_lake(PaimonLakeConfig::new())
        .with_tiering_service()
        .build()
        .await;
    let connection = Arc::new(cluster.get_fluss_connection().await);

    // The in-process Paimon reader must reach the same S3 store as the cluster:
    // rewrite the container-internal endpoint to the host-mapped one and supply
    // the credentials the server omits from table properties.
    set_test_lake_s3_endpoint_override(
        cluster.s3_endpoint_host().unwrap(),
        cluster.s3_access_key().unwrap(),
        cluster.s3_secret_key().unwrap(),
    );

    let admin = connection.get_admin().unwrap();
    admin.create_database(DATABASE, None, true).await.unwrap();

    let append_path = TablePath::new(DATABASE, APPEND_TABLE);
    admin
        .create_table(
            &append_path,
            &lake_descriptor(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .build()
                    .unwrap(),
            ),
            true,
        )
        .await
        .unwrap();
    let append_info = admin.get_table_info(&append_path).await.unwrap();

    let pk_path = TablePath::new(DATABASE, PK_TABLE);
    admin
        .create_table(
            &pk_path,
            &lake_descriptor(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .primary_key(vec!["id".to_string()])
                    .build()
                    .unwrap(),
            ),
            true,
        )
        .await
        .unwrap();
    let pk_info = admin.get_table_info(&pk_path).await.unwrap();

    // ---- Batch 1: the rows that will be tiered into the lake. ----
    write_append(
        &connection,
        &append_path,
        append_batch(&[1, 2, 3], &["a", "b", "c"]),
    )
    .await;

    let pk_table = connection.get_table(&pk_path).await.unwrap();
    let pk_writer = pk_table.new_upsert().unwrap().create_writer().unwrap();
    pk_writer.upsert(&pk_row(1, "v1")).unwrap();
    pk_writer.upsert(&pk_row(2, "v2")).unwrap();
    pk_writer.upsert(&pk_row(3, "v3")).unwrap();
    pk_writer.flush().await.unwrap();
    // PK seam target: the actual changelog end, robust to records-per-upsert.
    let pk_seam = latest_offset(&connection, &pk_path).await;

    // ---- Tier batch 1, then stop tiering to freeze both seams. ----
    let tiering = cluster.start_paimon_tiering().await;
    wait_for_lake_seam(&connection, &append_path, append_info.get_table_id(), 3).await;
    wait_for_lake_seam(&connection, &pk_path, pk_info.get_table_id(), pk_seam).await;
    tiering.stop().await;
    // Small margin so the cancelled job cannot commit batch 2 into the lake.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // ---- Batch 2: stays in the Fluss log tail (lake is frozen). ----
    write_append(
        &connection,
        &append_path,
        append_batch(&[4, 5, 6], &["d", "e", "f"]),
    )
    .await;
    pk_writer.upsert(&pk_row(2, "v2b")).unwrap(); // update existing key
    pk_writer.delete(&pk_row(3, "v3")).unwrap(); // delete tiered key
    pk_writer.upsert(&pk_row(4, "v4")).unwrap(); // insert new key
    pk_writer.flush().await.unwrap();

    // ---- Append: lake(1,2,3) ++ log_tail(4,5,6). ----
    let append_batches = facade_batches(connection.clone(), &append_info).await;
    assert_eq!(
        collect_ids(&append_batches),
        vec![1, 2, 3, 4, 5, 6],
        "append: lake(1,2,3 tiered) ++ log_tail(4,5,6)"
    );

    // ---- PK: merge lake current state with the changelog tail. ----
    let pk_batches = facade_batches(connection.clone(), &pk_info).await;
    assert_eq!(
        collect_pairs(&pk_batches),
        vec![
            (1, "v1".to_string()),  // untouched, from the lake
            (2, "v2b".to_string()), // updated by the changelog tail
            (4, "v4".to_string()),  // inserted by the changelog tail
        ],
        "pk: id3 deleted; id2 updated; id4 inserted; id1 from lake"
    );

    clear_test_lake_s3_endpoint_override();
    cluster.stop();
}
