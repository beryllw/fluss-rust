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

//! Full-stack SQL real-tiering e2e: a `SELECT` over a lake-enabled table whose
//! historical rows were tiered into Paimon by the real Flink tiering job and
//! whose recent rows remain in the Fluss log.
//!
//! Unlike `lake_union_sql` (seed + injected seam), this drives the production
//! path end to end: cluster-level Paimon datalake config behind RustFS (S3), a
//! `table.datalake.enabled = true` table discovered via its server-derived
//! properties (routing through the union provider), the real tiering job, and a
//! real `get_latest_lake_snapshot` seam. The only test hook is the S3 endpoint
//! rewrite so the in-process Paimon reader reaches the host-mapped store.

#![cfg(feature = "integration_tests")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::execution::context::SessionContext;
use fluss::client::FlussConnection;
use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TablePath};
use fluss_datafusion::{
    FlussDatafusion, RegisterCatalogOptions, clear_test_lake_s3_endpoint_override,
    set_test_lake_s3_endpoint_override,
};
use fluss_test_cluster::{FlussTestingClusterBuilder, PaimonLakeConfig};

use crate::integration::utils::helpers::{collect_i32, options};

const DATABASE: &str = "fluss";
const TABLE: &str = "df_lake_tiering_sql";
const CATALOG: &str = "fluss";

fn batch(ids: &[i32], names: &[&str]) -> RecordBatch {
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

async fn write_append(connection: &FlussConnection, table_path: &TablePath, batch: RecordBatch) {
    let table = connection.get_table(table_path).await.unwrap();
    let writer = table.new_append().unwrap().create_writer().unwrap();
    writer.append_arrow_batch(batch).unwrap();
    writer.flush().await.unwrap();
}

async fn wait_for_lake_seam(
    connection: &FlussConnection,
    table_path: &TablePath,
    table_id: i64,
    want_offset: i64,
    timeout: Duration,
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
        if start.elapsed() >= timeout {
            panic!(
                "lake seam did not reach offset {want_offset} within {timeout:?}; last = {last}"
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[tokio::test]
async fn sql_reads_real_tiered_lake_plus_log_tail() {
    let cluster = FlussTestingClusterBuilder::new("df-lake-tiering-sql")
        .with_port(9152)
        .with_paimon_lake(PaimonLakeConfig::new())
        .with_tiering_service()
        .build()
        .await;
    let connection = Arc::new(cluster.get_fluss_connection().await);
    set_test_lake_s3_endpoint_override(
        cluster.s3_endpoint_host().unwrap(),
        cluster.s3_access_key().unwrap(),
        cluster.s3_secret_key().unwrap(),
    );

    let table_path = TablePath::new(DATABASE, TABLE);
    let admin = connection.get_admin().unwrap();
    admin.create_database(DATABASE, None, true).await.unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .build()
                .unwrap(),
        )
        .distributed_by(Some(1), vec![])
        .property("table.datalake.enabled", "true")
        .property("table.datalake.freshness", "30s")
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, true)
        .await
        .unwrap();
    let table_id = admin
        .get_table_info(&table_path)
        .await
        .unwrap()
        .get_table_id();

    // Batch 1 (1,2,3) -> real tiering -> stop to freeze the seam.
    write_append(
        &connection,
        &table_path,
        batch(&[1, 2, 3], &["a", "b", "c"]),
    )
    .await;
    let tiering = cluster.start_paimon_tiering().await;
    wait_for_lake_seam(
        &connection,
        &table_path,
        table_id,
        3,
        Duration::from_secs(240),
    )
    .await;
    tiering.stop().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Batch 2 (4,5,6) stays in the Fluss log tail.
    write_append(
        &connection,
        &table_path,
        batch(&[4, 5, 6], &["d", "e", "f"]),
    )
    .await;

    // Read through the full SQL stack: discovery -> union provider -> union scan.
    let fd = FlussDatafusion::new(connection.clone(), options())
        .await
        .unwrap();
    let ctx = SessionContext::new();
    fd.register_catalog(&ctx, CATALOG, RegisterCatalogOptions::default())
        .await
        .unwrap();

    let sql = format!("SELECT id, name FROM {CATALOG}.{DATABASE}.{TABLE}");
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut ids = collect_i32(&batches, 0);
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![1, 2, 3, 4, 5, 6],
        "lake(1,2,3 tiered) ++ log_tail(4,5,6)"
    );

    clear_test_lake_s3_endpoint_override();
    cluster.stop();
}
