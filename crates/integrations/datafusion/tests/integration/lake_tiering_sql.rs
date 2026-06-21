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
//! real `get_latest_lake_snapshot` seam.
//!
//! Storage credentials are supplied through the PRODUCTION API
//! (`FlussDatafusionOptions.lake_storage_options`), NOT the test-only override —
//! the Fluss server strips S3 credentials from table properties, so this is the
//! path a real gateway must use, and exercising it here guards against the gap
//! where the test override previously masked the missing production injection.

#![cfg(feature = "integration_tests")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::execution::context::SessionContext;
use fluss::client::FlussConnection;
use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TablePath};
use fluss::row::GenericRow;
use fluss_datafusion::{FlussDatafusion, FlussDatafusionOptions, RegisterCatalogOptions};
use fluss_test_cluster::{FlussTestingClusterBuilder, PaimonLakeConfig};

use crate::integration::utils::helpers::{collect_i32, collect_strings};

const DATABASE: &str = "fluss";
const TABLE: &str = "df_lake_tiering_sql";
const PK_TABLE: &str = "df_lake_tiering_sql_pk";
const CATALOG: &str = "fluss";

fn pk_row(id: i32, name: &str) -> GenericRow<'static> {
    let mut r = GenericRow::new(2);
    r.set_field(0, id);
    r.set_field(1, name.to_string());
    r
}

/// Latest log offset for bucket 0 — the PK seam target, robust to how many
/// changelog records each upsert produces.
async fn latest_offset(connection: &FlussConnection, table_path: &TablePath) -> i64 {
    let admin = connection.get_admin().unwrap();
    let start = Instant::now();
    loop {
        let offset = admin
            .list_offsets(table_path, &[0], fluss::rpc::message::OffsetSpec::Latest)
            .await
            .ok()
            .and_then(|m| m.get(&0).copied());
        if let Some(off) = offset {
            return off;
        }
        if start.elapsed() >= Duration::from_secs(30) {
            panic!("bucket 0 of {table_path} not ready in 30s");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
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

    // Production credential injection: the server strips S3 credentials from the
    // table properties, so supply them (plus the host-mapped endpoint) through
    // FlussDatafusionOptions.lake_storage_options — caller-wins over server props.
    let df_options = FlussDatafusionOptions {
        lake_storage_options: HashMap::from([
            (
                "s3.endpoint".to_string(),
                cluster.s3_endpoint_host().unwrap().to_string(),
            ),
            (
                "s3.access-key".to_string(),
                cluster.s3_access_key().unwrap().to_string(),
            ),
            (
                "s3.secret-key".to_string(),
                cluster.s3_secret_key().unwrap().to_string(),
            ),
            ("s3.region".to_string(), "us-east-1".to_string()),
            ("s3.path-style-access".to_string(), "true".to_string()),
        ]),
    };

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

    // A PRIMARY-KEY lake table (the shape of e.g. an orders-history table), read
    // through the same union path but with PK merge instead of append stitch.
    let pk_path = TablePath::new(DATABASE, PK_TABLE);
    admin
        .create_table(
            &pk_path,
            &TableDescriptor::builder()
                .schema(
                    Schema::builder()
                        .column("id", DataTypes::int())
                        .column("name", DataTypes::string())
                        .primary_key(vec!["id".to_string()])
                        .build()
                        .unwrap(),
                )
                .distributed_by(Some(1), vec![])
                .property("table.datalake.enabled", "true")
                .property("table.datalake.freshness", "30s")
                .build()
                .unwrap(),
            true,
        )
        .await
        .unwrap();
    let pk_table_id = admin
        .get_table_info(&pk_path)
        .await
        .unwrap()
        .get_table_id();

    // Batch 1 -> real tiering -> stop to freeze the seam(s).
    write_append(
        &connection,
        &table_path,
        batch(&[1, 2, 3], &["a", "b", "c"]),
    )
    .await;

    // PK batch 1: upsert ids 1,2,3.
    let pk_table = connection.get_table(&pk_path).await.unwrap();
    let pk_writer = pk_table.new_upsert().unwrap().create_writer().unwrap();
    pk_writer.upsert(&pk_row(1, "v1")).unwrap();
    pk_writer.upsert(&pk_row(2, "v2")).unwrap();
    pk_writer.upsert(&pk_row(3, "v3")).unwrap();
    pk_writer.flush().await.unwrap();
    let pk_seam = latest_offset(&connection, &pk_path).await;

    let tiering = cluster.start_paimon_tiering().await;
    wait_for_lake_seam(
        &connection,
        &table_path,
        table_id,
        3,
        Duration::from_secs(240),
    )
    .await;
    wait_for_lake_seam(
        &connection,
        &pk_path,
        pk_table_id,
        pk_seam,
        Duration::from_secs(240),
    )
    .await;
    tiering.stop().await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Batch 2 stays in the Fluss log tail.
    write_append(
        &connection,
        &table_path,
        batch(&[4, 5, 6], &["d", "e", "f"]),
    )
    .await;

    // PK batch 2 (log tail): update id 2, delete id 3, insert id 4.
    pk_writer.upsert(&pk_row(2, "v2b")).unwrap();
    pk_writer.delete(&pk_row(3, "v3")).unwrap();
    pk_writer.upsert(&pk_row(4, "v4")).unwrap();
    pk_writer.flush().await.unwrap();

    // Read through the full SQL stack: discovery -> union provider -> union scan.
    let fd = FlussDatafusion::new(connection.clone(), df_options)
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

    // `<table>$lake` reads ONLY the Paimon lake snapshot — the tiered batch 1 —
    // and must NOT include batch 2, which is still in the Fluss log tail. `$` is a
    // valid identifier-part char in DataFusion's dialect, so the suffix needs no
    // quoting (same as Flink's `FROM <table>$lake`).
    let lake_sql = format!("SELECT id FROM {CATALOG}.{DATABASE}.{TABLE}$lake");
    let lake_batches = ctx.sql(&lake_sql).await.unwrap().collect().await.unwrap();
    let mut lake_ids = collect_i32(&lake_batches, 0);
    lake_ids.sort_unstable();
    assert_eq!(
        lake_ids,
        vec![1, 2, 3],
        "$lake returns only the tiered lake snapshot, excluding the log tail"
    );

    // PK lake table union read: lake current state (1,2,3) merged with the
    // changelog tail (update 2, delete 3, insert 4) => {1:v1, 2:v2b, 4:v4}.
    let pk_sql = format!("SELECT id, name FROM {CATALOG}.{DATABASE}.{PK_TABLE}");
    let pk_batches = ctx.sql(&pk_sql).await.unwrap().collect().await.unwrap();
    assert_eq!(
        collect_pairs(&pk_batches),
        vec![
            (1, "v1".to_string()),
            (2, "v2b".to_string()),
            (4, "v4".to_string()),
        ],
        "PK union read: id3 deleted, id2 updated, id4 inserted, id1 from lake"
    );

    // PK `<table>$lake`: only the tiered lake snapshot (1,2,3), no changelog tail.
    let pk_lake_sql = format!("SELECT id FROM {CATALOG}.{DATABASE}.{PK_TABLE}$lake");
    let pk_lake_batches = ctx.sql(&pk_lake_sql).await.unwrap().collect().await.unwrap();
    let mut pk_lake_ids = collect_i32(&pk_lake_batches, 0);
    pk_lake_ids.sort_unstable();
    assert_eq!(
        pk_lake_ids,
        vec![1, 2, 3],
        "PK $lake returns only the tiered lake snapshot"
    );

    // `<table>$options` reports the table is lake-enabled (湖流一体) and its format.
    let opt_value = |key: &str| {
        let sql = format!(
            "SELECT value FROM {CATALOG}.{DATABASE}.{TABLE}$options WHERE key = '{key}'"
        );
        let ctx = &ctx;
        async move {
            let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
            collect_strings(&batches, 0)
        }
    };
    assert_eq!(
        opt_value("datalake.enabled").await,
        vec!["true".to_string()],
        "$options reports datalake.enabled=true for a lake table"
    );
    assert_eq!(
        opt_value("datalake.format").await,
        vec!["paimon".to_string()],
        "$options reports datalake.format=paimon"
    );
    // The server strips S3 credentials from table properties, and the caller's
    // lake_storage_options are merged only at lake-read time (not into the table
    // metadata), so $options must never surface credentials.
    assert!(
        opt_value("datalake.paimon.s3.access-key").await.is_empty(),
        "$options must not surface S3 credentials"
    );
    assert!(
        opt_value("datalake.paimon.s3.secret-key").await.is_empty(),
        "$options must not surface S3 credentials"
    );

    cluster.stop();
}
