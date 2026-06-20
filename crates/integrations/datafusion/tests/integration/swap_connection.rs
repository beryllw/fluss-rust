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

//! R1: hot-swapping the underlying Fluss connection is transparent to an
//! already-registered catalog / `SessionContext`.
//!
//! Builds a `FlussDatafusion` over connection A, registers a catalog, queries
//! (uses A), then `swap_connection(B)`. The installer must now reference B (so a
//! query keeps working) without re-registering the catalog, and must have
//! released A. Both connections target the same cluster, so the proof that the
//! swap took effect is structural — `Arc::strong_count` on B rises once the
//! installer adopts it — rather than data-based.

#![cfg(feature = "integration_tests")]

use std::sync::Arc;

use fluss::client::FlussConnection;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss_datafusion::{FlussDatafusion, RegisterCatalogOptions};
use fluss_test_cluster::FlussTestingClusterBuilder;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::execution::context::SessionContext;

use crate::integration::utils::helpers::{collect_i32, options};

const DATABASE: &str = "swap_db";
const TABLE: &str = "swap_log";
const CATALOG: &str = "fluss";

fn log_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, true),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap()
}

async fn select_ids(connection: Arc<FlussConnection>, fd: &FlussDatafusion) -> Vec<i32> {
    let ctx = SessionContext::new();
    fd.register_catalog(&ctx, CATALOG, RegisterCatalogOptions::default())
        .await
        .unwrap();
    // Keep the connection arg in the signature so the caller documents which
    // connection is expected to back this query; the catalog reaches it through
    // the installer.
    let _ = &connection;
    let sql = format!("SELECT id FROM {CATALOG}.{DATABASE}.{TABLE}");
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let mut ids = collect_i32(&batches, 0);
    ids.sort_unstable();
    ids
}

#[tokio::test]
async fn swap_connection_is_transparent_to_registered_catalog() {
    let cluster = FlussTestingClusterBuilder::new("df-swap-connection")
        .with_port(9154)
        .build()
        .await;
    let conn_a = Arc::new(cluster.get_fluss_connection().await);
    let conn_b = Arc::new(cluster.get_fluss_connection().await);

    // Seed a plain log table + rows via connection A's admin.
    let table_path = TablePath::new(DATABASE, TABLE);
    let admin = conn_a.get_admin().unwrap();
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
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, true)
        .await
        .unwrap();
    let table = conn_a.get_table(&table_path).await.unwrap();
    let writer = table.new_append().unwrap().create_writer().unwrap();
    writer.append_arrow_batch(log_batch()).unwrap();
    writer.flush().await.unwrap();

    let fd = FlussDatafusion::new(conn_a.clone(), options())
        .await
        .unwrap();

    // Before the swap: query works on A, and the installer holds a reference to A.
    assert_eq!(select_ids(conn_a.clone(), &fd).await, vec![1, 2, 3]);
    assert!(
        Arc::strong_count(&conn_a) >= 2,
        "installer should hold a reference to connection A before swap"
    );
    let b_before = Arc::strong_count(&conn_b);

    // Swap to B with no re-register.
    fd.swap_connection(conn_b.clone()).unwrap();

    // The installer now references B (structural proof the swap took effect) and
    // a query keeps working through the same, never-re-registered catalog.
    assert!(
        Arc::strong_count(&conn_b) > b_before,
        "installer should adopt connection B after swap"
    );
    assert_eq!(
        select_ids(conn_b.clone(), &fd).await,
        vec![1, 2, 3],
        "query must keep working after swap_connection, without re-register"
    );

    cluster.stop();
}
