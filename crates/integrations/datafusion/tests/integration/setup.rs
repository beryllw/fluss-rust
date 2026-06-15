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

//! Shared real-cluster setup for the `integration_tests` path.
//!
//! Brings up a one-off Fluss cluster and creates/populates the known Phase 1
//! tables. The end-to-end path drives SQL through the real backend on top of the
//! DDL/DML defined here.
//!
//! Gated by `integration_tests` (needs a container runtime).

#![cfg(feature = "integration_tests")]

use std::time::Duration;

use fluss::client::FlussConnection;
use fluss::metadata::{DataTypes, Schema, TableInfo, TableDescriptor, TablePath};
use fluss::row::GenericRow;
use fluss_test_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};

use crate::integration::utils::names;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Brings up a one-off plaintext cluster and waits for it to serve metadata.
///
/// `name` namespaces the containers; `port` is the coordinator's host port. Two
/// clusters in the same test binary must use distinct names and ports so they do
/// not collide when the harness runs tests concurrently.
pub async fn start_cluster(name: &str, port: u16) -> FlussTestingCluster {
    let cluster = FlussTestingClusterBuilder::new(name)
        .with_port(port)
        .build()
        .await;
    wait_for_ready(&cluster).await;
    cluster
}

async fn wait_for_ready(cluster: &FlussTestingCluster) {
    let start = std::time::Instant::now();
    loop {
        let connection = cluster.get_fluss_connection().await;
        if connection
            .get_metadata()
            .get_cluster()
            .get_one_available_server()
            .is_some()
        {
            return;
        }
        if start.elapsed() >= READY_TIMEOUT {
            panic!("cluster did not become ready in {READY_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Creates and populates the single-PK KV table; returns its `TableInfo`.
pub async fn create_kv_simple(connection: &FlussConnection) -> TableInfo {
    let table_path = TablePath::new(names::DATABASE, names::KV_SIMPLE);
    let admin = connection.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .column("age", DataTypes::bigint())
                .primary_key(vec!["id"])
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, true)
        .await
        .unwrap();

    let table = connection.get_table(&table_path).await.unwrap();
    let writer = table.new_upsert().unwrap().create_writer().unwrap();
    let rows = [(1, "Verso", 32i64), (2, "Noco", 25), (3, "Esquie", 35)];
    for (id, name, age) in &rows {
        let mut row = GenericRow::new(3);
        row.set_field(0, *id);
        row.set_field(1, *name);
        row.set_field(2, *age);
        writer.upsert(&row).unwrap();
    }
    writer.flush().await.unwrap();

    admin.get_table_info(&table_path).await.unwrap()
}

/// Creates and populates the composite-PK KV table; returns its `TableInfo`.
pub async fn create_kv_composite(connection: &FlussConnection) -> TableInfo {
    let table_path = TablePath::new(names::DATABASE, names::KV_COMPOSITE);
    let admin = connection.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("region", DataTypes::string())
                .column("id", DataTypes::int())
                .column("score", DataTypes::bigint())
                .primary_key(vec!["region", "id"])
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, true)
        .await
        .unwrap();

    let table = connection.get_table(&table_path).await.unwrap();
    let writer = table.new_upsert().unwrap().create_writer().unwrap();
    let rows = [("us", 1, 100i64), ("us", 2, 200), ("eu", 1, 300)];
    for (region, id, score) in &rows {
        let mut row = GenericRow::new(3);
        row.set_field(0, *region);
        row.set_field(1, *id);
        row.set_field(2, *score);
        writer.upsert(&row).unwrap();
    }
    writer.flush().await.unwrap();

    admin.get_table_info(&table_path).await.unwrap()
}

/// Creates and populates the single-bucket log table, waits until its bucket can
/// serve reads, and returns its `TableInfo`.
pub async fn create_log_basic(connection: &FlussConnection) -> TableInfo {
    let table_path = TablePath::new(names::DATABASE, names::LOG_BASIC);
    let admin = connection.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .build()
                .unwrap(),
        )
        // Single bucket keeps the bounded scan deterministic.
        .distributed_by(Some(1), vec![])
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, true)
        .await
        .unwrap();

    let table = connection.get_table(&table_path).await.unwrap();
    let writer = table.new_append().unwrap().create_writer().unwrap();
    writer.append_arrow_batch(log_batch()).unwrap();
    writer.flush().await.unwrap();

    // Wait until the bucket can serve reads before any bounded scan runs.
    wait_for_offsets(connection, &table_path).await;

    admin.get_table_info(&table_path).await.unwrap()
}

/// Creates and populates a single-bucket log table under an arbitrary name,
/// waiting until its bucket can serve reads. Used to create tables on demand
/// (e.g. AFTER `register_catalog`) so the live-metadata test can prove visibility.
pub async fn create_log_named(connection: &FlussConnection, table_name: &str) -> TableInfo {
    let table_path = TablePath::new(names::DATABASE, table_name);
    let admin = connection.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .build()
                .unwrap(),
        )
        // Single bucket keeps the bounded scan deterministic.
        .distributed_by(Some(1), vec![])
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, true)
        .await
        .unwrap();

    let table = connection.get_table(&table_path).await.unwrap();
    let writer = table.new_append().unwrap().create_writer().unwrap();
    writer.append_arrow_batch(log_batch()).unwrap();
    writer.flush().await.unwrap();

    wait_for_offsets(connection, &table_path).await;

    admin.get_table_info(&table_path).await.unwrap()
}

/// Best-effort drop of a single table under `names::DATABASE`.
pub async fn drop_table_named(connection: &FlussConnection, table_name: &str) {
    let admin = connection.get_admin().unwrap();
    let _ = admin
        .drop_table(&TablePath::new(names::DATABASE, table_name), true)
        .await;
}

/// Builds the log table's input batch with explicit Arrow arrays.
///
/// Built by hand (rather than the `record_batch!` macro) because that macro
/// expands to `arrow_schema`/`Field` paths this crate does not re-export.
pub fn log_batch() -> arrow::array::RecordBatch {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ids = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
    let names = StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);
    arrow::array::RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)])
        .expect("build log batch")
}

async fn wait_for_offsets(connection: &FlussConnection, table_path: &TablePath) {
    use fluss::rpc::message::OffsetSpec;
    let admin = connection.get_admin().unwrap();
    let start = std::time::Instant::now();
    loop {
        if admin
            .list_offsets(table_path, &[0], OffsetSpec::Latest)
            .await
            .is_ok()
        {
            return;
        }
        if start.elapsed() >= READY_TIMEOUT {
            panic!("table {table_path} buckets not ready in {READY_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Best-effort drop of the Phase 1 tables so reruns start clean.
pub async fn drop_phase1_tables(connection: &FlussConnection) {
    let admin = connection.get_admin().unwrap();
    for name in [names::KV_SIMPLE, names::KV_COMPOSITE, names::LOG_BASIC] {
        let _ = admin
            .drop_table(&TablePath::new(names::DATABASE, name), true)
            .await;
    }
}
