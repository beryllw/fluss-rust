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

use std::collections::HashMap;
use std::time::Duration;

use fluss::client::FlussConnection;
use fluss::metadata::{
    DataTypes, PartitionSpec, Schema, TableDescriptor, TableInfo, TablePath,
};
use fluss::row::GenericRow;
use fluss::rpc::message::OffsetSpec;
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

/// Bucket count for the multi-bucket log table.
const MULTI_BUCKET_COUNT: i32 = 3;

/// Creates and populates the multi-bucket log table, waits until ALL of its
/// buckets can serve reads, and returns its `TableInfo`.
///
/// Seeds 12 rows across 3 buckets so a bounded scan exercises per-bucket
/// parallel partitions plus DataFusion's cross-bucket final `LIMIT`.
pub async fn create_log_multi_bucket(connection: &FlussConnection) -> TableInfo {
    let table_path = TablePath::new(names::DATABASE, names::LOG_MULTI_BUCKET);
    let admin = connection.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .build()
                .unwrap(),
        )
        // Multiple buckets exercise the per-bucket parallel scan path.
        .distributed_by(Some(MULTI_BUCKET_COUNT), vec![])
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, true)
        .await
        .unwrap();

    let table = connection.get_table(&table_path).await.unwrap();
    let writer = table.new_append().unwrap().create_writer().unwrap();
    writer.append_arrow_batch(multi_bucket_log_batch()).unwrap();
    writer.flush().await.unwrap();

    // Every bucket is read as its own partition, so all must be ready first.
    let buckets: Vec<i32> = (0..MULTI_BUCKET_COUNT).collect();
    wait_for_bucket_offsets(connection, &table_path, &buckets).await;

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

/// Creates and populates the partitioned log table (partition key `region`, one
/// bucket per partition), waits until each partition's bucket can serve reads, and
/// returns its `TableInfo`. Used to exercise partition pruning of bounded scans.
pub async fn create_log_partitioned(connection: &FlussConnection) -> TableInfo {
    let table_path = TablePath::new(names::DATABASE, names::LOG_PARTITIONED);
    let admin = connection.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .column("region", DataTypes::string())
                .build()
                .unwrap(),
        )
        .partitioned_by(vec!["region"])
        // Single bucket per partition keeps the bounded scan deterministic.
        .distributed_by(Some(1), vec![])
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, true)
        .await
        .unwrap();

    // Create the two partitions up front; the writer routes rows by region.
    for region in ["US", "EU"] {
        let spec = PartitionSpec::new(HashMap::from([("region", region)]));
        admin
            .create_partition(&table_path, &spec, true)
            .await
            .unwrap();
        wait_for_partition_offsets(connection, &table_path, region).await;
    }

    let table = connection.get_table(&table_path).await.unwrap();
    let writer = table.new_append().unwrap().create_writer().unwrap();
    let rows = [
        (1, "a", "US"),
        (2, "b", "US"),
        (3, "c", "EU"),
        (4, "d", "EU"),
    ];
    for (id, name, region) in &rows {
        let mut row = GenericRow::new(3);
        row.set_field(0, *id);
        row.set_field(1, *name);
        row.set_field(2, *region);
        writer.append(&row).unwrap();
    }
    writer.flush().await.unwrap();

    admin.get_table_info(&table_path).await.unwrap()
}

/// Creates and populates the partitioned KV table (partition key `region`, which
/// is also part of the primary key), waits until each partition can serve reads,
/// and returns its `TableInfo`. Used to prove a full-PK lookup resolves the
/// partition automatically via the Fluss `Lookuper`.
pub async fn create_kv_partitioned(connection: &FlussConnection) -> TableInfo {
    let table_path = TablePath::new(names::DATABASE, names::KV_PARTITIONED);
    let admin = connection.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("region", DataTypes::string())
                .column("id", DataTypes::int())
                .column("score", DataTypes::bigint())
                // For a partitioned PK table the partition key must be part of the PK.
                .primary_key(vec!["region", "id"])
                .build()
                .unwrap(),
        )
        .partitioned_by(vec!["region"])
        .distributed_by(Some(1), vec![])
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, true)
        .await
        .unwrap();

    for region in ["US", "EU"] {
        let spec = PartitionSpec::new(HashMap::from([("region", region)]));
        admin
            .create_partition(&table_path, &spec, true)
            .await
            .unwrap();
        wait_for_partition_offsets(connection, &table_path, region).await;
    }

    let table = connection.get_table(&table_path).await.unwrap();
    let writer = table.new_upsert().unwrap().create_writer().unwrap();
    let rows = [("US", 1, 100i64), ("US", 2, 200), ("EU", 1, 300)];
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

/// Creates and populates a single-bucket KV table with several rows, waits until
/// its bucket can serve reads, and returns its `TableInfo`. Used to prove a
/// bounded `LIMIT` scan over a primary-keyed table returns exactly `LIMIT` rows
/// while a full-PK equality still resolves a single row.
pub async fn create_kv_bounded(connection: &FlussConnection) -> TableInfo {
    let table_path = TablePath::new(names::DATABASE, names::KV_BOUNDED);
    let admin = connection.get_admin().unwrap();
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .primary_key(vec!["id"])
                .build()
                .unwrap(),
        )
        // Single bucket keeps the bounded scan deterministic and ready-to-read.
        .distributed_by(Some(1), vec![])
        .build()
        .unwrap();
    admin
        .create_table(&table_path, &descriptor, true)
        .await
        .unwrap();

    let table = connection.get_table(&table_path).await.unwrap();
    let writer = table.new_upsert().unwrap().create_writer().unwrap();
    let rows = [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (4, "d"),
        (5, "e"),
        (6, "f"),
    ];
    for (id, name) in &rows {
        let mut row = GenericRow::new(2);
        row.set_field(0, *id);
        row.set_field(1, *name);
        writer.upsert(&row).unwrap();
    }
    writer.flush().await.unwrap();

    // Wait until the bucket can serve reads before any bounded scan runs.
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

/// Builds the multi-bucket log table's input batch: 12 rows that spread across
/// the table's buckets, so a bounded scan must merge several partitions.
pub fn multi_bucket_log_batch() -> arrow::array::RecordBatch {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ids: Vec<i32> = (1..=12).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("r{i}")).collect();
    let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    arrow::array::RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(name_refs)),
        ],
    )
    .expect("build multi-bucket log batch")
}

/// Waits until bucket 0 of `table_path` can serve reads.
async fn wait_for_offsets(connection: &FlussConnection, table_path: &TablePath) {
    wait_for_bucket_offsets(connection, table_path, &[0]).await;
}

/// Waits until every bucket in `buckets` of `table_path` can serve reads.
///
/// A multi-bucket bounded scan reads each bucket as its own partition, so all of
/// them must be ready before the scan runs or a bucket could return zero rows.
async fn wait_for_bucket_offsets(
    connection: &FlussConnection,
    table_path: &TablePath,
    buckets: &[i32],
) {
    let admin = connection.get_admin().unwrap();
    let start = std::time::Instant::now();
    loop {
        if admin
            .list_offsets(table_path, buckets, OffsetSpec::Latest)
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

/// Waits until bucket 0 of partition `partition_value` of `table_path` can serve
/// reads. A partition-qualified bounded scan reads each partition's bucket, so the
/// partition must be ready before the scan runs.
async fn wait_for_partition_offsets(
    connection: &FlussConnection,
    table_path: &TablePath,
    partition_value: &str,
) {
    let admin = connection.get_admin().unwrap();
    let start = std::time::Instant::now();
    loop {
        if admin
            .list_partition_offsets(table_path, partition_value, &[0], OffsetSpec::Latest)
            .await
            .is_ok()
        {
            return;
        }
        if start.elapsed() >= READY_TIMEOUT {
            panic!("partition {partition_value} of {table_path} not ready in {READY_TIMEOUT:?}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Best-effort drop of the Phase 1 tables so reruns start clean.
pub async fn drop_phase1_tables(connection: &FlussConnection) {
    let admin = connection.get_admin().unwrap();
    for name in [
        names::KV_SIMPLE,
        names::KV_COMPOSITE,
        names::LOG_BASIC,
        names::LOG_MULTI_BUCKET,
        names::LOG_PARTITIONED,
        names::KV_PARTITIONED,
        names::KV_BOUNDED,
    ] {
        let _ = admin
            .drop_table(&TablePath::new(names::DATABASE, name), true)
            .await;
    }
}
