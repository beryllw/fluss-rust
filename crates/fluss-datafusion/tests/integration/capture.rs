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

//! Capture path: connects to a real Fluss cluster, creates known Phase 1
//! tables, writes known data, and records the responses the fake replays.
//!
//! Gated by `integration_tests` (needs a container runtime). Run with:
//!   cargo test -p fluss-datafusion --features integration_tests -- capture
//!
//! The capture goes through the low-level fluss client directly (no dependency
//! on fluss-datafusion's real source), so fixtures can be refreshed independent
//! of the crate's own wiring. It writes the same fixture format the fake reads.

#![cfg(feature = "integration_tests")]

use std::sync::Arc;
use std::time::Duration;

use fluss::client::FlussConnection;
use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TableInfo, TablePath};
use fluss::row::GenericRow;

use fluss_datafusion::testing::fixtures::{
    DatabaseFixture, FixtureSet, LogScanFixture, LookupFixture, TableFixture, batch_to_ipc,
};
use fluss_datafusion::testing::{FlussTableMeta, KeyValue, TableRef};

use crate::integration::utils::{fixture_path, fixtures_dir, names};

// Test-cluster crate is a dev-dependency; pull the harness in directly.
use fluss_test_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};

const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Brings up a one-off plaintext cluster for capture.
async fn start_cluster() -> FlussTestingCluster {
    let cluster = FlussTestingClusterBuilder::new("df-capture").build().await;
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

fn meta_from_info(table: &TableRef, info: &TableInfo) -> FlussTableMeta {
    FlussTableMeta {
        table_ref: table.clone(),
        table_id: info.get_table_id(),
        schema_id: info.get_schema_id(),
        schema: info.get_schema().clone(),
        primary_keys: info.get_primary_keys().clone(),
        num_buckets: info.get_num_buckets(),
    }
}

/// Captures a single-PK KV table: schema, rows, a present lookup, and an absent
/// lookup (so the fake can replay the "key not found" path too).
async fn capture_kv_simple(connection: &FlussConnection) -> TableFixture {
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

    let table_ref = TableRef::new(names::DATABASE, names::KV_SIMPLE);
    let info = admin.get_table_info(&table_path).await.unwrap();
    let meta = meta_from_info(&table_ref, &info);

    let mut lookuper = table.new_lookup().unwrap().create_lookuper().unwrap();
    let mut lookups = Vec::new();

    // Present key.
    let mut key_row = GenericRow::new(1);
    key_row.set_field(0, 2);
    let present = lookuper.lookup(&key_row).await.unwrap();
    lookups.push(LookupFixture {
        key: vec![KeyValue::Int32(2)],
        batch_ipc: batch_to_ipc(&present.to_record_batch().unwrap()).unwrap(),
    });

    // Absent key.
    let mut absent_row = GenericRow::new(1);
    absent_row.set_field(0, 999);
    let absent = lookuper.lookup(&absent_row).await.unwrap();
    lookups.push(LookupFixture {
        key: vec![KeyValue::Int32(999)],
        batch_ipc: batch_to_ipc(&absent.to_record_batch().unwrap()).unwrap(),
    });

    TableFixture {
        meta,
        lookups,
        log_scans: Vec::new(),
    }
}

/// Captures a composite-PK KV table and a present composite-key lookup.
async fn capture_kv_composite(connection: &FlussConnection) -> TableFixture {
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

    let table_ref = TableRef::new(names::DATABASE, names::KV_COMPOSITE);
    let info = admin.get_table_info(&table_path).await.unwrap();
    let meta = meta_from_info(&table_ref, &info);

    let mut lookuper = table.new_lookup().unwrap().create_lookuper().unwrap();
    let mut key_row = GenericRow::new(2);
    key_row.set_field(0, "us");
    key_row.set_field(1, 2);
    let result = lookuper.lookup(&key_row).await.unwrap();
    let lookups = vec![LookupFixture {
        key: vec![KeyValue::String("us".to_string()), KeyValue::Int32(2)],
        batch_ipc: batch_to_ipc(&result.to_record_batch().unwrap()).unwrap(),
    }];

    TableFixture {
        meta,
        lookups,
        log_scans: Vec::new(),
    }
}

/// Captures a log table: full bounded scan and a projected bounded scan.
async fn capture_log_basic(connection: &FlussConnection) -> TableFixture {
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
        // Single bucket keeps the bounded scan deterministic for capture.
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

    // Wait until the bucket can serve reads before the bounded scan.
    wait_for_offsets(connection, &table_path).await;

    let table_ref = TableRef::new(names::DATABASE, names::LOG_BASIC);
    let info = admin.get_table_info(&table_path).await.unwrap();
    let meta = meta_from_info(&table_ref, &info);
    let table_id = info.get_table_id();

    let mut log_scans = Vec::new();

    // Full bounded scan, limit 4.
    {
        let table = connection.get_table(&table_path).await.unwrap();
        let mut scanner = table
            .new_scan()
            .limit(4)
            .unwrap()
            .create_bucket_batch_scanner(TableBucket::new(table_id, 0))
            .unwrap();
        let batches = scanner.collect_all_batches().await.unwrap();
        log_scans.push(LogScanFixture {
            projection: None,
            limit: 4,
            batches_ipc: batches
                .into_iter()
                .map(|b| batch_to_ipc(&b.into_batch()).unwrap())
                .collect(),
        });
    }

    // Projected bounded scan (id only), limit 4.
    {
        let table = connection.get_table(&table_path).await.unwrap();
        let mut scanner = table
            .new_scan()
            .project(&[0])
            .unwrap()
            .limit(4)
            .unwrap()
            .create_bucket_batch_scanner(TableBucket::new(table_id, 0))
            .unwrap();
        let batches = scanner.collect_all_batches().await.unwrap();
        log_scans.push(LogScanFixture {
            projection: Some(vec![0]),
            limit: 4,
            batches_ipc: batches
                .into_iter()
                .map(|b| batch_to_ipc(&b.into_batch()).unwrap())
                .collect(),
        });
    }

    TableFixture {
        meta,
        lookups: Vec::new(),
        log_scans,
    }
}

/// Builds the log table's input batch with explicit Arrow arrays.
///
/// Built by hand (rather than the `record_batch!` macro) because that macro
/// expands to `arrow_schema`/`Field` paths this crate does not re-export.
fn log_batch() -> arrow::array::RecordBatch {
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

#[tokio::test]
async fn capture_phase1_fixtures() {
    let cluster = start_cluster().await;
    let connection = Arc::new(cluster.get_fluss_connection().await);

    let kv_simple = capture_kv_simple(&connection).await;
    let kv_composite = capture_kv_composite(&connection).await;
    let log_basic = capture_log_basic(&connection).await;

    let admin = connection.get_admin().unwrap();
    let tables = admin.list_tables(names::DATABASE).await.unwrap();
    let databases = admin.list_databases().await.unwrap();
    assert!(databases.iter().any(|d| d == names::DATABASE));

    let mut fixture = FixtureSet::new();
    fixture.databases = vec![DatabaseFixture {
        name: names::DATABASE.to_string(),
        tables,
    }];
    fixture.tables = vec![kv_simple, kv_composite, log_basic];

    std::fs::create_dir_all(fixtures_dir()).unwrap();
    let path = fixture_path();
    std::fs::write(&path, fixture.to_json().unwrap()).unwrap();
    eprintln!("wrote fixtures to {}", path.display());

    // Drop tables so reruns start clean (best effort).
    for name in [names::KV_SIMPLE, names::KV_COMPOSITE, names::LOG_BASIC] {
        let _ = admin
            .drop_table(&TablePath::new(names::DATABASE, name), true)
            .await;
    }

    cluster.stop();
}
