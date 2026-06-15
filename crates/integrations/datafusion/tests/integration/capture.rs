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
//! tables (via the shared `setup` module), and records the responses the fake
//! replays.
//!
//! Gated by `integration_tests` (needs a container runtime). Run with:
//!   cargo test -p fluss-datafusion --features integration_tests -- capture
//!
//! Table DDL/DML lives in `setup` so the capture and end-to-end paths share one
//! definition of the tables. Capture goes through the low-level fluss client
//! directly (no dependency on fluss-datafusion's real source) so fixtures can be
//! refreshed independent of the crate's own wiring; it writes the same fixture
//! format the fake reads.

#![cfg(feature = "integration_tests")]

use std::sync::Arc;

use fluss::client::FlussConnection;
use fluss::metadata::{TableBucket, TablePath};
use fluss::row::GenericRow;

use fluss_datafusion::testing::fixtures::{
    DatabaseFixture, FixtureSet, LogScanFixture, LookupFixture, TableFixture, batch_to_ipc,
};
use fluss_datafusion::testing::{KeyValue, TableRef};

use crate::integration::setup;
use crate::integration::utils::{fixture_path, fixtures_dir, names};

/// Captures the single-PK KV table: a present lookup and an absent lookup (so
/// the fake can replay the "key not found" path too).
async fn capture_kv_simple(connection: &FlussConnection) -> TableFixture {
    let info = setup::create_kv_simple(connection).await;
    let table_ref = TableRef::new(names::DATABASE, names::KV_SIMPLE);
    let meta = setup::meta_from_info(&table_ref, &info);

    let table_path = TablePath::new(names::DATABASE, names::KV_SIMPLE);
    let table = connection.get_table(&table_path).await.unwrap();
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

/// Captures the composite-PK KV table and a present composite-key lookup.
async fn capture_kv_composite(connection: &FlussConnection) -> TableFixture {
    let info = setup::create_kv_composite(connection).await;
    let table_ref = TableRef::new(names::DATABASE, names::KV_COMPOSITE);
    let meta = setup::meta_from_info(&table_ref, &info);

    let table_path = TablePath::new(names::DATABASE, names::KV_COMPOSITE);
    let table = connection.get_table(&table_path).await.unwrap();
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

/// Captures the log table: a full bounded scan and a projected bounded scan.
async fn capture_log_basic(connection: &FlussConnection) -> TableFixture {
    let info = setup::create_log_basic(connection).await;
    let table_ref = TableRef::new(names::DATABASE, names::LOG_BASIC);
    let meta = setup::meta_from_info(&table_ref, &info);
    let table_id = info.get_table_id();

    let table_path = TablePath::new(names::DATABASE, names::LOG_BASIC);
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

#[tokio::test]
async fn capture_phase1_fixtures() {
    let cluster = setup::start_cluster("df-capture", 9123).await;
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
    setup::drop_phase1_tables(&connection).await;

    cluster.stop();
}
