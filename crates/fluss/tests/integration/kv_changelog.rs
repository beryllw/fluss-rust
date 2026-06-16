/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Integration test for the KV (primary-key) table CHANGELOG record-mode
//! scanner (`TableScan::create_changelog_scanner`). This is the M3 gate for the
//! "changelog-only" KV full-scan feature (Option B): it proves the Rust client
//! can decode a KV table's CDC changelog returned by `FetchLog` into
//! `ScanRecord { row, offset, change_type }` with the correct `ChangeType` per
//! record.
//!
//! Runs against a DEDICATED single-node cluster on a free port so it never
//! contends with the shared 9123/9223 cluster; the cluster is torn down at the
//! end of the test.

#[cfg(test)]
mod kv_changelog_test {
    use crate::integration::fluss_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
    use crate::integration::utils::{create_table, wait_for_table_ready};
    use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
    use fluss::record::ChangeType;
    use fluss::row::{DataGetters, GenericRow};
    use fluss::rpc::message::OffsetSpec;
    use std::time::{Duration, Instant};

    /// A single decoded changelog record, flattened for assertions/printing.
    #[derive(Debug, Clone)]
    struct DecodedRecord {
        offset: i64,
        change_type: ChangeType,
        id: i32,
        name: Option<String>,
    }

    #[tokio::test]
    async fn changelog_scan_decodes_change_types() {
        // Dedicated cluster on a free port (NOT the shared 9123/9223 cluster).
        let cluster = FlussTestingClusterBuilder::new("kv-changelog")
            .with_port(9143)
            .build()
            .await;

        // Run the body, capturing any failure so the dedicated cluster is always
        // torn down before we re-raise.
        let outcome = run_changelog_scan(&cluster).await;

        cluster.stop();

        outcome.expect("KV changelog scan test body failed");
    }

    async fn run_changelog_scan(cluster: &FlussTestingCluster) -> Result<(), String> {
        let connection = cluster.get_fluss_connection().await;
        let admin = connection
            .get_admin()
            .map_err(|e| format!("get_admin: {e}"))?;

        let table_path = TablePath::new("fluss", "test_kv_changelog_scan");

        // Single-bucket KV table (id INT PRIMARY KEY, name STRING).
        let table_descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .primary_key(vec!["id"])
                    .build()
                    .map_err(|e| format!("schema: {e}"))?,
            )
            .distributed_by(Some(1), vec!["id".to_string()])
            .build()
            .map_err(|e| format!("table descriptor: {e}"))?;

        create_table(&admin, &table_path, &table_descriptor).await;
        wait_for_table_ready(&admin, &table_path).await;

        let table = connection
            .get_table(&table_path)
            .await
            .map_err(|e| format!("get_table: {e}"))?;

        // --- Mutate: upsert 1,2,3 ; update 2 ; delete 3 ---
        let upsert_writer = table
            .new_upsert()
            .map_err(|e| format!("new_upsert: {e}"))?
            .create_writer()
            .map_err(|e| format!("create_writer: {e}"))?;

        for (id, name) in [(1, "alpha"), (2, "bravo"), (3, "charlie")] {
            let mut row = GenericRow::new(2);
            row.set_field(0, id);
            row.set_field(1, name);
            upsert_writer
                .upsert(&row)
                .map_err(|e| format!("upsert {id}: {e}"))?;
        }
        upsert_writer
            .flush()
            .await
            .map_err(|e| format!("flush inserts: {e}"))?;

        // Update id=2 -> bravo-v2.
        let mut update_row = GenericRow::new(2);
        update_row.set_field(0, 2);
        update_row.set_field(1, "bravo-v2");
        upsert_writer
            .upsert(&update_row)
            .map_err(|e| format!("upsert update: {e}"))?
            .await
            .map_err(|e| format!("ack update: {e}"))?;

        // Delete id=3.
        let mut delete_row = GenericRow::new(2);
        delete_row.set_field(0, 3);
        upsert_writer
            .delete(&delete_row)
            .map_err(|e| format!("delete: {e}"))?
            .await
            .map_err(|e| format!("ack delete: {e}"))?;

        // --- Offsets for bucket 0 ---
        let earliest = *admin
            .list_offsets(&table_path, &[0], OffsetSpec::Earliest)
            .await
            .map_err(|e| format!("list earliest: {e}"))?
            .get(&0)
            .ok_or_else(|| "no earliest offset for bucket 0".to_string())?;
        let latest = *admin
            .list_offsets(&table_path, &[0], OffsetSpec::Latest)
            .await
            .map_err(|e| format!("list latest: {e}"))?
            .get(&0)
            .ok_or_else(|| "no latest offset for bucket 0".to_string())?;

        eprintln!("[kv-changelog] bucket 0 earliest={earliest} latest={latest}");
        if latest <= earliest {
            return Err(format!(
                "expected changelog records but latest({latest}) <= earliest({earliest})"
            ));
        }

        // --- Changelog scanner: this is the gate (PK table is allowed here) ---
        let scanner = table
            .new_scan()
            .create_changelog_scanner()
            .map_err(|e| format!("create_changelog_scanner: {e}"))?;
        scanner
            .subscribe(0, earliest)
            .await
            .map_err(|e| format!("subscribe: {e}"))?;

        let mut decoded: Vec<DecodedRecord> = Vec::new();
        let start = Instant::now();
        // Poll until we've consumed up to latest-1 (the last readable offset)
        // or we time out. A decode error here is the failure M3 must rule out.
        while decoded.last().map(|r| r.offset).unwrap_or(earliest - 1) < latest - 1 {
            if start.elapsed() > Duration::from_secs(30) {
                return Err(format!(
                    "timed out: decoded {} records, last offset {:?}, expected to reach {}",
                    decoded.len(),
                    decoded.last().map(|r| r.offset),
                    latest - 1
                ));
            }

            let scan_records = scanner
                .poll(Duration::from_millis(500))
                .await
                .map_err(|e| format!("poll decode error (M3 gate): {e}"))?;

            for rec in scan_records.records_by_buckets().values().flatten() {
                let row = rec.row();
                // id is the primary key and is always present, including for -D.
                let id = row.get_int(0).map_err(|e| format!("get_int(id): {e}"))?;
                // name may be null on delete tombstones depending on the server,
                // so read it defensively.
                let name = if row.is_null_at(1).unwrap_or(true) {
                    None
                } else {
                    Some(
                        row.get_string(1)
                            .map_err(|e| format!("get_string(name): {e}"))?
                            .to_string(),
                    )
                };
                decoded.push(DecodedRecord {
                    offset: rec.offset(),
                    change_type: *rec.change_type(),
                    id,
                    name,
                });
            }
        }

        decoded.sort_by_key(|r| r.offset);
        eprintln!("[kv-changelog] decoded {} records:", decoded.len());
        for r in &decoded {
            eprintln!(
                "  offset={} change_type={} id={} name={:?}",
                r.offset,
                r.change_type.short_string(),
                r.id,
                r.name
            );
        }

        // --- Assertions ---
        // 1. Inserts for 1,2,3 appear as +I or +A (tolerant about exact kind).
        for id in [1, 2, 3] {
            let has_insert = decoded.iter().any(|r| {
                r.id == id
                    && matches!(r.change_type, ChangeType::Insert | ChangeType::AppendOnly)
            });
            if !has_insert {
                return Err(format!("missing insert (+I/+A) record for id={id}"));
            }
        }

        // 2. The update of id=2 appears as +U carrying "bravo-v2".
        let has_update_v2 = decoded.iter().any(|r| {
            r.id == 2
                && r.change_type == ChangeType::UpdateAfter
                && r.name.as_deref() == Some("bravo-v2")
        });
        if !has_update_v2 {
            return Err("missing +U record for id=2 carrying \"bravo-v2\"".to_string());
        }

        // 3. The delete of id=3 appears as -D.
        let has_delete_3 = decoded
            .iter()
            .any(|r| r.id == 3 && r.change_type == ChangeType::Delete);
        if !has_delete_3 {
            return Err("missing -D record for id=3".to_string());
        }

        admin
            .drop_table(&table_path, false)
            .await
            .map_err(|e| format!("drop_table: {e}"))?;

        Ok(())
    }
}
