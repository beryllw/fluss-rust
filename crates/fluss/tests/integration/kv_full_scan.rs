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

//! Integration test for M4: the client-side KV current-state reader
//! (`TableScan::collect_kv_current_state_batch` / `KvFullScanner`). Proves the
//! changelog-only merge yields the correct per-bucket current state: a deleted
//! PK is absent, an updated PK carries its new value, and the row count is
//! correct.
//!
//! Runs against a DEDICATED single-node cluster on a free port (9147) so it never
//! contends with the shared cluster or other dedicated-port modules (the
//! fluss-datafusion crate's KV full-scan e2e uses 9145); the cluster is torn down
//! at the end.

#[cfg(test)]
mod kv_full_scan_test {
    use crate::integration::fluss_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
    use crate::integration::utils::{create_table, wait_for_table_ready};
    use arrow::array::{Int32Array, StringArray};
    use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TablePath};
    use fluss::row::GenericRow;

    #[tokio::test]
    async fn kv_full_scan_merges_to_current_state() {
        let cluster = FlussTestingClusterBuilder::new("kv-full-scan")
            .with_port(9147)
            .build()
            .await;

        let outcome = run(&cluster).await;

        cluster.stop();

        outcome.expect("KV full scan test body failed");
    }

    async fn run(cluster: &FlussTestingCluster) -> Result<(), String> {
        let connection = cluster.get_fluss_connection().await;
        let admin = connection
            .get_admin()
            .map_err(|e| format!("get_admin: {e}"))?;

        let table_path = TablePath::new("fluss", "test_kv_full_scan");

        // Single-bucket KV table (id INT PRIMARY KEY, name STRING) so the whole
        // table maps to bucket 0.
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
        let table_id = table.get_table_info().get_table_id();

        // Seed: insert 1,2,3,4 ; update 2 -> bravo-v2 ; delete 3.
        let upsert_writer = table
            .new_upsert()
            .map_err(|e| format!("new_upsert: {e}"))?
            .create_writer()
            .map_err(|e| format!("create_writer: {e}"))?;

        for (id, name) in [(1, "alpha"), (2, "bravo"), (3, "charlie"), (4, "delta")] {
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

        let mut update_row = GenericRow::new(2);
        update_row.set_field(0, 2);
        update_row.set_field(1, "bravo-v2");
        upsert_writer
            .upsert(&update_row)
            .map_err(|e| format!("upsert update: {e}"))?
            .await
            .map_err(|e| format!("ack update: {e}"))?;

        let mut delete_row = GenericRow::new(2);
        delete_row.set_field(0, 3);
        upsert_writer
            .delete(&delete_row)
            .map_err(|e| format!("delete: {e}"))?
            .await
            .map_err(|e| format!("ack delete: {e}"))?;

        // Merge bucket 0's changelog into current state.
        let bucket = TableBucket::new(table_id, 0);
        let batch = table
            .new_scan()
            .collect_kv_current_state_batch(bucket)
            .await
            .map_err(|e| format!("collect_kv_current_state_batch: {e}"))?;

        // Decode the (id, name) pairs.
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| "id column is not Int32".to_string())?;
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "name column is not Utf8".to_string())?;

        let mut rows: Vec<(i32, String)> = (0..batch.num_rows())
            .map(|i| (ids.value(i), names.value(i).to_string()))
            .collect();
        rows.sort_by_key(|(id, _)| *id);

        eprintln!("[kv-full-scan] merged current state: {rows:?}");

        // Expected current state: {1:alpha, 2:bravo-v2, 4:delta}; 3 deleted.
        let expected = vec![
            (1, "alpha".to_string()),
            (2, "bravo-v2".to_string()),
            (4, "delta".to_string()),
        ];
        if rows != expected {
            return Err(format!("current state mismatch: got {rows:?}, expected {expected:?}"));
        }

        // Projection: name-only.
        let projected = table
            .new_scan()
            .project_by_name(&["name"])
            .map_err(|e| format!("project_by_name: {e}"))?
            .collect_kv_current_state_batch(TableBucket::new(table_id, 0))
            .await
            .map_err(|e| format!("projected collect: {e}"))?;
        if projected.num_columns() != 1 {
            return Err(format!(
                "projected batch should have 1 column, got {}",
                projected.num_columns()
            ));
        }
        if projected.num_rows() != 3 {
            return Err(format!(
                "projected batch should have 3 rows, got {}",
                projected.num_rows()
            ));
        }

        admin
            .drop_table(&table_path, false)
            .await
            .map_err(|e| format!("drop_table: {e}"))?;

        Ok(())
    }
}
