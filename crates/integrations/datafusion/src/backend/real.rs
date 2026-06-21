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

//! Production [`FlussSource`] backed by the fluss client.
//!
//! Reuses fluss's existing Arrow helpers (`LookupResult::to_record_batch`,
//! `ScanBatch::into_batch`) so no Fluss-to-Arrow assembly is reimplemented here.
//!
//! `FlussTable` borrows the connection by reference, so this type holds an
//! `Arc<FlussConnection>` and rebuilds the table / lookuper / scanner per call.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use arrow::array::RecordBatch;
use fluss::client::FlussConnection;
use fluss::error::FlussError;
use fluss::metadata::{TableBucket, TableInfo, TablePath};
use fluss::record::ScanBatch;
use fluss::row::GenericRow;
use fluss::rpc::message::OffsetSpec;

use super::{FlussPartition, FlussSource, FlussTableMeta, KeyValue, LookupKey, TableRef};
use crate::error::{FlussDatafusionError, Result};

/// Wraps a shared [`FlussConnection`] and adapts it to the [`FlussSource`] seam.
///
/// The connection is held in an [`ArcSwap`] so it can be hot-swapped at runtime
/// (see [`Self::swap_connection`]) without rebuilding the source, the
/// `MetadataLoader`, or any registered catalog. Each access loads the current
/// connection, so an in-flight call uses one consistent snapshot while a
/// concurrent swap only affects calls that start after it.
pub(crate) struct RealFlussSource {
    connection: ArcSwap<FlussConnection>,
}

impl RealFlussSource {
    pub(crate) fn new(connection: Arc<FlussConnection>) -> Self {
        Self {
            connection: ArcSwap::from(connection),
        }
    }

    /// The connection currently in use. Each call snapshots the live value.
    pub(crate) fn connection(&self) -> Arc<FlussConnection> {
        self.connection.load_full()
    }

    /// Atomically replaces the underlying connection. Calls already in flight
    /// keep the connection they loaded; calls started after this use `new`.
    /// Closing the old connection is the caller's responsibility.
    pub(crate) fn swap_connection(&self, new: Arc<FlussConnection>) {
        self.connection.store(new);
    }

    fn meta_from_table_info(table: &TableRef, info: &TableInfo) -> FlussTableMeta {
        let config = info.get_table_config();
        // Lake metadata is best-effort: a malformed `table.datalake.*` config must
        // not break plain metadata loading, so a parse error degrades to "no lake".
        let datalake_format = config.get_datalake_format().ok().flatten();
        let lake_catalog_properties = config.get_lake_catalog_properties().ok().flatten();
        FlussTableMeta {
            table_ref: table.clone(),
            table_id: info.get_table_id(),
            schema_id: info.get_schema_id(),
            schema: info.get_schema().clone(),
            primary_keys: info.get_primary_keys().clone(),
            bucket_keys: info.get_bucket_keys().to_vec(),
            num_buckets: info.get_num_buckets(),
            partition_keys: info.get_partition_keys().to_vec(),
            datalake_format,
            lake_catalog_properties,
        }
    }
}

/// Small timeout reused by the finite log snapshot poll loop.
const LOG_SCAN_POLL_TIMEOUT: Duration = Duration::from_millis(200);

/// Narrows a scan `limit` to the `i32` the fluss scanner API expects.
fn limit_to_i32(limit: usize) -> Result<i32> {
    i32::try_from(limit).map_err(|_| {
        FlussDatafusionError::Internal(format!("bounded scan limit {limit} exceeds i32 range"))
    })
}

fn collect_rows_until_limit(
    batches: &mut Vec<RecordBatch>,
    batch: RecordBatch,
    rows_collected: &mut usize,
    row_limit: usize,
) {
    if *rows_collected >= row_limit {
        return;
    }

    let remaining = row_limit - *rows_collected;
    let keep = remaining.min(batch.num_rows());
    if keep > 0 {
        batches.push(batch.slice(0, keep));
        *rows_collected += keep;
    }
}

fn partition_bucket(
    table_id: i64,
    partition_id: Option<i64>,
    bucket: i32,
) -> TableBucket {
    TableBucket::new_with_partition(table_id, partition_id, bucket)
}

fn scan_batch_reaches_snapshot_end(scan_batch: &ScanBatch, snapshot_end: i64) -> bool {
    scan_batch.last_offset() >= snapshot_end.saturating_sub(1)
}

/// Builds a `GenericRow` lookup key from the primary-key value list.
///
/// The row has one field per primary-key column, in primary-key order, which is
/// what `Lookuper::lookup` expects for a full-primary-key equality lookup.
fn build_key_row(key: &LookupKey) -> GenericRow<'_> {
    let mut row = GenericRow::new(key.len());
    for (idx, value) in key.iter().enumerate() {
        match value {
            KeyValue::Boolean(v) => row.set_field(idx, *v),
            KeyValue::Int8(v) => row.set_field(idx, *v),
            KeyValue::Int16(v) => row.set_field(idx, *v),
            KeyValue::Int32(v) => row.set_field(idx, *v),
            KeyValue::Int64(v) => row.set_field(idx, *v),
            KeyValue::String(v) => row.set_field(idx, v.as_str()),
        }
    }
    row
}

#[async_trait::async_trait]
impl FlussSource for RealFlussSource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn list_databases(&self) -> Result<Vec<String>> {
        let admin = self.connection().get_admin()?;
        Ok(admin.list_databases().await?)
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>> {
        let admin = self.connection().get_admin()?;
        Ok(admin.list_tables(database).await?)
    }

    async fn get_table_meta(&self, table: &TableRef) -> Result<FlussTableMeta> {
        let admin = self.connection().get_admin()?;
        let path: TablePath = table.into();
        let info = admin.get_table_info(&path).await.map_err(|err| {
            // Translate the source's "table does not exist" API error into the
            // crate's structured `TableNotFound` so the catalog `table()` path can
            // map it to `Ok(None)` instead of propagating an opaque client error.
            if err.api_error() == Some(FlussError::TableNotExist) {
                FlussDatafusionError::TableNotFound(table.to_string())
            } else {
                FlussDatafusionError::from(err)
            }
        })?;
        Ok(Self::meta_from_table_info(table, &info))
    }

    async fn lookup(&self, table: &TableRef, key: &LookupKey) -> Result<RecordBatch> {
        let path: TablePath = table.into();
        let connection = self.connection();
        let table_handle = connection.get_table(&path).await?;
        let mut lookuper = table_handle.new_lookup()?.create_lookuper()?;
        let key_row = build_key_row(key);
        let result = lookuper.lookup(&key_row).await?;
        // Reuse fluss's Arrow assembly; returns a 0-row batch when absent.
        Ok(result.to_record_batch()?)
    }

    async fn prefix_lookup(
        &self,
        table: &TableRef,
        lookup_columns: &[String],
        key: &LookupKey,
    ) -> Result<RecordBatch> {
        let path: TablePath = table.into();
        let connection = self.connection();
        let table_handle = connection.get_table(&path).await?;
        // `lookup_by` switches the builder into bucket-key prefix mode; the column
        // list (partition keys + bucket keys, in order) is validated by
        // `create_lookuper`. The key row carries one field per lookup column, in
        // the same `lookup_columns` order, mirroring the point-lookup row build.
        let mut lookuper = table_handle
            .new_lookup()?
            .lookup_by(lookup_columns.to_vec())
            .create_lookuper()?;
        let key_row = build_key_row(key);
        let result = lookuper.lookup(&key_row).await?;
        // Reuse fluss's Arrow assembly; returns a 0-row batch when nothing matches.
        Ok(result.to_record_batch()?)
    }

    async fn list_partitions(&self, table: &TableRef) -> Result<Vec<FlussPartition>> {
        let admin = self.connection().get_admin()?;
        let path: TablePath = table.into();
        let infos = admin.list_partition_infos(&path).await?;
        Ok(infos
            .into_iter()
            .map(|info| {
                let spec = info.get_resolved_partition_spec();
                let values = spec
                    .get_partition_keys()
                    .iter()
                    .cloned()
                    .zip(spec.get_partition_values().iter().cloned())
                    .collect();
                FlussPartition {
                    partition_id: info.get_partition_id(),
                    values,
                }
            })
            .collect())
    }

    async fn bounded_scan(
        &self,
        table: &TableRef,
        partition_id: Option<i64>,
        bucket: i32,
        projection: Option<&[usize]>,
        limit: usize,
    ) -> Result<Vec<RecordBatch>> {
        let path: TablePath = table.into();
        let connection = self.connection();
        let table_handle = connection.get_table(&path).await?;
        let table_id = table_handle.get_table_info().get_table_id();
        let limit_i32 = limit_to_i32(limit)?;

        // The scan builder is single-use; one fresh builder scans one bucket.
        // The fluss API validates `bucket` is in `0..num_buckets` and errors if not.
        let mut scan = table_handle.new_scan();
        if let Some(indices) = projection {
            scan = scan.project(indices)?;
        }
        let mut scanner = scan
            .limit(limit_i32)?
            .create_bucket_batch_scanner(partition_bucket(table_id, partition_id, bucket))?;

        let batches = scanner.collect_all_batches().await?;
        Ok(batches.into_iter().map(|b| b.into_batch()).collect())
    }

    async fn log_scan(
        &self,
        table: &TableRef,
        partition_id: Option<i64>,
        bucket: i32,
        projection: Option<&[usize]>,
        row_limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let path: TablePath = table.into();
        let connection = self.connection();
        let table_handle = connection.get_table(&path).await?;
        let admin = self.connection().get_admin()?;

        let start_offset = if let Some(partition_id) = partition_id {
            let partition_infos = admin.list_partition_infos(&path).await?;
            let partition_name = partition_infos
                .into_iter()
                .find(|info| info.get_partition_id() == partition_id)
                .map(|info| info.get_partition_name())
                .ok_or_else(|| {
                    FlussDatafusionError::Internal(format!(
                        "unknown partition id {partition_id} for log table {table}"
                    ))
                })?;
            let mut earliest = admin
                .list_partition_offsets(&path, &partition_name, &[bucket], OffsetSpec::Earliest)
                .await?;
            let mut latest = admin
                .list_partition_offsets(&path, &partition_name, &[bucket], OffsetSpec::Latest)
                .await?;
            let start = earliest.remove(&bucket).ok_or_else(|| {
                FlussDatafusionError::Internal(format!(
                    "missing earliest offset for {table} partition {partition_id} bucket {bucket}"
                ))
            })?;
            let end = latest.remove(&bucket).ok_or_else(|| {
                FlussDatafusionError::Internal(format!(
                    "missing latest offset for {table} partition {partition_id} bucket {bucket}"
                ))
            })?;
            (start, end)
        } else {
            let mut earliest = admin.list_offsets(&path, &[bucket], OffsetSpec::Earliest).await?;
            let mut latest = admin.list_offsets(&path, &[bucket], OffsetSpec::Latest).await?;
            let start = earliest.remove(&bucket).ok_or_else(|| {
                FlussDatafusionError::Internal(format!(
                    "missing earliest offset for {table} bucket {bucket}"
                ))
            })?;
            let end = latest.remove(&bucket).ok_or_else(|| {
                FlussDatafusionError::Internal(format!(
                    "missing latest offset for {table} bucket {bucket}"
                ))
            })?;
            (start, end)
        };

        let (start_offset, snapshot_end) = start_offset;
        if snapshot_end <= start_offset {
            return Ok(Vec::new());
        }

        let mut scan = table_handle.new_scan();
        if let Some(indices) = projection {
            scan = scan.project(indices)?;
        }
        let scanner = scan.create_record_batch_log_scanner()?;
        if let Some(partition_id) = partition_id {
            scanner
                .subscribe_partition(partition_id, bucket, start_offset)
                .await?;
        } else {
            scanner.subscribe(bucket, start_offset).await?;
        }

        let mut batches = Vec::new();
        let mut rows_collected = 0usize;
        let mut reached_snapshot_end = false;

        while !reached_snapshot_end {
            let polled = scanner.poll(LOG_SCAN_POLL_TIMEOUT).await?;
            if polled.is_empty() {
                continue;
            }

            for scan_batch in polled {
                let base_offset = scan_batch.base_offset();
                let last_offset = scan_batch.last_offset();
                reached_snapshot_end |= scan_batch_reaches_snapshot_end(&scan_batch, snapshot_end);
                let batch = scan_batch.into_batch();
                // `snapshot_end` is the exclusive latest offset captured at query
                // start. Trim a boundary-crossing batch to rows with offset <
                // snapshot_end so records appended after the snapshot do not leak
                // into this finite scan.
                let batch = if last_offset >= snapshot_end {
                    let keep = (snapshot_end - base_offset).max(0) as usize;
                    batch.slice(0, keep.min(batch.num_rows()))
                } else {
                    batch
                };
                if let Some(limit) = row_limit {
                    collect_rows_until_limit(&mut batches, batch, &mut rows_collected, limit);
                    if rows_collected >= limit {
                        return Ok(batches);
                    }
                } else if batch.num_rows() > 0 {
                    batches.push(batch);
                }
            }
        }

        Ok(batches)
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_within_i32_passes_through() {
        assert_eq!(limit_to_i32(4).unwrap(), 4);
    }

    #[test]
    fn limit_beyond_i32_is_rejected() {
        let err = limit_to_i32(usize::MAX).unwrap_err();
        assert!(matches!(err, FlussDatafusionError::Internal(_)));
    }
}
