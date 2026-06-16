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

//! Client-side KV current-state reader (Option B: changelog-only).
//!
//! [`KvFullScanner`] reads ONE bucket's CDC changelog from the earliest offset to
//! the latest offset captured at call time, then merges the per-record change
//! types ([`ChangeType`]) by primary key into the bucket's current state and
//! materializes the surviving rows into an Arrow [`RecordBatch`].
//!
//! Merge semantics, applied in changelog order:
//! - `Insert` / `UpdateAfter` / `AppendOnly` => upsert (replace) the row for that PK;
//! - `Delete` => remove the PK;
//! - `UpdateBefore` => ignore (the matching `UpdateAfter` carries the new value).
//!
//! A KV table's rows for a given primary key live in exactly one bucket (the
//! bucket key is a subset of the primary key), so a per-bucket merge needs no
//! cross-bucket deduplication.
//!
//! # Completeness limitation
//!
//! Completeness is bounded by changelog retention: rows whose changelog records
//! have already aged out of the retained window (i.e. been compacted into a kv
//! snapshot and dropped from the log) are NOT reflected here. This reader is
//! changelog-only by design — it issues no kv-snapshot RPC and depends on no
//! RocksDB state — so it returns the current state only for the portion of
//! history still present in the changelog. This is acceptable for the current
//! feature; a snapshot-backed reader would be required for full historical
//! completeness.

use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use linked_hash_map::LinkedHashMap;

use crate::client::connection::FlussConnection;
use crate::client::metadata::Metadata;
use crate::client::table::{LogScanner, TableScan};
use crate::error::{Error, Result};
use crate::metadata::{DataType, TableBucket, TableInfo};
use crate::record::{ChangeType, RowAppendRecordBatchBuilder, ScanRecord, to_arrow_schema};
use crate::row::{Decimal, InternalRow};
use crate::rpc::message::OffsetSpec;
use std::sync::Arc;

/// Maximum wall-clock time to spend polling one bucket's changelog before giving
/// up. Bounds the merge so it always terminates even if the changelog never
/// reaches the captured `latest` offset (e.g. records compacted away).
const KV_FULL_SCAN_DEADLINE: Duration = Duration::from_secs(60);

/// Per-poll timeout for the changelog scanner.
const KV_FULL_SCAN_POLL_TIMEOUT: Duration = Duration::from_millis(500);

/// Maximum number of consecutive empty polls tolerated before concluding the
/// readable changelog is exhausted (e.g. the captured `latest` is unreachable
/// because the tail was compacted). Each empty poll waits up to
/// [`KV_FULL_SCAN_POLL_TIMEOUT`], so this also bounds idle time.
const KV_FULL_SCAN_MAX_EMPTY_POLLS: usize = 20;

/// An owned, hashable primary-key value used as the merge map key.
///
/// Covers the scalar types Fluss permits in a primary key. Floating-point and
/// complex types (ARRAY/MAP/ROW) are rejected: they are not valid primary-key
/// column types.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum PkKeyValue {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    String(String),
    Bytes(Vec<u8>),
    /// Decimal carried by its unscaled big-integer string + scale, which is a
    /// stable, hashable representation of the exact value.
    Decimal(String, i32),
    Date(i32),
    Time(i32),
    /// (epoch millis, nanos-of-millisecond) for NTZ / LTZ timestamps.
    TimestampNtz(i64, i32),
    TimestampLtz(i64, i32),
}

/// The composite primary key of one changelog record: one [`PkKeyValue`] per
/// primary-key column, in primary-key order.
type PkKey = Vec<PkKeyValue>;

/// Reads and merges ONE bucket's KV changelog into its current state.
///
/// Construct via [`TableScan::kv_full_scanner`], then call
/// [`collect_current_state`](Self::collect_current_state). Borrows the
/// connection and metadata like [`TableScan`] so it can rebuild a changelog
/// scanner per call.
pub struct KvFullScanner<'a> {
    conn: &'a FlussConnection,
    metadata: Arc<Metadata>,
    table_info: TableInfo,
    /// Column indices to project into the output batch; `None` = all columns.
    projection: Option<Vec<usize>>,
}

impl<'a> KvFullScanner<'a> {
    pub(crate) fn new(
        conn: &'a FlussConnection,
        metadata: Arc<Metadata>,
        table_info: TableInfo,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            conn,
            metadata,
            table_info,
            projection,
        }
    }

    /// Reads `table_bucket`'s changelog from earliest to the latest offset
    /// captured now, merges by primary key, and returns the bucket's current
    /// state as a [`RecordBatch`] with the table's (optionally projected) Arrow
    /// schema.
    ///
    /// Returns an empty batch (0 rows, correct schema) when the bucket holds no
    /// readable changelog (`latest <= earliest`).
    pub async fn collect_current_state(&self, table_bucket: TableBucket) -> Result<RecordBatch> {
        if !self.table_info.has_primary_key() {
            return Err(Error::UnsupportedOperation {
                message: format!(
                    "collect_current_state requires a primary-key (KV) table; {} has none",
                    self.table_info.table_path
                ),
            });
        }

        let partition_id = table_bucket.partition_id();
        let bucket = table_bucket.bucket_id();

        // 1. Snapshot boundary: resolve [earliest, latest) at call time.
        let (earliest, latest) = self.resolve_offsets(partition_id, bucket).await?;

        let output_schema = self.output_schema()?;
        if latest <= earliest {
            // No readable changelog for this bucket => empty current state.
            return Ok(RecordBatch::new_empty(output_schema));
        }

        // 2. Subscribe the changelog scanner at `earliest` and poll up to
        //    `latest - 1` (the last readable offset).
        let scanner = self.build_changelog_scanner()?;
        if let Some(partition_id) = partition_id {
            scanner.subscribe_partition(partition_id, bucket, earliest).await?;
        } else {
            scanner.subscribe(bucket, earliest).await?;
        }

        // 3. Merge by primary key in changelog order, preserving first-seen order.
        let pk_indexes = self.table_info.get_schema().primary_key_indexes();
        let mut merged: LinkedHashMap<PkKey, ScanRecord> = LinkedHashMap::new();

        let deadline = Instant::now() + KV_FULL_SCAN_DEADLINE;
        let mut last_offset = earliest - 1;
        let mut empty_polls = 0usize;

        while last_offset < latest - 1 {
            if Instant::now() >= deadline {
                return Err(Error::UnexpectedError {
                    message: format!(
                        "KV full scan timed out for bucket {table_bucket}: reached offset \
                         {last_offset}, expected {}",
                        latest - 1
                    ),
                    source: None,
                });
            }

            let scan_records = scanner.poll(KV_FULL_SCAN_POLL_TIMEOUT).await?;
            if scan_records.is_empty() {
                empty_polls += 1;
                if empty_polls >= KV_FULL_SCAN_MAX_EMPTY_POLLS {
                    // The captured `latest` is unreachable from the retained
                    // changelog (likely compacted). Stop with what we have.
                    break;
                }
                continue;
            }
            empty_polls = 0;

            // Records for the only subscribed bucket; drain them in offset order.
            let mut records: Vec<ScanRecord> = scan_records
                .records_by_buckets()
                .values()
                .flatten()
                .map(clone_scan_record)
                .collect();
            records.sort_by_key(|r| r.offset());

            for record in records {
                if record.offset() >= latest {
                    // Defensive: never merge beyond the captured snapshot boundary.
                    continue;
                }
                last_offset = last_offset.max(record.offset());
                self.apply_record(&pk_indexes, &mut merged, record)?;
            }
        }

        // 4. Materialize the surviving rows into a RecordBatch.
        self.materialize(merged, output_schema)
    }

    /// Resolves the earliest and latest offsets for the target bucket.
    async fn resolve_offsets(
        &self,
        partition_id: Option<i64>,
        bucket: i32,
    ) -> Result<(i64, i64)> {
        let admin = self.conn.get_admin()?;
        let table_path = &self.table_info.table_path;

        if let Some(partition_id) = partition_id {
            let partition_infos = admin.list_partition_infos(table_path).await?;
            let partition_name = partition_infos
                .into_iter()
                .find(|info| info.get_partition_id() == partition_id)
                .map(|info| info.get_partition_name())
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!(
                        "unknown partition id {partition_id} for KV table {table_path}"
                    ),
                    source: None,
                })?;
            let earliest = *admin
                .list_partition_offsets(table_path, &partition_name, &[bucket], OffsetSpec::Earliest)
                .await?
                .get(&bucket)
                .ok_or_else(|| missing_offset(table_path, bucket, "earliest"))?;
            let latest = *admin
                .list_partition_offsets(table_path, &partition_name, &[bucket], OffsetSpec::Latest)
                .await?
                .get(&bucket)
                .ok_or_else(|| missing_offset(table_path, bucket, "latest"))?;
            Ok((earliest, latest))
        } else {
            let earliest = *admin
                .list_offsets(table_path, &[bucket], OffsetSpec::Earliest)
                .await?
                .get(&bucket)
                .ok_or_else(|| missing_offset(table_path, bucket, "earliest"))?;
            let latest = *admin
                .list_offsets(table_path, &[bucket], OffsetSpec::Latest)
                .await?
                .get(&bucket)
                .ok_or_else(|| missing_offset(table_path, bucket, "latest"))?;
            Ok((earliest, latest))
        }
    }

    fn build_changelog_scanner(&self) -> Result<LogScanner> {
        let scan = TableScan::new(self.conn, self.table_info.clone(), self.metadata.clone());
        // The changelog scanner decodes the FULL row (id + value columns); we
        // project after merging, so do NOT push projection into the scanner.
        scan.create_changelog_scanner()
    }

    fn output_schema(&self) -> Result<arrow_schema::SchemaRef> {
        let full_row_type = self.table_info.get_row_type();
        match &self.projection {
            None => to_arrow_schema(full_row_type),
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| full_row_type.fields()[i].clone())
                    .collect();
                to_arrow_schema(&crate::metadata::RowType::new(fields))
            }
        }
    }

    fn apply_record(
        &self,
        pk_indexes: &[usize],
        merged: &mut LinkedHashMap<PkKey, ScanRecord>,
        record: ScanRecord,
    ) -> Result<()> {
        let key = build_pk_key(record.row(), pk_indexes, self.table_info.get_row_type())?;
        match record.change_type() {
            ChangeType::Insert | ChangeType::UpdateAfter | ChangeType::AppendOnly => {
                // Upsert: replace any existing row for this PK. Re-insert moves
                // the key to the end of iteration order, which is fine — order is
                // not part of the contract for a full scan.
                merged.insert(key, record);
            }
            ChangeType::Delete => {
                merged.remove(&key);
            }
            ChangeType::UpdateBefore => {
                // The before-image carries no new state; the paired UpdateAfter
                // applies the change.
            }
        }
        Ok(())
    }

    fn materialize(
        &self,
        merged: LinkedHashMap<PkKey, ScanRecord>,
        output_schema: arrow_schema::SchemaRef,
    ) -> Result<RecordBatch> {
        if merged.is_empty() {
            return Ok(RecordBatch::new_empty(output_schema));
        }

        // Build the full-width batch first (reusing the row->Arrow builder), then
        // project columns. This keeps row materialization in one shared helper.
        let full_row_type = self.table_info.get_row_type();
        let mut builder = RowAppendRecordBatchBuilder::new(full_row_type)?;
        for record in merged.values() {
            builder.append(record.row())?;
        }
        let full_batch = Arc::unwrap_or_clone(builder.build_arrow_record_batch()?);

        match &self.projection {
            None => Ok(full_batch),
            Some(indices) => {
                let columns = indices
                    .iter()
                    .map(|&i| full_batch.column(i).clone())
                    .collect::<Vec<_>>();
                Ok(RecordBatch::try_new(output_schema, columns)?)
            }
        }
    }
}

/// Clones a [`ScanRecord`] by re-wrapping its (Arc-backed) columnar row. The
/// underlying typed batch is shared, so this is cheap.
fn clone_scan_record(record: &ScanRecord) -> ScanRecord {
    ScanRecord::new(
        record.row().clone(),
        record.offset(),
        record.timestamp(),
        *record.change_type(),
    )
}

fn missing_offset(
    table_path: &crate::metadata::TablePath,
    bucket: i32,
    which: &str,
) -> Error {
    Error::UnexpectedError {
        message: format!("missing {which} offset for KV table {table_path} bucket {bucket}"),
        source: None,
    }
}

/// Builds the composite primary-key merge key from a record's PK columns.
fn build_pk_key(
    row: &dyn InternalRow,
    pk_indexes: &[usize],
    row_type: &crate::metadata::RowType,
) -> Result<PkKey> {
    let mut key = Vec::with_capacity(pk_indexes.len());
    for &idx in pk_indexes {
        if row.is_null_at(idx)? {
            key.push(PkKeyValue::Null);
            continue;
        }
        let data_type = row_type.fields()[idx].data_type();
        key.push(pk_value(row, idx, data_type)?);
    }
    Ok(key)
}

fn pk_value(row: &dyn InternalRow, idx: usize, data_type: &DataType) -> Result<PkKeyValue> {
    Ok(match data_type {
        DataType::Boolean(_) => PkKeyValue::Bool(row.get_boolean(idx)?),
        DataType::TinyInt(_) => PkKeyValue::Int8(row.get_byte(idx)?),
        DataType::SmallInt(_) => PkKeyValue::Int16(row.get_short(idx)?),
        DataType::Int(_) => PkKeyValue::Int32(row.get_int(idx)?),
        DataType::BigInt(_) => PkKeyValue::Int64(row.get_long(idx)?),
        DataType::Char(_) | DataType::String(_) => {
            PkKeyValue::String(row.get_string(idx)?.to_string())
        }
        DataType::Bytes(_) => PkKeyValue::Bytes(row.get_bytes(idx)?.to_vec()),
        DataType::Binary(b) => PkKeyValue::Bytes(row.get_binary(idx, b.length())?.to_vec()),
        DataType::Decimal(d) => {
            let decimal: Decimal =
                row.get_decimal(idx, d.precision() as usize, d.scale() as usize)?;
            PkKeyValue::Decimal(decimal.to_string(), d.scale() as i32)
        }
        DataType::Date(_) => PkKeyValue::Date(row.get_date(idx)?.get_inner()),
        DataType::Time(_) => PkKeyValue::Time(row.get_time(idx)?.get_inner()),
        DataType::Timestamp(t) => {
            let ts = row.get_timestamp_ntz(idx, t.precision())?;
            PkKeyValue::TimestampNtz(ts.get_millisecond(), ts.get_nano_of_millisecond())
        }
        DataType::TimestampLTz(t) => {
            let ts = row.get_timestamp_ltz(idx, t.precision())?;
            PkKeyValue::TimestampLtz(ts.get_epoch_millisecond(), ts.get_nano_of_millisecond())
        }
        other => {
            return Err(Error::UnsupportedOperation {
                message: format!(
                    "primary-key column type {other:?} is not supported by the KV full scan merge"
                ),
            });
        }
    })
}
