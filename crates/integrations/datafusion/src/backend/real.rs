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

use arrow::array::RecordBatch;
use fluss::client::FlussConnection;
use fluss::error::FlussError;
use fluss::metadata::{TableBucket, TableInfo, TablePath};
use fluss::row::GenericRow;

use super::{FlussSource, FlussTableMeta, KeyValue, LookupKey, TableRef};
use crate::error::{FlussDatafusionError, Result};

/// Wraps a shared [`FlussConnection`] and adapts it to the [`FlussSource`] seam.
pub(crate) struct RealFlussSource {
    connection: Arc<FlussConnection>,
}

impl RealFlussSource {
    pub(crate) fn new(connection: Arc<FlussConnection>) -> Self {
        Self { connection }
    }

    fn meta_from_table_info(table: &TableRef, info: &TableInfo) -> FlussTableMeta {
        FlussTableMeta {
            table_ref: table.clone(),
            table_id: info.get_table_id(),
            schema_id: info.get_schema_id(),
            schema: info.get_schema().clone(),
            primary_keys: info.get_primary_keys().clone(),
            num_buckets: info.get_num_buckets(),
        }
    }
}

/// Narrows a scan `limit` to the `i32` the fluss scanner API expects.
fn limit_to_i32(limit: usize) -> Result<i32> {
    i32::try_from(limit).map_err(|_| {
        FlussDatafusionError::Internal(format!("log scan limit {limit} exceeds i32 range"))
    })
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
    async fn list_databases(&self) -> Result<Vec<String>> {
        let admin = self.connection.get_admin()?;
        Ok(admin.list_databases().await?)
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>> {
        let admin = self.connection.get_admin()?;
        Ok(admin.list_tables(database).await?)
    }

    async fn get_table_meta(&self, table: &TableRef) -> Result<FlussTableMeta> {
        let admin = self.connection.get_admin()?;
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
        let table_handle = self.connection.get_table(&path).await?;
        let mut lookuper = table_handle.new_lookup()?.create_lookuper()?;
        let key_row = build_key_row(key);
        let result = lookuper.lookup(&key_row).await?;
        // Reuse fluss's Arrow assembly; returns a 0-row batch when absent.
        Ok(result.to_record_batch()?)
    }

    async fn log_scan(
        &self,
        table: &TableRef,
        bucket: i32,
        projection: Option<&[usize]>,
        limit: usize,
    ) -> Result<Vec<RecordBatch>> {
        let path: TablePath = table.into();
        let table_handle = self.connection.get_table(&path).await?;
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
            .create_bucket_batch_scanner(TableBucket::new(table_id, bucket))?;

        let batches = scanner.collect_all_batches().await?;
        Ok(batches.into_iter().map(|b| b.into_batch()).collect())
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
