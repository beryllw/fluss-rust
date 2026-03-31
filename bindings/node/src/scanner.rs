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

use crate::error::{client_error, from_core_error};
use crate::fcore;
use crate::metadata::{ChangeType, TableBucket};
use crate::row::internal_row_to_json_with_row_type;
use crate::TOKIO_RUNTIME;
use napi_derive::napi;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[napi]
pub struct LogScanner {
    scanner: Arc<fcore::client::LogScanner>,
    #[allow(dead_code)]
    table_info: fcore::metadata::TableInfo,
    projected_row_type: fcore::metadata::RowType,
}

#[napi]
impl LogScanner {
    #[napi]
    pub async fn subscribe(&self, bucket_id: i32, start_offset: i64) -> napi::Result<()> {
        let scanner = self.scanner.clone();
        TOKIO_RUNTIME
            .spawn(async move { scanner.subscribe(bucket_id, start_offset).await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn subscribe_buckets(
        &self,
        bucket_offsets: std::collections::HashMap<String, i64>,
    ) -> napi::Result<()> {
        // Convert string keys to i32 bucket IDs
        let mut map = HashMap::new();
        for (key, offset) in &bucket_offsets {
            let bucket_id: i32 = key
                .parse()
                .map_err(|_| client_error(format!("Invalid bucket_id key: {key}")))?;
            map.insert(bucket_id, *offset);
        }

        let scanner = self.scanner.clone();
        TOKIO_RUNTIME
            .spawn(async move { scanner.subscribe_buckets(&map).await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn subscribe_partition(
        &self,
        partition_id: i64,
        bucket_id: i32,
        start_offset: i64,
    ) -> napi::Result<()> {
        let scanner = self.scanner.clone();
        TOKIO_RUNTIME
            .spawn(async move {
                scanner
                    .subscribe_partition(partition_id, bucket_id, start_offset)
                    .await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn unsubscribe(&self, bucket_id: i32) -> napi::Result<()> {
        let scanner = self.scanner.clone();
        TOKIO_RUNTIME
            .spawn(async move { scanner.unsubscribe(bucket_id).await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn unsubscribe_partition(
        &self,
        partition_id: i64,
        bucket_id: i32,
    ) -> napi::Result<()> {
        let scanner = self.scanner.clone();
        TOKIO_RUNTIME
            .spawn(async move {
                scanner
                    .unsubscribe_partition(partition_id, bucket_id)
                    .await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn poll(&self, timeout_ms: i64) -> napi::Result<ScanRecords> {
        if timeout_ms < 0 {
            return Err(client_error(format!(
                "timeout_ms must be non-negative, got: {timeout_ms}"
            )));
        }

        let scanner = self.scanner.clone();
        let timeout = Duration::from_millis(timeout_ms as u64);

        let scan_records = TOKIO_RUNTIME
            .spawn(async move { scanner.poll(timeout).await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))?;

        let row_type = &self.projected_row_type;
        let mut records = Vec::new();

        for (bucket, bucket_records) in scan_records.into_records_by_buckets() {
            let tb = TableBucket::from_core(&bucket);
            for record in &bucket_records {
                let change_type = ChangeType::from_core(*record.change_type());

                // Convert row to JSON object
                let row_data =
                    internal_row_to_json_with_row_type(record.row(), row_type)?;

                records.push(ScanRecord {
                    offset: record.offset(),
                    timestamp: record.timestamp(),
                    change_type: change_type.short_string().to_string(),
                    bucket: tb.clone(),
                    row: row_data,
                });
            }
        }

        Ok(ScanRecords { records })
    }
}

impl LogScanner {
    pub fn new(
        scanner: fcore::client::LogScanner,
        table_info: fcore::metadata::TableInfo,
        projected_row_type: fcore::metadata::RowType,
    ) -> Self {
        Self {
            scanner: Arc::new(scanner),
            table_info,
            projected_row_type,
        }
    }
}

// --- ScanRecord ---

#[napi(object)]
#[derive(Clone)]
pub struct ScanRecord {
    pub offset: i64,
    pub timestamp: i64,
    pub change_type: String,
    pub bucket: TableBucket,
    pub row: serde_json::Value,
}

// --- ScanRecords ---

#[napi]
pub struct ScanRecords {
    records: Vec<ScanRecord>,
}

#[napi]
impl ScanRecords {
    #[napi(getter)]
    pub fn count(&self) -> u32 {
        self.records.len() as u32
    }

    #[napi(getter)]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    #[napi]
    pub fn records(&self) -> Vec<ScanRecord> {
        self.records.clone()
    }
}
