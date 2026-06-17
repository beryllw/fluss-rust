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

//! Reads the log (Fluss) side of a union: the residual log tail of one bucket,
//! from the lake seam offset up to a snapshot-end offset.
//!
//! This is the only part of the union that requires a live Fluss connection, so
//! it sits behind the [`LogTailReader`] trait. The union orchestration depends
//! on the trait, letting unit tests drive it with a fake while the real
//! [`FlussLogTailReader`] is exercised by an integration test against a cluster.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;
use fluss::rpc::message::OffsetSpec;
use futures::StreamExt;
use futures::stream;

use crate::error::{FlussLakeError, Result};
use crate::reader::RecordBatchStream;

/// Reads the residual log tail of a single bucket: the records from
/// `start_offset` (the lake seam) up to, but not including, `stop_offset`.
///
/// `partition_id` is `None` for a non-partitioned table. `stop_offset = None`
/// means "read to the latest offset captured at read time". `projection` is the
/// column indices into the Fluss table schema, or `None` for all columns.
#[async_trait]
pub trait LogTailReader: Send + Sync {
    async fn read_tail(
        &self,
        partition_id: Option<i64>,
        bucket: i32,
        start_offset: i64,
        stop_offset: Option<i64>,
        projection: Option<Vec<usize>>,
    ) -> Result<RecordBatchStream>;
}

/// Poll cadence for draining the log tail. Mirrors the value used by the
/// DataFusion backend's finite log scan.
const LOG_TAIL_POLL_TIMEOUT: Duration = Duration::from_millis(200);

/// The production [`LogTailReader`], backed by a live Fluss connection. One
/// reader is bound to one table; `read_tail` subscribes a fresh log scanner at
/// the seam offset and drains it up to the snapshot-end (or the latest offset
/// captured now when the caller passes `None`).
///
/// This mirrors the proven finite-snapshot poll loop of the DataFusion backend's
/// `log_scan`, with the single difference that the start offset is the lake seam
/// rather than `Earliest`. It is exercised by an integration test against a real
/// cluster (no tiering needed — a plain log table with a chosen start offset).
pub struct FlussLogTailReader {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
}

impl FlussLogTailReader {
    pub fn new(connection: Arc<FlussConnection>, table_path: TablePath) -> Self {
        Self {
            connection,
            table_path,
        }
    }

    /// Resolves the latest offset for one bucket when the caller did not pin a
    /// snapshot-end (`stop_offset = None`).
    async fn latest_offset(&self, partition_id: Option<i64>, bucket: i32) -> Result<i64> {
        let admin = self.connection.get_admin()?;
        let mut latest = if let Some(partition_id) = partition_id {
            let partition_name = self.partition_name(partition_id).await?;
            admin
                .list_partition_offsets(
                    &self.table_path,
                    &partition_name,
                    &[bucket],
                    OffsetSpec::Latest,
                )
                .await?
        } else {
            admin
                .list_offsets(&self.table_path, &[bucket], OffsetSpec::Latest)
                .await?
        };
        latest.remove(&bucket).ok_or_else(|| {
            FlussLakeError::Internal(format!(
                "missing latest offset for {} partition {partition_id:?} bucket {bucket}",
                self.table_path
            ))
        })
    }

    async fn partition_name(&self, partition_id: i64) -> Result<String> {
        let admin = self.connection.get_admin()?;
        admin
            .list_partition_infos(&self.table_path)
            .await?
            .into_iter()
            .find(|info| info.get_partition_id() == partition_id)
            .map(|info| info.get_partition_name())
            .ok_or_else(|| {
                FlussLakeError::SchemaMismatch(format!(
                    "unknown partition id {partition_id} for log table {}",
                    self.table_path
                ))
            })
    }
}

#[async_trait]
impl LogTailReader for FlussLogTailReader {
    async fn read_tail(
        &self,
        partition_id: Option<i64>,
        bucket: i32,
        start_offset: i64,
        stop_offset: Option<i64>,
        projection: Option<Vec<usize>>,
    ) -> Result<RecordBatchStream> {
        let snapshot_end = match stop_offset {
            Some(end) => end,
            None => self.latest_offset(partition_id, bucket).await?,
        };
        // Nothing in the tail: lake already covers up to (or past) the latest.
        if snapshot_end <= start_offset {
            return Ok(stream::iter(Vec::new()).boxed());
        }

        let table_handle = self.connection.get_table(&self.table_path).await?;
        let mut scan = table_handle.new_scan();
        if let Some(indices) = projection.as_deref() {
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

        // Drain from the seam until a batch reaches the snapshot-end boundary.
        let mut batches = Vec::new();
        let mut reached_end = false;
        while !reached_end {
            let polled = scanner.poll(LOG_TAIL_POLL_TIMEOUT).await?;
            if polled.is_empty() {
                continue;
            }
            for scan_batch in polled {
                reached_end |= scan_batch.last_offset() >= snapshot_end.saturating_sub(1);
                let batch = scan_batch.into_batch();
                if batch.num_rows() > 0 {
                    batches.push(Ok(batch));
                }
            }
        }
        Ok(stream::iter(batches).boxed())
    }
}
