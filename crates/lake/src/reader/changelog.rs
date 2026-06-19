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

//! Reads the changelog tail of one primary-key bucket: the CDC records from the
//! lake seam offset up to the frozen snapshot-end offset.
//!
//! Unlike the append [`crate::reader::log`] reader (record-batch mode, which
//! drops per-row change types and rejects PK tables), the PK merge needs each
//! record's [`ChangeType`]. This reader therefore uses the record-mode changelog
//! scanner ([`fluss`]'s `create_changelog_scanner`) and materializes the bounded
//! tail into a single full-schema Arrow batch plus a parallel change-type vector,
//! ordered by log offset.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use fluss::client::FlussConnection;
use fluss::metadata::{RowType, TablePath};
use fluss::record::{ChangeType, RowAppendRecordBatchBuilder, ScanRecord, to_arrow_schema};

use crate::error::{FlussLakeError, Result};

/// Per-poll timeout while draining the changelog tail.
const POLL_TIMEOUT: Duration = Duration::from_millis(200);
/// Wall-clock bound so a tail that never reaches `stop_offset` still terminates.
const DEADLINE: Duration = Duration::from_secs(60);
/// Consecutive empty polls tolerated before concluding the tail is exhausted.
const MAX_EMPTY_POLLS: usize = 20;

/// The bounded changelog tail of one bucket, materialized for the PK merge: the
/// records as one full-schema Arrow batch, with a parallel change-type vector
/// (same row order, ordered by log offset).
pub(crate) struct PkChangelogTail {
    pub batch: RecordBatch,
    pub change_types: Vec<ChangeType>,
}

/// Reads `[start_offset, stop_offset)` of one bucket's changelog (record mode,
/// change-type aware) and materializes it into a [`PkChangelogTail`].
///
/// `row_type` is the full Fluss row type used both to build the changelog
/// scanner's output and the materialized batch schema. `stop_offset` is the
/// exclusive boundary frozen at plan time.
pub(crate) async fn read_pk_changelog_tail(
    connection: Arc<FlussConnection>,
    table_path: &TablePath,
    row_type: &RowType,
    partition_id: Option<i64>,
    bucket: i32,
    start_offset: i64,
    stop_offset: i64,
) -> Result<PkChangelogTail> {
    let output_schema = to_arrow_schema(row_type)?;
    if stop_offset <= start_offset {
        // Nothing in the tail: the lake snapshot already covers the latest state.
        return Ok(PkChangelogTail {
            batch: RecordBatch::new_empty(output_schema),
            change_types: Vec::new(),
        });
    }

    let table = connection.get_table(table_path).await?;
    let scanner = table.new_scan().create_changelog_scanner()?;
    if let Some(partition_id) = partition_id {
        scanner
            .subscribe_partition(partition_id, bucket, start_offset)
            .await?;
    } else {
        scanner.subscribe(bucket, start_offset).await?;
    }

    // Drain the tail into offset order, stopping once a record reaches the
    // exclusive snapshot-end. Records at or past `stop_offset` are never kept.
    let mut records: Vec<ScanRecord> = Vec::new();
    let deadline = Instant::now() + DEADLINE;
    let mut empty_polls = 0usize;
    let mut reached_end = false;
    while !reached_end {
        if Instant::now() >= deadline {
            return Err(FlussLakeError::Internal(format!(
                "changelog tail poll timed out for {table_path} bucket {bucket}: \
                 did not reach offset {}",
                stop_offset - 1
            )));
        }
        let polled = scanner.poll(POLL_TIMEOUT).await?;
        if polled.is_empty() {
            empty_polls += 1;
            if empty_polls >= MAX_EMPTY_POLLS {
                break;
            }
            continue;
        }
        empty_polls = 0;
        for record in polled.into_iter() {
            let offset = record.offset();
            if offset >= stop_offset {
                reached_end = true;
                continue;
            }
            records.push(record);
            if offset >= stop_offset - 1 {
                reached_end = true;
            }
        }
    }

    records.sort_by_key(|r| r.offset());

    if records.is_empty() {
        return Ok(PkChangelogTail {
            batch: RecordBatch::new_empty(output_schema),
            change_types: Vec::new(),
        });
    }

    let mut builder = RowAppendRecordBatchBuilder::new(row_type)?;
    let mut change_types = Vec::with_capacity(records.len());
    for record in &records {
        builder.append(record.row())?;
        change_types.push(*record.change_type());
    }
    let batch = Arc::unwrap_or_clone(builder.build_arrow_record_batch()?);
    Ok(PkChangelogTail {
        batch,
        change_types,
    })
}
