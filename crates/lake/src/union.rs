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

//! Orchestrates one partition of an append-table union: pair the lake stream
//! with the bucket's log tail read from the seam offset, then concatenate.
//!
//! The lake stream is produced upstream (by [`crate::reader::lake`], tested
//! against a seeded warehouse); the log tail comes through the
//! [`LogTailReader`] trait (faked in tests here, real Fluss in integration
//! tests). This keeps the seam-offset wiring — the bug-prone "read from the
//! seam, not from earliest" decision — under fast, infra-free unit tests.

use arrow::datatypes::SchemaRef;

use crate::error::Result;
use crate::reader::RecordBatchStream;
use crate::reader::append::append_union;
use crate::reader::log::LogTailReader;

/// One execution unit of a union scan: a single `(partition, bucket)` whose lake
/// data sits in the Paimon snapshot and whose residual log tail runs from
/// `log_start_offset` (the lake seam) to `log_stop_offset` (the snapshot end, or
/// the latest offset when `None`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnionPartition {
    pub partition_id: Option<i64>,
    pub bucket: i32,
    pub log_start_offset: i64,
    pub log_stop_offset: Option<i64>,
}

/// Produces the unioned Arrow stream for one append-table partition: the
/// (schema-aligned) lake stream followed by the bucket's log tail.
///
/// `lake_stream` is the already-opened lake read for this partition's bucket.
/// `projection` is the column indices into the Fluss schema, forwarded to the
/// log reader (the lake stream is already projected by the lake reader).
pub async fn union_append_partition(
    lake_stream: RecordBatchStream,
    log_reader: &dyn LogTailReader,
    partition: &UnionPartition,
    target_schema: SchemaRef,
    projection: Option<Vec<usize>>,
) -> Result<RecordBatchStream> {
    let log_tail = log_reader
        .read_tail(
            partition.partition_id,
            partition.bucket,
            partition.log_start_offset,
            partition.log_stop_offset,
            projection,
        )
        .await?;
    Ok(append_union(lake_stream, log_tail, target_schema))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex;

    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use async_trait::async_trait;
    use futures::StreamExt;
    use futures::TryStreamExt;
    use futures::stream;

    /// Records every read_tail call and replays a canned tail stream.
    #[derive(Default)]
    struct FakeLogTailReader {
        calls: Mutex<Vec<(Option<i64>, i32, i64, Option<i64>, Option<Vec<usize>>)>>,
        tail: Mutex<Vec<RecordBatch>>,
    }

    impl FakeLogTailReader {
        fn with_tail(batches: Vec<RecordBatch>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                tail: Mutex::new(batches),
            }
        }
    }

    #[async_trait]
    impl LogTailReader for FakeLogTailReader {
        async fn read_tail(
            &self,
            partition_id: Option<i64>,
            bucket: i32,
            start_offset: i64,
            stop_offset: Option<i64>,
            projection: Option<Vec<usize>>,
        ) -> Result<RecordBatchStream> {
            self.calls.lock().unwrap().push((
                partition_id,
                bucket,
                start_offset,
                stop_offset,
                projection,
            ));
            let batches: Vec<_> = self.tail.lock().unwrap().drain(..).map(Ok).collect();
            Ok(stream::iter(batches).boxed())
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn batch(s: SchemaRef, vals: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(s, vec![Arc::new(Int32Array::from(vals))]).unwrap()
    }

    fn ids(b: &RecordBatch) -> Vec<i32> {
        b.column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    #[tokio::test]
    async fn concatenates_lake_then_log_tail() {
        let s = schema();
        let lake: RecordBatchStream = stream::iter(vec![Ok(batch(s.clone(), vec![1, 2]))]).boxed();
        let reader = FakeLogTailReader::with_tail(vec![batch(s.clone(), vec![3, 4, 5])]);
        let partition = UnionPartition {
            partition_id: None,
            bucket: 0,
            log_start_offset: 100,
            log_stop_offset: Some(103),
        };

        let out: Vec<RecordBatch> =
            union_append_partition(lake, &reader, &partition, s.clone(), None)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();

        let all: Vec<i32> = out.iter().flat_map(ids).collect();
        assert_eq!(all, vec![1, 2, 3, 4, 5], "lake rows first, then log tail");
    }

    #[tokio::test]
    async fn reads_log_tail_from_seam_offset_not_earliest() {
        // The crux: the log tail MUST be requested from the seam offset, with the
        // partition/bucket/stop forwarded verbatim.
        let s = schema();
        let lake: RecordBatchStream = stream::iter(Vec::new()).boxed();
        let reader = FakeLogTailReader::with_tail(vec![]);
        let partition = UnionPartition {
            partition_id: Some(7),
            bucket: 3,
            log_start_offset: 4096,
            log_stop_offset: Some(5000),
        };

        let _ = union_append_partition(lake, &reader, &partition, s, Some(vec![0]))
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let calls = reader.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0],
            (Some(7), 3, 4096, Some(5000), Some(vec![0])),
            "seam offset + partition/bucket/stop/projection must be forwarded"
        );
    }

    #[tokio::test]
    async fn empty_log_tail_yields_only_lake() {
        let s = schema();
        let lake: RecordBatchStream = stream::iter(vec![Ok(batch(s.clone(), vec![9]))]).boxed();
        let reader = FakeLogTailReader::with_tail(vec![]);
        let partition = UnionPartition {
            partition_id: None,
            bucket: 0,
            log_start_offset: 0,
            log_stop_offset: None,
        };

        let out: Vec<RecordBatch> =
            union_append_partition(lake, &reader, &partition, s.clone(), None)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
        let all: Vec<i32> = out.iter().flat_map(ids).collect();
        assert_eq!(all, vec![9]);
    }
}
