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

//! Append-table union: concatenate the lake stream with the log-tail stream.
//!
//! For an append (log) Fluss table the union is a plain concatenation. The lake
//! snapshot covers `[earliest, seam_offset)` and the log tail covers
//! `[seam_offset, latest)`; the per-bucket seam offset is an exact cut, so no
//! row is read twice and no dedup is needed. Cross-batch row order within a
//! bucket is lake-first then log; global cross-bucket order is not guaranteed
//! (the engine merges partitions).

use arrow::datatypes::SchemaRef;
use futures::StreamExt;

use crate::reader::RecordBatchStream;
use crate::schema::align_batch_to;

/// Concatenates `lake` then `log_tail` into one stream, realigning every lake
/// batch to `target_schema` (dropping lake system columns / fixing column
/// order). Log-tail batches already carry the Fluss schema and pass through.
///
/// The realignment is applied only to the lake side; if the lake reader has
/// already projected to the Fluss columns the realignment is a cheap identity
/// reorder. Errors from either side propagate in stream order.
pub fn append_union(
    lake: RecordBatchStream,
    log_tail: RecordBatchStream,
    target_schema: SchemaRef,
) -> RecordBatchStream {
    let aligned_lake = lake.map(move |item| match item {
        Ok(batch) => align_batch_to(&batch, &target_schema),
        Err(e) => Err(e),
    });
    aligned_lake.chain(log_tail).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use futures::stream;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn batch(schema: SchemaRef, vals: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vals))]).unwrap()
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    async fn concatenates_lake_then_log_in_order() {
        let target = schema();
        // lake batch carries an extra system column to exercise realignment.
        let lake_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("__offset", DataType::Int64, false),
        ]));
        let lake_batch = RecordBatch::try_new(
            lake_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(arrow::array::Int64Array::from(vec![100, 101])),
            ],
        )
        .unwrap();

        let lake: RecordBatchStream = stream::iter(vec![Ok(lake_batch)]).boxed();
        let log: RecordBatchStream =
            stream::iter(vec![Ok(batch(target.clone(), vec![3, 4, 5]))]).boxed();

        let out: Vec<RecordBatch> = append_union(lake, log, target.clone())
            .try_collect()
            .await
            .unwrap();

        assert_eq!(total_rows(&out), 5);
        // First emitted batch is the (realigned) lake batch: schema == target.
        assert_eq!(out[0].schema().fields().len(), 1);
        assert_eq!(out[0].schema().field(0).name(), "id");
        let first = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(first.values(), &[1, 2]);
        // Log tail follows.
        let last = out[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(last.values(), &[3, 4, 5]);
    }

    #[tokio::test]
    async fn empty_lake_yields_only_log() {
        let target = schema();
        let lake: RecordBatchStream = stream::iter(Vec::new()).boxed();
        let log: RecordBatchStream = stream::iter(vec![Ok(batch(target.clone(), vec![7]))]).boxed();
        let out: Vec<RecordBatch> = append_union(lake, log, target).try_collect().await.unwrap();
        assert_eq!(total_rows(&out), 1);
    }

    #[tokio::test]
    async fn lake_error_propagates_before_log() {
        let target = schema();
        let lake: RecordBatchStream = stream::iter(vec![Err(
            crate::error::FlussLakeError::SchemaMismatch("x".into()),
        )])
        .boxed();
        let log: RecordBatchStream = stream::iter(vec![Ok(batch(target.clone(), vec![1]))]).boxed();
        let result: Result<Vec<RecordBatch>, _> =
            append_union(lake, log, target).try_collect().await;
        assert!(result.is_err());
    }
}
