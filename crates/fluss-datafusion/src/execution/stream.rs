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

//! Adapts a single async batch-producing future into a DataFusion stream.
//!
//! The future is wrapped with `futures::stream::once`, so dropping the returned
//! stream drops the in-flight future too: cancellation is cooperative and needs
//! no extra bookkeeping.

use std::future::Future;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::error::Result as DfResult;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::TryStreamExt;

/// Builds a single-batch stream from an async future that yields one batch.
///
/// `schema` is the stream's declared schema; it must match the produced batch.
pub(crate) fn single_batch_stream<F>(
    schema: SchemaRef,
    future: F,
) -> SendableRecordBatchStream
where
    F: Future<Output = DfResult<RecordBatch>> + Send + 'static,
{
    let stream = futures::stream::once(future);
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}

/// Builds a stream from an async future that yields a `Vec` of batches.
///
/// Like [`single_batch_stream`], dropping the returned stream drops the in-flight
/// future (cooperative cancellation). The resolved `Vec` is flattened into a
/// stream of `Ok(batch)` items; a future error surfaces as a single `Err`.
///
/// `schema` is the stream's declared schema; it must match the produced batches.
pub(crate) fn bounded_batches_stream<F>(
    schema: SchemaRef,
    future: F,
) -> SendableRecordBatchStream
where
    F: Future<Output = DfResult<Vec<RecordBatch>>> + Send + 'static,
{
    let stream = futures::stream::once(future)
        .map_ok(|batches| futures::stream::iter(batches.into_iter().map(DfResult::Ok)))
        .try_flatten();
    Box::pin(RecordBatchStreamAdapter::new(schema, stream))
}
