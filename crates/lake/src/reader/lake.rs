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

//! Reads the lake (Paimon) side of a union: a pinned snapshot of a Paimon table
//! to an Arrow stream.
//!
//! Wraps Paimon's `ReadBuilder -> TableScan::plan -> TableRead::to_arrow`. The
//! projection is pushed down by Fluss column name, which also drops the lake
//! system columns. The returned stream is `'static` (Paimon clones the splits /
//! file IO into it), so the borrowed `Table` need not outlive it.
//!
//! When a `bucket` is given, the planned splits are filtered to that bucket so a
//! per-bucket union execution partition reads ONLY its own lake data. This
//! relies on Fluss and Paimon assigning the same bucket id to a row, which holds
//! because a lake-enabled Fluss table buckets with the data lake's algorithm.

use futures::StreamExt;
use paimon::Table;

use crate::error::{FlussLakeError, Result};
use crate::reader::RecordBatchStream;

/// Reads `table` (already pinned to a snapshot via
/// [`crate::catalog::get_table_at_snapshot`]) to an Arrow stream.
///
/// `projection` is the list of Fluss column names to read, in output order;
/// `None` reads the table's full declared schema. Passing the Fluss column names
/// is also what excludes Paimon system columns from the lake batches.
///
/// `bucket` restricts the read to one Paimon bucket; `None` reads every bucket.
/// A per-bucket union partition MUST pass its target bucket, otherwise every
/// partition would re-emit the whole lake snapshot.
pub async fn read_lake_table(
    table: &Table,
    projection: Option<&[String]>,
    bucket: Option<i32>,
) -> Result<RecordBatchStream> {
    let mut read_builder = table.new_read_builder();
    if let Some(columns) = projection {
        let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        read_builder.with_projection(&refs);
    }
    let plan = read_builder.new_scan().plan().await?;
    let read = read_builder.new_read()?;
    let splits = match bucket {
        Some(target) => plan
            .splits()
            .iter()
            .filter(|split| split.bucket() == target)
            .cloned()
            .collect(),
        None => plan.splits().to_vec(),
    };
    let stream = read.to_arrow(&splits)?;
    Ok(stream.map(|item| item.map_err(FlussLakeError::from)).boxed())
}
