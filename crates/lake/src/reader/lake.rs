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
//!
//! For a partitioned table the read is additionally restricted to one partition
//! by pushing a partition-equality predicate to Paimon, so a `(partition,
//! bucket)` split never mixes rows from other partitions.

use futures::StreamExt;
use paimon::Table;
use paimon::spec::{DataType, Datum, Predicate, PredicateBuilder};

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
/// `partition_values` (empty for non-partitioned tables) restricts the read to
/// one partition via a pushed partition-equality predicate.
pub async fn read_lake_table(
    table: &Table,
    projection: Option<&[String]>,
    bucket: Option<i32>,
    partition_values: &[(String, String)],
) -> Result<RecordBatchStream> {
    let mut read_builder = table.new_read_builder();
    if let Some(columns) = projection {
        let refs: Vec<&str> = columns.iter().map(String::as_str).collect();
        read_builder.with_projection(&refs);
    }
    if !partition_values.is_empty() {
        let predicate = build_partition_predicate(table, partition_values)?;
        read_builder.with_filter(predicate);
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
    Ok(stream
        .map(|item| item.map_err(FlussLakeError::from))
        .boxed())
}

/// Builds a conjunctive partition-equality predicate (`k1 = v1 AND ...`) so the
/// Paimon scan prunes to exactly one partition.
fn build_partition_predicate(
    table: &Table,
    partition_values: &[(String, String)],
) -> Result<Predicate> {
    let fields = table.schema().fields();
    let builder = PredicateBuilder::new(fields);
    let mut preds = Vec::with_capacity(partition_values.len());
    for (key, value) in partition_values {
        let field = fields.iter().find(|f| f.name() == key).ok_or_else(|| {
            FlussLakeError::SchemaIncompatible(format!(
                "partition key `{key}` is absent from the lake table schema"
            ))
        })?;
        let datum = string_to_datum(field.data_type(), value)?;
        preds.push(builder.equal(key, datum)?);
    }
    Ok(Predicate::and(preds))
}

/// Converts a canonical partition value string into a Paimon [`Datum`] of the
/// partition column's type. Covers the scalar types allowed as partition keys.
fn string_to_datum(dt: &DataType, value: &str) -> Result<Datum> {
    let parse_err = |ty: &str| {
        FlussLakeError::PlanningFailed(format!(
            "cannot parse partition value `{value}` as {ty}"
        ))
    };
    Ok(match dt {
        DataType::Boolean(_) => Datum::Bool(value.parse().map_err(|_| parse_err("boolean"))?),
        DataType::TinyInt(_) => Datum::TinyInt(value.parse().map_err(|_| parse_err("tinyint"))?),
        DataType::SmallInt(_) => Datum::SmallInt(value.parse().map_err(|_| parse_err("smallint"))?),
        DataType::Int(_) => Datum::Int(value.parse().map_err(|_| parse_err("int"))?),
        DataType::BigInt(_) => Datum::Long(value.parse().map_err(|_| parse_err("bigint"))?),
        DataType::Float(_) => Datum::Float(value.parse().map_err(|_| parse_err("float"))?),
        DataType::Double(_) => Datum::Double(value.parse().map_err(|_| parse_err("double"))?),
        DataType::Char(_) | DataType::VarChar(_) => Datum::String(value.to_string()),
        DataType::Date(_) => Datum::Date(value.parse().map_err(|_| parse_err("date (epoch days)"))?),
        other => {
            return Err(FlussLakeError::SchemaIncompatible(format!(
                "partition key type {other:?} is not supported for lake partition pruning"
            )));
        }
    })
}
