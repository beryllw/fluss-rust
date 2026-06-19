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

//! Primary-key merge: overlay a bounded Fluss changelog tail onto the streamed
//! Paimon lake current state, per bucket, for a bounded batch read.
//!
//! The lake current state (deduplicated by Paimon) is the base; the changelog
//! tail `[seam, stop)` applies on top, last-writer-wins, with DELETE /
//! UPDATE_BEFORE acting as tombstones. The big side (lake) is streamed and never
//! fully buffered; only the bounded changelog tail is held in memory as a
//! per-key overlay map.
//!
//! Algorithm (equivalent to Fluss's `LakeSnapshotAndLogSplitScanner`, but using
//! a hash overlay since the Paimon Rust reader does not emit a globally
//! key-sorted stream):
//! 1. Fold the tail into `overlay: PkKey -> Option<row>` in offset order
//!    (`Insert`/`UpdateAfter`/`AppendOnly` => value, `Delete` => tombstone,
//!    `UpdateBefore` => ignore).
//! 2. Stream each lake batch, dropping rows whose key the overlay owns
//!    (overridden or deleted), passing the rest through.
//! 3. After the lake is drained, emit the overlay's surviving values (updates +
//!    pure inserts); tombstones are not emitted.
//!
//! Keys are extracted from Arrow rows only (both sides are full-schema Arrow by
//! this point), so a single key encoder governs both — there is no risk of two
//! encoders disagreeing.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BooleanArray, BooleanBuilder, Date32Array, Decimal128Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, UInt32Array,
};
use arrow::compute::{filter_record_batch, take};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use fluss::record::ChangeType;
use futures::StreamExt;

use crate::error::{FlussLakeError, Result};
use crate::reader::RecordBatchStream;
use crate::schema::align_batch_to;

/// An owned, hashable primary-key value. Mirrors the scalar set Fluss permits in
/// a primary key, read here from Arrow arrays.
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
    Decimal(i128, i8),
    Date(i32),
    Timestamp(i64, TimeUnit),
}

/// The composite primary key of one row: one [`PkKeyValue`] per PK column.
type PkKey = Vec<PkKeyValue>;

/// Extracts the composite primary key of `row` in `batch` from the PK columns.
fn arrow_pk_key(batch: &RecordBatch, row: usize, pk_indexes: &[usize]) -> Result<PkKey> {
    let mut key = Vec::with_capacity(pk_indexes.len());
    for &idx in pk_indexes {
        let col = batch.column(idx);
        if col.is_null(row) {
            key.push(PkKeyValue::Null);
            continue;
        }
        key.push(arrow_pk_value(col.as_ref(), row)?);
    }
    Ok(key)
}

fn arrow_pk_value(col: &dyn Array, row: usize) -> Result<PkKeyValue> {
    macro_rules! downcast {
        ($ty:ty) => {
            col.as_any().downcast_ref::<$ty>().ok_or_else(|| {
                FlussLakeError::Internal(format!(
                    "primary-key column array downcast failed for {:?}",
                    col.data_type()
                ))
            })?
        };
    }
    Ok(match col.data_type() {
        DataType::Boolean => PkKeyValue::Bool(downcast!(BooleanArray).value(row)),
        DataType::Int8 => PkKeyValue::Int8(downcast!(Int8Array).value(row)),
        DataType::Int16 => PkKeyValue::Int16(downcast!(Int16Array).value(row)),
        DataType::Int32 => PkKeyValue::Int32(downcast!(Int32Array).value(row)),
        DataType::Int64 => PkKeyValue::Int64(downcast!(Int64Array).value(row)),
        DataType::Utf8 => PkKeyValue::String(downcast!(StringArray).value(row).to_string()),
        DataType::LargeUtf8 => {
            PkKeyValue::String(downcast!(LargeStringArray).value(row).to_string())
        }
        DataType::Binary => PkKeyValue::Bytes(downcast!(BinaryArray).value(row).to_vec()),
        DataType::LargeBinary => PkKeyValue::Bytes(downcast!(LargeBinaryArray).value(row).to_vec()),
        DataType::Decimal128(_, scale) => {
            PkKeyValue::Decimal(downcast!(Decimal128Array).value(row), *scale)
        }
        DataType::Date32 => PkKeyValue::Date(downcast!(Date32Array).value(row)),
        DataType::Timestamp(unit, _) => {
            let v = match unit {
                TimeUnit::Second => downcast!(arrow::array::TimestampSecondArray).value(row),
                TimeUnit::Millisecond => {
                    downcast!(arrow::array::TimestampMillisecondArray).value(row)
                }
                TimeUnit::Microsecond => {
                    downcast!(arrow::array::TimestampMicrosecondArray).value(row)
                }
                TimeUnit::Nanosecond => {
                    downcast!(arrow::array::TimestampNanosecondArray).value(row)
                }
            };
            PkKeyValue::Timestamp(v, *unit)
        }
        other => {
            return Err(FlussLakeError::Internal(format!(
                "primary-key column type {other:?} is not supported by the PK merge"
            )));
        }
    })
}

/// Builds the per-key overlay from the changelog tail, in offset order.
/// `Some(row_idx)` is the surviving value row in `tail_batch`; `None` is a
/// tombstone (the key is deleted).
fn build_overlay(
    tail_batch: &RecordBatch,
    change_types: &[ChangeType],
    pk_indexes: &[usize],
) -> Result<HashMap<PkKey, Option<usize>>> {
    let mut overlay: HashMap<PkKey, Option<usize>> = HashMap::new();
    for (row, change) in change_types.iter().enumerate() {
        let key = arrow_pk_key(tail_batch, row, pk_indexes)?;
        match change {
            ChangeType::Insert | ChangeType::UpdateAfter | ChangeType::AppendOnly => {
                overlay.insert(key, Some(row));
            }
            ChangeType::Delete => {
                overlay.insert(key, None);
            }
            ChangeType::UpdateBefore => {
                // The before-image carries no new state; the paired UpdateAfter
                // applies the change.
            }
        }
    }
    Ok(overlay)
}

fn project_schema(full_schema: &SchemaRef, projection: &Option<Vec<usize>>) -> SchemaRef {
    match projection {
        None => full_schema.clone(),
        Some(indices) => {
            let fields = indices
                .iter()
                .map(|&i| full_schema.field(i).clone())
                .collect::<Vec<_>>();
            Arc::new(arrow::datatypes::Schema::new(fields))
        }
    }
}

fn project_batch(
    batch: &RecordBatch,
    projection: &Option<Vec<usize>>,
    projected_schema: &SchemaRef,
) -> Result<RecordBatch> {
    match projection {
        None => Ok(batch.clone()),
        Some(indices) => {
            let columns = indices
                .iter()
                .map(|&i| batch.column(i).clone())
                .collect::<Vec<_>>();
            Ok(RecordBatch::try_new(projected_schema.clone(), columns)?)
        }
    }
}

/// Builds the batch of the overlay's surviving value rows (updates + pure
/// inserts), taken from `tail_batch`, then projected.
fn overlay_values_batch(
    tail_batch: &RecordBatch,
    overlay: &HashMap<PkKey, Option<usize>>,
    projection: &Option<Vec<usize>>,
    projected_schema: &SchemaRef,
) -> Result<Option<RecordBatch>> {
    let mut rows: Vec<u32> = overlay
        .values()
        .filter_map(|v| v.map(|r| r as u32))
        .collect();
    if rows.is_empty() {
        return Ok(None);
    }
    // Stable order keeps output deterministic for tests; order is not contractual.
    rows.sort_unstable();
    let indices = UInt32Array::from(rows);
    let columns = tail_batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let full = RecordBatch::try_new(tail_batch.schema(), columns)?;
    Ok(Some(project_batch(&full, projection, projected_schema)?))
}

/// Merges the streamed lake current state with the bounded changelog tail.
///
/// `full_schema` is the full Fluss Arrow schema (every lake batch is aligned to
/// it before keying). `projection` is the output column indices into the full
/// schema, applied after the merge.
pub(crate) fn pk_merge(
    lake_stream: RecordBatchStream,
    tail_batch: RecordBatch,
    change_types: Vec<ChangeType>,
    pk_indexes: Vec<usize>,
    full_schema: SchemaRef,
    projection: Option<Vec<usize>>,
) -> Result<RecordBatchStream> {
    let overlay = Arc::new(build_overlay(&tail_batch, &change_types, &pk_indexes)?);
    let projected_schema = project_schema(&full_schema, &projection);

    // Final pass: the overlay's surviving values (updates + inserts).
    let values_batch = overlay_values_batch(&tail_batch, &overlay, &projection, &projected_schema)?;

    // Streamed pass: each lake batch, aligned, with overlay-owned keys dropped.
    let lake_overlay = Arc::clone(&overlay);
    let lake_pk = pk_indexes.clone();
    let lake_full_schema = full_schema.clone();
    let lake_projection = projection.clone();
    let lake_projected_schema = projected_schema.clone();
    let lake_mapped = lake_stream.map(move |item| {
        let batch = item?;
        let aligned = align_batch_to(&batch, &lake_full_schema)?;
        let mut keep = BooleanBuilder::with_capacity(aligned.num_rows());
        for row in 0..aligned.num_rows() {
            let key = arrow_pk_key(&aligned, row, &lake_pk)?;
            keep.append_value(!lake_overlay.contains_key(&key));
        }
        let mask: BooleanArray = keep.finish();
        let filtered = filter_record_batch(&aligned, &mask)?;
        project_batch(&filtered, &lake_projection, &lake_projected_schema)
    });

    let tail_stream = futures::stream::iter(values_batch.map(Ok));
    Ok(lake_mapped.chain(tail_stream).boxed())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use futures::TryStreamExt;
    use futures::stream;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch(schema: SchemaRef, ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    /// Collects (id, name) pairs from result batches, sorted by id.
    async fn collect_pairs(stream: RecordBatchStream) -> Vec<(i32, String)> {
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut out = Vec::new();
        for b in &batches {
            let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let names = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                out.push((ids.value(i), names.value(i).to_string()));
            }
        }
        out.sort();
        out
    }

    fn lake(schema: SchemaRef, ids: Vec<i32>, names: Vec<&str>) -> RecordBatchStream {
        stream::iter(vec![Ok(batch(schema, ids, names))]).boxed()
    }

    #[tokio::test]
    async fn update_overrides_lake_insert_adds_delete_removes() {
        let s = schema();
        // lake current state: id 1=a, 2=b, 3=c
        let lake_stream = lake(s.clone(), vec![1, 2, 3], vec!["a", "b", "c"]);
        // changelog tail: update 2->b2, insert 4=d, delete 3
        let tail = batch(s.clone(), vec![2, 4, 3], vec!["b2", "d", "c"]);
        let changes = vec![
            ChangeType::UpdateAfter,
            ChangeType::Insert,
            ChangeType::Delete,
        ];

        let merged = pk_merge(lake_stream, tail, changes, vec![0], s.clone(), None).unwrap();
        let pairs = collect_pairs(merged).await;
        assert_eq!(
            pairs,
            vec![(1, "a".into()), (2, "b2".into()), (4, "d".into()),],
            "2 updated, 3 deleted, 4 inserted, 1 untouched"
        );
    }

    #[tokio::test]
    async fn last_writer_wins_within_tail() {
        let s = schema();
        let lake_stream = lake(s.clone(), vec![1], vec!["a"]);
        // two updates to the same key in offset order: final value is the last.
        let tail = batch(s.clone(), vec![1, 1], vec!["v1", "v2"]);
        let changes = vec![ChangeType::UpdateAfter, ChangeType::UpdateAfter];

        let merged = pk_merge(lake_stream, tail, changes, vec![0], s.clone(), None).unwrap();
        assert_eq!(collect_pairs(merged).await, vec![(1, "v2".into())]);
    }

    #[tokio::test]
    async fn delete_then_reinsert_same_key_keeps_reinsert() {
        let s = schema();
        let lake_stream = lake(s.clone(), vec![1], vec!["a"]);
        let tail = batch(s.clone(), vec![1, 1], vec!["a", "a2"]);
        let changes = vec![ChangeType::Delete, ChangeType::Insert];

        let merged = pk_merge(lake_stream, tail, changes, vec![0], s.clone(), None).unwrap();
        assert_eq!(collect_pairs(merged).await, vec![(1, "a2".into())]);
    }

    #[tokio::test]
    async fn delete_of_absent_key_is_noop() {
        let s = schema();
        let lake_stream = lake(s.clone(), vec![1], vec!["a"]);
        let tail = batch(s.clone(), vec![9], vec!["x"]);
        let changes = vec![ChangeType::Delete];

        let merged = pk_merge(lake_stream, tail, changes, vec![0], s.clone(), None).unwrap();
        assert_eq!(collect_pairs(merged).await, vec![(1, "a".into())]);
    }

    #[tokio::test]
    async fn update_before_is_ignored() {
        let s = schema();
        let lake_stream = lake(s.clone(), vec![1], vec!["a"]);
        // a -U/+U pair: only +U applies.
        let tail = batch(s.clone(), vec![1, 1], vec!["a", "a2"]);
        let changes = vec![ChangeType::UpdateBefore, ChangeType::UpdateAfter];

        let merged = pk_merge(lake_stream, tail, changes, vec![0], s.clone(), None).unwrap();
        assert_eq!(collect_pairs(merged).await, vec![(1, "a2".into())]);
    }

    #[tokio::test]
    async fn empty_tail_yields_lake_unchanged() {
        let s = schema();
        let lake_stream = lake(s.clone(), vec![1, 2], vec!["a", "b"]);
        let tail = RecordBatch::new_empty(s.clone());

        let merged = pk_merge(lake_stream, tail, vec![], vec![0], s.clone(), None).unwrap();
        assert_eq!(
            collect_pairs(merged).await,
            vec![(1, "a".into()), (2, "b".into())]
        );
    }

    #[tokio::test]
    async fn projection_selects_columns_after_merge() {
        let s = schema();
        let lake_stream = lake(s.clone(), vec![1, 2], vec!["a", "b"]);
        let tail = batch(s.clone(), vec![2], vec!["b2"]);
        let changes = vec![ChangeType::UpdateAfter];

        // Project only the name column (index 1); PK column 0 still used for keying.
        let merged = pk_merge(
            lake_stream,
            tail,
            changes,
            vec![0],
            s.clone(),
            Some(vec![1]),
        )
        .unwrap();
        let batches: Vec<RecordBatch> = merged.try_collect().await.unwrap();
        let mut names: Vec<String> = Vec::new();
        for b in &batches {
            assert_eq!(b.num_columns(), 1);
            let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..b.num_rows() {
                names.push(col.value(i).to_string());
            }
        }
        names.sort();
        assert_eq!(names, vec!["a".to_string(), "b2".to_string()]);
    }

    #[test]
    fn pk_key_distinguishes_types_and_nulls() {
        let s = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        let b = RecordBatch::try_new(
            s,
            vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(1)]))],
        )
        .unwrap();
        let k0 = arrow_pk_key(&b, 0, &[0]).unwrap();
        let k1 = arrow_pk_key(&b, 1, &[0]).unwrap();
        let k2 = arrow_pk_key(&b, 2, &[0]).unwrap();
        assert_eq!(k0, k2, "same value => same key");
        assert_ne!(k0, k1, "null differs from a value");
    }
}
