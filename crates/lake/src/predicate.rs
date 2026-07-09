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

//! Engine-agnostic unified predicate for `fluss-lake`.
//!
//! [`FlussLakePredicate`] is the single filter type the public `with_filter`
//! accepts. The SDK decomposes it (by the table's partition keys) into a
//! partition predicate (used for partition pruning at plan time) and data
//! predicates (pushed to the lake reader for conservative pruning). Whatever the
//! lower layers prune, the kernel applies this predicate exactly on every output
//! `RecordBatch` via [`apply_exact_filter`], so the result strictly satisfies the
//! pushed predicate and callers need no residual filtering.
//!
//! This module is intentionally free of Paimon types so it can be unit-tested
//! without a warehouse; the Paimon `Predicate` translation lives next to the
//! lake reader.

use std::collections::BTreeSet;

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray,
};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::error::{FlussLakeError, Result};
use crate::reader::RecordBatchStream;

/// A comparison operator for a [`FlussLakePredicate::Comparison`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlussLakeComparisonOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

/// A literal value in a predicate. Timestamps are microseconds since the epoch
/// (UTC); dates are days since the epoch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FlussLakeLiteral {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    /// Microseconds since epoch (UTC).
    Timestamp(i64),
    /// Days since epoch.
    Date(i32),
}

/// The engine-agnostic unified predicate. `with_filter` accepts exactly this
/// type; the SDK auto-decomposes it into partition / bucket / data parts.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub enum FlussLakePredicate {
    #[default]
    AlwaysTrue,
    AlwaysFalse,
    And(Vec<FlussLakePredicate>),
    Or(Vec<FlussLakePredicate>),
    Not(Box<FlussLakePredicate>),
    IsNull {
        column: String,
        negated: bool,
    },
    In {
        column: String,
        values: Vec<FlussLakeLiteral>,
        negated: bool,
    },
    Comparison {
        column: String,
        op: FlussLakeComparisonOp,
        value: FlussLakeLiteral,
    },
}

impl FlussLakePredicate {
    fn comparison(
        column: impl Into<String>,
        op: FlussLakeComparisonOp,
        value: FlussLakeLiteral,
    ) -> Self {
        FlussLakePredicate::Comparison {
            column: column.into(),
            op,
            value,
        }
    }

    pub fn eq(column: impl Into<String>, value: FlussLakeLiteral) -> Self {
        Self::comparison(column, FlussLakeComparisonOp::Eq, value)
    }

    pub fn not_eq(column: impl Into<String>, value: FlussLakeLiteral) -> Self {
        Self::comparison(column, FlussLakeComparisonOp::NotEq, value)
    }

    pub fn lt(column: impl Into<String>, value: FlussLakeLiteral) -> Self {
        Self::comparison(column, FlussLakeComparisonOp::Lt, value)
    }

    pub fn lt_eq(column: impl Into<String>, value: FlussLakeLiteral) -> Self {
        Self::comparison(column, FlussLakeComparisonOp::LtEq, value)
    }

    pub fn gt(column: impl Into<String>, value: FlussLakeLiteral) -> Self {
        Self::comparison(column, FlussLakeComparisonOp::Gt, value)
    }

    pub fn gt_eq(column: impl Into<String>, value: FlussLakeLiteral) -> Self {
        Self::comparison(column, FlussLakeComparisonOp::GtEq, value)
    }

    pub fn is_null(column: impl Into<String>) -> Self {
        FlussLakePredicate::IsNull {
            column: column.into(),
            negated: false,
        }
    }

    pub fn is_not_null(column: impl Into<String>) -> Self {
        FlussLakePredicate::IsNull {
            column: column.into(),
            negated: true,
        }
    }

    pub fn is_in(column: impl Into<String>, values: Vec<FlussLakeLiteral>) -> Self {
        FlussLakePredicate::In {
            column: column.into(),
            values,
            negated: false,
        }
    }

    pub fn and(predicates: Vec<FlussLakePredicate>) -> Self {
        FlussLakePredicate::And(predicates)
    }

    pub fn or(predicates: Vec<FlussLakePredicate>) -> Self {
        FlussLakePredicate::Or(predicates)
    }

    /// Convenience constructor for logical negation. Named per the FIP; not the
    /// `std::ops::Not` trait.
    #[allow(clippy::should_implement_trait)]
    pub fn not(predicate: FlussLakePredicate) -> Self {
        FlussLakePredicate::Not(Box::new(predicate))
    }

    /// Whether this predicate is the trivial `AlwaysTrue` (no filtering).
    pub fn is_always_true(&self) -> bool {
        matches!(self, FlussLakePredicate::AlwaysTrue)
    }

    /// Collects every column name referenced anywhere in this predicate.
    pub fn referenced_columns(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        self.collect_columns(&mut out);
        out
    }

    fn collect_columns(&self, out: &mut BTreeSet<String>) {
        match self {
            FlussLakePredicate::AlwaysTrue | FlussLakePredicate::AlwaysFalse => {}
            FlussLakePredicate::And(children) | FlussLakePredicate::Or(children) => {
                for c in children {
                    c.collect_columns(out);
                }
            }
            FlussLakePredicate::Not(inner) => inner.collect_columns(out),
            FlussLakePredicate::IsNull { column, .. }
            | FlussLakePredicate::In { column, .. }
            | FlussLakePredicate::Comparison { column, .. } => {
                out.insert(column.clone());
            }
        }
    }
}

/// The result of decomposing a predicate against the table's partition keys.
#[derive(Debug, Clone, Default)]
pub struct DecomposedPredicate {
    /// Top-level conjuncts that reference only partition-key columns. Used for
    /// partition pruning at plan time.
    pub partition: Vec<FlussLakePredicate>,
    /// All other top-level conjuncts. Pushed to the lake reader and re-applied
    /// exactly on the output.
    pub data: Vec<FlussLakePredicate>,
}

/// Flattens a predicate's top-level `AND` structure into a conjunct list.
/// `AlwaysTrue` yields no conjuncts; a non-`And` predicate yields itself.
fn into_conjuncts(pred: &FlussLakePredicate) -> Vec<FlussLakePredicate> {
    match pred {
        FlussLakePredicate::AlwaysTrue => Vec::new(),
        FlussLakePredicate::And(children) => {
            children.iter().flat_map(into_conjuncts).collect()
        }
        other => vec![other.clone()],
    }
}

/// Decomposes `pred` into partition conjuncts (referencing only partition keys)
/// and data conjuncts (everything else). Bucket-key column predicates fall into
/// `data` (see the FIP: bucket pruning is best-effort, exactness is guaranteed
/// by the final Arrow filter).
pub fn decompose(pred: &FlussLakePredicate, partition_keys: &[String]) -> DecomposedPredicate {
    let partition_set: BTreeSet<&str> = partition_keys.iter().map(String::as_str).collect();
    let mut out = DecomposedPredicate::default();
    for conjunct in into_conjuncts(pred) {
        let cols = conjunct.referenced_columns();
        let only_partition =
            !cols.is_empty() && cols.iter().all(|c| partition_set.contains(c.as_str()));
        if only_partition {
            out.partition.push(conjunct);
        } else {
            out.data.push(conjunct);
        }
    }
    out
}

/// Conservatively decides whether a partition (given as its ordered key/value
/// pairs) can be excluded by the partition conjuncts. Returns `true` (keep) when
/// the conjuncts do not definitively exclude the partition. Correctness of the
/// final result never depends on this being precise: the Arrow exact filter
/// re-checks partition-key predicates on the actual rows.
pub fn partition_matches(
    conjuncts: &[FlussLakePredicate],
    partition_values: &[(String, String)],
) -> bool {
    conjuncts
        .iter()
        .all(|c| partition_conjunct_keeps(c, partition_values))
}

fn partition_value<'a>(
    partition_values: &'a [(String, String)],
    column: &str,
) -> Option<&'a str> {
    partition_values
        .iter()
        .find(|(k, _)| k == column)
        .map(|(_, v)| v.as_str())
}

/// Evaluates one partition conjunct against a partition's string values. Returns
/// `false` only when the conjunct is definitively violated; anything uncertain
/// keeps the partition.
fn partition_conjunct_keeps(
    pred: &FlussLakePredicate,
    partition_values: &[(String, String)],
) -> bool {
    match pred {
        FlussLakePredicate::AlwaysTrue => true,
        FlussLakePredicate::AlwaysFalse => false,
        FlussLakePredicate::And(children) => children
            .iter()
            .all(|c| partition_conjunct_keeps(c, partition_values)),
        FlussLakePredicate::Or(children) => children
            .iter()
            .any(|c| partition_conjunct_keeps(c, partition_values)),
        // Be conservative for negation: keep the partition.
        FlussLakePredicate::Not(_) => true,
        FlussLakePredicate::IsNull { column, negated } => {
            match partition_value(partition_values, column) {
                // A resolved partition value is never SQL NULL.
                Some(_) => *negated,
                None => true,
            }
        }
        FlussLakePredicate::In {
            column,
            values,
            negated,
        } => match partition_value(partition_values, column) {
            Some(actual) => {
                let hit = values
                    .iter()
                    .any(|lit| literal_matches_partition_string(lit, actual, FlussLakeComparisonOp::Eq));
                if *negated { !hit } else { hit }
            }
            None => true,
        },
        FlussLakePredicate::Comparison { column, op, value } => {
            match partition_value(partition_values, column) {
                Some(actual) => literal_matches_partition_string(value, actual, *op),
                None => true,
            }
        }
    }
}

/// Compares a partition's string value against a literal for the given op.
/// Numeric literals are compared numerically when the partition value parses as
/// a number; otherwise a lexicographic string compare is used.
fn literal_matches_partition_string(
    literal: &FlussLakeLiteral,
    actual: &str,
    op: FlussLakeComparisonOp,
) -> bool {
    let lit_str = match literal {
        FlussLakeLiteral::Null => return false,
        FlussLakeLiteral::Boolean(b) => b.to_string(),
        FlussLakeLiteral::Int32(v) => v.to_string(),
        FlussLakeLiteral::Int64(v) => v.to_string(),
        FlussLakeLiteral::Float64(v) => v.to_string(),
        FlussLakeLiteral::String(s) => s.clone(),
        FlussLakeLiteral::Date(v) => v.to_string(),
        FlussLakeLiteral::Timestamp(v) => v.to_string(),
        FlussLakeLiteral::Binary(_) => return true, // cannot compare; keep.
    };
    // Prefer numeric comparison when both parse as f64.
    if let (Ok(a), Ok(b)) = (actual.parse::<f64>(), lit_str.parse::<f64>()) {
        return apply_ordering(a.partial_cmp(&b), op);
    }
    apply_ordering(Some(actual.cmp(lit_str.as_str())), op)
}

fn apply_ordering(ord: Option<std::cmp::Ordering>, op: FlussLakeComparisonOp) -> bool {
    use std::cmp::Ordering::*;
    let Some(ord) = ord else { return false };
    match op {
        FlussLakeComparisonOp::Eq => ord == Equal,
        FlussLakeComparisonOp::NotEq => ord != Equal,
        FlussLakeComparisonOp::Lt => ord == Less,
        FlussLakeComparisonOp::LtEq => ord != Greater,
        FlussLakeComparisonOp::Gt => ord == Greater,
        FlussLakeComparisonOp::GtEq => ord != Less,
    }
}

// ---------------------------------------------------------------------------
// Arrow exact filter
// ---------------------------------------------------------------------------

/// Wraps `stream` so every emitted `RecordBatch` is filtered to exactly the rows
/// satisfying `predicate`. `AlwaysTrue` is a cheap pass-through.
pub fn apply_exact_filter(
    stream: RecordBatchStream,
    predicate: FlussLakePredicate,
    _schema: SchemaRef,
) -> RecordBatchStream {
    if predicate.is_always_true() {
        return stream;
    }
    stream
        .map(move |item| {
            let batch = item?;
            let mask = eval_mask(&batch, &predicate)?;
            Ok(filter_record_batch(&batch, &mask)?)
        })
        .boxed()
}

/// A scalar value pulled from an Arrow cell or a literal, reduced to a small set
/// of comparable categories.
#[derive(Debug, Clone, PartialEq)]
enum Scalar {
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(String),
    Bytes(Vec<u8>),
}

fn literal_scalar(lit: &FlussLakeLiteral) -> Option<Scalar> {
    match lit {
        FlussLakeLiteral::Null => None,
        FlussLakeLiteral::Boolean(b) => Some(Scalar::Bool(*b)),
        FlussLakeLiteral::Int32(v) => Some(Scalar::Int(*v as i128)),
        FlussLakeLiteral::Int64(v) => Some(Scalar::Int(*v as i128)),
        FlussLakeLiteral::Float64(v) => Some(Scalar::Float(*v)),
        FlussLakeLiteral::String(s) => Some(Scalar::Str(s.clone())),
        FlussLakeLiteral::Binary(b) => Some(Scalar::Bytes(b.clone())),
        FlussLakeLiteral::Date(v) => Some(Scalar::Int(*v as i128)),
        FlussLakeLiteral::Timestamp(v) => Some(Scalar::Int(*v as i128)),
    }
}

/// Reads the value of `col[row]` into a [`Scalar`], or `None` when the cell is
/// null or the column type is unsupported by the filter.
fn cell_scalar(col: &dyn Array, row: usize) -> Option<Scalar> {
    if col.is_null(row) {
        return None;
    }
    macro_rules! cast {
        ($ty:ty) => {
            col.as_any().downcast_ref::<$ty>()
        };
    }
    match col.data_type() {
        DataType::Boolean => cast!(BooleanArray).map(|a| Scalar::Bool(a.value(row))),
        DataType::Int8 => cast!(Int8Array).map(|a| Scalar::Int(a.value(row) as i128)),
        DataType::Int16 => cast!(Int16Array).map(|a| Scalar::Int(a.value(row) as i128)),
        DataType::Int32 => cast!(Int32Array).map(|a| Scalar::Int(a.value(row) as i128)),
        DataType::Int64 => cast!(Int64Array).map(|a| Scalar::Int(a.value(row) as i128)),
        DataType::Float32 => cast!(Float32Array).map(|a| Scalar::Float(a.value(row) as f64)),
        DataType::Float64 => cast!(Float64Array).map(|a| Scalar::Float(a.value(row))),
        DataType::Utf8 => cast!(StringArray).map(|a| Scalar::Str(a.value(row).to_string())),
        DataType::LargeUtf8 => {
            cast!(LargeStringArray).map(|a| Scalar::Str(a.value(row).to_string()))
        }
        DataType::Binary => cast!(BinaryArray).map(|a| Scalar::Bytes(a.value(row).to_vec())),
        DataType::LargeBinary => {
            cast!(LargeBinaryArray).map(|a| Scalar::Bytes(a.value(row).to_vec()))
        }
        DataType::Decimal128(_, _) => {
            cast!(Decimal128Array).map(|a| Scalar::Float(a.value(row) as f64))
        }
        DataType::Date32 => cast!(Date32Array).map(|a| Scalar::Int(a.value(row) as i128)),
        DataType::Timestamp(unit, _) => {
            let v = match unit {
                TimeUnit::Second => cast!(arrow::array::TimestampSecondArray).map(|a| a.value(row)),
                TimeUnit::Millisecond => {
                    cast!(arrow::array::TimestampMillisecondArray).map(|a| a.value(row))
                }
                TimeUnit::Microsecond => {
                    cast!(arrow::array::TimestampMicrosecondArray).map(|a| a.value(row))
                }
                TimeUnit::Nanosecond => {
                    cast!(arrow::array::TimestampNanosecondArray).map(|a| a.value(row))
                }
            };
            v.map(|v| Scalar::Int(v as i128))
        }
        _ => None,
    }
}

/// Compares two scalars for the given op. Mismatched categories compare unequal
/// (and unordered), yielding `false`.
fn compare_scalars(a: &Scalar, b: &Scalar, op: FlussLakeComparisonOp) -> bool {
    use std::cmp::Ordering;
    let ord: Option<Ordering> = match (a, b) {
        (Scalar::Bool(x), Scalar::Bool(y)) => Some(x.cmp(y)),
        (Scalar::Int(x), Scalar::Int(y)) => Some(x.cmp(y)),
        (Scalar::Float(x), Scalar::Float(y)) => x.partial_cmp(y),
        (Scalar::Int(x), Scalar::Float(y)) => (*x as f64).partial_cmp(y),
        (Scalar::Float(x), Scalar::Int(y)) => x.partial_cmp(&(*y as f64)),
        (Scalar::Str(x), Scalar::Str(y)) => Some(x.cmp(y)),
        (Scalar::Bytes(x), Scalar::Bytes(y)) => Some(x.cmp(y)),
        _ => None,
    };
    apply_ordering(ord, op)
}

/// Evaluates `pred` over `batch`, producing a keep-mask (one bool per row).
fn eval_mask(batch: &RecordBatch, pred: &FlussLakePredicate) -> Result<BooleanArray> {
    let n = batch.num_rows();
    Ok(match pred {
        FlussLakePredicate::AlwaysTrue => BooleanArray::from(vec![true; n]),
        FlussLakePredicate::AlwaysFalse => BooleanArray::from(vec![false; n]),
        FlussLakePredicate::And(children) => {
            let mut mask = BooleanArray::from(vec![true; n]);
            for c in children {
                mask = and_masks(&mask, &eval_mask(batch, c)?);
            }
            mask
        }
        FlussLakePredicate::Or(children) => {
            let mut mask = BooleanArray::from(vec![false; n]);
            for c in children {
                mask = or_masks(&mask, &eval_mask(batch, c)?);
            }
            mask
        }
        FlussLakePredicate::Not(inner) => negate_mask(&eval_mask(batch, inner)?),
        FlussLakePredicate::IsNull { column, negated } => {
            let col = column_ref(batch, column)?;
            let bits: Vec<bool> = (0..n)
                .map(|r| {
                    let is_null = col.is_null(r);
                    if *negated { !is_null } else { is_null }
                })
                .collect();
            BooleanArray::from(bits)
        }
        FlussLakePredicate::In {
            column,
            values,
            negated,
        } => {
            let col = column_ref(batch, column)?;
            let literals: Vec<Option<Scalar>> = values.iter().map(literal_scalar).collect();
            let bits: Vec<bool> = (0..n)
                .map(|r| {
                    let hit = match cell_scalar(col.as_ref(), r) {
                        Some(cell) => literals.iter().flatten().any(|lit| {
                            compare_scalars(&cell, lit, FlussLakeComparisonOp::Eq)
                        }),
                        None => false,
                    };
                    if *negated { !hit } else { hit }
                })
                .collect();
            BooleanArray::from(bits)
        }
        FlussLakePredicate::Comparison { column, op, value } => {
            let col = column_ref(batch, column)?;
            let literal = literal_scalar(value);
            let bits: Vec<bool> = (0..n)
                .map(|r| match (cell_scalar(col.as_ref(), r), literal.as_ref()) {
                    (Some(cell), Some(lit)) => compare_scalars(&cell, lit, *op),
                    _ => false,
                })
                .collect();
            BooleanArray::from(bits)
        }
    })
}

fn column_ref<'a>(batch: &'a RecordBatch, column: &str) -> Result<&'a std::sync::Arc<dyn Array>> {
    let idx = batch.schema().index_of(column).map_err(|_| {
        FlussLakeError::SchemaIncompatible(format!(
            "filter references column `{column}` absent from the output schema"
        ))
    })?;
    Ok(batch.column(idx))
}

fn and_masks(a: &BooleanArray, b: &BooleanArray) -> BooleanArray {
    arrow::compute::and(a, b).expect("equal-length masks")
}

fn or_masks(a: &BooleanArray, b: &BooleanArray) -> BooleanArray {
    arrow::compute::or(a, b).expect("equal-length masks")
}

fn negate_mask(a: &BooleanArray) -> BooleanArray {
    arrow::compute::not(a).expect("boolean mask")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use futures::TryStreamExt;
    use futures::stream;

    #[test]
    fn decompose_splits_partition_and_data_conjuncts() {
        let pred = FlussLakePredicate::and(vec![
            FlussLakePredicate::eq("dt", FlussLakeLiteral::String("2024".into())),
            FlussLakePredicate::gt("age", FlussLakeLiteral::Int32(18)),
        ]);
        let d = decompose(&pred, &["dt".to_string()]);
        assert_eq!(d.partition.len(), 1);
        assert_eq!(d.data.len(), 1);
        assert!(d.partition[0].referenced_columns().contains("dt"));
        assert!(d.data[0].referenced_columns().contains("age"));
    }

    #[test]
    fn always_true_has_no_conjuncts() {
        let d = decompose(&FlussLakePredicate::AlwaysTrue, &["dt".to_string()]);
        assert!(d.partition.is_empty());
        assert!(d.data.is_empty());
    }

    #[test]
    fn partition_matches_keeps_and_excludes() {
        let conj = vec![FlussLakePredicate::eq(
            "dt",
            FlussLakeLiteral::String("2024".into()),
        )];
        assert!(partition_matches(&conj, &[("dt".into(), "2024".into())]));
        assert!(!partition_matches(&conj, &[("dt".into(), "2023".into())]));
    }

    #[test]
    fn partition_matches_numeric_ordering() {
        let conj = vec![FlussLakePredicate::gt("id", FlussLakeLiteral::Int64(10))];
        assert!(partition_matches(&conj, &[("id".into(), "20".into())]));
        assert!(!partition_matches(&conj, &[("id".into(), "5".into())]));
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn batch(ids: Vec<Option<i32>>, names: Vec<Option<&str>>) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn exact_filter_applies_comparison() {
        let b = batch(
            vec![Some(1), Some(2), Some(3)],
            vec![Some("a"), Some("b"), Some("c")],
        );
        let s: RecordBatchStream = stream::iter(vec![Ok(b)]).boxed();
        let pred = FlussLakePredicate::gt("id", FlussLakeLiteral::Int32(1));
        let out: Vec<RecordBatch> = apply_exact_filter(s, pred, schema())
            .try_collect()
            .await
            .unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "id > 1 keeps 2 rows");
    }

    #[tokio::test]
    async fn exact_filter_null_dropped_by_comparison() {
        let b = batch(vec![Some(1), None, Some(5)], vec![Some("a"), None, Some("c")]);
        let s: RecordBatchStream = stream::iter(vec![Ok(b)]).boxed();
        let pred = FlussLakePredicate::gt_eq("id", FlussLakeLiteral::Int32(1));
        let out: Vec<RecordBatch> = apply_exact_filter(s, pred, schema())
            .try_collect()
            .await
            .unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2, "null id row is excluded");
    }

    #[tokio::test]
    async fn exact_filter_in_and_is_null() {
        let b = batch(
            vec![Some(1), Some(2), None, Some(4)],
            vec![Some("a"), Some("b"), Some("c"), Some("d")],
        );
        let s: RecordBatchStream = stream::iter(vec![Ok(b)]).boxed();
        let pred = FlussLakePredicate::or(vec![
            FlussLakePredicate::is_in("id", vec![FlussLakeLiteral::Int32(2), FlussLakeLiteral::Int32(4)]),
            FlussLakePredicate::is_null("id"),
        ]);
        let out: Vec<RecordBatch> = apply_exact_filter(s, pred, schema())
            .try_collect()
            .await
            .unwrap();
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "id in (2,4) or id is null keeps 3 rows");
    }
}
