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

//! KV predicate recognition.
//!
//! Phase 1 accepts exactly one KV pushdown shape: a full-primary-key equality
//! conjunction (`pk1 = lit AND pk2 = lit ...`). DataFusion splits top-level `AND`
//! into separate conjuncts, so `filters` arrives as a list of `col = literal`
//! binary expressions. Anything else (partial key, non-key column, `IN`, range,
//! column-vs-column, prefix, duplicate/missing key) is rejected with a clear
//! [`FlussDatafusionError::UnsupportedQueryPattern`] so an unsupported query never
//! degrades into a silent full scan.

use std::collections::HashMap;

use datafusion::logical_expr::{BinaryExpr, Operator};
use datafusion::prelude::Expr;

use crate::backend::{KeyValue, LookupKey};
use crate::error::{FlussDatafusionError, Result};
use crate::types::scalar::{scalar_to_key_value, scalar_to_partition_string};

/// True if a single filter is a `column = literal` equality whose column is one of
/// `columns`. Shared by the KV (primary-key) and Log (partition-key) pushdown
/// classifiers below.
fn is_equality_on(filter: &Expr, columns: &[String]) -> bool {
    matches!(
        single_equality(filter),
        Some((column, _)) if columns.iter().any(|c| c == column)
    )
}

/// True if a single filter is a `primary_key_column = literal` equality.
///
/// Used by `supports_filters_pushdown` to mark only PK-equality filters as
/// consumed (`Exact`); every other filter stays `Unsupported`.
pub(crate) fn is_primary_key_equality(filter: &Expr, primary_keys: &[String]) -> bool {
    is_equality_on(filter, primary_keys)
}

/// Analyzes the filter conjunction and, only when it forms a complete
/// primary-key equality, returns the [`LookupKey`] in primary-key order.
///
/// Returns `UnsupportedQueryPattern` for every shape outside the Phase 1 KV
/// contract; the caller must surface that rather than fall back to a scan.
pub(crate) fn analyze_kv_filters(
    filters: &[Expr],
    primary_keys: &[String],
) -> Result<LookupKey> {
    if primary_keys.is_empty() {
        return Err(FlussDatafusionError::UnsupportedQueryPattern(
            "KV pushdown requires a primary key".to_string(),
        ));
    }
    if filters.is_empty() {
        return Err(FlussDatafusionError::UnsupportedQueryPattern(
            "KV tables require a full primary-key equality predicate; \
             a full scan is not supported"
                .to_string(),
        ));
    }

    // Collect each conjunct as a (column -> value) equality, rejecting any
    // conjunct that is not a plain `col = literal`.
    let mut bindings: HashMap<&str, KeyValue> = HashMap::with_capacity(filters.len());
    for filter in filters {
        let (column, scalar) = single_equality(filter).ok_or_else(|| {
            FlussDatafusionError::UnsupportedQueryPattern(format!(
                "only full primary-key equality is supported; rejected predicate: {filter}"
            ))
        })?;

        if !primary_keys.iter().any(|pk| pk == column) {
            return Err(FlussDatafusionError::UnsupportedQueryPattern(format!(
                "predicate on non-primary-key column `{column}` is not supported"
            )));
        }

        let value = scalar_to_key_value(&scalar)?;
        if bindings.insert(column, value).is_some() {
            return Err(FlussDatafusionError::UnsupportedQueryPattern(format!(
                "duplicate predicate on primary-key column `{column}`"
            )));
        }
    }

    if bindings.len() != primary_keys.len() {
        return Err(FlussDatafusionError::UnsupportedQueryPattern(format!(
            "KV pushdown requires all {} primary-key column(s) ({}); got {} equality predicate(s)",
            primary_keys.len(),
            primary_keys.join(", "),
            bindings.len()
        )));
    }

    // Emit the key in primary-key order (lookup keys are positional).
    let mut key = LookupKey::with_capacity(primary_keys.len());
    for pk in primary_keys {
        let value = bindings.remove(pk.as_str()).ok_or_else(|| {
            FlussDatafusionError::UnsupportedQueryPattern(format!(
                "missing equality predicate for primary-key column `{pk}`"
            ))
        })?;
        key.push(value);
    }
    Ok(key)
}

/// True if a single filter is a `partition_column = literal` equality.
///
/// Used by the log table's `supports_filters_pushdown` to mark partition-column
/// equality filters as (inexact) pushdown candidates; every other filter stays
/// `Unsupported`.
pub(crate) fn is_partition_equality(filter: &Expr, partition_keys: &[String]) -> bool {
    is_equality_on(filter, partition_keys)
}

/// Extracts the partition-column equality bindings (column -> value string) from
/// the filter conjunction, considering ONLY columns in `partition_keys`. Equality
/// only; non-equality or non-partition filters are ignored (left to FilterExec).
/// Returns the bindings found (possibly empty => no pruning => scan all).
///
/// Conversion of a literal to the partition-value string is best-effort: a value
/// that cannot be rendered (e.g. NULL, an unsupported type) is skipped rather than
/// failing, because the residual `FilterExec` guarantees correctness regardless of
/// how aggressively pruning narrows the partition set.
pub(crate) fn analyze_partition_filters(
    filters: &[Expr],
    partition_keys: &[String],
) -> HashMap<String, String> {
    let mut bindings: HashMap<String, String> = HashMap::new();
    for filter in filters {
        let Some((column, scalar)) = single_equality(filter) else {
            continue;
        };
        if !partition_keys.iter().any(|pk| pk == column) {
            continue;
        }
        if let Ok(value) = scalar_to_partition_string(&scalar) {
            // Last write wins; duplicates are not an error for best-effort pruning.
            bindings.insert(column.to_string(), value);
        }
    }
    bindings
}

/// Matches a single `column = literal` (in either operand order) and returns the
/// column name and the literal scalar. Returns `None` for anything else
/// (column-vs-column, non-`Eq` operator, `IN`, ranges, function calls, ...).
fn single_equality(filter: &Expr) -> Option<(&str, datafusion::scalar::ScalarValue)> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = filter else {
        return None;
    };
    if *op != Operator::Eq {
        return None;
    }
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(col), Expr::Literal(scalar, _)) => Some((col.name.as_str(), scalar.clone())),
        (Expr::Literal(scalar, _), Expr::Column(col)) => Some((col.name.as_str(), scalar.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::{col, lit, Expr};

    use super::*;

    fn pks(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn full_single_key_equality_is_accepted() {
        let pk = pks(&["id"]);
        let filters = vec![col("id").eq(lit(2i32))];
        let key = analyze_kv_filters(&filters, &pk).unwrap();
        assert_eq!(key, vec![KeyValue::Int32(2)]);
    }

    #[test]
    fn composite_key_is_ordered_by_primary_key() {
        let pk = pks(&["region", "id"]);
        // Provide filters in the opposite order to prove ordering by PK.
        let filters = vec![col("id").eq(lit(2i32)), col("region").eq(lit("us"))];
        let key = analyze_kv_filters(&filters, &pk).unwrap();
        assert_eq!(
            key,
            vec![KeyValue::String("us".to_string()), KeyValue::Int32(2)]
        );
    }

    #[test]
    fn literal_on_either_side_is_accepted() {
        let pk = pks(&["id"]);
        let filters = vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(lit(2i32)),
            op: Operator::Eq,
            right: Box::new(col("id")),
        })];
        let key = analyze_kv_filters(&filters, &pk).unwrap();
        assert_eq!(key, vec![KeyValue::Int32(2)]);
    }

    #[test]
    fn empty_filters_are_rejected() {
        let pk = pks(&["id"]);
        let err = analyze_kv_filters(&[], &pk).unwrap_err();
        assert!(matches!(err, FlussDatafusionError::UnsupportedQueryPattern(_)));
    }

    #[test]
    fn partial_composite_key_is_rejected() {
        let pk = pks(&["region", "id"]);
        let filters = vec![col("region").eq(lit("us"))];
        let err = analyze_kv_filters(&filters, &pk).unwrap_err();
        assert!(matches!(err, FlussDatafusionError::UnsupportedQueryPattern(_)));
    }

    #[test]
    fn non_primary_key_column_is_rejected() {
        let pk = pks(&["id"]);
        let filters = vec![col("name").eq(lit("x"))];
        let err = analyze_kv_filters(&filters, &pk).unwrap_err();
        assert!(matches!(err, FlussDatafusionError::UnsupportedQueryPattern(_)));
    }

    #[test]
    fn range_predicate_is_rejected() {
        let pk = pks(&["id"]);
        let filters = vec![col("id").gt(lit(2i32))];
        let err = analyze_kv_filters(&filters, &pk).unwrap_err();
        assert!(matches!(err, FlussDatafusionError::UnsupportedQueryPattern(_)));
    }

    #[test]
    fn in_list_is_rejected() {
        let pk = pks(&["id"]);
        let filters = vec![col("id").in_list(vec![lit(1i32), lit(2i32)], false)];
        let err = analyze_kv_filters(&filters, &pk).unwrap_err();
        assert!(matches!(err, FlussDatafusionError::UnsupportedQueryPattern(_)));
    }

    #[test]
    fn column_vs_column_equality_is_rejected() {
        let pk = pks(&["id"]);
        let filters = vec![col("id").eq(col("name"))];
        let err = analyze_kv_filters(&filters, &pk).unwrap_err();
        assert!(matches!(err, FlussDatafusionError::UnsupportedQueryPattern(_)));
    }

    #[test]
    fn duplicate_key_predicate_is_rejected() {
        let pk = pks(&["id"]);
        let filters = vec![col("id").eq(lit(1i32)), col("id").eq(lit(2i32))];
        let err = analyze_kv_filters(&filters, &pk).unwrap_err();
        assert!(matches!(err, FlussDatafusionError::UnsupportedQueryPattern(_)));
    }

    #[test]
    fn extra_non_key_predicate_is_rejected() {
        let pk = pks(&["id"]);
        let filters = vec![col("id").eq(lit(1i32)), col("name").eq(lit("x"))];
        let err = analyze_kv_filters(&filters, &pk).unwrap_err();
        assert!(matches!(err, FlussDatafusionError::UnsupportedQueryPattern(_)));
    }

    #[test]
    fn null_literal_is_rejected() {
        let pk = pks(&["id"]);
        let filters = vec![col("id").eq(Expr::Literal(
            datafusion::scalar::ScalarValue::Int32(None),
            None,
        ))];
        let err = analyze_kv_filters(&filters, &pk).unwrap_err();
        assert!(matches!(err, FlussDatafusionError::TypeConversion(_)));
    }

    #[test]
    fn is_primary_key_equality_classifies_filters() {
        let pk = pks(&["id"]);
        assert!(is_primary_key_equality(&col("id").eq(lit(1i32)), &pk));
        assert!(!is_primary_key_equality(&col("name").eq(lit("x")), &pk));
        assert!(!is_primary_key_equality(&col("id").gt(lit(1i32)), &pk));
    }

    #[test]
    fn partition_equality_binding_is_extracted() {
        let parts = pks(&["region"]);
        let filters = vec![col("region").eq(lit("US"))];
        let bindings = analyze_partition_filters(&filters, &parts);
        assert_eq!(bindings.get("region"), Some(&"US".to_string()));
        assert_eq!(bindings.len(), 1);
    }

    #[test]
    fn non_partition_column_yields_no_binding() {
        let parts = pks(&["region"]);
        let filters = vec![col("name").eq(lit("x"))];
        assert!(analyze_partition_filters(&filters, &parts).is_empty());
    }

    #[test]
    fn range_on_partition_column_yields_no_binding() {
        let parts = pks(&["region"]);
        let filters = vec![col("region").gt(lit("US"))];
        assert!(analyze_partition_filters(&filters, &parts).is_empty());
    }

    #[test]
    fn is_partition_equality_classifies_filters() {
        let parts = pks(&["region"]);
        assert!(is_partition_equality(&col("region").eq(lit("US")), &parts));
        assert!(!is_partition_equality(&col("name").eq(lit("x")), &parts));
        assert!(!is_partition_equality(&col("region").gt(lit("US")), &parts));
    }
}
