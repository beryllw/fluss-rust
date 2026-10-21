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

//! Lake-enabled `TableProvider`, backed entirely by the `fluss-lake` kernel.
//!
//! This provider is a thin adapter: it constructs a [`FlussLakeTable`], plans the
//! read, and hands the plan + reader to a [`FlussLakeScanExec`]. All lake+log
//! union semantics (seam resolution, lake planning, append stitch, PK merge) live
//! inside `fluss-lake`; the connector never sees seams, lake splits, or merge
//! state.
//!
//! Value-filter pushdown: filters that translate to a `FlussLakePredicate` are
//! reported `Exact` and pushed into the scan (the kernel applies them exactly);
//! anything that does not translate stays `Unsupported` (residual `FilterExec`).

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{BinaryExpr, Operator, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use fluss_lake::{FlussLakeComparisonOp, FlussLakeLiteral, FlussLakePredicate, FlussLakeTable};

use crate::backend::{SharedFlussSource, TableRef};
use crate::error::FlussDatafusionError;
use crate::execution::lookup::FlussKvLookupExec;
use crate::execution::prefix_lookup::FlussKvPrefixLookupExec;
use crate::execution::lake_scan::FlussLakeScanExec;
use crate::table::predicate::{
    analyze_kv_filters, analyze_kv_prefix_filters, is_complete_prefix_key_equality,
    is_complete_primary_key_equality, is_prefix_key_equality, is_primary_key_equality,
};
use crate::types::record_batch::normalize_projection;

use arrow::datatypes::SchemaRef;

/// A lake-enabled Fluss table (`table.datalake.format = paimon`), append/log or
/// primary-key. The kernel selects append stitch vs PK merge internally.
///
/// `lake_only` selects the `<table>$lake` read shape: only the Paimon lake
/// snapshot (current state) is returned, with no Fluss log tail union.
///
/// For a primary-key lake table, an exact primary-key equality (point lookup) or
/// a complete bucket-key prefix equality (prefix lookup) is served directly from
/// Fluss — the KV store holds the current state, so the lake is not needed and a
/// full union read is avoided. Everything else (full scan, `LIMIT`, other
/// predicates) falls through to the lake+log union read. `lake_only` skips this
/// routing (it must read the Paimon snapshot regardless).
pub(crate) struct FlussLakeTableProvider {
    source: SharedFlussSource,
    table_ref: TableRef,
    schema: SchemaRef,
    fluss_schema: fluss::metadata::Schema,
    num_buckets: i32,
    partition_keys: Vec<String>,
    /// Primary-key column names; empty for an append/log lake table. Drives the
    /// point-lookup fast path.
    primary_keys: Vec<String>,
    /// Bucket-key column names; a strict prefix of the primary key enables the
    /// prefix-lookup fast path.
    bucket_keys: Vec<String>,
    lake_catalog_properties: std::collections::HashMap<String, String>,
    lake_only: bool,
}

impl std::fmt::Debug for FlussLakeTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussLakeTableProvider")
            .field("table_ref", &self.table_ref)
            .finish()
    }
}

impl FlussLakeTableProvider {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        source: SharedFlussSource,
        table_ref: TableRef,
        schema: SchemaRef,
        fluss_schema: fluss::metadata::Schema,
        num_buckets: i32,
        partition_keys: Vec<String>,
        primary_keys: Vec<String>,
        bucket_keys: Vec<String>,
        lake_catalog_properties: std::collections::HashMap<String, String>,
    ) -> Self {
        Self::with_lake_only(
            source,
            table_ref,
            schema,
            fluss_schema,
            num_buckets,
            partition_keys,
            primary_keys,
            bucket_keys,
            lake_catalog_properties,
            false,
        )
    }

    /// Builds the provider in lake-only mode (`<table>$lake`): only the Paimon
    /// lake snapshot is read, with no Fluss log tail union (and no lookup fast
    /// path).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_lake_only(
        source: SharedFlussSource,
        table_ref: TableRef,
        schema: SchemaRef,
        fluss_schema: fluss::metadata::Schema,
        num_buckets: i32,
        partition_keys: Vec<String>,
        primary_keys: Vec<String>,
        bucket_keys: Vec<String>,
        lake_catalog_properties: std::collections::HashMap<String, String>,
        lake_only: bool,
    ) -> Self {
        Self {
            source,
            table_ref,
            schema,
            fluss_schema,
            num_buckets,
            partition_keys,
            primary_keys,
            bucket_keys,
            lake_catalog_properties,
            lake_only,
        }
    }

    /// Whether the point/prefix-lookup fast path applies to this provider: a
    /// primary-key table read in normal (not lake-only) mode.
    fn lookup_fast_path_enabled(&self) -> bool {
        !self.lake_only && !self.primary_keys.is_empty()
    }
}

#[async_trait]
impl TableProvider for FlussLakeTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        // For a primary-key lake table, an exact PK / bucket-key-prefix equality
        // is served by a point/prefix lookup (the fast path in `scan`), which
        // consumes exactly those conjuncts — mark them `Exact` so the planner
        // drops the matching `FilterExec`, mirroring the non-lake KV provider.
        if self.lookup_fast_path_enabled() {
            if is_complete_primary_key_equality(filters, &self.primary_keys) {
                return Ok(filters
                    .iter()
                    .map(|f| {
                        if is_primary_key_equality(f, &self.primary_keys) {
                            TableProviderFilterPushDown::Exact
                        } else {
                            TableProviderFilterPushDown::Unsupported
                        }
                    })
                    .collect());
            }
            if is_complete_prefix_key_equality(
                filters,
                &self.bucket_keys,
                &self.partition_keys,
                &self.primary_keys,
            ) {
                return Ok(filters
                    .iter()
                    .map(|f| {
                        if is_prefix_key_equality(f, &self.bucket_keys, &self.partition_keys) {
                            TableProviderFilterPushDown::Exact
                        } else {
                            TableProviderFilterPushDown::Unsupported
                        }
                    })
                    .collect());
            }
        }
        // Otherwise the union read runs; the kernel applies pushed predicates
        // exactly, so any filter we can translate to a FlussLakePredicate is
        // `Exact` (no residual). Filters we cannot translate stay `Unsupported`
        // and are re-applied by a `FilterExec` above the scan.
        Ok(filters
            .iter()
            .map(|f| {
                if expr_to_lake_predicate(f).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let (projection, projected_schema) = normalize_projection(projection, &self.schema)?;

        // Fast path for a primary-key lake table: an exact PK equality is a point
        // lookup and a complete bucket-key prefix equality is a prefix lookup,
        // both served directly from Fluss (the KV store holds the current state),
        // so the lake is not opened and a full union read is avoided.
        if self.lookup_fast_path_enabled() {
            if let Ok(key) = analyze_kv_filters(filters, &self.primary_keys) {
                return Ok(Arc::new(FlussKvLookupExec::new(
                    self.source.clone(),
                    self.table_ref.clone(),
                    key,
                    projected_schema.clone(),
                    projection.clone(),
                )));
            }
            if let Some(prefix) = analyze_kv_prefix_filters(
                filters,
                &self.bucket_keys,
                &self.partition_keys,
                &self.primary_keys,
            ) {
                return Ok(Arc::new(FlussKvPrefixLookupExec::new(
                    self.source.clone(),
                    self.table_ref.clone(),
                    prefix.lookup_columns,
                    prefix.key,
                    projected_schema.clone(),
                    projection.clone(),
                )));
            }
        }

        if self.num_buckets == 0 {
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        let real = self
            .source
            .as_any()
            .downcast_ref::<crate::backend::real::RealFlussSource>()
            .ok_or_else(|| {
                FlussDatafusionError::Internal(
                    "lake union read requires the production RealFlussSource".to_string(),
                )
            })?;
        let connection = real.connection();
        let table_path: fluss::metadata::TablePath = (&self.table_ref).into();

        // Merge caller-supplied lake storage options (e.g. S3 credentials the
        // server strips from table properties) over the server-derived catalog
        // properties, caller-wins, just before opening the Paimon catalog. This
        // mirrors how the Flink Fluss catalog forwards its `paimon.*` options.
        let lake_catalog_properties =
            merged_lake_properties(&self.lake_catalog_properties, real.lake_storage_options());

        // The kernel owns everything: seam resolution, lake planning, append
        // stitch / PK merge. We only build the scan, plan it, and create a reader.
        let lake_table = FlussLakeTable::new(
            connection,
            table_path,
            self.fluss_schema.clone(),
            self.num_buckets,
            self.partition_keys.clone(),
            lake_catalog_properties,
        );
        let mut scan = lake_table.new_scan().with_lake_only(self.lake_only);
        if let Some(indices) = projection {
            scan = scan.with_projection(indices);
        }
        if let Some(predicate) = filters_to_lake_predicate(filters) {
            scan = scan.with_filter(predicate);
        }
        let plan = scan
            .plan()
            .await
            .map_err(|e| FlussDatafusionError::Internal(e.to_string()))?;
        let read = scan
            .new_read()
            .map_err(|e| FlussDatafusionError::Internal(e.to_string()))?;

        Ok(Arc::new(FlussLakeScanExec::new(plan, read)))
    }
}

/// Merges caller-supplied lake storage options over server-derived catalog
/// properties, **caller-wins** (the caller may both add missing keys like
/// credentials and override server values like the S3 endpoint/region). Returns
/// the server properties unchanged when the caller supplies none.
fn merged_lake_properties(
    server: &std::collections::HashMap<String, String>,
    caller: &std::collections::HashMap<String, String>,
) -> std::collections::HashMap<String, String> {
    if caller.is_empty() {
        return server.clone();
    }
    let mut merged = server.clone();
    for (k, v) in caller {
        merged.insert(k.clone(), v.clone());
    }
    merged
}

/// Combines the DataFusion filters into a single `FlussLakePredicate` (top-level
/// `AND`), translating each conjunct. Untranslatable conjuncts are dropped here
/// but are reported `Unsupported` in `supports_filters_pushdown`, so they remain
/// residual `FilterExec`s and are never silently lost.
fn filters_to_lake_predicate(filters: &[Expr]) -> Option<FlussLakePredicate> {
    let mut conjuncts: Vec<FlussLakePredicate> = filters
        .iter()
        .filter_map(expr_to_lake_predicate)
        .collect();
    match conjuncts.len() {
        0 => None,
        1 => Some(conjuncts.pop().unwrap()),
        _ => Some(FlussLakePredicate::and(conjuncts)),
    }
}

/// Translates a DataFusion [`Expr`] into a [`FlussLakePredicate`], or `None` when
/// the shape is not supported (the caller keeps it as a residual filter).
fn expr_to_lake_predicate(expr: &Expr) -> Option<FlussLakePredicate> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => Some(FlussLakePredicate::and(vec![
                expr_to_lake_predicate(left)?,
                expr_to_lake_predicate(right)?,
            ])),
            Operator::Or => Some(FlussLakePredicate::or(vec![
                expr_to_lake_predicate(left)?,
                expr_to_lake_predicate(right)?,
            ])),
            Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt
            | Operator::GtEq => {
                let (column, scalar, flipped) = column_and_literal(left, right)?;
                let op = map_comparison_op(*op, flipped)?;
                Some(FlussLakePredicate::Comparison {
                    column,
                    op,
                    value: scalar_to_literal(&scalar)?,
                })
            }
            _ => None,
        },
        Expr::IsNull(inner) => Some(FlussLakePredicate::is_null(as_column(inner)?)),
        Expr::IsNotNull(inner) => Some(FlussLakePredicate::is_not_null(as_column(inner)?)),
        Expr::InList(in_list) => {
            let column = as_column(&in_list.expr)?;
            let mut values = Vec::with_capacity(in_list.list.len());
            for item in &in_list.list {
                values.push(scalar_to_literal(&as_literal(item)?)?);
            }
            Some(FlussLakePredicate::In {
                column,
                values,
                negated: in_list.negated,
            })
        }
        _ => None,
    }
}

/// Extracts `(column_name, literal, flipped)` from a binary comparison's two
/// sides, accepting either `col OP lit` (flipped=false) or `lit OP col`
/// (flipped=true).
fn column_and_literal(left: &Expr, right: &Expr) -> Option<(String, ScalarValue, bool)> {
    match (left, right) {
        (Expr::Column(col), Expr::Literal(scalar, _)) => {
            Some((col.name.clone(), scalar.clone(), false))
        }
        (Expr::Literal(scalar, _), Expr::Column(col)) => {
            Some((col.name.clone(), scalar.clone(), true))
        }
        _ => None,
    }
}

fn as_column(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        _ => None,
    }
}

fn as_literal(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(scalar, _) => Some(scalar.clone()),
        _ => None,
    }
}

/// Maps a DataFusion comparison operator to a `FlussLakeComparisonOp`, flipping
/// the direction when the column and literal were swapped (`lit OP col`).
fn map_comparison_op(op: Operator, flipped: bool) -> Option<FlussLakeComparisonOp> {
    Some(match (op, flipped) {
        (Operator::Eq, _) => FlussLakeComparisonOp::Eq,
        (Operator::NotEq, _) => FlussLakeComparisonOp::NotEq,
        (Operator::Lt, false) | (Operator::Gt, true) => FlussLakeComparisonOp::Lt,
        (Operator::LtEq, false) | (Operator::GtEq, true) => FlussLakeComparisonOp::LtEq,
        (Operator::Gt, false) | (Operator::Lt, true) => FlussLakeComparisonOp::Gt,
        (Operator::GtEq, false) | (Operator::LtEq, true) => FlussLakeComparisonOp::GtEq,
        _ => return None,
    })
}

/// Translates a DataFusion [`ScalarValue`] into a [`FlussLakeLiteral`], or `None`
/// for types the kernel filter does not model.
fn scalar_to_literal(scalar: &ScalarValue) -> Option<FlussLakeLiteral> {
    Some(match scalar {
        ScalarValue::Boolean(Some(v)) => FlussLakeLiteral::Boolean(*v),
        ScalarValue::Int8(Some(v)) => FlussLakeLiteral::Int32(*v as i32),
        ScalarValue::Int16(Some(v)) => FlussLakeLiteral::Int32(*v as i32),
        ScalarValue::Int32(Some(v)) => FlussLakeLiteral::Int32(*v),
        ScalarValue::Int64(Some(v)) => FlussLakeLiteral::Int64(*v),
        ScalarValue::Float32(Some(v)) => FlussLakeLiteral::Float64(*v as f64),
        ScalarValue::Float64(Some(v)) => FlussLakeLiteral::Float64(*v),
        ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
            FlussLakeLiteral::String(v.clone())
        }
        ScalarValue::Binary(Some(v)) | ScalarValue::LargeBinary(Some(v)) => {
            FlussLakeLiteral::Binary(v.clone())
        }
        ScalarValue::Date32(Some(v)) => FlussLakeLiteral::Date(*v),
        ScalarValue::TimestampMicrosecond(Some(v), _) => FlussLakeLiteral::Timestamp(*v),
        ScalarValue::Boolean(None)
        | ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Utf8(None)
        | ScalarValue::LargeUtf8(None)
        | ScalarValue::Binary(None)
        | ScalarValue::LargeBinary(None)
        | ScalarValue::Date32(None)
        | ScalarValue::TimestampMicrosecond(None, _) => FlussLakeLiteral::Null,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use datafusion::prelude::{col, lit};

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn caller_adds_missing_credentials_and_keeps_server_values() {
        let server = map(&[("warehouse", "s3://b/wh"), ("s3.endpoint", "http://rustfs:9000")]);
        let caller = map(&[("s3.access-key", "AK"), ("s3.secret-key", "SK")]);
        let merged = merged_lake_properties(&server, &caller);
        assert_eq!(merged.get("warehouse").unwrap(), "s3://b/wh");
        assert_eq!(merged.get("s3.endpoint").unwrap(), "http://rustfs:9000");
        assert_eq!(merged.get("s3.access-key").unwrap(), "AK");
        assert_eq!(merged.get("s3.secret-key").unwrap(), "SK");
    }

    #[test]
    fn caller_wins_on_key_conflicts() {
        let server = map(&[("s3.endpoint", "http://rustfs:9000"), ("s3.region", "us-east-1")]);
        let caller = map(&[("s3.endpoint", "http://localhost:1234")]);
        let merged = merged_lake_properties(&server, &caller);
        assert_eq!(merged.get("s3.endpoint").unwrap(), "http://localhost:1234");
        assert_eq!(merged.get("s3.region").unwrap(), "us-east-1");
    }

    #[test]
    fn empty_caller_returns_server_unchanged() {
        let server = map(&[("warehouse", "s3://b/wh")]);
        let merged = merged_lake_properties(&server, &HashMap::new());
        assert_eq!(merged, server);
    }

    #[test]
    fn translates_column_op_literal() {
        let pred = expr_to_lake_predicate(&col("age").gt(lit(18i32))).unwrap();
        assert_eq!(
            pred,
            FlussLakePredicate::Comparison {
                column: "age".to_string(),
                op: FlussLakeComparisonOp::Gt,
                value: FlussLakeLiteral::Int32(18),
            }
        );
    }

    #[test]
    fn flips_literal_op_column() {
        // `18 < age` is equivalent to `age > 18`.
        let expr = lit(18i32).lt(col("age"));
        let pred = expr_to_lake_predicate(&expr).unwrap();
        assert_eq!(
            pred,
            FlussLakePredicate::Comparison {
                column: "age".to_string(),
                op: FlussLakeComparisonOp::Gt,
                value: FlussLakeLiteral::Int32(18),
            }
        );
    }

    #[test]
    fn translates_and_of_conjuncts() {
        let filters = vec![
            col("dt").eq(lit("2024")),
            col("age").gt_eq(lit(18i32)),
        ];
        let pred = filters_to_lake_predicate(&filters).unwrap();
        match pred {
            FlussLakePredicate::And(children) => assert_eq!(children.len(), 2),
            other => panic!("expected And, got {other:?}"),
        }
    }

    #[test]
    fn untranslatable_expr_returns_none() {
        // Column-vs-column comparison is not a supported pushdown shape.
        assert!(expr_to_lake_predicate(&col("a").eq(col("b"))).is_none());
    }
}
