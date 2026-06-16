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

//! KV `TableProvider`.
//!
//! Surfaces a primary-keyed Fluss table to DataFusion with two read shapes, in
//! precedence order:
//!
//! 1. **Point lookup** — a COMPLETE primary-key equality (`pk1 = v AND ...`)
//!    resolves a single row via [`FlussKvLookupExec`].
//! 2. **Bounded scan** — any other predicate shape combined with a `LIMIT` runs a
//!    bounded scan (the same machinery the log table uses), reading each bucket's
//!    last-`limit` rows; DataFusion layers the final cross-target `LIMIT`. For a
//!    partitioned table, partition-column equality prunes the scanned partitions.
//!
//! Anything else (no complete PK equality AND no `LIMIT`) returns a clear
//! `UnsupportedQueryPattern` rather than degrading into an unbounded full scan.
//!
//! `supports_filters_pushdown` is precedence-aware so it is sound: a filter is
//! `Exact` (consumed, no residual `FilterExec`) ONLY when the full set forms a
//! complete PK equality that the point lookup will actually apply. Otherwise the
//! bounded-scan path runs, which does NOT apply arbitrary filters, so partition
//! equality is only `Inexact` (drives pruning, residual `FilterExec` re-applies)
//! and everything else is `Unsupported`.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use crate::backend::{SharedFlussSource, TableRef};
use crate::error::FlussDatafusionError;
use crate::execution::log_scan::FlussLogScanExec;
use crate::execution::lookup::FlussKvLookupExec;
use crate::table::predicate::{
    analyze_kv_filters, analyze_partition_filters, is_complete_primary_key_equality,
    is_partition_equality, is_primary_key_equality,
};
use crate::types::record_batch::normalize_projection;

/// `EXPLAIN`/`name()` label for the KV bounded-scan plan, distinct from the log
/// table's `FlussLogScanExec` so the two read shapes are distinguishable.
const KV_SCAN_PLAN_NAME: &str = "FlussKvScanExec";

/// A primary-keyed Fluss table backed by a point lookup or a bounded `LIMIT` scan.
pub(crate) struct FlussKvTableProvider {
    source: SharedFlussSource,
    table_ref: TableRef,
    schema: SchemaRef,
    primary_keys: Vec<String>,
    /// Bucket count = number of bounded-scan partitions read in parallel (per
    /// partition).
    num_buckets: i32,
    /// Partition-key column names; empty when the table is not partitioned.
    partition_keys: Vec<String>,
}

impl std::fmt::Debug for FlussKvTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussKvTableProvider")
            .field("table_ref", &self.table_ref)
            .field("primary_keys", &self.primary_keys)
            .finish()
    }
}

impl FlussKvTableProvider {
    pub(crate) fn new(
        source: SharedFlussSource,
        table_ref: TableRef,
        schema: SchemaRef,
        primary_keys: Vec<String>,
        num_buckets: i32,
        partition_keys: Vec<String>,
    ) -> Self {
        Self {
            source,
            table_ref,
            schema,
            primary_keys,
            num_buckets,
            partition_keys,
        }
    }
}

#[async_trait]
impl TableProvider for FlussKvTableProvider {
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
        // Precedence-aware and sound: only mark a filter `Exact` (planner drops the
        // matching `FilterExec`) when the point lookup will actually apply it.
        //
        // If the FULL set forms a complete PK equality, a point lookup runs and
        // consumes exactly the PK-equality conjuncts; mark those `Exact`, the rest
        // `Unsupported`. Otherwise the bounded-scan path runs, which does NOT apply
        // arbitrary filters — so nothing may be `Exact`. There, partition equality
        // is `Inexact` (drives pruning; the residual `FilterExec` re-applies it),
        // and everything else (including partial PK equality) is `Unsupported`.
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

        // Bounded-scan path. A non-partitioned KV table supports no pushdown.
        if self.partition_keys.is_empty() {
            return Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect());
        }
        // Partitioned: partition-column equality drives pruning, reported `Inexact`
        // so DataFusion still layers a `FilterExec` re-applying the predicate.
        Ok(filters
            .iter()
            .map(|f| {
                if is_partition_equality(f, &self.partition_keys) {
                    TableProviderFilterPushDown::Inexact
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
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let (projection, projected_schema) = normalize_projection(projection, &self.schema)?;

        // 1. A COMPLETE primary-key equality => point lookup. `analyze_kv_filters`'s
        //    `Err` is the "not a full PK" signal; do NOT propagate it here, fall
        //    through to the bounded-scan / error branches instead.
        if let Ok(key) = analyze_kv_filters(filters, &self.primary_keys) {
            return Ok(Arc::new(FlussKvLookupExec::new(
                self.source.clone(),
                self.table_ref.clone(),
                key,
                projected_schema,
                projection,
            )));
        }

        // 2. Any other predicate shape + a `LIMIT` => bounded KV scan, computing
        //    targets exactly like the log table.
        let Some(limit) = limit else {
            // 3. Neither a complete PK equality nor a `LIMIT`: refuse clearly rather
            //    than falling back to an unbounded full scan.
            return Err(DataFusionError::from(
                FlussDatafusionError::UnsupportedQueryPattern(format!(
                    "KV table {} requires either a full primary-key equality (point lookup) \
                     or a LIMIT (bounded scan); an unbounded scan is not supported",
                    self.table_ref
                )),
            ));
        };

        // Compute the scan targets: one (partition_id, bucket) per DataFusion
        // output partition (mirrors `FlussLogTableProvider::scan`).
        let buckets = 0..self.num_buckets;
        let targets: Vec<(Option<i64>, i32)> = if self.partition_keys.is_empty() {
            // Non-partitioned: one target per bucket, no partition id.
            buckets.map(|b| (None, b)).collect()
        } else {
            // Partitioned: list partitions, keep those matching every equality
            // binding (no bindings => keep all), then cross with the buckets.
            let partitions = self.source.list_partitions(&self.table_ref).await?;
            let bindings = analyze_partition_filters(filters, &self.partition_keys);
            partitions
                .into_iter()
                .filter(|partition| {
                    bindings.iter().all(|(k, v)| {
                        partition
                            .values
                            .iter()
                            .any(|(pk, pv)| pk == k && pv == v)
                    })
                })
                .flat_map(|partition| {
                    buckets
                        .clone()
                        .map(move |b| (Some(partition.partition_id), b))
                })
                .collect()
        };

        // No target (partitioned table with zero partitions, or none matched the
        // predicate) means zero rows: return an `EmptyExec` with the projected
        // schema rather than a 0-partition plan.
        if targets.is_empty() {
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        // Per-target pushdown of `limit` returns each bucket's last-`limit` rows;
        // DataFusion layers a global `LIMIT` above this multi-partition scan.
        Ok(Arc::new(FlussLogScanExec::new(
            self.source.clone(),
            self.table_ref.clone(),
            projection,
            limit,
            targets,
            projected_schema,
            KV_SCAN_PLAN_NAME,
        )))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::RecordBatch;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::logical_expr::TableProviderFilterPushDown as PD;
    use datafusion::prelude::{col, lit, Expr};

    use super::*;
    use crate::backend::{FlussPartition, FlussSource, FlussTableMeta, LookupKey};
    use crate::error::Result as CrateResult;

    /// A do-nothing source: `supports_filters_pushdown` is sync and never touches
    /// the source, so these unit tests need no real Fluss access.
    struct StubSource;

    #[async_trait]
    impl FlussSource for StubSource {
        async fn list_databases(&self) -> CrateResult<Vec<String>> {
            Ok(vec![])
        }
        async fn list_tables(&self, _database: &str) -> CrateResult<Vec<String>> {
            Ok(vec![])
        }
        async fn get_table_meta(&self, _table: &TableRef) -> CrateResult<FlussTableMeta> {
            unreachable!("not exercised by pushdown unit tests")
        }
        async fn lookup(&self, _table: &TableRef, _key: &LookupKey) -> CrateResult<RecordBatch> {
            unreachable!("not exercised by pushdown unit tests")
        }
        async fn list_partitions(&self, _table: &TableRef) -> CrateResult<Vec<FlussPartition>> {
            Ok(vec![])
        }
        async fn bounded_scan(
            &self,
            _table: &TableRef,
            _partition_id: Option<i64>,
            _bucket: i32,
            _projection: Option<&[usize]>,
            _limit: usize,
        ) -> CrateResult<Vec<RecordBatch>> {
            unreachable!("not exercised by pushdown unit tests")
        }
    }

    fn pks(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn schema(cols: &[&str]) -> SchemaRef {
        Arc::new(ArrowSchema::new(
            cols.iter()
                .map(|c| Field::new(*c, DataType::Int32, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn provider(primary_keys: Vec<String>, partition_keys: Vec<String>) -> FlussKvTableProvider {
        // Schema is irrelevant to `supports_filters_pushdown`; supply a minimal one.
        FlussKvTableProvider::new(
            Arc::new(StubSource),
            TableRef::new("db", "t"),
            schema(&["a", "b", "c"]),
            primary_keys,
            1,
            partition_keys,
        )
    }

    fn pushdown(provider: &FlussKvTableProvider, filters: &[Expr]) -> Vec<PD> {
        let refs: Vec<&Expr> = filters.iter().collect();
        provider.supports_filters_pushdown(&refs).unwrap()
    }

    #[test]
    fn complete_single_pk_equality_is_exact() {
        let p = provider(pks(&["id"]), vec![]);
        let filters = vec![col("id").eq(lit(1i32))];
        assert_eq!(pushdown(&p, &filters), vec![PD::Exact]);
    }

    #[test]
    fn complete_composite_pk_equality_marks_each_pk_filter_exact() {
        let p = provider(pks(&["region", "id"]), vec![]);
        let filters = vec![col("region").eq(lit("us")), col("id").eq(lit(2i32))];
        assert_eq!(pushdown(&p, &filters), vec![PD::Exact, PD::Exact]);
    }

    #[test]
    fn complete_pk_equality_with_extra_non_pk_filter_keeps_extra_unsupported() {
        // The full set is NOT a complete PK equality (the extra `name` conjunct
        // breaks it), so this is the bounded-scan path: nothing is `Exact`.
        let p = provider(pks(&["id"]), vec![]);
        let filters = vec![col("id").eq(lit(1i32)), col("name").eq(lit("x"))];
        assert_eq!(pushdown(&p, &filters), vec![PD::Unsupported, PD::Unsupported]);
    }

    #[test]
    fn partial_composite_pk_is_unsupported_not_exact() {
        // Soundness guard: a partial PK equality + LIMIT must NOT be `Exact`,
        // otherwise the planner would drop the `FilterExec` the bounded scan
        // cannot apply, returning wrong rows.
        let p = provider(pks(&["region", "id"]), vec![]);
        let filters = vec![col("region").eq(lit("us"))];
        assert_eq!(pushdown(&p, &filters), vec![PD::Unsupported]);
    }

    #[test]
    fn non_pk_filter_on_nonpartitioned_table_is_unsupported() {
        let p = provider(pks(&["id"]), vec![]);
        let filters = vec![col("name").eq(lit("x"))];
        assert_eq!(pushdown(&p, &filters), vec![PD::Unsupported]);
    }

    #[test]
    fn partition_equality_on_partitioned_table_is_inexact() {
        // Bounded-scan path on a partitioned table: partition equality drives
        // pruning and is reported `Inexact` (residual FilterExec re-applies it),
        // while a non-PK/non-partition filter stays `Unsupported`.
        let p = provider(pks(&["region", "id"]), pks(&["region"]));
        let filters = vec![col("region").eq(lit("us")), col("name").eq(lit("x"))];
        assert_eq!(pushdown(&p, &filters), vec![PD::Inexact, PD::Unsupported]);
    }

    #[test]
    fn complete_pk_on_partitioned_table_still_exact() {
        // When the full set is a complete PK equality, the point-lookup path wins
        // even on a partitioned table: PK-equality conjuncts are `Exact`.
        let p = provider(pks(&["region", "id"]), pks(&["region"]));
        let filters = vec![col("region").eq(lit("us")), col("id").eq(lit(2i32))];
        assert_eq!(pushdown(&p, &filters), vec![PD::Exact, PD::Exact]);
    }
}
