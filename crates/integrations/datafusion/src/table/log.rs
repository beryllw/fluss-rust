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

//! Log `TableProvider`.
//!
//! Surfaces an append-only Fluss table to DataFusion via a bounded scan. Phase 1
//! requires a `LIMIT`: `scan` fails clearly with `LimitRequired` when none is
//! present rather than degrading into a full scan. Projection is pushed down to
//! [`FlussSource::log_scan`].
//!
//! Partition pruning (equality-only): for a partitioned table, a
//! `partition_col = 'value'` filter is reported as `Inexact` pushdown, partitions
//! are listed and filtered to those matching every binding, and the scan targets
//! become the cross product of the kept partitions and the buckets. Pruning is
//! best-effort and never required: with no partition predicate the scan reads all
//! partitions, and the `Inexact` pushdown keeps a residual `FilterExec` above the
//! scan so correctness never depends on the pruning. A non-partitioned table
//! supports no filter pushdown (every filter is `Unsupported`).

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
use crate::table::predicate::{analyze_partition_filters, is_partition_equality};

/// An append-only Fluss table backed by a required-`LIMIT` bounded scan.
pub(crate) struct FlussLogTableProvider {
    source: SharedFlussSource,
    table_ref: TableRef,
    schema: SchemaRef,
    /// Bucket count = number of scan partitions read in parallel (per partition).
    num_buckets: i32,
    /// Partition-key column names; empty when the table is not partitioned.
    partition_keys: Vec<String>,
}

impl std::fmt::Debug for FlussLogTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussLogTableProvider")
            .field("table_ref", &self.table_ref)
            .finish()
    }
}

impl FlussLogTableProvider {
    pub(crate) fn new(
        source: SharedFlussSource,
        table_ref: TableRef,
        schema: SchemaRef,
        num_buckets: i32,
        partition_keys: Vec<String>,
    ) -> Self {
        Self {
            source,
            table_ref,
            schema,
            num_buckets,
            partition_keys,
        }
    }
}

#[async_trait]
impl TableProvider for FlussLogTableProvider {
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
        // A non-partitioned log table supports no filter pushdown; residual filters
        // become a `FilterExec` above the scan.
        if self.partition_keys.is_empty() {
            return Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect());
        }
        // For a partitioned table, partition-column equality drives pruning. Report
        // it as `Inexact` (not `Exact`) so DataFusion still layers a `FilterExec`
        // that re-applies the predicate: pruning is best-effort and this keeps the
        // result correct even if string-matching is imperfect.
        Ok(filters
            .iter()
            .map(|filter| {
                if is_partition_equality(filter, &self.partition_keys) {
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
        // A bounded scan requires a `LIMIT`; fail clearly rather than full-scan.
        let limit = limit.ok_or_else(|| {
            DataFusionError::from(FlussDatafusionError::LimitRequired(format!(
                "log table {} requires a LIMIT for bounded scan",
                self.table_ref
            )))
        })?;

        // Normalize a full identity projection (e.g. `SELECT *` may arrive as
        // `Some([0,1,..])`) to `None` so the bounded scan reads all columns.
        let full_count = self.schema.fields().len();
        let projection = match projection {
            Some(indices) if indices.iter().copied().eq(0..full_count) => None,
            other => other.cloned(),
        };
        let projected_schema = match &projection {
            None => self.schema.clone(),
            Some(indices) => Arc::new(self.schema.project(indices)?),
        };

        // Compute the scan targets: one (partition_id, bucket) per DataFusion
        // output partition.
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

        // No target (partitioned table with zero partitions, or no partition
        // matched the predicate) means zero rows: return an `EmptyExec` with the
        // projected schema rather than a 0-partition plan.
        if targets.is_empty() {
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        // Per-target pushdown of `limit` returns each bucket's last-`limit` rows;
        // DataFusion still layers a global `LIMIT` above this multi-partition scan,
        // so the merged result is capped at exactly `limit` rows.
        Ok(Arc::new(FlussLogScanExec::new(
            self.source.clone(),
            self.table_ref.clone(),
            projection,
            limit,
            targets,
            projected_schema,
        )))
    }
}
