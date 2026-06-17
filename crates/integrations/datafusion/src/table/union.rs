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

//! Lake-enabled append/log `TableProvider`.
//!
//! This provider reuses the existing Fluss metadata / partition-pruning logic to
//! compute the `(partition_id, bucket)` scan targets, then delegates execution to
//! the `fluss-lake` kernel (`plan_append_union` + `open_append_partition`).
//!
//! Phase M2 scope is intentionally narrow:
//! - only **append/log** tables (`!has_primary_key()`), because PK cross-source
//!   merge is an M3 concern in the kernel;
//! - no value-filter pushdown yet — partition equality remains `Inexact` for
//!   best-effort pruning, everything else is `Unsupported`, keeping residual
//!   `FilterExec`s above the scan.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use fluss_lake::{LakeSeam, plan_append_union};

use crate::backend::{SharedFlussSource, TableRef};
use crate::error::FlussDatafusionError;
use crate::execution::union_scan::FlussUnionScanExec;
use crate::table::predicate::{analyze_partition_filters, is_partition_equality};
use crate::types::record_batch::normalize_projection;

use arrow::datatypes::SchemaRef;

/// A Fluss append/log table with lakehouse storage enabled (`table.datalake.format = paimon`).
pub(crate) struct FlussUnionTableProvider {
    source: SharedFlussSource,
    table_ref: TableRef,
    schema: SchemaRef,
    fluss_schema: fluss::metadata::Schema,
    num_buckets: i32,
    partition_keys: Vec<String>,
    lake_catalog_properties: std::collections::HashMap<String, String>,
}

impl std::fmt::Debug for FlussUnionTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussUnionTableProvider")
            .field("table_ref", &self.table_ref)
            .finish()
    }
}

impl FlussUnionTableProvider {
    pub(crate) fn new(
        source: SharedFlussSource,
        table_ref: TableRef,
        schema: SchemaRef,
        fluss_schema: fluss::metadata::Schema,
        num_buckets: i32,
        partition_keys: Vec<String>,
        lake_catalog_properties: std::collections::HashMap<String, String>,
    ) -> Self {
        Self {
            source,
            table_ref,
            schema,
            fluss_schema,
            num_buckets,
            partition_keys,
            lake_catalog_properties,
        }
    }
}

#[async_trait]
impl TableProvider for FlussUnionTableProvider {
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
        if self.partition_keys.is_empty() {
            return Ok(filters
                .iter()
                .map(|_| TableProviderFilterPushDown::Unsupported)
                .collect());
        }
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
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let (projection, projected_schema) = normalize_projection(projection, &self.schema)?;

        let buckets = 0..self.num_buckets;
        let targets: Vec<(Option<i64>, i32)> = if self.partition_keys.is_empty() {
            buckets.map(|b| (None, b)).collect()
        } else {
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

        if targets.is_empty() {
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
        let admin = connection.get_admin().map_err(FlussDatafusionError::from)?;
        let seam = LakeSeam::from_lake_snapshot(
            &admin
                .get_latest_lake_snapshot(&table_path)
                .await
                .map_err(FlussDatafusionError::from)?,
        );
        let plan = plan_append_union(
            &self.fluss_schema,
            &self.lake_catalog_properties,
            &seam,
            projection.clone(),
            targets,
        )
        .map_err(|e| FlussDatafusionError::Internal(e.to_string()))?;

        Ok(Arc::new(FlussUnionScanExec::new(
            connection,
            table_path,
            self.lake_catalog_properties.clone(),
            seam,
            plan,
        )))
    }
}
