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
//! read, and hands the plan + reader to a [`FlussUnionScanExec`]. All lake+log
//! union semantics (seam resolution, lake planning, append stitch, PK merge) live
//! inside `fluss-lake`; the connector never sees seams, lake splits, or merge
//! state.
//!
//! Value-filter pushdown is not offered yet: every filter stays `Unsupported`, so
//! residual `FilterExec`s remain above the scan.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::prelude::Expr;
use fluss_lake::FlussLakeTable;

use crate::backend::{SharedFlussSource, TableRef};
use crate::error::FlussDatafusionError;
use crate::execution::union_scan::FlussUnionScanExec;
use crate::types::record_batch::normalize_projection;

use arrow::datatypes::SchemaRef;

/// A lake-enabled Fluss table (`table.datalake.format = paimon`), append/log or
/// primary-key. The kernel selects append stitch vs PK merge internally.
///
/// `lake_only` selects the `<table>$lake` read shape: only the Paimon lake
/// snapshot (current state) is returned, with no Fluss log tail union.
pub(crate) struct FlussUnionTableProvider {
    source: SharedFlussSource,
    table_ref: TableRef,
    schema: SchemaRef,
    fluss_schema: fluss::metadata::Schema,
    num_buckets: i32,
    partition_keys: Vec<String>,
    lake_catalog_properties: std::collections::HashMap<String, String>,
    lake_only: bool,
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
        Self::with_lake_only(
            source,
            table_ref,
            schema,
            fluss_schema,
            num_buckets,
            partition_keys,
            lake_catalog_properties,
            false,
        )
    }

    /// Builds the provider in lake-only mode (`<table>$lake`): only the Paimon
    /// lake snapshot is read, with no Fluss log tail union.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_lake_only(
        source: SharedFlussSource,
        table_ref: TableRef,
        schema: SchemaRef,
        fluss_schema: fluss::metadata::Schema,
        num_buckets: i32,
        partition_keys: Vec<String>,
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
            lake_catalog_properties,
            lake_only,
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
        // Only non-partitioned lake tables are supported today (see `scan`), and
        // value/partition filters are not pushed to the union: residual filters
        // are re-applied by a `FilterExec` above the scan.
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let (projection, projected_schema) = normalize_projection(projection, &self.schema)?;

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

        // The kernel owns everything: seam resolution, lake planning, append
        // stitch / PK merge. We only build the scan, plan it, and create a reader.
        let lake_table = FlussLakeTable::new(
            connection,
            table_path,
            self.fluss_schema.clone(),
            self.num_buckets,
            self.partition_keys.clone(),
            self.lake_catalog_properties.clone(),
        );
        let mut scan = lake_table.new_scan().with_lake_only(self.lake_only);
        if let Some(indices) = projection {
            scan = scan.with_projection(indices);
        }
        let plan = scan
            .plan()
            .await
            .map_err(|e| FlussDatafusionError::Internal(e.to_string()))?;
        let read = scan
            .new_read()
            .map_err(|e| FlussDatafusionError::Internal(e.to_string()))?;

        Ok(Arc::new(FlussUnionScanExec::new(plan, read)))
    }
}
