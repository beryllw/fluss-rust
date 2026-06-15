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
//! [`FlussSource::log_scan`]. No filter pushdown is supported yet (every filter is
//! `Unsupported`, so a residual `FilterExec` is layered above the scan).

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use crate::backend::{SharedFlussSource, TableRef};
use crate::error::FlussDatafusionError;
use crate::execution::log_scan::FlussLogScanExec;

/// An append-only Fluss table backed by a required-`LIMIT` bounded scan.
pub(crate) struct FlussLogTableProvider {
    source: SharedFlussSource,
    table_ref: TableRef,
    schema: SchemaRef,
}

impl std::fmt::Debug for FlussLogTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussLogTableProvider")
            .field("table_ref", &self.table_ref)
            .finish()
    }
}

impl FlussLogTableProvider {
    pub(crate) fn new(source: SharedFlussSource, table_ref: TableRef, schema: SchemaRef) -> Self {
        Self {
            source,
            table_ref,
            schema,
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
        // Phase 1 log tables support no filter pushdown; residual filters become a
        // `FilterExec` above the scan.
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

        Ok(Arc::new(FlussLogScanExec::new(
            self.source.clone(),
            self.table_ref.clone(),
            projection,
            limit,
            projected_schema,
        )))
    }
}
