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

//! Custom `ExecutionPlan` for a bounded log scan.
//!
//! A single-partition leaf plan that performs exactly one
//! [`FlussSource::log_scan`] with a required `limit`. The display name
//! `FlussLogScanExec` makes the bounded scan visible in `EXPLAIN`.
//!
//! Asymmetry vs the KV path: `log_scan` already returns batches projected to
//! `projection`, so this plan passes the projection down and must NOT re-project
//! the returned batches.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::error::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};

use crate::backend::{SharedFlussSource, TableRef};
use crate::execution::stream::bounded_batches_stream;

/// Physical plan executing one bounded log scan against [`FlussSource`].
pub(crate) struct FlussLogScanExec {
    source: SharedFlussSource,
    table_ref: TableRef,
    /// Column indices into the full table schema; `None` means all columns.
    projection: Option<Vec<usize>>,
    limit: usize,
    /// Output (post-projection) schema declared to DataFusion.
    projected_schema: SchemaRef,
    properties: PlanProperties,
}

impl FlussLogScanExec {
    pub(crate) fn new(
        source: SharedFlussSource,
        table_ref: TableRef,
        projection: Option<Vec<usize>>,
        limit: usize,
        projected_schema: SchemaRef,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            source,
            table_ref,
            projection,
            limit,
            projected_schema,
            properties,
        }
    }
}

impl Debug for FlussLogScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FlussLogScanExec(table={}, limit={})",
            self.table_ref, self.limit
        )
    }
}

impl DisplayAs for FlussLogScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "FlussLogScanExec: table={}, limit={}",
                    self.table_ref, self.limit
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "FlussLogScanExec\ntable={}", self.table_ref)
            }
        }
    }
}

impl ExecutionPlan for FlussLogScanExec {
    fn name(&self) -> &str {
        "FlussLogScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Leaf node: the bounded scan is the data source.
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // No children to replace; return self unchanged.
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let source = self.source.clone();
        let table_ref = self.table_ref.clone();
        let projection = self.projection.clone();
        let limit = self.limit;

        let future = async move {
            // `log_scan` already projects to `projection`; do NOT re-project.
            let batches = source
                .log_scan(&table_ref, projection.as_deref(), limit)
                .await?;
            Ok(batches)
        };

        Ok(bounded_batches_stream(self.projected_schema.clone(), future))
    }
}
