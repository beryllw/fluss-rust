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

//! DataFusion `ExecutionPlan` over a `fluss-lake` read plan.
//!
//! One public [`fluss_lake::FlussLakeReadSplit`] maps to one DataFusion
//! partition. `execute(partition)` reads the i-th split through the kernel reader
//! and adapts the resulting stream to a `SendableRecordBatchStream`. The exec
//! holds only the opaque plan + reader; it has no notion of seams, lake splits,
//! append vs PK, or merge state.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::error::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use fluss_lake::{FlussLakeRead, FlussLakeReadPlan};
use futures::StreamExt;
use futures::stream::TryStreamExt;

/// A lake scan: one DataFusion partition per public read split.
pub(crate) struct FlussUnionScanExec {
    plan: FlussLakeReadPlan,
    read: FlussLakeRead,
    properties: Arc<PlanProperties>,
}

impl FlussUnionScanExec {
    pub(crate) fn new(plan: FlussLakeReadPlan, read: FlussLakeRead) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(plan.schema()),
            Partitioning::UnknownPartitioning(plan.split_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            plan,
            read,
            properties: Arc::new(properties),
        }
    }
}

impl Debug for FlussUnionScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlussUnionScanExec(splits={})", self.plan.split_count())
    }
}

impl DisplayAs for FlussUnionScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "FlussUnionScanExec: splits={}", self.plan.split_count())
            }
            DisplayFormatType::TreeRender => {
                write!(f, "FlussUnionScanExec\nsplits={}", self.plan.split_count())
            }
        }
    }
}

impl ExecutionPlan for FlussUnionScanExec {
    fn name(&self) -> &str {
        "FlussUnionScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "FlussUnionScanExec has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let schema = self.plan.schema();
        let plan = self.plan.clone();
        let read = self.read.clone();

        let future = async move {
            let split = plan.splits().get(partition).ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(format!(
                    "split index {partition} out of bounds (plan has {})",
                    plan.split_count()
                ))
            })?;
            let stream = read
                .read_split(split)
                .await
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            let stream = stream.map(
                |item: std::result::Result<
                    arrow::array::RecordBatch,
                    fluss_lake::FlussLakeError,
                >| {
                    item.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
                },
            );
            Ok::<_, datafusion::error::DataFusionError>(stream)
        };

        let stream = futures::stream::once(future).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
