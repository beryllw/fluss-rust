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

//! DataFusion `ExecutionPlan` that delegates one partition of a lake-enabled
//! append/log table to the `fluss-lake` kernel.
//!
//! One `UnionPartition` (one `(partition_id, bucket)` target) maps to one
//! DataFusion partition. `execute(partition)` opens a `lake ++ log_tail` stream
//! through the M1 kernel facade (`open_append_partition`) and adapts it to a
//! `SendableRecordBatchStream`.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use datafusion::error::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;
use fluss_lake::{LakeSeam, UnionScanPlan, open_append_partition};
use futures::StreamExt;
use futures::stream::TryStreamExt;

/// One append/log lake-union scan: one DataFusion partition per target bucket.
pub(crate) struct FlussUnionScanExec {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    lake_catalog_properties: std::collections::HashMap<String, String>,
    seam: LakeSeam,
    plan: UnionScanPlan,
    properties: Arc<PlanProperties>,
}

impl FlussUnionScanExec {
    pub(crate) fn new(
        connection: Arc<FlussConnection>,
        table_path: TablePath,
        lake_catalog_properties: std::collections::HashMap<String, String>,
        seam: LakeSeam,
        plan: UnionScanPlan,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(plan.projected_schema.clone()),
            Partitioning::UnknownPartitioning(plan.partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            connection,
            table_path,
            lake_catalog_properties,
            seam,
            plan,
            properties: Arc::new(properties),
        }
    }
}

impl Debug for FlussUnionScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlussUnionScanExec(table={})", self.table_path)
    }
}

impl DisplayAs for FlussUnionScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "FlussUnionScanExec: table={}, partitions={}",
                self.table_path,
                self.plan.partitions.len()
            ),
            DisplayFormatType::TreeRender => {
                write!(f, "FlussUnionScanExec\ntable={}", self.table_path)
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
        let schema = self.plan.projected_schema.clone();
        let connection = self.connection.clone();
        let table_path = TablePath::new(self.table_path.database(), self.table_path.table());
        let lake_catalog_properties = self.lake_catalog_properties.clone();
        let seam = self.seam.clone();
        let plan = self.plan.clone();

        let future = async move {
            let stream = open_append_partition(
                connection,
                &table_path,
                &lake_catalog_properties,
                &seam,
                &plan,
                partition,
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            let stream = stream.map(|item| {
                item.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
            });
            Ok::<_, datafusion::error::DataFusionError>(stream)
        };

        let stream = futures::stream::once(future).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
