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

//! Custom `ExecutionPlan` for a bounded scan (log or KV).
//!
//! A leaf plan reporting one DataFusion partition per scan target, where a target
//! is a `(partition_id, bucket)` pair (`partition_id` is `None` for a
//! non-partitioned table). `execute(partition)` scans exactly that target via
//! [`FlussSource::bounded_scan`] with a required `limit`, so targets read in
//! parallel. For a partitioned table the target set is the cross product of the
//! partitions kept after pruning and the table's buckets. Each target returns its
//! bucket's last-`limit` rows; DataFusion applies a final cross-target `LIMIT`
//! above this plan.
//!
//! The same plan backs both the append-only (log) and primary-keyed (KV) bounded
//! scans because the fluss client decodes log vs KV batches per table type. The
//! `plan_name` field parameterizes the `EXPLAIN` label so the log provider shows
//! `FlussLogScanExec` and the KV provider shows `FlussKvScanExec`.
//!
//! Asymmetry vs the KV point-lookup path: `bounded_scan` already returns batches
//! projected to `projection`, so this plan passes the projection down and must NOT
//! re-project the returned batches.

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

/// Physical plan executing one bounded scan (log or KV) against [`FlussSource`].
pub(crate) struct FlussLogScanExec {
    source: SharedFlussSource,
    table_ref: TableRef,
    /// Column indices into the full table schema; `None` means all columns.
    projection: Option<Vec<usize>>,
    limit: usize,
    /// Scan targets: one `(partition_id, bucket)` per DataFusion output partition.
    /// `partition_id` is `None` for a non-partitioned table.
    targets: Vec<(Option<i64>, i32)>,
    /// Output (post-projection) schema declared to DataFusion.
    projected_schema: SchemaRef,
    /// `EXPLAIN`/`name()` label: `"FlussLogScanExec"` for log tables,
    /// `"FlussKvScanExec"` for KV tables. Lets one plan serve both without
    /// changing the log table's existing `EXPLAIN` output.
    plan_name: &'static str,
    properties: PlanProperties,
}

impl FlussLogScanExec {
    pub(crate) fn new(
        source: SharedFlussSource,
        table_ref: TableRef,
        projection: Option<Vec<usize>>,
        limit: usize,
        targets: Vec<(Option<i64>, i32)>,
        projected_schema: SchemaRef,
        plan_name: &'static str,
    ) -> Self {
        // One DataFusion partition per target so targets read in parallel;
        // `execute(partition)` scans `targets[partition]`.
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(targets.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            source,
            table_ref,
            projection,
            limit,
            targets,
            projected_schema,
            plan_name,
            properties,
        }
    }
}

impl Debug for FlussLogScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}(table={}, limit={}, partitions={})",
            self.plan_name,
            self.table_ref,
            self.limit,
            self.targets.len()
        )
    }
}

impl DisplayAs for FlussLogScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "{}: table={}, limit={}, partitions={}",
                    self.plan_name,
                    self.table_ref,
                    self.limit,
                    self.targets.len()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "{}\ntable={}", self.plan_name, self.table_ref)
            }
        }
    }
}

impl ExecutionPlan for FlussLogScanExec {
    fn name(&self) -> &str {
        self.plan_name
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
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let source = self.source.clone();
        let table_ref = self.table_ref.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        // One DataFusion partition maps to one (partition_id, bucket) target.
        let (partition_id, bucket) = self.targets[partition];

        let future = async move {
            // `bounded_scan` already projects to `projection`; do NOT re-project.
            let batches = source
                .bounded_scan(&table_ref, partition_id, bucket, projection.as_deref(), limit)
                .await?;
            Ok(batches)
        };

        Ok(bounded_batches_stream(self.projected_schema.clone(), future))
    }
}
