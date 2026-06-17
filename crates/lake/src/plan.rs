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

//! Engine-facing planning structs for the append union read.
//!
//! This is the thin, reusable contract between the engine-agnostic kernel and
//! a query-engine connector such as `fluss-datafusion`: the kernel resolves the
//! lake snapshot / per-bucket seam and hands back one [`UnionPartition`] per
//! `(partition, bucket)`, while the engine keeps ownership of its own residual
//! filters and final `LIMIT`.
//!
//! M1 intentionally keeps the planner narrow and append-focused:
//! - no value-filter routing yet (the engine leaves filters as residuals);
//! - no primary-key cross-source merge yet;
//! - no global row ordering guarantees.

use arrow::datatypes::SchemaRef;

use crate::union::UnionPartition;

/// A planned append union read: one execution partition per bucket target.
#[derive(Debug, Clone)]
pub struct UnionScanPlan {
    /// One execution unit per `(partition_id, bucket)` target.
    pub partitions: Vec<UnionPartition>,
    /// The Fluss Arrow schema after projection (or the full table schema when
    /// there is no projection). Every lake/log stream the kernel emits is
    /// aligned to this schema.
    pub projected_schema: SchemaRef,
    /// Column names in `projected_schema` order. Passed to the Paimon reader so
    /// the lake side projects by name and drops any system columns.
    pub projected_column_names: Option<Vec<String>>,
    /// Column indices into the Fluss schema, forwarded to the log reader.
    pub projected_column_indices: Option<Vec<usize>>,
}

impl UnionScanPlan {
    pub fn new(
        partitions: Vec<UnionPartition>,
        projected_schema: SchemaRef,
        projected_column_names: Option<Vec<String>>,
        projected_column_indices: Option<Vec<usize>>,
    ) -> Self {
        Self {
            partitions,
            projected_schema,
            projected_column_names,
            projected_column_indices,
        }
    }
}
