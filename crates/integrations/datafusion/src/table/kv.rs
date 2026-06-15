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
//! Surfaces a primary-keyed Fluss table to DataFusion. Only a full-primary-key
//! equality predicate is supported; `supports_filters_pushdown` marks exactly the
//! PK-equality filters as `Exact` (consumed, no residual `FilterExec`), and `scan`
//! requires them to form a complete key. Any other shape returns a clear
//! `UnsupportedQueryPattern` instead of degrading into a full scan.

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use crate::backend::{SharedFlussSource, TableRef};
use crate::execution::lookup::FlussKvLookupExec;
use crate::table::predicate::{analyze_kv_filters, is_primary_key_equality};

/// A primary-keyed Fluss table backed by point lookups.
pub(crate) struct FlussKvTableProvider {
    source: SharedFlussSource,
    table_ref: TableRef,
    schema: SchemaRef,
    primary_keys: Vec<String>,
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
    ) -> Self {
        Self {
            source,
            table_ref,
            schema,
            primary_keys,
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
        // Consume only PK-equality filters so the planner drops the matching
        // `FilterExec`; everything else stays unsupported (and thus residual).
        Ok(filters
            .iter()
            .map(|f| {
                if is_primary_key_equality(f, &self.primary_keys) {
                    TableProviderFilterPushDown::Exact
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
        // Recognize a complete primary-key equality or fail clearly. The error is
        // surfaced verbatim; this provider never falls back to a full scan.
        let key = analyze_kv_filters(filters, &self.primary_keys)?;

        let projection = projection.cloned();
        let projected_schema = match &projection {
            None => self.schema.clone(),
            Some(indices) => Arc::new(self.schema.project(indices)?),
        };

        Ok(Arc::new(FlussKvLookupExec::new(
            self.source.clone(),
            self.table_ref.clone(),
            key,
            projected_schema,
            projection,
        )))
    }
}
