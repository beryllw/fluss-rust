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

//! `SchemaProvider` for a single Fluss database.
//!
//! `table_names()` is synchronous and served from the snapshot captured at
//! `register_catalog` time. `table()` is async: per-table metadata + Arrow schema
//! are loaded lazily through the shared loader (and cached there).

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, SchemaProvider, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use crate::backend::{FlussTableMeta, TableRef};
use crate::error::FlussDatafusionError;
use crate::metadata::MetadataLoader;
use crate::table::kv::FlussKvTableProvider;

/// One Fluss database surfaced as a DataFusion schema.
#[derive(Debug)]
pub(crate) struct FlussSchemaProvider {
    database: String,
    /// Table-name snapshot captured at registration (no async listing here).
    table_names: Vec<String>,
    loader: Arc<MetadataLoader>,
}

impl FlussSchemaProvider {
    pub(crate) fn new(
        database: String,
        table_names: Vec<String>,
        loader: Arc<MetadataLoader>,
    ) -> Self {
        Self {
            database,
            table_names,
            loader,
        }
    }
}

#[async_trait]
impl SchemaProvider for FlussSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.table_names.clone()
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        if !self.table_names.iter().any(|t| t == name) {
            return Ok(None);
        }
        let table_ref = TableRef::new(self.database.clone(), name.to_string());
        let entry = self.loader.table_entry(&table_ref).await?;
        if entry.meta.has_primary_key() {
            // KV table: a real point-lookup provider (Task 4).
            let provider = FlussKvTableProvider::new(
                self.loader.source(),
                table_ref,
                entry.arrow_schema,
                entry.meta.primary_keys.clone(),
            );
            Ok(Some(Arc::new(provider)))
        } else {
            // Log table: still a conservative placeholder until Task 5 lands the
            // bounded-scan provider. `scan()` fails rather than guess.
            let provider = FlussTablePlaceholder::new(table_ref, &entry.meta, entry.arrow_schema);
            Ok(Some(Arc::new(provider)))
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.iter().any(|t| t == name)
    }
}

/// Minimal log-table `TableProvider` exposing only the correct Arrow `schema()`.
///
/// KV tables now use [`FlussKvTableProvider`] (Task 4). The log bounded-scan
/// `scan()` (Task 5) is still unimplemented here; `scan()` fails conservatively so
/// an unsupported pattern never degrades into a silent full scan.
#[derive(Debug)]
struct FlussTablePlaceholder {
    table_ref: TableRef,
    schema: SchemaRef,
}

impl FlussTablePlaceholder {
    fn new(table_ref: TableRef, _meta: &FlussTableMeta, schema: SchemaRef) -> Self {
        Self { table_ref, schema }
    }
}

#[async_trait]
impl TableProvider for FlussTablePlaceholder {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::from(
            FlussDatafusionError::UnsupportedQueryPattern(format!(
                "log table scan for {} is not implemented yet (Task 5)",
                self.table_ref
            )),
        ))
    }
}
