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

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::Result as DfResult;

use crate::backend::TableRef;
use crate::metadata::MetadataLoader;
use crate::table::kv::FlussKvTableProvider;
use crate::table::log::FlussLogTableProvider;

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
            // Log table: a required-`LIMIT` bounded-scan provider (Task 5).
            let provider = FlussLogTableProvider::new(
                self.loader.source(),
                table_ref,
                entry.arrow_schema,
            );
            Ok(Some(Arc::new(provider)))
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names.iter().any(|t| t == name)
    }
}
