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

//! `CatalogProvider` over a Fluss cluster.
//!
//! Holds only the shared loader and serves every call live: the synchronous
//! `schema_names()`/`schema()` bridge to the async source via the global runtime,
//! so databases created after `register_catalog` are visible in the same session.

use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use crate::catalog::schema::FlussSchemaProvider;
use crate::metadata::MetadataLoader;
use crate::runtime::{block_on_with_runtime, ACCESS_PANIC};

/// Read-only catalog provider that lists Fluss databases live on every call.
#[derive(Debug)]
pub(crate) struct FlussCatalogProvider {
    loader: Arc<MetadataLoader>,
}

impl FlussCatalogProvider {
    pub(crate) fn new(loader: Arc<MetadataLoader>) -> Self {
        Self { loader }
    }
}

impl CatalogProvider for FlussCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let source = self.loader.source();
        block_on_with_runtime(
            // Only database names are needed here; list them directly rather than
            // also fetching per-database tables. Sync trait can't propagate errors;
            // degrade to empty, as paimon does.
            async move { source.list_databases().await.unwrap_or_default() },
            ACCESS_PANIC,
        )
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let loader = self.loader.clone();
        let name = name.to_string();
        // Check existence live so an unknown database returns `None` (matching the
        // paimon `DatabaseNotExist -> None` behaviour and the e2e expectation that
        // `catalog.schema("does_not_exist")` is `None`).
        let exists = block_on_with_runtime(
            {
                let loader = loader.clone();
                let name = name.clone();
                async move {
                    matches!(loader.source().list_databases().await, Ok(dbs) if dbs.iter().any(|d| d == &name))
                }
            },
            ACCESS_PANIC,
        );
        if !exists {
            return None;
        }
        Some(Arc::new(FlussSchemaProvider::new(name, loader)) as Arc<dyn SchemaProvider>)
    }
}
