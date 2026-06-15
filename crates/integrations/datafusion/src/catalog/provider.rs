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
//! Built once from a shared metadata snapshot at `register_catalog` time, then
//! handed to one or more `SessionContext`s. Schemas (databases) are pre-built from
//! the snapshot so the synchronous `schema_names()`/`schema()` can serve them
//! without async work.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use crate::catalog::schema::FlussSchemaProvider;

/// Read-only catalog provider backed by a shared metadata snapshot.
#[derive(Debug)]
pub(crate) struct FlussCatalogProvider {
    /// Insertion-ordered schema names for stable listing.
    schema_names: Vec<String>,
    schemas: HashMap<String, Arc<dyn SchemaProvider>>,
}

impl FlussCatalogProvider {
    pub(crate) fn new(schemas: Vec<(String, Arc<FlussSchemaProvider>)>) -> Self {
        let mut schema_names = Vec::with_capacity(schemas.len());
        let mut map: HashMap<String, Arc<dyn SchemaProvider>> = HashMap::new();
        for (name, provider) in schemas {
            schema_names.push(name.clone());
            map.insert(name, provider);
        }
        Self {
            schema_names,
            schemas: map,
        }
    }
}

impl CatalogProvider for FlussCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schema_names.clone()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).cloned()
    }
}
