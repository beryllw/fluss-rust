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

//! Builds the Fluss `CatalogProvider` tree from the shared metadata snapshot.
//!
//! The single full-cluster read (database/table listing) goes through the shared
//! loader, so a second `register_catalog` on a fresh context reuses the cache and
//! does not re-hit the source.

use std::sync::Arc;

use crate::catalog::provider::FlussCatalogProvider;
use crate::catalog::schema::FlussSchemaProvider;
use crate::error::Result;
use crate::metadata::MetadataLoader;

/// Builds a catalog provider from the shared (cache-fronted) listing snapshot.
pub(crate) async fn build_catalog_provider(
    loader: Arc<MetadataLoader>,
) -> Result<Arc<FlussCatalogProvider>> {
    let listing = loader.databases().await?;
    let schemas = listing
        .into_iter()
        .map(|(database, tables)| {
            let provider =
                Arc::new(FlussSchemaProvider::new(database.clone(), tables, loader.clone()));
            (database, provider)
        })
        .collect();
    Ok(Arc::new(FlussCatalogProvider::new(schemas)))
}
