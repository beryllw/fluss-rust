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

//! Shared metadata loader.
//!
//! Depends only on [`FlussSource`] (via `SharedFlussSource`), never on
//! `FlussConnection`/`FlussAdmin` directly, and fronts every source read with the
//! shared [`MetadataCache`]. The database/table listing is loaded once; per-table
//! metadata + Arrow schema are loaded lazily and cached on first `table()`.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;

use crate::backend::{FlussTableMeta, SharedFlussSource, TableRef};
use crate::error::Result;
use crate::metadata::cache::{MetadataCache, TableEntry};

/// Loads and caches Fluss metadata behind the [`FlussSource`] seam.
pub(crate) struct MetadataLoader {
    source: SharedFlussSource,
    cache: Arc<MetadataCache>,
}

// `SharedFlussSource` (a trait object) and the lock-guarded cache are not `Debug`;
// catalog providers that embed the loader still need `Debug`, so provide a terse one.
impl std::fmt::Debug for MetadataLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataLoader").finish_non_exhaustive()
    }
}

impl MetadataLoader {
    pub(crate) fn new(source: SharedFlussSource, cache: Arc<MetadataCache>) -> Self {
        Self { source, cache }
    }

    /// Returns the database -> table-name listing, fetching from the source only
    /// when the shared cache has no fresh snapshot. This is the single full-cluster
    /// read; it is shared across all `SessionContext`s.
    pub(crate) async fn databases(&self) -> Result<Vec<(String, Vec<String>)>> {
        if let Some(cached) = self.cache.databases() {
            return Ok(cached);
        }

        let db_names = self.source.list_databases().await?;
        let mut listing = Vec::with_capacity(db_names.len());
        for db in db_names {
            let tables = self.source.list_tables(&db).await?;
            listing.push((db, tables));
        }
        self.cache.store_databases(listing.clone());
        Ok(listing)
    }

    /// Returns the cached/loaded Arrow schema + meta for one table.
    pub(crate) async fn table_entry(&self, table: &TableRef) -> Result<TableEntry> {
        let key = table.to_string();
        if let Some(entry) = self.cache.table(&key) {
            return Ok(entry);
        }

        let meta = self.source.get_table_meta(table).await?;
        let arrow_schema = arrow_schema_of(&meta)?;
        let entry = TableEntry { meta, arrow_schema };
        self.cache.store_table(key, entry.clone());
        Ok(entry)
    }
}

/// Derives the Arrow schema from Fluss metadata, reusing fluss's own mapping.
fn arrow_schema_of(meta: &FlussTableMeta) -> Result<SchemaRef> {
    Ok(fluss::record::to_arrow_schema(meta.schema.row_type())?)
}
