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

//! Live metadata loader.
//!
//! Depends only on [`FlussSource`] (via `SharedFlussSource`), never on
//! `FlussConnection`/`FlussAdmin` directly. It holds no cache: every call goes
//! straight to the source so DDL is visible in the same session immediately
//! (matching SQL catalog semantics). The loader stays the home for Arrow-schema
//! derivation so callers do not re-implement the Fluss-to-Arrow mapping.

use arrow::datatypes::SchemaRef;

use crate::backend::{FlussTableMeta, SharedFlussSource, TableRef};
use crate::error::Result;

/// Per-table metadata: the crate-owned meta plus its derived Arrow schema.
#[derive(Clone)]
pub(crate) struct TableEntry {
    pub meta: FlussTableMeta,
    pub arrow_schema: SchemaRef,
}

/// Loads Fluss metadata live behind the [`FlussSource`] seam.
pub(crate) struct MetadataLoader {
    source: SharedFlussSource,
}

// `SharedFlussSource` (a trait object) is not `Debug`; catalog providers that
// embed the loader still need `Debug`, so provide a terse one.
impl std::fmt::Debug for MetadataLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetadataLoader").finish_non_exhaustive()
    }
}

impl MetadataLoader {
    pub(crate) fn new(source: SharedFlussSource) -> Self {
        Self { source }
    }

    /// Returns a clone of the shared [`FlussSource`] handle.
    ///
    /// KV/log `TableProvider`s need the execution channel to run lookups/scans.
    /// Exposing only the source (not the connection) keeps execution dependent on
    /// the seam alone, per the crate's dependency rule.
    pub(crate) fn source(&self) -> SharedFlussSource {
        self.source.clone()
    }

    /// Loads the Arrow schema + meta for one table live every call.
    pub(crate) async fn table_entry(&self, table: &TableRef) -> Result<TableEntry> {
        let meta = self.source.get_table_meta(table).await?;
        let arrow_schema = arrow_schema_of(&meta)?;
        Ok(TableEntry { meta, arrow_schema })
    }
}

/// Derives the Arrow schema from Fluss metadata, reusing fluss's own mapping.
fn arrow_schema_of(meta: &FlussTableMeta) -> Result<SchemaRef> {
    Ok(fluss::record::to_arrow_schema(meta.schema.row_type())?)
}
