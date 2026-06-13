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

//! Shared metadata cache state.
//!
//! Keyed only by database/table identity, never by session: one `MetadataCache`
//! lives behind the installer `Arc` and is reused across every `SessionContext`.
//! TTL handling is intentionally simple (a load timestamp per entry); Phase 1
//! does not need eviction or capacity limits here.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;

use crate::backend::FlussTableMeta;

/// A cached value tagged with the instant it was loaded, for TTL checks.
struct Stamped<T> {
    value: T,
    loaded_at: Instant,
}

impl<T> Stamped<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            loaded_at: Instant::now(),
        }
    }

    fn fresh(&self, ttl: Duration) -> bool {
        self.loaded_at.elapsed() < ttl
    }
}

/// Per-table cached metadata: the crate-owned meta plus its derived Arrow schema.
#[derive(Clone)]
pub(crate) struct TableEntry {
    pub meta: FlussTableMeta,
    pub arrow_schema: SchemaRef,
}

/// Shared, lock-protected metadata snapshot.
///
/// IMPORTANT: guards are never held across `.await`; callers read/clone under the
/// lock, drop the guard, then await any source I/O.
pub(crate) struct MetadataCache {
    ttl: Duration,
    inner: RwLock<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Cached database -> ordered table-name listing.
    databases: Option<Stamped<Vec<(String, Vec<String>)>>>,
    /// Cached per-table metadata, keyed by `database.table`.
    tables: HashMap<String, Stamped<TableEntry>>,
}

impl MetadataCache {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            inner: RwLock::new(Inner::default()),
        }
    }

    /// Returns the cached database/table listing if present and within TTL.
    pub(crate) fn databases(&self) -> Option<Vec<(String, Vec<String>)>> {
        let guard = self.inner.read().expect("metadata cache poisoned");
        guard
            .databases
            .as_ref()
            .filter(|s| s.fresh(self.ttl))
            .map(|s| s.value.clone())
    }

    pub(crate) fn store_databases(&self, listing: Vec<(String, Vec<String>)>) {
        let mut guard = self.inner.write().expect("metadata cache poisoned");
        guard.databases = Some(Stamped::new(listing));
    }

    /// Returns the cached table entry if present and within TTL.
    pub(crate) fn table(&self, key: &str) -> Option<TableEntry> {
        let guard = self.inner.read().expect("metadata cache poisoned");
        guard
            .tables
            .get(key)
            .filter(|s| s.fresh(self.ttl))
            .map(|s| s.value.clone())
    }

    pub(crate) fn store_table(&self, key: String, entry: TableEntry) {
        let mut guard = self.inner.write().expect("metadata cache poisoned");
        guard.tables.insert(key, Stamped::new(entry));
    }
}
