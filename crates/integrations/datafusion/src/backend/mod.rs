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

//! Internal Fluss access seam.
//!
//! [`FlussSource`] is the single trait the rest of the crate depends on for all
//! Fluss access (metadata discovery, KV point lookup, bounded log scan). It is a
//! `pub(crate)` test seam, deliberately NOT a public gateway-shaped backend
//! abstraction: it carries no session, protocol, or auth concepts.
//!
//! Two implementations exist:
//! - [`real::RealFlussSource`] wraps the production fluss client.
//! - [`fake::FakeFlussSource`] (feature `test-fake`) replays committed fixtures
//!   captured from a real cluster, opening zero sockets.

use std::sync::Arc;

use arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::error::Result;

pub(crate) mod real;

// Fixture format is shared by the capture path (integration_tests) and the
// replay path (test-fake). Compiled whenever either is active.
#[cfg(any(feature = "test-fake", feature = "integration_tests"))]
pub mod fixtures;

#[cfg(feature = "test-fake")]
pub mod fake;

/// Identifies a Fluss table by `database.table`.
///
/// Crate-local so callers and fixtures share one representation without pulling
/// in `fluss::metadata::TablePath` everywhere.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableRef {
    pub database: String,
    pub table: String,
}

impl TableRef {
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
        }
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

impl From<&TableRef> for fluss::metadata::TablePath {
    fn from(value: &TableRef) -> Self {
        fluss::metadata::TablePath::new(value.database.clone(), value.table.clone())
    }
}

/// Minimal table metadata the crate needs in Phase 1.
///
/// This is intentionally a small, serde-able, crate-owned struct rather than
/// `fluss::metadata::TableInfo` (which is not `Serialize`). It carries exactly
/// what catalog wiring, predicate analysis, and execution need, and it round
/// trips cleanly through fixtures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlussTableMeta {
    pub table_ref: TableRef,
    pub table_id: i64,
    pub schema_id: i32,
    /// The Fluss `Schema` (column types + primary key). Serde-able via fluss.
    pub schema: fluss::metadata::Schema,
    pub primary_keys: Vec<String>,
    pub num_buckets: i32,
}

impl FlussTableMeta {
    pub fn has_primary_key(&self) -> bool {
        !self.primary_keys.is_empty()
    }
}

/// One scalar key field for a full-primary-key equality lookup.
///
/// Phase 1 only needs the value types reachable from a primary-key equality
/// predicate. Construction of the actual `GenericRow` key from these values is
/// owned by [`real`] (and replayed by [`fake`]); higher layers (Task 4) build
/// these from DataFusion `ScalarValue`s.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum KeyValue {
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    String(String),
}

/// A complete primary-key lookup key: one [`KeyValue`] per primary-key column,
/// in primary-key order.
pub type LookupKey = Vec<KeyValue>;

/// The single internal seam for all Fluss access used by Phase 1.
///
/// `#[async_trait]` lets the crate hold an `Arc<dyn FlussSource>` and dispatch
/// async metadata/lookup/scan calls dynamically (real vs fake).
#[async_trait::async_trait]
pub trait FlussSource: Send + Sync {
    /// Lists all database names in the cluster.
    async fn list_databases(&self) -> Result<Vec<String>>;

    /// Lists table names within a database.
    async fn list_tables(&self, database: &str) -> Result<Vec<String>>;

    /// Loads minimal metadata for a table.
    async fn get_table_meta(&self, table: &TableRef) -> Result<FlussTableMeta>;

    /// Performs a full-primary-key equality point lookup.
    ///
    /// Returns a `RecordBatch` with the matched row, or an empty (0-row) batch
    /// with the table's projected schema when the key is absent.
    async fn lookup(&self, table: &TableRef, key: &LookupKey) -> Result<RecordBatch>;

    /// Performs a bounded log scan.
    ///
    /// `projection` is column indices into the table schema (`None` = all
    /// columns). `limit` is the required maximum number of rows. Returns the
    /// batches produced by the bounded scan (possibly several).
    async fn log_scan(
        &self,
        table: &TableRef,
        projection: Option<&[usize]>,
        limit: usize,
    ) -> Result<Vec<RecordBatch>>;
}

/// Convenience alias for the shared, dynamically-dispatched source handle.
pub(crate) type SharedFlussSource = Arc<dyn FlussSource>;
