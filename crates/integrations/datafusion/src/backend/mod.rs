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
//! Fluss access (metadata discovery, KV point lookup, KV bounded scan, log
//! snapshot scan). It is a `pub(crate)` test seam, deliberately NOT a public
//! gateway-shaped backend abstraction: it carries no session, protocol, or auth
//! concepts.
//!
//! One implementation exists:
//! - [`real::RealFlussSource`] wraps the production fluss client.

use std::sync::Arc;

use arrow::array::RecordBatch;

use crate::error::Result;

pub(crate) mod real;

/// Identifies a Fluss table by `database.table`.
///
/// Crate-local so callers share one representation without pulling in
/// `fluss::metadata::TablePath` everywhere.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
/// This is intentionally a small, crate-owned struct rather than
/// `fluss::metadata::TableInfo`. It carries exactly what catalog wiring,
/// predicate analysis, and execution need.
///
/// `table_id`, `schema_id`, and `table_ref` are captured from the source but not
/// yet read by Phase 1 execution. They are retained for the work that consumes
/// them. `partition_keys` is empty for a non-partitioned table and otherwise
/// names the partition columns (in partition-key order) used for equality
/// pruning of log scans.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FlussTableMeta {
    pub table_ref: TableRef,
    pub table_id: i64,
    pub schema_id: i32,
    /// The Fluss `Schema` (column types + primary key).
    pub schema: fluss::metadata::Schema,
    pub primary_keys: Vec<String>,
    /// Bucket-key column names, in bucket-key order. For a primary-keyed table a
    /// bucket key that is a STRICT prefix of the physical primary key (PK minus
    /// partition keys) enables the prefix-lookup read shape; when it equals the
    /// physical primary key only the full point lookup applies.
    pub bucket_keys: Vec<String>,
    pub num_buckets: i32,
    /// Partition-key column names, in partition-key order. Empty when the table
    /// is not partitioned.
    pub partition_keys: Vec<String>,
}

impl FlussTableMeta {
    pub fn has_primary_key(&self) -> bool {
        !self.primary_keys.is_empty()
    }
}

/// One partition of a partitioned Fluss table: its id plus the partition-key
/// values (as strings, in partition-key order) used for equality pruning.
#[derive(Debug, Clone)]
pub struct FlussPartition {
    pub partition_id: i64,
    /// (partition_key, value) pairs in partition-key order. Fluss stores
    /// partition values as strings.
    pub values: Vec<(String, String)>,
}

/// One scalar key field for a full-primary-key equality lookup.
///
/// Phase 1 only needs the value types reachable from a primary-key equality
/// predicate. Construction of the actual `GenericRow` key from these values is
/// owned by [`real`]; higher layers (Task 4) build these from DataFusion
/// `ScalarValue`s.
#[derive(Debug, Clone, PartialEq)]
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
/// async metadata/lookup/scan calls dynamically.
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

    /// Performs a bucket-key PREFIX lookup.
    ///
    /// Returns every row whose primary key starts with the given bucket-key
    /// prefix (possibly many rows, or a 0-row batch with the table schema when no
    /// row matches). `lookup_columns` names the lookup columns in `lookup_by`
    /// order (partition keys followed by bucket keys for a partitioned table; just
    /// the bucket keys otherwise) and `key` carries one [`KeyValue`] per lookup
    /// column, in that same order.
    async fn prefix_lookup(
        &self,
        table: &TableRef,
        lookup_columns: &[String],
        key: &LookupKey,
    ) -> Result<RecordBatch>;

    /// Lists the partitions of a partitioned table. Returns an empty vec for a
    /// non-partitioned table (callers should not call it in that case).
    async fn list_partitions(&self, table: &TableRef) -> Result<Vec<FlussPartition>>;

    /// Performs a bounded scan of ONE bucket.
    ///
    /// Used by KV tables only. The fluss client's `LimitBatchScanner` returns the
    /// bucket's LAST `limit` rows (tail semantics).
    ///
    /// `partition_id` selects the partition to scan (`None` for a non-partitioned
    /// table). `bucket` is the bucket id to scan; one `(partition, bucket)` target
    /// maps to one DataFusion partition. `projection` is column indices into the
    /// table schema (`None` = all columns). `limit` is the required maximum number
    /// of rows. A cross-bucket final cap is applied by DataFusion above the
    /// per-bucket scans.
    async fn bounded_scan(
        &self,
        table: &TableRef,
        partition_id: Option<i64>,
        bucket: i32,
        projection: Option<&[usize]>,
        limit: usize,
    ) -> Result<Vec<RecordBatch>>;

    /// Performs a finite earliest-to-latest snapshot scan of ONE log bucket.
    ///
    /// `row_limit = Some(n)` returns the first `n` rows observed from the earliest
    /// offset onward for that target. `row_limit = None` reads a finite snapshot
    /// from the earliest offset up to the latest offset captured when the scan
    /// starts. Projection is pushed into the scanner; returned batches are already
    /// projected and should not be re-projected by callers.
    async fn log_scan(
        &self,
        table: &TableRef,
        partition_id: Option<i64>,
        bucket: i32,
        projection: Option<&[usize]>,
        row_limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>>;
}

/// Convenience alias for the shared, dynamically-dispatched source handle.
pub(crate) type SharedFlussSource = Arc<dyn FlussSource>;
