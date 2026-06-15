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

//! On-disk fixture format shared by capture (write) and fake (replay).
//!
//! Captured from a real cluster and committed under `tests/fixtures/`, then
//! replayed by [`super::fake::FakeFlussSource`] with zero open sockets. Metadata
//! serializes via serde; `RecordBatch`es serialize via Arrow IPC stream bytes
//! (stored as a byte array inside JSON so no extra encoding dependency is
//! required).
//!
//! This module is always compiled so both the `integration_tests` capture path
//! and the `test-fake` replay path can share one definition.

use std::io::Cursor;

use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use serde::{Deserialize, Serialize};

use super::{FlussTableMeta, KeyValue};
use crate::error::{FlussDatafusionError, Result};

/// Current fixture schema version. Bump if the on-disk layout changes.
pub const FIXTURE_VERSION: u32 = 1;

/// One captured KV lookup: the key that was looked up and the resulting batch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupFixture {
    pub key: Vec<KeyValue>,
    /// Arrow IPC stream bytes of the lookup result batch (0 or 1 row).
    pub batch_ipc: Vec<u8>,
}

/// One captured bounded log scan: the request shape and resulting batches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogScanFixture {
    pub projection: Option<Vec<usize>>,
    pub limit: usize,
    /// Arrow IPC stream bytes per produced batch, in order.
    pub batches_ipc: Vec<Vec<u8>>,
}

/// All captured responses for a single table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableFixture {
    pub meta: FlussTableMeta,
    pub lookups: Vec<LookupFixture>,
    pub log_scans: Vec<LogScanFixture>,
}

/// The full fixture document: catalog shape plus per-table captures.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FixtureSet {
    pub version: u32,
    /// database -> table names (the catalog tree).
    pub databases: Vec<DatabaseFixture>,
    pub tables: Vec<TableFixture>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseFixture {
    pub name: String,
    pub tables: Vec<String>,
}

impl FixtureSet {
    pub fn new() -> Self {
        Self {
            version: FIXTURE_VERSION,
            databases: Vec::new(),
            tables: Vec::new(),
        }
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    pub fn from_json(json: &str) -> Result<Self> {
        let set: FixtureSet = serde_json::from_str(json)?;
        if set.version != FIXTURE_VERSION {
            return Err(FlussDatafusionError::Fixture(format!(
                "fixture version {} does not match expected {FIXTURE_VERSION}",
                set.version
            )));
        }
        Ok(set)
    }
}

impl Default for FixtureSet {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializes a single `RecordBatch` to Arrow IPC stream bytes.
pub fn batch_to_ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf)
}

/// Reads Arrow IPC stream bytes back into a single `RecordBatch`.
///
/// The stream is expected to contain exactly one batch. If it is empty (no
/// batches were written), an empty batch with the stream schema is returned so
/// the replayed shape matches a real "key absent" lookup.
pub fn batch_from_ipc(bytes: &[u8]) -> Result<RecordBatch> {
    let cursor = Cursor::new(bytes);
    let mut reader = StreamReader::try_new(cursor, None)?;
    let schema = reader.schema();
    match reader.next() {
        Some(batch) => Ok(batch?),
        None => Ok(RecordBatch::new_empty(schema)),
    }
}
