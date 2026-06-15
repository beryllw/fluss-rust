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

//! Cluster-free [`FlussSource`] that replays committed fixtures.
//!
//! Available only under the `test-fake` feature. Opens zero sockets: every
//! response is served from a [`FixtureSet`] captured from a real cluster, so the
//! schema, encoding, and row contents do not drift from real Fluss.
//!
//! Exposed `pub` (under the feature) because integration tests under `tests/`
//! are separate crates and cannot see `pub(crate)` items.

use std::collections::HashMap;
use std::path::Path;

use arrow::array::RecordBatch;

use super::fixtures::{FixtureSet, batch_from_ipc};
use super::{FlussSource, FlussTableMeta, LookupKey, TableRef};
use crate::error::{FlussDatafusionError, Result};

/// In-memory [`FlussSource`] backed by a [`FixtureSet`].
pub struct FakeFlussSource {
    /// database -> ordered table names.
    databases: Vec<(String, Vec<String>)>,
    /// `database.table` -> captured table data.
    tables: HashMap<String, TableData>,
}

struct TableData {
    meta: FlussTableMeta,
    lookups: Vec<(LookupKey, Vec<u8>)>,
    log_scans: Vec<LogScanEntry>,
}

struct LogScanEntry {
    projection: Option<Vec<usize>>,
    limit: usize,
    batches_ipc: Vec<Vec<u8>>,
}

impl FakeFlussSource {
    /// Builds a fake source from an already-parsed fixture set.
    pub fn from_fixture_set(set: FixtureSet) -> Self {
        let databases = set
            .databases
            .into_iter()
            .map(|db| (db.name, db.tables))
            .collect();

        let mut tables = HashMap::new();
        for table in set.tables {
            let key = table.meta.table_ref.to_string();
            tables.insert(
                key,
                TableData {
                    meta: table.meta,
                    lookups: table
                        .lookups
                        .into_iter()
                        .map(|l| (l.key, l.batch_ipc))
                        .collect(),
                    log_scans: table
                        .log_scans
                        .into_iter()
                        .map(|s| LogScanEntry {
                            projection: s.projection,
                            limit: s.limit,
                            batches_ipc: s.batches_ipc,
                        })
                        .collect(),
                },
            );
        }

        Self { databases, tables }
    }

    /// Loads a fixture set from a JSON file and builds a fake source.
    pub fn from_fixture_file(path: impl AsRef<Path>) -> Result<Self> {
        let json = std::fs::read_to_string(path.as_ref())?;
        let set = FixtureSet::from_json(&json)?;
        Ok(Self::from_fixture_set(set))
    }

    fn table_data(&self, table: &TableRef) -> Result<&TableData> {
        self.tables
            .get(&table.to_string())
            .ok_or_else(|| FlussDatafusionError::TableNotFound(table.to_string()))
    }
}

#[async_trait::async_trait]
impl FlussSource for FakeFlussSource {
    async fn list_databases(&self) -> Result<Vec<String>> {
        Ok(self
            .databases
            .iter()
            .map(|(name, _)| name.clone())
            .collect())
    }

    async fn list_tables(&self, database: &str) -> Result<Vec<String>> {
        self.databases
            .iter()
            .find(|(name, _)| name == database)
            .map(|(_, tables)| tables.clone())
            .ok_or_else(|| FlussDatafusionError::DatabaseNotFound(database.to_string()))
    }

    async fn get_table_meta(&self, table: &TableRef) -> Result<FlussTableMeta> {
        Ok(self.table_data(table)?.meta.clone())
    }

    async fn lookup(&self, table: &TableRef, key: &LookupKey) -> Result<RecordBatch> {
        let data = self.table_data(table)?;
        // Replay fidelity: a genuinely-absent key is captured too (its 0-row
        // batch replays exactly like real's). An *uncaptured* key is a fixture
        // gap, surfaced as an error rather than a misleading empty result. This
        // is why the fake errors here while real returns an empty batch for any
        // absent key.
        let entry = data
            .lookups
            .iter()
            .find(|(captured_key, _)| captured_key == key)
            .ok_or_else(|| {
                FlussDatafusionError::Fixture(format!(
                    "no captured lookup for {table} with key {key:?}"
                ))
            })?;
        batch_from_ipc(&entry.1)
    }

    async fn log_scan(
        &self,
        table: &TableRef,
        projection: Option<&[usize]>,
        limit: usize,
    ) -> Result<Vec<RecordBatch>> {
        let data = self.table_data(table)?;
        let entry = data
            .log_scans
            .iter()
            .find(|e| e.projection.as_deref() == projection && e.limit == limit)
            .ok_or_else(|| {
                FlussDatafusionError::Fixture(format!(
                    "no captured log scan for {table} with projection {projection:?} limit {limit}"
                ))
            })?;
        entry
            .batches_ipc
            .iter()
            .map(|bytes| batch_from_ipc(bytes))
            .collect()
    }
}
