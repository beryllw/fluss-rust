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

//! Opens the Paimon catalog backing a Fluss lake table and resolves the table
//! pinned to a specific lake snapshot.
//!
//! A Fluss table is tiered into Paimon under the same `database.table` identity
//! (FIP-1), so the mapping is a direct [`Identifier`]. The lake snapshot id the
//! Fluss coordinator reports is pinned through Paimon's `scan.version` option
//! (`copy_with_options`), which `TableScan::plan` resolves to that exact
//! snapshot.

use std::collections::HashMap;
use std::sync::Arc;

use paimon::catalog::Identifier;
use paimon::{Catalog, CatalogFactory, Table};

use crate::config::LakeCatalogConfig;
use crate::error::Result;

/// Paimon's table option that pins a scan to a specific snapshot id (or tag).
const SCAN_VERSION_OPTION: &str = "scan.version";

/// Opens the Paimon catalog described by `config`. Dispatches on the `metastore`
/// option (filesystem / rest) inside Paimon's [`CatalogFactory`].
pub async fn open_catalog(config: &LakeCatalogConfig) -> Result<Arc<dyn Catalog>> {
    let catalog = CatalogFactory::create(config.options().clone()).await?;
    Ok(catalog)
}

/// Resolves the lake table `database.table` from `catalog`, pinned to read
/// exactly `snapshot_id`.
pub async fn get_table_at_snapshot(
    catalog: &Arc<dyn Catalog>,
    database: &str,
    table: &str,
    snapshot_id: i64,
) -> Result<Table> {
    let identifier = Identifier::new(database, table);
    let table = catalog.get_table(&identifier).await?;
    Ok(table.copy_with_options(HashMap::from([(
        SCAN_VERSION_OPTION.to_string(),
        snapshot_id.to_string(),
    )])))
}
