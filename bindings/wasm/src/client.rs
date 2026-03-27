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

//! Fluss client for WASM

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;
use js_sys::Promise;
use std::sync::Arc;

use crate::config::WasmConfig;
use crate::admin::FlussAdmin;
use crate::table::FlussTable;
use crate::error::{FlussWasmError, Result};

/// Main client for connecting to a Fluss cluster
#[wasm_bindgen]
pub struct FlussClient {
    inner: Arc<fluss::client::FlussConnection>,
}

#[wasm_bindgen]
impl FlussClient {
    /// Create a new Fluss client
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration object or bootstrap server string
    ///
    /// # Example
    ///
    /// ```javascript
    /// // Using string (shortcut)
    /// const client = await FlussClient.new("localhost:9123");
    ///
    /// // Using config object
    /// const config = { bootstrap_servers: "localhost:9123" };
    /// const client = await FlussClient.newWithConfig(config);
    /// ```
    #[wasm_bindgen(constructor)]
    pub fn new(config: JsValue) -> Promise {
        future_to_promise(async move {
            let config = parse_config(config)?;
            let connection = fluss::client::FlussConnection::new(config)
                .await
                .map_err(FlussWasmError::from)?;
            let client = FlussClient { inner: Arc::new(connection) };
            Ok(JsValue::from(client))
        })
    }

    /// Get the admin interface for this client
    ///
    /// # Example
    ///
    /// ```javascript
    /// const client = await FlussClient.new("localhost:9123");
    /// const admin = client.getAdmin();
    /// const tables = await admin.listTables("fluss");
    /// ```
    #[wasm_bindgen]
    pub fn get_admin(&self) -> Result<FlussAdmin> {
        let admin = self.inner.get_admin().map_err(FlussWasmError::from)?;
        Ok(FlussAdmin::new(admin))
    }

    /// Get a table handle for the given database and table name
    ///
    /// # Arguments
    ///
    /// * `database` - Database name
    /// * `table` - Table name
    ///
    /// # Example
    ///
    /// ```javascript
    /// const table = await client.getTable("fluss", "users");
    /// ```
    #[wasm_bindgen]
    pub fn get_table(&self, database: String, table: String) -> Promise {
        let conn = Arc::clone(&self.inner);
        future_to_promise(async move {
            let table_path = fluss::metadata::TablePath::new(&database, &table);
            let table_info = conn
                .get_table(&table_path)
                .await
                .map_err(FlussWasmError::from)?;

            let fluss_table = FlussTable::new(
                table_path.to_string(),
                database,
                table,
                table_info.has_primary_key(),
            );
            Ok(JsValue::from(fluss_table))
        })
    }

    /// Close the client and release resources
    #[wasm_bindgen]
    pub fn close(&self) {
        // Cleanup if needed
        log::debug!("FlussClient closed");
    }
}

/// Parse JavaScript config value into Rust Config
fn parse_config(value: JsValue) -> Result<fluss::config::Config> {
    // If it's a string, treat as bootstrap servers
    if let Some(s) = value.as_string() {
        let mut config = fluss::config::Config::default();
        config.bootstrap_servers = s;
        return Ok(config);
    }

    // Otherwise, try to parse as WasmConfig
    let wasm_config: WasmConfig = serde_wasm_bindgen::from_value(value)?;
    Ok(wasm_config.into())
}
