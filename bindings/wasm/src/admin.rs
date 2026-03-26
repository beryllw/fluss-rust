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

//! Admin API for WASM bindings

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;
use js_sys::Promise;

use crate::error::Result;

/// Admin interface for managing databases, tables, and partitions
#[wasm_bindgen]
pub struct FlussAdmin {
    inner: fluss::client::FlussAdmin,
}

impl FlussAdmin {
    pub fn new(inner: fluss::client::FlussAdmin) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl FlussAdmin {
    /// Create a new database
    ///
    /// # Arguments
    ///
    /// * `databaseName` - Name of the database to create
    ///
    /// # Example
    ///
    /// ```javascript
    /// await admin.createDatabase("mydb");
    /// ```
    #[wasm_bindgen]
    pub fn create_database(&self, database_name: String) -> Promise {
        future_to_promise(async move {
            self.inner
                .create_database(&database_name, None)
                .await
                .map_err(crate::error::FlussWasmError::from)?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// List all databases
    ///
    /// # Example
    ///
    /// ```javascript
    /// const databases = await admin.listDatabases();
    /// console.log(databases); // ["fluss", "mydb", ...]
    /// ```
    #[wasm_bindgen]
    pub fn list_databases(&self) -> Promise {
        future_to_promise(async move {
            let dbs = self
                .inner
                .list_databases()
                .await
                .map_err(crate::error::FlussWasmError::from)?;
            let js_array = js_sys::Array::new();
            for db in dbs {
                js_array.push(&JsValue::from(db));
            }
            Ok(JsValue::from(js_array))
        })
    }

    /// Check if a database exists
    ///
    /// # Arguments
    ///
    /// * `databaseName` - Name of the database to check
    #[wasm_bindgen]
    pub fn database_exists(&self, database_name: String) -> Promise {
        future_to_promise(async move {
            let exists = self
                .inner
                .database_exists(&database_name)
                .await
                .map_err(crate::error::FlussWasmError::from)?;
            Ok(JsValue::from(exists))
        })
    }

    /// Drop a database
    ///
    /// # Arguments
    ///
    /// * `databaseName` - Name of the database to drop
    /// * `ifExists` - If true, don't error if database doesn't exist
    #[wasm_bindgen]
    pub fn drop_database(&self, database_name: String, if_exists: Option<bool>) -> Promise {
        future_to_promise(async move {
            self.inner
                .drop_database(&database_name, if_exists.unwrap_or(false))
                .await
                .map_err(crate::error::FlussWasmError::from)?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// List tables in a database
    ///
    /// # Arguments
    ///
    /// * `databaseName` - Name of the database
    ///
    /// # Example
    ///
    /// ```javascript
    /// const tables = await admin.listTables("fluss");
    /// console.log(tables); // ["users", "events", ...]
    /// ```
    #[wasm_bindgen]
    pub fn list_tables(&self, database_name: String) -> Promise {
        future_to_promise(async move {
            let tables = self
                .inner
                .list_tables(&database_name)
                .await
                .map_err(crate::error::FlussWasmError::from)?;
            let js_array = js_sys::Array::new();
            for table in tables {
                js_array.push(&JsValue::from(table));
            }
            Ok(JsValue::from(js_array))
        })
    }

    /// Get information about a table
    ///
    /// # Arguments
    ///
    /// * `databaseName` - Database name
    /// * `tableName` - Table name
    #[wasm_bindgen]
    pub fn get_table_info(&self, database_name: String, table_name: String) -> Promise {
        future_to_promise(async move {
            let table_path = fluss::metadata::TablePath::new(&database_name, &table_name);
            let info = self
                .inner
                .get_table_info(&table_path)
                .await
                .map_err(crate::error::FlussWasmError::from)?;
            // Convert to JSON-serializable value
            let json = serde_json::to_string(&info)
                .map_err(crate::error::FlussWasmError::from)?;
            Ok(JsValue::from_str(&json))
        })
    }

    /// Check if a table exists
    #[wasm_bindgen]
    pub fn table_exists(&self, database_name: String, table_name: String) -> Promise {
        future_to_promise(async move {
            let table_path = fluss::metadata::TablePath::new(&database_name, &table_name);
            let exists = self
                .inner
                .table_exists(&table_path)
                .await
                .map_err(crate::error::FlussWasmError::from)?;
            Ok(JsValue::from(exists))
        })
    }
}
