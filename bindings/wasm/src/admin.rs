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
use js_sys::{Array, Promise};
use std::sync::Arc;

use crate::error::FlussWasmError;

/// Admin interface for managing databases, tables, and partitions
#[wasm_bindgen]
pub struct FlussAdmin {
    inner: Arc<fluss::client::FlussAdmin>,
}

impl FlussAdmin {
    pub fn new(inner: Arc<fluss::client::FlussAdmin>) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl FlussAdmin {
    /// Create a new database
    #[wasm_bindgen]
    pub fn create_database(&self, database_name: String, ignore_if_exists: Option<bool>) -> Promise {
        let admin = Arc::clone(&self.inner);
        future_to_promise(async move {
            admin
                .create_database(&database_name, None, ignore_if_exists.unwrap_or(false))
                .await
                .map_err(FlussWasmError::from)?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// List all databases
    #[wasm_bindgen]
    pub fn list_databases(&self) -> Promise {
        let admin = Arc::clone(&self.inner);
        future_to_promise(async move {
            let dbs = admin
                .list_databases()
                .await
                .map_err(FlussWasmError::from)?;
            let js_array = Array::new();
            for db in dbs {
                js_array.push(&JsValue::from(db));
            }
            Ok(JsValue::from(js_array))
        })
    }

    /// Check if a database exists
    #[wasm_bindgen]
    pub fn database_exists(&self, database_name: String) -> Promise {
        let admin = Arc::clone(&self.inner);
        future_to_promise(async move {
            let exists = admin
                .database_exists(&database_name)
                .await
                .map_err(FlussWasmError::from)?;
            Ok(JsValue::from(exists))
        })
    }

    /// Drop a database
    #[wasm_bindgen]
    pub fn drop_database(&self, database_name: String, if_exists: Option<bool>, ignore_if_not_exists: Option<bool>) -> Promise {
        let admin = Arc::clone(&self.inner);
        future_to_promise(async move {
            admin
                .drop_database(&database_name, if_exists.unwrap_or(false), ignore_if_not_exists.unwrap_or(false))
                .await
                .map_err(FlussWasmError::from)?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// List tables in a database
    #[wasm_bindgen]
    pub fn list_tables(&self, database_name: String) -> Promise {
        let admin = Arc::clone(&self.inner);
        future_to_promise(async move {
            let tables = admin
                .list_tables(&database_name)
                .await
                .map_err(FlussWasmError::from)?;
            let js_array = Array::new();
            for table in tables {
                js_array.push(&JsValue::from(table));
            }
            Ok(JsValue::from(js_array))
        })
    }

    /// Get information about a table
    #[wasm_bindgen]
    pub fn get_table_info(&self, database_name: String, table_name: String) -> Promise {
        let admin = Arc::clone(&self.inner);
        future_to_promise(async move {
            let table_path = fluss::metadata::TablePath::new(&database_name, &table_name);
            let info = admin
                .get_table_info(&table_path)
                .await
                .map_err(FlussWasmError::from)?;
            // Convert to JSON-serializable string
            let json = format!(
                "{{\"table_id\":{},\"table_name\":\"{}\",\"database_name\":\"{}\",\"has_primary_key\":{}}}",
                info.table_id,
                table_name,
                database_name,
                info.has_primary_key()
            );
            Ok(JsValue::from_str(&json))
        })
    }

    /// Check if a table exists
    #[wasm_bindgen]
    pub fn table_exists(&self, database_name: String, table_name: String) -> Promise {
        let admin = Arc::clone(&self.inner);
        future_to_promise(async move {
            let table_path = fluss::metadata::TablePath::new(&database_name, &table_name);
            let exists = admin
                .table_exists(&table_path)
                .await
                .map_err(FlussWasmError::from)?;
            Ok(JsValue::from(exists))
        })
    }
}
