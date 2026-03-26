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

//! Table API for WASM bindings

use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::future_to_promise;
use js_sys::Promise;

use crate::error::Result;

/// Represents a Fluss table for data operations
#[wasm_bindgen]
pub struct FlussTable {
    inner: fluss::client::FlussTable,
}

impl FlussTable {
    pub fn new(inner: fluss::client::FlussTable) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl FlussTable {
    /// Get the table name
    #[wasm_bindgen(getter)]
    pub fn table_name(&self) -> String {
        self.inner.table_path().table_name().to_string()
    }

    /// Get the database name
    #[wasm_bindgen(getter)]
    pub fn database_name(&self) -> String {
        self.inner.table_path().database_name().to_string()
    }

    /// Create a new append writer for log tables
    ///
    /// # Example
    ///
    /// ```javascript
    /// const writer = await table.newAppend();
    /// writer.append(row);
    /// await writer.flush();
    /// ```
    #[wasm_bindgen]
    pub fn new_append(&self) -> Result<TableAppend> {
        Ok(TableAppend::new(self.inner.new_append()))
    }

    /// Create a new upsert writer for primary key tables
    ///
    /// # Example
    ///
    /// ```javascript
    /// const writer = await table.newUpsert();
    /// writer.upsert(row);
    /// await writer.flush();
    /// ```
    #[wasm_bindgen]
    pub fn new_upsert(&self) -> Result<TableUpsert> {
        Ok(TableUpsert::new(self.inner.new_upsert()))
    }

    /// Create a new log scanner for log tables
    ///
    /// # Example
    ///
    /// ```javascript
    /// const scanner = await table.newScan();
    /// await scanner.subscribe(0, OffsetSpec.earliest());
    /// const records = await scanner.poll(1000);
    /// ```
    #[wasm_bindgen]
    pub fn new_scan(&self) -> Result<TableScan> {
        Ok(TableScan::new(self.inner.new_scan()))
    }

    /// Create a new lookuper for primary key tables
    ///
    /// # Example
    ///
    /// ```javascript
    /// const lookuper = await table.newLookup();
    /// const result = await lookuper.lookup(key);
    /// ```
    #[wasm_bindgen]
    pub fn new_lookup(&self) -> Result<TableLookup> {
        Ok(TableLookup::new(self.inner.new_lookup()))
    }
}

/// Append writer for log tables
#[wasm_bindgen]
pub struct TableAppend {
    inner: fluss::client::TableAppend,
}

impl TableAppend {
    pub fn new(inner: fluss::client::TableAppend) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl TableAppend {
    /// Append a row to the table
    #[wasm_bindgen]
    pub fn append(&self, row: JsValue) -> Result<()> {
        // TODO: Convert JS value to internal row format
        log::debug!("Append called with: {:?}", row);
        Ok(())
    }

    /// Flush pending writes
    #[wasm_bindgen]
    pub fn flush(&self) -> Promise {
        future_to_promise(async move {
            self.inner
                .flush()
                .await
                .map_err(crate::error::FlussWasmError::from)?;
            Ok(JsValue::UNDEFINED)
        })
    }
}

/// Upsert writer for primary key tables
#[wasm_bindgen]
pub struct TableUpsert {
    inner: fluss::client::TableUpsert,
}

impl TableUpsert {
    pub fn new(inner: fluss::client::TableUpsert) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl TableUpsert {
    /// Upsert a row to the table
    #[wasm_bindgen]
    pub fn upsert(&self, row: JsValue) -> Result<()> {
        // TODO: Convert JS value to internal row format
        log::debug!("Upsert called with: {:?}", row);
        Ok(())
    }

    /// Flush pending writes
    #[wasm_bindgen]
    pub fn flush(&self) -> Promise {
        future_to_promise(async move {
            self.inner
                .flush()
                .await
                .map_err(crate::error::FlussWasmError::from)?;
            Ok(JsValue::UNDEFINED)
        })
    }
}

/// Log scanner for reading data
#[wasm_bindgen]
pub struct TableScan {
    inner: fluss::client::TableScan,
}

impl TableScan {
    pub fn new(inner: fluss::client::TableScan) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl TableScan {
    /// Subscribe to a bucket
    #[wasm_bindgen]
    pub fn subscribe(&self, bucket_id: i32, offset_spec: JsValue) -> Promise {
        future_to_promise(async move {
            // TODO: Parse offset spec from JS
            self.inner
                .subscribe(bucket_id, fluss::rpc::message::OffsetSpec::Earliest)
                .await
                .map_err(crate::error::FlussWasmError::from)?;
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Poll for new records
    #[wasm_bindgen]
    pub fn poll(&self, timeout_ms: u64) -> Promise {
        future_to_promise(async move {
            let _records = self
                .inner
                .poll(std::time::Duration::from_millis(timeout_ms))
                .await
                .map_err(crate::error::FlussWasmError::from)?;
            // TODO: Convert records to JS format
            Ok(JsValue::from(js_sys::Array::new()))
        })
    }
}

/// Lookuper for primary key lookups
#[wasm_bindgen]
pub struct TableLookup {
    inner: fluss::client::TableLookup,
}

impl TableLookup {
    pub fn new(inner: fluss::client::TableLookup) -> Self {
        Self { inner }
    }
}

#[wasm_bindgen]
impl TableLookup {
    /// Lookup a key
    #[wasm_bindgen]
    pub fn lookup(&self, key: JsValue) -> Promise {
        future_to_promise(async move {
            // TODO: Convert key from JS and perform lookup
            log::debug!("Lookup called with: {:?}", key);
            Ok(JsValue::NULL)
        })
    }
}
