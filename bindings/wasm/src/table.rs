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

use crate::error::{FlussWasmError, Result};

/// Represents a Fluss table for data operations
#[wasm_bindgen]
pub struct FlussTable {
    table_path: String,
    database_name: String,
    table_name: String,
    has_primary_key: bool,
}

#[wasm_bindgen]
impl FlussTable {
    /// Internal constructor
    pub fn new(table_path: String, database_name: String, table_name: String, has_primary_key: bool) -> Self {
        Self {
            table_path,
            database_name,
            table_name,
            has_primary_key,
        }
    }

    /// Get the table name
    #[wasm_bindgen(getter)]
    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }

    /// Get the database name
    #[wasm_bindgen(getter)]
    pub fn database_name(&self) -> String {
        self.database_name.clone()
    }

    /// Check if this table has a primary key
    #[wasm_bindgen(getter)]
    pub fn has_primary_key(&self) -> bool {
        self.has_primary_key
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
        if self.has_primary_key {
            return Err(FlussWasmError::InvalidArgument(
                "Append is only supported for log tables (without primary key)".to_string()
            ));
        }
        Ok(TableAppend::new())
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
        if !self.has_primary_key {
            return Err(FlussWasmError::InvalidArgument(
                "Upsert is only supported for primary key tables".to_string()
            ));
        }
        Ok(TableUpsert::new())
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
        if self.has_primary_key {
            return Err(FlussWasmError::InvalidArgument(
                "Scan is only supported for log tables (without primary key)".to_string()
            ));
        }
        Ok(TableScan::new())
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
        if !self.has_primary_key {
            return Err(FlussWasmError::InvalidArgument(
                "Lookup is only supported for primary key tables".to_string()
            ));
        }
        Ok(TableLookup::new())
    }
}

/// Append writer for log tables
#[wasm_bindgen]
pub struct TableAppend {
    // Internal state will be added here
}

impl TableAppend {
    pub fn new() -> Self {
        Self {}
    }
}

#[wasm_bindgen]
impl TableAppend {
    /// Append a row to the table
    ///
    /// # Example
    ///
    /// ```javascript
    /// const writer = table.newAppend();
    /// writer.append({ ts: Date.now(), message: 'hello' });
    /// await writer.flush();
    /// ```
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
            // TODO: Implement actual flush logic
            Ok(JsValue::UNDEFINED)
        })
    }
}

/// Upsert writer for primary key tables
#[wasm_bindgen]
pub struct TableUpsert {
    // Internal state will be added here
}

impl TableUpsert {
    pub fn new() -> Self {
        Self {}
    }
}

#[wasm_bindgen]
impl TableUpsert {
    /// Upsert a row to the table
    ///
    /// # Example
    ///
    /// ```javascript
    /// const writer = table.newUpsert();
    /// writer.upsert({ id: 1, name: 'Alice', age: 30 });
    /// await writer.flush();
    /// ```
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
            // TODO: Implement actual flush logic
            Ok(JsValue::UNDEFINED)
        })
    }
}

/// Log scanner for reading data
#[wasm_bindgen]
pub struct TableScan {
    // Internal state will be added here
}

impl TableScan {
    pub fn new() -> Self {
        Self {}
    }
}

#[wasm_bindgen]
impl TableScan {
    /// Subscribe to a bucket
    ///
    /// # Example
    ///
    /// ```javascript
    /// const scanner = table.newScan();
    /// await scanner.subscribe(0, OffsetSpec.earliest());
    /// ```
    #[wasm_bindgen]
    pub fn subscribe(&self, bucket_id: i32, offset_spec: JsValue) -> Promise {
        future_to_promise(async move {
            // TODO: Parse offset spec from JS and implement actual subscribe
            log::debug!("Subscribe to bucket {} with offset {:?}", bucket_id, offset_spec);
            Ok(JsValue::UNDEFINED)
        })
    }

    /// Poll for new records
    ///
    /// # Example
    ///
    /// ```javascript
    /// const records = await scanner.poll(1000);
    /// for (const record of records) {
    ///     console.log(record);
    /// }
    /// ```
    #[wasm_bindgen]
    pub fn poll(&self, timeout_ms: u64) -> Promise {
        future_to_promise(async move {
            // TODO: Implement actual poll and return records
            log::debug!("Poll with timeout {}ms", timeout_ms);
            Ok(JsValue::from(js_sys::Array::new()))
        })
    }
}

/// Lookuper for primary key lookups
#[wasm_bindgen]
pub struct TableLookup {
    // Internal state will be added here
}

impl TableLookup {
    pub fn new() -> Self {
        Self {}
    }
}

#[wasm_bindgen]
impl TableLookup {
    /// Lookup a key
    ///
    /// # Example
    ///
    /// ```javascript
    /// const lookuper = table.newLookup();
    /// const result = await lookuper.lookup({ id: 1 });
    /// if (result) {
    ///     console.log(result);
    /// }
    /// ```
    #[wasm_bindgen]
    pub fn lookup(&self, key: JsValue) -> Promise {
        future_to_promise(async move {
            // TODO: Convert key from JS and perform lookup
            log::debug!("Lookup called with: {:?}", key);
            Ok(JsValue::NULL)
        })
    }
}

/// Offset specification for scanner subscription
#[wasm_bindgen]
pub struct OffsetSpec {
    spec_type: String,
    timestamp: Option<i64>,
}

#[wasm_bindgen]
impl OffsetSpec {
    /// Create an OffsetSpec for the earliest available offset
    #[wasm_bindgen]
    pub fn earliest() -> OffsetSpec {
        OffsetSpec {
            spec_type: "earliest".to_string(),
            timestamp: None,
        }
    }

    /// Create an OffsetSpec for the latest available offset
    #[wasm_bindgen]
    pub fn latest() -> OffsetSpec {
        OffsetSpec {
            spec_type: "latest".to_string(),
            timestamp: None,
        }
    }

    /// Create an OffsetSpec for the offset at or after the given timestamp
    #[wasm_bindgen]
    pub fn timestamp(ts: i64) -> OffsetSpec {
        OffsetSpec {
            spec_type: "timestamp".to_string(),
            timestamp: Some(ts),
        }
    }
}
