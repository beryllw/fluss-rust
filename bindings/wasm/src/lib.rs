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

//! WASM bindings for Apache Fluss (Incubating)
//!
//! This crate provides WebAssembly bindings for the Apache Fluss Rust client,
//! enabling Node.js applications to interact with Fluss clusters.
//!
//! # Usage
//!
//! ```javascript
//! const fluss = require('fluss-wasm');
//!
//! async function main() {
//!     const client = await fluss.FlussClient.new("localhost:9123");
//!     const table = await client.getTable("fluss", "users");
//!     // ... use table for append, scan, upsert, lookup
//! }
//! ```

use wasm_bindgen::prelude::*;

// Error handling module
mod error;

// Core client modules
mod client;
mod config;
mod admin;
mod table;

// Re-export main types
pub use client::*;
pub use config::*;
pub use admin::*;
pub use table::*;
pub use error::FlussWasmError;

/// Initialize the WASM module - must be called before using any other APIs
#[wasm_bindgen(start)]
pub fn main() {
    // Set up panic hook for better error messages in WASM
    console_error_panic_hook::set_once();

    // Initialize logger for WASM
    let _ = wasm_logger::init(wasm_logger::Config::default());

    log::debug!("fluss-wasm initialized");
}

/// Version information
#[wasm_bindgen]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
