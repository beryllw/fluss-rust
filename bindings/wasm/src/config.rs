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

//! Configuration for WASM bindings

use serde::{Deserialize, Serialize};
use wasm_bindgen::prelude::*;

/// Configuration for Fluss client
///
/// Can be created from JavaScript:
/// ```javascript
/// const config = {
///     bootstrapServers: "localhost:9123",
///     requestTimeoutMs: 30000,
///     // ... other options
/// };
/// ```
#[wasm_bindgen]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmConfig {
    /// Bootstrap servers, e.g., "localhost:9123"
    #[wasm_bindgen(getter_with_clone)]
    pub bootstrap_servers: String,

    /// Request timeout in milliseconds
    #[wasm_bindgen(getter_with_clone)]
    pub request_timeout_ms: Option<u64>,

    /// Maximum request size in bytes
    #[wasm_bindgen(getter_with_clone)]
    pub max_request_size: Option<usize>,
}

#[wasm_bindgen]
impl WasmConfig {
    /// Create a new config with the given bootstrap servers
    #[wasm_bindgen(constructor)]
    pub fn new(bootstrap_servers: String) -> Self {
        Self {
            bootstrap_servers,
            request_timeout_ms: None,
            max_request_size: None,
        }
    }

    /// Create default config
    #[wasm_bindgen(static_method_of = WasmConfig)]
    pub fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            request_timeout_ms: None,
            max_request_size: None,
        }
    }

    /// Set bootstrap servers
    pub fn with_bootstrap_servers(mut self, servers: String) -> Self {
        self.bootstrap_servers = servers;
        self
    }

    /// Set request timeout in milliseconds
    pub fn with_request_timeout(mut self, timeout_ms: u64) -> Self {
        self.request_timeout_ms = Some(timeout_ms);
        self
    }

    /// Set max request size in bytes
    pub fn with_max_request_size(mut self, size: usize) -> Self {
        self.max_request_size = Some(size);
        self
    }
}

impl From<WasmConfig> for fluss::config::Config {
    fn from(config: WasmConfig) -> Self {
        let mut inner = fluss::config::Config::default();
        inner.bootstrap_servers = config.bootstrap_servers;
        if let Some(timeout_ms) = config.request_timeout_ms {
            inner.request_timeout = Some(std::time::Duration::from_millis(timeout_ms));
        }
        inner
    }
}
