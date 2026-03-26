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

//! Error types for WASM bindings

use wasm_bindgen::JsValue;

/// Main error type for WASM bindings
#[derive(Debug, thiserror::Error)]
pub enum FlussWasmError {
    #[error("Fluss client error: {0}")]
    Fluss(#[from] fluss::error::FlussError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Not initialized: {0}")]
    NotInitialized(String),
}

impl From<FlussWasmError> for JsValue {
    fn from(err: FlussWasmError) -> Self {
        JsValue::from_str(&err.to_string())
    }
}

impl From<JsValue> for FlussWasmError {
    fn from(err: JsValue) -> Self {
        FlussWasmError::InvalidArgument(err.as_string().unwrap_or_default())
    }
}

/// Result type alias
pub type Result<T> = std::result::Result<T, FlussWasmError>;
