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

//! Crate-local error type for the lake+log union read kernel.

/// Errors raised while planning or executing a lake+log union read.
#[derive(Debug, thiserror::Error)]
pub enum FlussLakeError {
    /// The table is not configured for a readable lake (no `table.datalake.*`),
    /// or the configured format is not supported by this kernel.
    #[error("table is not lake-readable: {0}")]
    NotLakeReadable(String),

    /// The lake catalog config on the table is missing a required key (e.g.
    /// `warehouse`) or is otherwise malformed.
    #[error("invalid lake catalog config: {0}")]
    InvalidCatalogConfig(String),

    /// The lake (Paimon) schema could not be aligned to the Fluss table schema.
    #[error("lake/Fluss schema mismatch: {0}")]
    SchemaMismatch(String),

    /// An error surfaced by the underlying Fluss client.
    #[error("fluss client error: {0}")]
    Fluss(#[from] fluss::error::Error),

    /// An error surfaced by the underlying Paimon reader.
    #[error("paimon error: {0}")]
    Paimon(#[from] paimon::Error),

    /// An Arrow-level error (schema/array assembly).
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

/// Result alias for the kernel.
pub type Result<T> = std::result::Result<T, FlussLakeError>;
