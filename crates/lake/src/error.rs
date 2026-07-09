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
///
/// The public surface is the six FIP variants below. Underlying Fluss / Paimon /
/// Arrow errors are mapped into these via the `From` impls so `?` still works
/// throughout the kernel without leaking third-party error types.
#[derive(Debug, thiserror::Error)]
pub enum FlussLakeError {
    /// The table is not configured for a readable lake (no `table.datalake.*`),
    /// the lake catalog config is malformed, or the format is unsupported.
    #[error("table is not lake-readable: {0}")]
    NotLakeReadable(String),

    /// Planning (snapshot/seam resolution, lake-side planning, split freezing)
    /// failed.
    #[error("planning failed: {0}")]
    PlanningFailed(String),

    /// A split references data that is no longer available (e.g. a log offset
    /// that has been truncated below the seam).
    #[error("data unavailable: {0}")]
    DataUnavailable(String),

    /// The lake (Paimon) schema could not be aligned to the Fluss table schema.
    #[error("schema incompatible: {0}")]
    SchemaIncompatible(String),

    /// An error surfaced by the underlying Fluss connection / client.
    #[error("connection error: {0}")]
    ConnectionError(String),

    /// An internal invariant was violated. Should not happen for a well-formed
    /// table.
    #[error("internal error: {0}")]
    Internal(String),
}

impl From<fluss::error::Error> for FlussLakeError {
    fn from(e: fluss::error::Error) -> Self {
        FlussLakeError::ConnectionError(e.to_string())
    }
}

impl From<paimon::Error> for FlussLakeError {
    fn from(e: paimon::Error) -> Self {
        FlussLakeError::PlanningFailed(e.to_string())
    }
}

impl From<arrow::error::ArrowError> for FlussLakeError {
    fn from(e: arrow::error::ArrowError) -> Self {
        FlussLakeError::Internal(e.to_string())
    }
}

/// Result alias for the kernel.
pub type Result<T> = std::result::Result<T, FlussLakeError>;
