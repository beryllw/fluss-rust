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

//! `fluss-lake`: an engine-agnostic kernel for **lake + log union reads** of
//! Apache Fluss tables.
//!
//! A lake-enabled Fluss table tiers historical data into a data lake (Paimon).
//! A complete batch read is the union of two sources:
//!
//! - the **lake** side — a Paimon snapshot holding the data already tiered, read
//!   through [`paimon`]'s `ReadBuilder` / `to_arrow`;
//! - the **log** side — the residual Fluss log from the snapshot's per-bucket
//!   seam offset up to the latest offset, read through the Fluss client's log
//!   scanner.
//!
//! The kernel owns the parts that must not be re-implemented per query engine —
//! seam alignment, pushdown routing (what reaches the lake vs. the log vs. the
//! residual the caller must re-apply), and (for primary-key tables) the
//! cross-source merge. Engine integrations (e.g. DataFusion) are thin shims that
//! translate predicates, wrap the produced splits as engine partitions, and
//! apply the residual.
//!
//! It is deliberately Arrow-native and free of any engine concept so that other
//! Arrow consumers (Python / ADBC / Flight) can reuse the same union logic.

pub mod api;
pub mod catalog;
pub mod config;
pub mod error;
pub mod reader;

// Internal kernel modules: not part of the public surface. The public read API
// is the `FlussLake*` façade re-exported below.
mod append;
mod pk;
mod schema;
mod snapshot;
#[cfg(feature = "integration_tests")]
mod test_overrides;
mod union;

pub use api::{
    FlussLakeFilter, FlussLakePartitionFilter, FlussLakePartitionIdentity, FlussLakePlanStatistics,
    FlussLakeRead, FlussLakeReadBoundaryMode, FlussLakeReadMode, FlussLakeReadOptions,
    FlussLakeReadPlan, FlussLakeReadSplit, FlussLakeScan, FlussLakeTable,
};
pub use config::LakeCatalogConfig;
pub use error::{FlussLakeError, Result};
pub use reader::RecordBatchStream;
#[cfg(feature = "integration_tests")]
pub use test_overrides::{
    clear_test_lake_s3_endpoint_override, clear_test_lake_seam_override,
    set_test_lake_s3_endpoint_override, set_test_lake_seam_override,
    set_test_lake_snapshot_override,
};
