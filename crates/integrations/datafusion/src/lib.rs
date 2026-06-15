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

mod backend;
mod catalog;
mod config;
mod error;
mod execution;
mod install;
mod metadata;
mod table;
mod types;

pub use config::{FlussDatafusionOptions, RegisterCatalogOptions};
pub use error::{FlussDatafusionError, Result};
pub use install::FlussDatafusion;

// Test-only seam surface. Integration tests live in separate crates and cannot
// reach `pub(crate)` items, so the fake source, fixture format, and the source
// type alias are re-exported publicly ONLY under the `test-fake` feature. This
// keeps the seam out of the real public API while letting `tests/` inject a fake.
#[cfg(feature = "test-fake")]
pub mod testing {
    pub use crate::backend::fake::FakeFlussSource;
    pub use crate::backend::fixtures;
    pub use crate::backend::{FlussSource, FlussTableMeta, KeyValue, LookupKey, TableRef};
}

// The capture path (integration_tests) writes the same fixture format the fake
// replays. Expose the fixture format and crate-owned metadata/key types so the
// capture test (a separate crate) can build and serialize a `FixtureSet`.
#[cfg(all(feature = "integration_tests", not(feature = "test-fake")))]
pub mod testing {
    pub use crate::backend::fixtures;
    pub use crate::backend::{FlussTableMeta, KeyValue, LookupKey, TableRef};
}
