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

//! Integration test modules for fluss-datafusion.
//!
//! - `utils`: shared SQL-path helpers (always compiled).
//! - `setup`: shared real-cluster bootstrap + table DDL/DML (`integration_tests`).
//! - `e2e`: real-cluster end-to-end SQL through the real backend (`integration_tests`).
//! - `live_metadata`: real-cluster proof that post-registration DDL is visible
//!   live in the same session (`integration_tests`).
//! - `example_smoke`: real-cluster smoke test for the package-local runnable demo
//!   path (`integration_tests`).

pub mod utils;

#[cfg(feature = "integration_tests")]
pub mod setup;

#[cfg(feature = "integration_tests")]
pub mod e2e;

#[cfg(feature = "integration_tests")]
pub mod live_metadata;

#[cfg(feature = "integration_tests")]
pub mod example_smoke;
