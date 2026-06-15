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
//! - `utils`: shared helpers + fixture paths (always compiled).
//! - `replay`: cluster-free fake replay tests (`test-fake`).
//! - `catalog`: cluster-free `register_catalog` + metadata-cache tests (`test-fake`).
//! - `kv_lookup`: cluster-free KV point-lookup SQL tests (`test-fake`).
//! - `log_scan`: cluster-free log bounded-scan SQL tests (`test-fake`).
//! - `setup`: shared real-cluster bootstrap + table DDL/DML (`integration_tests`).
//! - `capture`: real-cluster fixture capture (`integration_tests`).
//! - `e2e`: real-cluster end-to-end SQL through the real backend (`integration_tests`).

pub mod utils;

#[cfg(feature = "test-fake")]
pub mod replay;

#[cfg(feature = "test-fake")]
pub mod catalog;

#[cfg(feature = "test-fake")]
pub mod kv_lookup;

#[cfg(feature = "test-fake")]
pub mod log_scan;

#[cfg(feature = "integration_tests")]
pub mod setup;

#[cfg(feature = "integration_tests")]
pub mod capture;

#[cfg(feature = "integration_tests")]
pub mod e2e;
