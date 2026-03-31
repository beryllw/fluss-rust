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

use std::sync::LazyLock;
use tokio::runtime::Runtime;

pub use ::fluss as fcore;

mod admin;
mod config;
mod connection;
mod error;
mod lookup;
mod metadata;
mod row;
mod scanner;
mod table;
mod upsert;
mod write_handle;

pub use error::*;

static TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

/// Offset constant: start reading from the earliest available offset.
#[napi_derive::napi]
pub const EARLIEST_OFFSET: i64 = fcore::client::EARLIEST_OFFSET;
