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

/// Installer-wide options.
#[derive(Debug, Clone, Default)]
pub struct FlussDatafusionOptions {
    /// Prefix-stripped lake storage options (e.g. `s3.access-key`,
    /// `s3.secret-key`, `s3.region`, `s3.endpoint`) merged into every lake
    /// table's server-derived catalog properties just before the Paimon catalog
    /// is opened, with CALLER-WINS precedence.
    ///
    /// Use this to supply the storage credentials the Fluss server strips from
    /// table properties by policy (the caller holds them via its own secure
    /// config) — mirroring how the Flink Fluss catalog forwards its `paimon.*`
    /// options into the lake catalog. Empty by default (no behavior change), and
    /// never merged into the table's displayed `$options`.
    pub lake_storage_options: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Default)]
pub struct RegisterCatalogOptions {}
