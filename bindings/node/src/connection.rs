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

use crate::admin::FlussAdmin;
use crate::config::Config;
use crate::error::{client_error, from_core_error};
use crate::fcore;
use crate::metadata::TablePath;
use crate::table::FlussTable;
use crate::TOKIO_RUNTIME;
use napi_derive::napi;
use std::sync::Arc;

#[napi]
pub struct FlussConnection {
    inner: Arc<fcore::client::FlussConnection>,
}

#[napi]
impl FlussConnection {
    /// Create a new connection to a Fluss cluster (async factory).
    #[napi(factory)]
    pub async fn create(config: &Config) -> napi::Result<Self> {
        let rust_config = config.get_core_config();

        let connection = TOKIO_RUNTIME
            .spawn(async move {
                fcore::client::FlussConnection::new(rust_config).await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))?;

        Ok(Self {
            inner: Arc::new(connection),
        })
    }

    /// Get admin interface for managing databases and tables.
    #[napi]
    pub fn get_admin(&self) -> napi::Result<FlussAdmin> {
        let admin = self.inner.get_admin().map_err(|e| from_core_error(&e))?;
        Ok(FlussAdmin::from_core(admin))
    }

    /// Get a table reference for data operations.
    #[napi]
    pub async fn get_table(&self, table_path: &TablePath) -> napi::Result<FlussTable> {
        let client = self.inner.clone();
        let core_path = table_path.to_core();

        // Extract all needed data inside the spawn block to avoid lifetime issues.
        // FlussTable<'_> borrows FlussConnection, so we extract owned data
        // and construct our wrapper outside.
        let (metadata, table_info, path, has_pk) = TOKIO_RUNTIME
            .spawn(async move {
                let core_table = client.get_table(&core_path).await?;
                Ok::<_, fcore::error::Error>((
                    core_table.metadata().clone(),
                    core_table.get_table_info().clone(),
                    core_table.table_path().clone(),
                    core_table.has_primary_key(),
                ))
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))?;

        Ok(FlussTable::new_table(
            self.inner.clone(),
            metadata,
            table_info,
            path,
            has_pk,
        ))
    }

    /// Close the connection.
    #[napi]
    pub fn close(&self) {}
}
