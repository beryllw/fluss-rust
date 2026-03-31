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

use crate::error::{client_error, from_core_error};
use crate::fcore;
use crate::row::{internal_row_to_json, json_to_dense_generic_row};
use crate::TOKIO_RUNTIME;
use napi_derive::napi;
use std::sync::Arc;
use tokio::sync::Mutex;

#[napi]
pub struct Lookuper {
    inner: Arc<Mutex<fcore::client::Lookuper>>,
    table_info: Arc<fcore::metadata::TableInfo>,
}

#[napi]
impl Lookuper {
    #[napi]
    pub async fn lookup(
        &self,
        pk: serde_json::Value,
    ) -> napi::Result<Option<serde_json::Value>> {
        let pk_indices = self.table_info.get_schema().primary_key_indexes();
        let generic_row = json_to_dense_generic_row(&pk, &self.table_info, &pk_indices)?;
        let inner = self.inner.clone();
        let table_info = self.table_info.clone();

        // Perform async lookup on the runtime
        let result = TOKIO_RUNTIME
            .spawn(async move {
                let mut lookuper = inner.lock().await;
                lookuper.lookup(&generic_row).await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))?;

        let row_opt = result
            .get_single_row()
            .map_err(|e| from_core_error(&e))?;

        match row_opt {
            Some(compacted_row) => {
                let json = internal_row_to_json(&compacted_row, &table_info)?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }
}

impl Lookuper {
    pub fn new(
        connection: &Arc<fcore::client::FlussConnection>,
        metadata: Arc<fcore::client::Metadata>,
        table_info: fcore::metadata::TableInfo,
    ) -> napi::Result<Self> {
        let fluss_table =
            fcore::client::FlussTable::new(connection, metadata, table_info.clone());

        let table_lookup = fluss_table
            .new_lookup()
            .map_err(|e| from_core_error(&e))?;

        let lookuper = table_lookup
            .create_lookuper()
            .map_err(|e| from_core_error(&e))?;

        Ok(Self {
            inner: Arc::new(Mutex::new(lookuper)),
            table_info: Arc::new(table_info),
        })
    }
}
