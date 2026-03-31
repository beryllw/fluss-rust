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
use crate::row::{json_to_generic_row, json_to_sparse_generic_row};
use crate::write_handle::WriteResultHandle;
use crate::TOKIO_RUNTIME;
use napi_derive::napi;
use std::sync::Arc;

#[napi]
pub struct UpsertWriter {
    writer: Arc<fcore::client::UpsertWriter>,
    table_info: fcore::metadata::TableInfo,
    target_columns: Option<Vec<usize>>,
}

#[napi]
impl UpsertWriter {
    #[napi]
    pub fn upsert(&self, row: serde_json::Value) -> napi::Result<WriteResultHandle> {
        let generic_row = if let Some(target_cols) = &self.target_columns {
            json_to_sparse_generic_row(&row, &self.table_info, target_cols)?
        } else {
            json_to_generic_row(&row, &self.table_info)?
        };

        let result_future = self
            .writer
            .upsert(&generic_row)
            .map_err(|e| from_core_error(&e))?;
        Ok(WriteResultHandle::new(result_future))
    }

    #[napi]
    pub fn delete(&self, pk: serde_json::Value) -> napi::Result<WriteResultHandle> {
        let pk_indices = self.table_info.get_schema().primary_key_indexes();
        let generic_row = json_to_sparse_generic_row(&pk, &self.table_info, &pk_indices)?;

        let result_future = self
            .writer
            .delete(&generic_row)
            .map_err(|e| from_core_error(&e))?;
        Ok(WriteResultHandle::new(result_future))
    }

    #[napi]
    pub async fn flush(&self) -> napi::Result<()> {
        let writer = self.writer.clone();
        TOKIO_RUNTIME
            .spawn(async move { writer.flush().await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }
}

impl UpsertWriter {
    pub fn new(
        table_upsert: &fcore::client::TableUpsert,
        table_info: fcore::metadata::TableInfo,
        target_columns: Option<Vec<usize>>,
    ) -> napi::Result<Self> {
        let writer = table_upsert
            .create_writer()
            .map_err(|e| from_core_error(&e))?;
        Ok(Self {
            writer: Arc::new(writer),
            table_info,
            target_columns,
        })
    }
}
