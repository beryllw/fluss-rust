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
use crate::metadata::{
    DatabaseDescriptor, DatabaseInfo, LakeSnapshot, OffsetSpec, PartitionInfo, ServerNode,
    TableDescriptor, TableInfo, TablePath,
};
use crate::TOKIO_RUNTIME;
use napi_derive::napi;
use std::collections::HashMap;
use std::sync::Arc;

#[napi]
pub struct FlussAdmin {
    inner: Arc<fcore::client::FlussAdmin>,
}

#[napi]
impl FlussAdmin {
    // --- Database operations ---

    #[napi]
    pub async fn create_database(
        &self,
        database_name: String,
        database_descriptor: Option<&DatabaseDescriptor>,
        ignore_if_exists: Option<bool>,
    ) -> napi::Result<()> {
        let admin = self.inner.clone();
        let descriptor = database_descriptor.map(|d| d.inner.clone());
        let ignore = ignore_if_exists.unwrap_or(false);

        TOKIO_RUNTIME
            .spawn(async move {
                admin
                    .create_database(&database_name, descriptor.as_ref(), ignore)
                    .await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn drop_database(
        &self,
        database_name: String,
        ignore_if_not_exists: Option<bool>,
        cascade: Option<bool>,
    ) -> napi::Result<()> {
        let admin = self.inner.clone();
        let ignore = ignore_if_not_exists.unwrap_or(false);
        let cascade = cascade.unwrap_or(true);

        TOKIO_RUNTIME
            .spawn(async move {
                admin
                    .drop_database(&database_name, ignore, cascade)
                    .await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn list_databases(&self) -> napi::Result<Vec<String>> {
        let admin = self.inner.clone();
        TOKIO_RUNTIME
            .spawn(async move { admin.list_databases().await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn database_exists(&self, database_name: String) -> napi::Result<bool> {
        let admin = self.inner.clone();
        TOKIO_RUNTIME
            .spawn(async move { admin.database_exists(&database_name).await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn get_database_info(&self, database_name: String) -> napi::Result<DatabaseInfo> {
        let admin = self.inner.clone();
        let info = TOKIO_RUNTIME
            .spawn(async move { admin.get_database_info(&database_name).await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))?;
        Ok(DatabaseInfo::from_core(info))
    }

    // --- Table operations ---

    #[napi]
    pub async fn create_table(
        &self,
        table_path: &TablePath,
        table_descriptor: &TableDescriptor,
        ignore_if_exists: Option<bool>,
    ) -> napi::Result<()> {
        let admin = self.inner.clone();
        let core_path = table_path.to_core();
        let core_descriptor = table_descriptor.inner.clone();
        let ignore = ignore_if_exists.unwrap_or(false);

        TOKIO_RUNTIME
            .spawn(async move {
                admin
                    .create_table(&core_path, &core_descriptor, ignore)
                    .await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn drop_table(
        &self,
        table_path: &TablePath,
        ignore_if_not_exists: Option<bool>,
    ) -> napi::Result<()> {
        let admin = self.inner.clone();
        let core_path = table_path.to_core();
        let ignore = ignore_if_not_exists.unwrap_or(false);

        TOKIO_RUNTIME
            .spawn(async move { admin.drop_table(&core_path, ignore).await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn list_tables(&self, database_name: String) -> napi::Result<Vec<String>> {
        let admin = self.inner.clone();
        TOKIO_RUNTIME
            .spawn(async move { admin.list_tables(&database_name).await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn table_exists(&self, table_path: &TablePath) -> napi::Result<bool> {
        let admin = self.inner.clone();
        let core_path = table_path.to_core();
        TOKIO_RUNTIME
            .spawn(async move { admin.table_exists(&core_path).await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn get_table_info(&self, table_path: &TablePath) -> napi::Result<TableInfo> {
        let admin = self.inner.clone();
        let core_path = table_path.to_core();
        let info = TOKIO_RUNTIME
            .spawn(async move { admin.get_table_info(&core_path).await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))?;
        Ok(TableInfo::from_core(info))
    }

    // --- Partition operations ---

    #[napi]
    pub async fn list_partition_infos(
        &self,
        table_path: &TablePath,
    ) -> napi::Result<Vec<PartitionInfo>> {
        let admin = self.inner.clone();
        let core_path = table_path.to_core();
        let infos = TOKIO_RUNTIME
            .spawn(async move { admin.list_partition_infos(&core_path).await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))?;
        Ok(infos.into_iter().map(PartitionInfo::from_core).collect())
    }

    #[napi]
    pub async fn create_partition(
        &self,
        table_path: &TablePath,
        partition_spec: HashMap<String, String>,
        ignore_if_exists: Option<bool>,
    ) -> napi::Result<()> {
        let admin = self.inner.clone();
        let core_path = table_path.to_core();
        let core_spec = fcore::metadata::PartitionSpec::new(partition_spec);
        let ignore = ignore_if_exists.unwrap_or(false);

        TOKIO_RUNTIME
            .spawn(async move {
                admin
                    .create_partition(&core_path, &core_spec, ignore)
                    .await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    #[napi]
    pub async fn drop_partition(
        &self,
        table_path: &TablePath,
        partition_spec: HashMap<String, String>,
        ignore_if_not_exists: Option<bool>,
    ) -> napi::Result<()> {
        let admin = self.inner.clone();
        let core_path = table_path.to_core();
        let core_spec = fcore::metadata::PartitionSpec::new(partition_spec);
        let ignore = ignore_if_not_exists.unwrap_or(false);

        TOKIO_RUNTIME
            .spawn(async move {
                admin
                    .drop_partition(&core_path, &core_spec, ignore)
                    .await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))
    }

    // --- Offset operations ---

    #[napi]
    pub async fn list_offsets(
        &self,
        table_path: &TablePath,
        bucket_ids: Vec<i32>,
        offset_spec: &OffsetSpec,
    ) -> napi::Result<serde_json::Value> {
        for &id in &bucket_ids {
            if id < 0 {
                return Err(client_error(format!(
                    "Invalid bucket_id: {id}. Must be non-negative"
                )));
            }
        }
        let admin = self.inner.clone();
        let core_path = table_path.to_core();
        let spec = offset_spec.inner.clone();

        let offsets = TOKIO_RUNTIME
            .spawn(async move {
                admin
                    .list_offsets(&core_path, &bucket_ids, spec)
                    .await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))?;

        // Convert HashMap<i32, i64> to JSON object with string keys
        let map: serde_json::Map<String, serde_json::Value> = offsets
            .into_iter()
            .map(|(k, v)| (k.to_string(), serde_json::json!(v)))
            .collect();
        Ok(serde_json::Value::Object(map))
    }

    #[napi]
    pub async fn list_partition_offsets(
        &self,
        table_path: &TablePath,
        partition_name: String,
        bucket_ids: Vec<i32>,
        offset_spec: &OffsetSpec,
    ) -> napi::Result<serde_json::Value> {
        for &id in &bucket_ids {
            if id < 0 {
                return Err(client_error(format!(
                    "Invalid bucket_id: {id}. Must be non-negative"
                )));
            }
        }
        let admin = self.inner.clone();
        let core_path = table_path.to_core();
        let spec = offset_spec.inner.clone();

        let offsets = TOKIO_RUNTIME
            .spawn(async move {
                admin
                    .list_partition_offsets(&core_path, &partition_name, &bucket_ids, spec)
                    .await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))?;

        // Convert HashMap<i32, i64> to JSON object with string keys
        let map: serde_json::Map<String, serde_json::Value> = offsets
            .into_iter()
            .map(|(k, v)| (k.to_string(), serde_json::json!(v)))
            .collect();
        Ok(serde_json::Value::Object(map))
    }

    // --- Server nodes ---

    #[napi]
    pub async fn get_server_nodes(&self) -> napi::Result<Vec<ServerNode>> {
        let admin = self.inner.clone();
        let nodes = TOKIO_RUNTIME
            .spawn(async move { admin.get_server_nodes().await })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))?;
        Ok(nodes.into_iter().map(ServerNode::from_core).collect())
    }

    // --- Lake snapshot ---

    #[napi]
    pub async fn get_latest_lake_snapshot(
        &self,
        table_path: &TablePath,
    ) -> napi::Result<LakeSnapshot> {
        let admin = self.inner.clone();
        let core_path = table_path.to_core();
        let snapshot = TOKIO_RUNTIME
            .spawn(async move {
                admin.get_latest_lake_snapshot(&core_path).await
            })
            .await
            .map_err(|e| client_error(format!("Runtime error: {e}")))?
            .map_err(|e| from_core_error(&e))?;
        Ok(LakeSnapshot::from_core(&snapshot))
    }
}

impl FlussAdmin {
    pub fn from_core(admin: Arc<fcore::client::FlussAdmin>) -> Self {
        Self { inner: admin }
    }
}
