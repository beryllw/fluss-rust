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

use crate::error::client_error;
use crate::fcore;
use napi_derive::napi;
use std::collections::HashMap;

// --- DataType ---

#[napi]
pub struct DataType {
    pub(crate) inner: fcore::metadata::DataType,
}

#[napi]
impl DataType {
    #[napi]
    pub fn to_string_repr(&self) -> String {
        format!("{}", self.inner)
    }
}

// --- DataTypes factory ---

#[napi]
pub struct DataTypes;

#[napi]
impl DataTypes {
    #[napi]
    pub fn boolean() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::boolean() }
    }
    #[napi]
    pub fn tinyint() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::tinyint() }
    }
    #[napi]
    pub fn smallint() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::smallint() }
    }
    #[napi]
    pub fn int() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::int() }
    }
    #[napi]
    pub fn bigint() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::bigint() }
    }
    #[napi]
    pub fn float() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::float() }
    }
    #[napi]
    pub fn double() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::double() }
    }
    #[napi]
    pub fn string() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::string() }
    }
    #[napi]
    pub fn bytes() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::bytes() }
    }
    #[napi]
    pub fn binary(length: u32) -> DataType {
        DataType { inner: fcore::metadata::DataTypes::binary(length as usize) }
    }
    #[napi]
    pub fn char(length: u32) -> DataType {
        DataType { inner: fcore::metadata::DataTypes::char(length) }
    }
    #[napi]
    pub fn decimal(precision: u32, scale: u32) -> DataType {
        DataType { inner: fcore::metadata::DataTypes::decimal(precision, scale) }
    }
    #[napi]
    pub fn date() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::date() }
    }
    #[napi]
    pub fn time() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::time() }
    }
    #[napi]
    pub fn time_with_precision(precision: u32) -> DataType {
        DataType { inner: fcore::metadata::DataTypes::time_with_precision(precision) }
    }
    #[napi]
    pub fn timestamp() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::timestamp() }
    }
    #[napi]
    pub fn timestamp_with_precision(precision: u32) -> DataType {
        DataType { inner: fcore::metadata::DataTypes::timestamp_with_precision(precision) }
    }
    #[napi]
    pub fn timestamp_ltz() -> DataType {
        DataType { inner: fcore::metadata::DataTypes::timestamp_ltz() }
    }
    #[napi]
    pub fn timestamp_ltz_with_precision(precision: u32) -> DataType {
        DataType { inner: fcore::metadata::DataTypes::timestamp_ltz_with_precision(precision) }
    }
}

// --- SchemaBuilder ---

#[napi]
pub struct SchemaBuilder {
    inner: fcore::metadata::SchemaBuilder,
}

#[napi]
impl SchemaBuilder {
    #[napi]
    pub fn column(&mut self, name: String, data_type: &DataType) -> &Self {
        // We need to take ownership, so we use a swap trick
        let builder = std::mem::replace(
            &mut self.inner,
            fcore::metadata::Schema::builder(),
        );
        self.inner = builder.column(name, data_type.inner.clone());
        self
    }

    #[napi]
    pub fn primary_key(&mut self, columns: Vec<String>) -> &Self {
        let builder = std::mem::replace(
            &mut self.inner,
            fcore::metadata::Schema::builder(),
        );
        self.inner = builder.primary_key(columns);
        self
    }

    #[napi]
    pub fn build(&mut self) -> napi::Result<Schema> {
        let builder = std::mem::replace(
            &mut self.inner,
            fcore::metadata::Schema::builder(),
        );
        let schema = builder
            .build()
            .map_err(|e| client_error(format!("Failed to build schema: {e}")))?;
        Ok(Schema { inner: schema })
    }
}

// --- Schema ---

#[napi]
pub struct Schema {
    pub(crate) inner: fcore::metadata::Schema,
}

#[napi]
impl Schema {
    #[napi]
    pub fn builder() -> SchemaBuilder {
        SchemaBuilder {
            inner: fcore::metadata::Schema::builder(),
        }
    }

    #[napi(getter)]
    pub fn column_names(&self) -> Vec<String> {
        self.inner
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect()
    }

    #[napi(getter)]
    pub fn primary_keys(&self) -> Vec<String> {
        self.inner
            .primary_key()
            .map(|pk| pk.column_names().iter().map(|s| s.to_string()).collect())
            .unwrap_or_default()
    }

    #[napi]
    pub fn get_columns(&self) -> Vec<Vec<String>> {
        self.inner
            .columns()
            .iter()
            .map(|col| {
                vec![
                    col.name().to_string(),
                    format!("{}", col.data_type()),
                ]
            })
            .collect()
    }
}

// --- TableDescriptorBuilder ---

#[napi]
pub struct TableDescriptorBuilder {
    schema: Option<fcore::metadata::Schema>,
    partition_keys: Vec<String>,
    bucket_count: Option<i32>,
    bucket_keys: Vec<String>,
    properties: HashMap<String, String>,
    custom_properties: HashMap<String, String>,
    comment: Option<String>,
    log_format: Option<String>,
    kv_format: Option<String>,
}

#[napi]
impl TableDescriptorBuilder {
    #[napi]
    pub fn schema(&mut self, schema: &Schema) -> &Self {
        self.schema = Some(schema.inner.clone());
        self
    }

    #[napi]
    pub fn distributed_by(&mut self, bucket_count: Option<i32>, bucket_keys: Option<Vec<String>>) -> &Self {
        self.bucket_count = bucket_count;
        if let Some(keys) = bucket_keys {
            self.bucket_keys = keys;
        }
        self
    }

    #[napi]
    pub fn partitioned_by(&mut self, keys: Vec<String>) -> &Self {
        self.partition_keys = keys;
        self
    }

    #[napi]
    pub fn comment(&mut self, comment: String) -> &Self {
        self.comment = Some(comment);
        self
    }

    #[napi]
    pub fn property(&mut self, key: String, value: String) -> &Self {
        self.properties.insert(key, value);
        self
    }

    #[napi]
    pub fn custom_property(&mut self, key: String, value: String) -> &Self {
        self.custom_properties.insert(key, value);
        self
    }

    #[napi]
    pub fn log_format(&mut self, format: String) -> &Self {
        self.log_format = Some(format);
        self
    }

    #[napi]
    pub fn kv_format(&mut self, format: String) -> &Self {
        self.kv_format = Some(format);
        self
    }

    #[napi]
    pub fn build(&self) -> napi::Result<TableDescriptor> {
        let schema = self
            .schema
            .clone()
            .ok_or_else(|| client_error("Schema is required"))?;

        let mut builder = fcore::metadata::TableDescriptor::builder()
            .schema(schema)
            .properties(self.properties.clone())
            .custom_properties(self.custom_properties.clone())
            .partitioned_by(self.partition_keys.clone())
            .distributed_by(self.bucket_count, self.bucket_keys.clone());

        if let Some(comment) = &self.comment {
            builder = builder.comment(comment);
        }
        if let Some(log_format) = &self.log_format {
            let format = fcore::metadata::LogFormat::parse(log_format)
                .map_err(|e| client_error(e.to_string()))?;
            builder = builder.log_format(format);
        }
        if let Some(kv_format) = &self.kv_format {
            let format = fcore::metadata::KvFormat::parse(kv_format)
                .map_err(|e| client_error(e.to_string()))?;
            builder = builder.kv_format(format);
        }

        let core = builder
            .build()
            .map_err(|e| client_error(format!("Failed to build TableDescriptor: {e}")))?;

        Ok(TableDescriptor { inner: core })
    }
}

// --- TableDescriptor ---

#[napi]
pub struct TableDescriptor {
    pub(crate) inner: fcore::metadata::TableDescriptor,
}

#[napi]
impl TableDescriptor {
    #[napi]
    pub fn builder() -> TableDescriptorBuilder {
        TableDescriptorBuilder {
            schema: None,
            partition_keys: Vec::new(),
            bucket_count: None,
            bucket_keys: Vec::new(),
            properties: HashMap::new(),
            custom_properties: HashMap::new(),
            comment: None,
            log_format: None,
            kv_format: None,
        }
    }
}

// --- TablePath ---

#[napi]
#[derive(Clone)]
pub struct TablePath {
    database_name: String,
    table_name: String,
}

#[napi]
impl TablePath {
    #[napi(constructor)]
    pub fn new(database_name: String, table_name: String) -> Self {
        Self {
            database_name,
            table_name,
        }
    }

    #[napi(getter)]
    pub fn database_name(&self) -> String {
        self.database_name.clone()
    }

    #[napi(getter)]
    pub fn table_name(&self) -> String {
        self.table_name.clone()
    }

    #[napi]
    pub fn to_string_repr(&self) -> String {
        format!("{}.{}", self.database_name, self.table_name)
    }
}

impl TablePath {
    pub fn to_core(&self) -> fcore::metadata::TablePath {
        fcore::metadata::TablePath::new(self.database_name.clone(), self.table_name.clone())
    }

    pub fn from_core(core_path: &fcore::metadata::TablePath) -> Self {
        Self {
            database_name: core_path.database().to_string(),
            table_name: core_path.table().to_string(),
        }
    }
}

// --- TableInfo ---

#[napi]
pub struct TableInfo {
    pub(crate) inner: fcore::metadata::TableInfo,
}

#[napi]
impl TableInfo {
    #[napi(getter)]
    pub fn table_id(&self) -> i64 {
        self.inner.get_table_id()
    }

    #[napi(getter)]
    pub fn schema_id(&self) -> i32 {
        self.inner.get_schema_id()
    }

    #[napi(getter)]
    pub fn table_path(&self) -> TablePath {
        TablePath::from_core(self.inner.get_table_path())
    }

    #[napi(getter)]
    pub fn num_buckets(&self) -> i32 {
        self.inner.get_num_buckets()
    }

    #[napi(getter)]
    pub fn created_time(&self) -> i64 {
        self.inner.get_created_time()
    }

    #[napi(getter)]
    pub fn modified_time(&self) -> i64 {
        self.inner.get_modified_time()
    }

    #[napi]
    pub fn has_primary_key(&self) -> bool {
        self.inner.has_primary_key()
    }

    #[napi]
    pub fn is_partitioned(&self) -> bool {
        self.inner.is_partitioned()
    }

    #[napi]
    pub fn get_primary_keys(&self) -> Vec<String> {
        self.inner.get_primary_keys().clone()
    }

    #[napi]
    pub fn get_bucket_keys(&self) -> Vec<String> {
        self.inner.get_bucket_keys().to_vec()
    }

    #[napi]
    pub fn get_partition_keys(&self) -> Vec<String> {
        self.inner.get_partition_keys().to_vec()
    }

    #[napi]
    pub fn get_column_names(&self) -> Vec<String> {
        self.inner
            .get_schema()
            .columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect()
    }

    #[napi(getter)]
    pub fn comment(&self) -> Option<String> {
        self.inner.get_comment().map(|s| s.to_string())
    }
}

impl TableInfo {
    pub fn from_core(info: fcore::metadata::TableInfo) -> Self {
        Self { inner: info }
    }
}

// --- ChangeType ---

#[napi]
pub enum ChangeType {
    AppendOnly = 0,
    Insert = 1,
    UpdateBefore = 2,
    UpdateAfter = 3,
    Delete = 4,
}

impl ChangeType {
    pub fn from_core(ct: fcore::record::ChangeType) -> Self {
        match ct {
            fcore::record::ChangeType::AppendOnly => ChangeType::AppendOnly,
            fcore::record::ChangeType::Insert => ChangeType::Insert,
            fcore::record::ChangeType::UpdateBefore => ChangeType::UpdateBefore,
            fcore::record::ChangeType::UpdateAfter => ChangeType::UpdateAfter,
            fcore::record::ChangeType::Delete => ChangeType::Delete,
        }
    }

    #[allow(dead_code)]
    pub fn short_string(&self) -> &'static str {
        match self {
            ChangeType::AppendOnly => "+A",
            ChangeType::Insert => "+I",
            ChangeType::UpdateBefore => "-U",
            ChangeType::UpdateAfter => "+U",
            ChangeType::Delete => "-D",
        }
    }
}

// --- TableBucket ---

#[napi(object)]
#[derive(Clone)]
pub struct TableBucket {
    pub table_id: i64,
    pub partition_id: Option<i64>,
    pub bucket_id: i32,
}

impl TableBucket {
    pub fn from_core(bucket: &fcore::metadata::TableBucket) -> Self {
        Self {
            table_id: bucket.table_id(),
            partition_id: bucket.partition_id(),
            bucket_id: bucket.bucket_id(),
        }
    }
}

// --- PartitionInfo ---

#[napi(object)]
#[derive(Clone)]
pub struct PartitionInfo {
    pub partition_id: i64,
    pub partition_name: String,
}

impl PartitionInfo {
    pub fn from_core(info: fcore::metadata::PartitionInfo) -> Self {
        Self {
            partition_id: info.get_partition_id(),
            partition_name: info.get_partition_name(),
        }
    }
}

// --- ServerNode ---

#[napi(object)]
#[derive(Clone)]
pub struct ServerNode {
    pub id: i32,
    pub host: String,
    pub port: u32,
    pub server_type: String,
    pub uid: String,
}

impl ServerNode {
    pub fn from_core(node: fcore::ServerNode) -> Self {
        Self {
            id: node.id(),
            host: node.host().to_string(),
            port: node.port(),
            server_type: node.server_type().to_string(),
            uid: node.uid().to_string(),
        }
    }
}

// --- OffsetSpec ---

#[napi]
#[derive(Clone)]
pub struct OffsetSpec {
    pub(crate) inner: fcore::rpc::message::OffsetSpec,
}

#[napi]
impl OffsetSpec {
    #[napi(factory)]
    pub fn earliest() -> Self {
        Self {
            inner: fcore::rpc::message::OffsetSpec::Earliest,
        }
    }

    #[napi(factory)]
    pub fn latest() -> Self {
        Self {
            inner: fcore::rpc::message::OffsetSpec::Latest,
        }
    }

    #[napi(factory)]
    pub fn timestamp(ts: i64) -> Self {
        Self {
            inner: fcore::rpc::message::OffsetSpec::Timestamp(ts),
        }
    }
}

// --- LakeSnapshot ---

#[napi(object)]
#[derive(Clone)]
pub struct LakeSnapshotBucketOffset {
    pub table_id: i64,
    pub partition_id: Option<i64>,
    pub bucket_id: i32,
    pub offset: i64,
}

#[napi]
pub struct LakeSnapshot {
    pub(crate) inner: fcore::metadata::LakeSnapshot,
}

#[napi]
impl LakeSnapshot {
    #[napi(getter)]
    pub fn snapshot_id(&self) -> i64 {
        self.inner.snapshot_id
    }

    #[napi]
    pub fn get_bucket_offsets(&self) -> Vec<LakeSnapshotBucketOffset> {
        self.inner
            .table_buckets_offset()
            .iter()
            .map(|(bucket, &offset)| LakeSnapshotBucketOffset {
                table_id: bucket.table_id(),
                partition_id: bucket.partition_id(),
                bucket_id: bucket.bucket_id(),
                offset,
            })
            .collect()
    }
}

impl LakeSnapshot {
    pub fn from_core(snapshot: &fcore::metadata::LakeSnapshot) -> Self {
        Self {
            inner: snapshot.clone(),
        }
    }
}

// --- DatabaseDescriptor ---

#[napi]
pub struct DatabaseDescriptor {
    pub(crate) inner: fcore::metadata::DatabaseDescriptor,
}

#[napi]
impl DatabaseDescriptor {
    #[napi(constructor)]
    pub fn new(comment: Option<String>, custom_properties: Option<HashMap<String, String>>) -> Self {
        let mut builder = fcore::metadata::DatabaseDescriptor::builder();
        if let Some(c) = comment {
            builder = builder.comment(&c);
        }
        if let Some(props) = custom_properties {
            builder = builder.custom_properties(props);
        }
        Self {
            inner: builder.build(),
        }
    }

    #[napi(getter)]
    pub fn comment(&self) -> Option<String> {
        self.inner.comment().map(|s| s.to_string())
    }
}

// --- DatabaseInfo ---

#[napi(object)]
#[derive(Clone)]
pub struct DatabaseInfo {
    pub database_name: String,
    pub created_time: i64,
    pub modified_time: i64,
}

impl DatabaseInfo {
    pub fn from_core(info: fcore::metadata::DatabaseInfo) -> Self {
        Self {
            database_name: info.database_name().to_string(),
            created_time: info.created_time(),
            modified_time: info.modified_time(),
        }
    }
}
