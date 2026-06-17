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

//! `SchemaProvider` for a single Fluss database.
//!
//! Every method is live: `table_names()`/`table_exist()` bridge the synchronous
//! catalog callbacks to the async source, and `table()` loads per-table metadata +
//! Arrow schema fresh each call. Tables created after `register_catalog` are
//! therefore visible in the same session.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::error::Result as DfResult;

use crate::backend::TableRef;
use crate::error::FlussDatafusionError;
use crate::metadata::MetadataLoader;
use crate::runtime::{block_on_with_runtime, ACCESS_PANIC};
use crate::table::kv::FlussKvTableProvider;
use crate::table::log::FlussLogTableProvider;
use crate::table::union::FlussUnionTableProvider;

/// One Fluss database surfaced as a DataFusion schema, listing tables live.
#[derive(Debug)]
pub(crate) struct FlussSchemaProvider {
    database: String,
    loader: Arc<MetadataLoader>,
}

impl FlussSchemaProvider {
    pub(crate) fn new(database: String, loader: Arc<MetadataLoader>) -> Self {
        Self { database, loader }
    }
}

#[async_trait]
impl SchemaProvider for FlussSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let loader = self.loader.clone();
        let database = self.database.clone();
        block_on_with_runtime(
            // Sync trait can't propagate errors; degrade to empty, as paimon does.
            async move { loader.source().list_tables(&database).await.unwrap_or_default() },
            ACCESS_PANIC,
        )
    }

    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        let table_ref = TableRef::new(self.database.clone(), name.to_string());
        let entry = match self.loader.table_entry(&table_ref).await {
            Ok(entry) => entry,
            // A missing table is `Ok(None)`, not an error; any other failure propagates.
            Err(FlussDatafusionError::TableNotFound(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        if entry.meta.has_primary_key() {
            // KV table: point lookup (full PK equality) or bounded `LIMIT` scan.
            // `num_buckets`/`partition_keys` drive the bounded-scan targets and
            // partition pruning, mirroring the log provider.
            let provider = FlussKvTableProvider::new(
                self.loader.source(),
                table_ref,
                entry.arrow_schema,
                entry.meta.primary_keys.clone(),
                entry.meta.bucket_keys.clone(),
                entry.meta.num_buckets,
                entry.meta.partition_keys.clone(),
            );
            Ok(Some(Arc::new(provider)))
        } else if entry.meta.is_paimon_lake() {
            // Lake-enabled append/log table: delegate to the fluss-lake kernel
            // (lake snapshot + residual Fluss log tail) instead of the plain log
            // snapshot scan.
            let provider = FlussUnionTableProvider::new(
                self.loader.source(),
                table_ref,
                entry.arrow_schema,
                entry.meta.schema.clone(),
                entry.meta.num_buckets,
                entry.meta.partition_keys.clone(),
                entry.meta
                    .lake_catalog_properties
                    .clone()
                    .unwrap_or_default(),
            );
            Ok(Some(Arc::new(provider)))
        } else {
            // Plain append/log table with no lakehouse storage: finite snapshot
            // scan directly from Fluss.
            let provider = FlussLogTableProvider::new(
                self.loader.source(),
                table_ref,
                entry.arrow_schema,
                entry.meta.num_buckets,
                entry.meta.partition_keys.clone(),
            );
            Ok(Some(Arc::new(provider)))
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        let loader = self.loader.clone();
        let database = self.database.clone();
        let name = name.to_string();
        block_on_with_runtime(
            async move {
                matches!(
                    loader.source().list_tables(&database).await,
                    Ok(tables) if tables.iter().any(|t| t == &name)
                )
            },
            ACCESS_PANIC,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::RecordBatch;
    use fluss::metadata::{DataLakeFormat, DataTypes, Schema as FlussSchema};

    use crate::backend::{FlussPartition, FlussSource, FlussTableMeta, LookupKey};
    use crate::error::Result as CrateResult;

    struct StubSource {
        meta: FlussTableMeta,
    }

    #[async_trait]
    impl FlussSource for StubSource {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        async fn list_databases(&self) -> CrateResult<Vec<String>> {
            Ok(vec!["db".to_string()])
        }

        async fn list_tables(&self, _database: &str) -> CrateResult<Vec<String>> {
            Ok(vec!["t".to_string()])
        }

        async fn get_table_meta(&self, _table: &TableRef) -> CrateResult<FlussTableMeta> {
            Ok(self.meta.clone())
        }

        async fn lookup(&self, _table: &TableRef, _key: &LookupKey) -> CrateResult<RecordBatch> {
            unreachable!()
        }

        async fn prefix_lookup(
            &self,
            _table: &TableRef,
            _lookup_columns: &[String],
            _key: &LookupKey,
        ) -> CrateResult<RecordBatch> {
            unreachable!()
        }

        async fn list_partitions(&self, _table: &TableRef) -> CrateResult<Vec<FlussPartition>> {
            Ok(vec![])
        }

        async fn bounded_scan(
            &self,
            _table: &TableRef,
            _partition_id: Option<i64>,
            _bucket: i32,
            _projection: Option<&[usize]>,
            _limit: usize,
        ) -> CrateResult<Vec<RecordBatch>> {
            unreachable!()
        }

        async fn log_scan(
            &self,
            _table: &TableRef,
            _partition_id: Option<i64>,
            _bucket: i32,
            _projection: Option<&[usize]>,
            _row_limit: Option<usize>,
        ) -> CrateResult<Vec<RecordBatch>> {
            unreachable!()
        }

        async fn kv_full_scan(
            &self,
            _table: &TableRef,
            _partition_id: Option<i64>,
            _bucket: i32,
            _projection: Option<&[usize]>,
        ) -> CrateResult<Vec<RecordBatch>> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn lake_enabled_append_table_uses_union_provider() {
        let fluss_schema = FlussSchema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .build()
            .unwrap();
        let meta = FlussTableMeta {
            table_ref: TableRef::new("db", "t"),
            table_id: 1,
            schema_id: 1,
            schema: fluss_schema,
            primary_keys: vec![],
            bucket_keys: vec![],
            num_buckets: 1,
            partition_keys: vec![],
            datalake_format: Some(DataLakeFormat::Paimon),
            lake_catalog_properties: Some(HashMap::from([(
                "warehouse".to_string(),
                "/tmp/wh".to_string(),
            )])),
        };
        let loader = Arc::new(MetadataLoader::new(Arc::new(StubSource { meta })));
        let schema_provider = FlussSchemaProvider::new("db".to_string(), loader);

        let provider = schema_provider.table("t").await.unwrap().unwrap();
        assert!(provider.as_any().is::<FlussUnionTableProvider>());
    }
}
