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

//! DataFusion `ExecutionPlan` that delegates one partition of a lake-enabled
//! append/log table to the `fluss-lake` kernel.
//!
//! One `UnionPartition` (one `(partition_id, bucket)` target) maps to one
//! DataFusion partition. `execute(partition)` opens a `lake ++ log_tail` stream
//! through the M1 kernel facade (`open_append_partition`) and adapts it to a
//! `SendableRecordBatchStream`.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use datafusion::error::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;
use fluss_lake::{LakeSeam, UnionScanPlan, open_append_partition};
use futures::StreamExt;
use futures::stream::TryStreamExt;

/// One append/log lake-union scan: one DataFusion partition per target bucket.
pub(crate) struct FlussUnionScanExec {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    lake_catalog_properties: std::collections::HashMap<String, String>,
    seam: LakeSeam,
    plan: UnionScanPlan,
    properties: Arc<PlanProperties>,
}

impl FlussUnionScanExec {
    pub(crate) fn new(
        connection: Arc<FlussConnection>,
        table_path: TablePath,
        lake_catalog_properties: std::collections::HashMap<String, String>,
        seam: LakeSeam,
        plan: UnionScanPlan,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(plan.projected_schema.clone()),
            Partitioning::UnknownPartitioning(plan.partitions.len()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            connection,
            table_path,
            lake_catalog_properties,
            seam,
            plan,
            properties: Arc::new(properties),
        }
    }
}

impl Debug for FlussUnionScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FlussUnionScanExec(table={})", self.table_path)
    }
}

impl DisplayAs for FlussUnionScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "FlussUnionScanExec: table={}, partitions={}",
                self.table_path,
                self.plan.partitions.len()
            ),
            DisplayFormatType::TreeRender => {
                write!(f, "FlussUnionScanExec\ntable={}", self.table_path)
            }
        }
    }
}

impl ExecutionPlan for FlussUnionScanExec {
    fn name(&self) -> &str {
        "FlussUnionScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "FlussUnionScanExec has no children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let schema = self.plan.projected_schema.clone();
        let connection = self.connection.clone();
        let table_path = TablePath::new(self.table_path.database(), self.table_path.table());
        let lake_catalog_properties = self.lake_catalog_properties.clone();
        let seam = self.seam.clone();
        let plan = self.plan.clone();

        let future = async move {
            let stream = open_append_partition(
                connection,
                &table_path,
                &lake_catalog_properties,
                &seam,
                &plan,
                partition,
            )
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
            let stream = stream.map(
                |item: std::result::Result<arrow::array::RecordBatch, fluss_lake::FlussLakeError>| {
                    item.map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))
                },
            );
            Ok::<_, datafusion::error::DataFusionError>(stream)
        };

        let stream = futures::stream::once(future).try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(all(test, feature = "integration_tests"))]
mod integration_tests {
    use super::*;
    use std::collections::HashMap;

    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use datafusion::execution::context::SessionContext;
    use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TablePath};
    use fluss_test_cluster::FlussTestingClusterBuilder;
    use futures::TryStreamExt;
    use paimon::catalog::Identifier;
    use paimon::spec::{DataType, IntType, Schema as PaimonSchema, VarCharType};
    use paimon::{Catalog, CatalogFactory, CatalogOptions, Options};

    use fluss_lake::{LakeSeam, plan_append_union};

    const DATABASE: &str = "lake_exec_it_db";
    const TABLE: &str = "lake_exec_log";

    fn fluss_log_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, true),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let ids = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let names = StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    fn paimon_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        let ids = Int32Array::from(vec![1, 2]);
        let names = StringArray::from(vec!["a", "b"]);
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    async fn seed_paimon_warehouse(warehouse: &str) {
        let mut options = Options::new();
        options.set(CatalogOptions::WAREHOUSE, warehouse);
        let catalog = CatalogFactory::create(options).await.expect("create paimon catalog");
        catalog
            .create_database(DATABASE, true, HashMap::new())
            .await
            .expect("create paimon db");
        let schema = PaimonSchema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column("name", DataType::VarChar(VarCharType::string_type()))
            .option("bucket", "1")
            .option("bucket-key", "id")
            .build()
            .unwrap();
        let id = Identifier::new(DATABASE, TABLE);
        catalog.create_table(&id, schema, false).await.unwrap();
        let table = catalog.get_table(&id).await.unwrap();
        let write_builder = table.new_write_builder();
        let mut writer = write_builder.new_write().unwrap();
        writer.write_arrow_batch(&paimon_batch()).await.unwrap();
        let messages = writer.prepare_commit().await.unwrap();
        drop(writer);
        write_builder.new_commit().commit(messages).await.unwrap();
    }

    async fn wait_for_offsets(connection: &FlussConnection, table_path: &TablePath) {
        let admin = connection.get_admin().unwrap();
        let start = std::time::Instant::now();
        loop {
            if admin
                .list_offsets(table_path, &[0], fluss::rpc::message::OffsetSpec::Latest)
                .await
                .is_ok()
            {
                return;
            }
            if start.elapsed() >= std::time::Duration::from_secs(30) {
                panic!("table {table_path} bucket not ready in 30s");
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
    }

    #[tokio::test]
    async fn executes_lake_plus_log_tail_stream() {
        let tmp = tempfile::tempdir().unwrap();
        let warehouse = format!("file://{}", tmp.path().display());
        seed_paimon_warehouse(&warehouse).await;

        let cluster = FlussTestingClusterBuilder::new("df-lake-union-exec")
            .with_port(9146)
            .build()
            .await;
        let connection = Arc::new(cluster.get_fluss_connection().await);

        let table_path = TablePath::new(DATABASE, TABLE);
        let admin = connection.get_admin().unwrap();
        admin.create_database(DATABASE, None, true).await.unwrap();
        let descriptor = TableDescriptor::builder()
            .schema(
                Schema::builder()
                    .column("id", DataTypes::int())
                    .column("name", DataTypes::string())
                    .build()
                    .unwrap(),
            )
            .distributed_by(Some(1), vec![])
            .build()
            .unwrap();
        admin.create_table(&table_path, &descriptor, true).await.unwrap();
        let fluss_table = connection.get_table(&table_path).await.unwrap();
        let writer = fluss_table.new_append().unwrap().create_writer().unwrap();
        writer.append_arrow_batch(fluss_log_batch()).unwrap();
        writer.flush().await.unwrap();
        wait_for_offsets(&connection, &table_path).await;

        let table_info = admin.get_table_info(&table_path).await.unwrap();
        // lake covers rows at offsets [0,2); log tail begins at offset 2.
        let seam = LakeSeam::from_lake_snapshot(&fluss::metadata::LakeSnapshot::new(
            1,
            HashMap::from([(TableBucket::new(table_info.get_table_id(), 0), 2)]),
        ));
        let plan = plan_append_union(
            table_info.get_schema(),
            &HashMap::from([("warehouse".to_string(), warehouse.clone())]),
            &seam,
            None,
            vec![(None, 0)],
        )
        .unwrap();
        let exec = FlussUnionScanExec::new(
            connection.clone(),
            table_path.clone(),
            HashMap::from([("warehouse".to_string(), warehouse)]),
            seam,
            plan,
        );

        let task_ctx = SessionContext::new().task_ctx();
        let batches: Vec<RecordBatch> = exec
            .execute(0, task_ctx)
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5, 6]);

        cluster.stop();
    }
}
