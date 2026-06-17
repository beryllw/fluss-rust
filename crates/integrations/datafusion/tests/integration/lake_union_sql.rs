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

#![cfg(feature = "integration_tests")]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use datafusion::execution::context::SessionContext;
use fluss::metadata::{DataTypes, Schema, TableBucket, TableDescriptor, TablePath};
use fluss_datafusion::{
    FlussDatafusion, FlussDatafusionOptions, RegisterCatalogOptions,
    clear_test_lake_seam_override, set_test_lake_seam_override,
};
use fluss_test_cluster::FlussTestingClusterBuilder;
use futures::TryStreamExt;
use paimon::catalog::Identifier;
use paimon::spec::{DataType, IntType, Schema as PaimonSchema, VarCharType};
use paimon::{Catalog, CatalogFactory, CatalogOptions, Options};

use fluss_lake::LakeSeam;

use crate::integration::utils::helpers::{collect_i32, options};

const DATABASE: &str = "fluss";
const TABLE: &str = "df_lake_union_sql";
const CATALOG: &str = "fluss";

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

async fn wait_for_offsets(connection: &fluss::client::FlussConnection, table_path: &TablePath) {
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
async fn sql_reads_lake_plus_log_tail_via_union_provider() {
    let tmp = tempfile::tempdir().unwrap();
    let warehouse = format!("file://{}", tmp.path().display());
    seed_paimon_warehouse(&warehouse).await;

    let cluster = FlussTestingClusterBuilder::new("df-lake-union-sql")
        .with_port(9147)
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
        .property("table.datalake.format", "paimon")
        .property("table.datalake.paimon.warehouse", &warehouse)
        .build()
        .unwrap();
    admin.create_table(&table_path, &descriptor, true).await.unwrap();
    let fluss_table = connection.get_table(&table_path).await.unwrap();
    let writer = fluss_table.new_append().unwrap().create_writer().unwrap();
    writer.append_arrow_batch(fluss_log_batch()).unwrap();
    writer.flush().await.unwrap();
    wait_for_offsets(&connection, &table_path).await;

    let table_info = admin.get_table_info(&table_path).await.unwrap();
    let seam = LakeSeam::from_lake_snapshot(&fluss::metadata::LakeSnapshot::new(
        1,
        HashMap::from([(TableBucket::new(table_info.get_table_id(), 0), 2)]),
    ));
    set_test_lake_seam_override(&table_path, seam);

    let fd = FlussDatafusion::new(connection.clone(), options()).await.unwrap();
    let ctx = SessionContext::new();
    fd.register_catalog(&ctx, CATALOG, RegisterCatalogOptions::default())
        .await
        .unwrap();

    let sql = format!("SELECT id, name FROM {CATALOG}.{DATABASE}.{TABLE}");
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    assert_eq!(collect_i32(&batches, 0), vec![1, 2, 3, 4, 5, 6]);

    clear_test_lake_seam_override(&table_path);
    cluster.stop();
}
