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

//! Integration test for [`FlussLogTailReader`] against a real Fluss cluster.
//!
//! No tiering is involved: a plain log table is created and appended, then the
//! reader is asked for the tail from a chosen start offset. This validates the
//! one piece of the union that needs a live connection — that the log tail is
//! read from the seam offset (not earliest) up to the latest offset.
//!
//! Run with: `cargo test -p fluss-lake --features integration_tests`
//! (requires a container runtime).
#![cfg(feature = "integration_tests")]

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use futures::TryStreamExt;

use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss_lake::reader::log::{FlussLogTailReader, LogTailReader};
use fluss_test_cluster::FlussTestingClusterBuilder;

const DATABASE: &str = "lake_it_db";
const TABLE: &str = "log_tail";

fn log_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let ids = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
    let names = StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
}

#[tokio::test]
async fn reads_log_tail_from_seam_offset() {
    let cluster = FlussTestingClusterBuilder::new("lake-log-tail")
        .with_port(9145)
        .build()
        .await;
    let connection = cluster.get_fluss_connection().await;

    // Create + seed a single-bucket log table: 6 rows at offsets 0..6. Scoped so
    // the table/writer borrows of `connection` end before it moves into the reader.
    let table_path = TablePath::new(DATABASE, TABLE);
    {
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
        admin
            .create_table(&table_path, &descriptor, true)
            .await
            .unwrap();
        let table = connection.get_table(&table_path).await.unwrap();
        let writer = table.new_append().unwrap().create_writer().unwrap();
        writer.append_arrow_batch(log_batch()).unwrap();
        writer.flush().await.unwrap();
    }

    // Read the tail from the seam at offset 2 -> expect rows at offsets 2..6,
    // i.e. ids 3,4,5,6. Starting at the seam (not earliest) is the whole point.
    let reader = FlussLogTailReader::new(Arc::new(connection), table_path);
    let batches: Vec<RecordBatch> = reader
        .read_tail(None, 0, 2, None, None)
        .await
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
    assert_eq!(
        ids,
        vec![3, 4, 5, 6],
        "tail must start at the seam offset 2"
    );

    cluster.stop();
}
