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

use std::error::Error;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::execution::context::SessionContext;
use fluss::client::FlussConnection;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::GenericRow;
use fluss::rpc::message::OffsetSpec;
use fluss_datafusion::{FlussDatafusion, FlussDatafusionOptions, RegisterCatalogOptions};

pub const CATALOG: &str = "fluss";
pub const DATABASE: &str = "fluss";
pub const KV_TABLE: &str = "df_demo_kv";
pub const LOG_TABLE: &str = "df_demo_log";

const READY_TIMEOUT: Duration = Duration::from_secs(30);

type DynError = Box<dyn Error + Send + Sync>;
pub type DemoResult<T> = Result<T, DynError>;

pub struct DemoSummary {
    pub kv_name: String,
    pub kv_age: i64,
    pub log_ids: Vec<i32>,
    pub log_actions: Vec<String>,
    pub kv_explain: String,
    pub log_explain: String,
}

pub async fn run_demo(connection: Arc<FlussConnection>) -> DemoResult<DemoSummary> {
    drop_demo_tables(&connection).await;
    create_kv_demo_table(&connection).await?;
    create_log_demo_table(&connection).await?;

    let fd = FlussDatafusion::new(connection.clone(), FlussDatafusionOptions::default()).await?;
    let ctx = SessionContext::new();
    fd.register_catalog(&ctx, CATALOG, RegisterCatalogOptions::default())
        .await?;

    let kv_batches = ctx
        .sql(&format!(
            "SELECT id, name, age FROM {CATALOG}.{DATABASE}.{KV_TABLE} WHERE id = 2"
        ))
        .await?
        .collect()
        .await?;
    let log_batches = ctx
        .sql(&format!(
            "SELECT id, action FROM {CATALOG}.{DATABASE}.{LOG_TABLE} LIMIT 3"
        ))
        .await?
        .collect()
        .await?;
    let kv_explain = ctx
        .sql(&format!(
            "EXPLAIN SELECT * FROM {CATALOG}.{DATABASE}.{KV_TABLE} WHERE id = 2"
        ))
        .await?
        .collect()
        .await?;
    let log_explain = ctx
        .sql(&format!(
            "EXPLAIN SELECT * FROM {CATALOG}.{DATABASE}.{LOG_TABLE} LIMIT 3"
        ))
        .await?
        .collect()
        .await?;

    let summary = DemoSummary {
        kv_name: single_string(&kv_batches, 1)?,
        kv_age: single_i64(&kv_batches, 2)?,
        log_ids: collect_i32(&log_batches, 0)?,
        log_actions: collect_strings(&log_batches, 1)?,
        kv_explain: render_explain(&kv_explain),
        log_explain: render_explain(&log_explain),
    };

    drop_demo_tables(&connection).await;
    Ok(summary)
}

async fn create_kv_demo_table(connection: &FlussConnection) -> DemoResult<()> {
    let table_path = TablePath::new(DATABASE, KV_TABLE);
    let admin = connection.get_admin()?;
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .column("age", DataTypes::bigint())
                .primary_key(vec!["id"])
                .build()?,
        )
        .build()?;
    admin.create_table(&table_path, &descriptor, true).await?;

    let table = connection.get_table(&table_path).await?;
    let writer = table.new_upsert()?.create_writer()?;
    let rows = [(1, "Verso", 32i64), (2, "Noco", 25), (3, "Esquie", 35)];
    for (id, name, age) in rows {
        let mut row = GenericRow::new(3);
        row.set_field(0, id);
        row.set_field(1, name);
        row.set_field(2, age);
        writer.upsert(&row)?;
    }
    writer.flush().await?;
    Ok(())
}

async fn create_log_demo_table(connection: &FlussConnection) -> DemoResult<()> {
    let table_path = TablePath::new(DATABASE, LOG_TABLE);
    let admin = connection.get_admin()?;
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("action", DataTypes::string())
                .build()?,
        )
        .distributed_by(Some(1), vec![])
        .build()?;
    admin.create_table(&table_path, &descriptor, true).await?;

    let table = connection.get_table(&table_path).await?;
    let writer = table.new_append()?.create_writer()?;
    let rows = [(1, "open"), (2, "click"), (3, "scroll"), (4, "close")];
    for (id, action) in rows {
        let mut row = GenericRow::new(2);
        row.set_field(0, id);
        row.set_field(1, action);
        writer.append(&row)?;
    }
    writer.flush().await?;
    wait_for_log_offsets(connection, &table_path).await?;
    Ok(())
}

async fn wait_for_log_offsets(
    connection: &FlussConnection,
    table_path: &TablePath,
) -> DemoResult<()> {
    let admin = connection.get_admin()?;
    let start = Instant::now();
    loop {
        if admin.list_offsets(table_path, &[0], OffsetSpec::Latest).await.is_ok() {
            return Ok(());
        }
        if start.elapsed() >= READY_TIMEOUT {
            return Err(io::Error::other(format!(
                "table {table_path} bucket 0 not ready in {READY_TIMEOUT:?}"
            ))
            .into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn drop_demo_tables(connection: &FlussConnection) {
    let admin = match connection.get_admin() {
        Ok(admin) => admin,
        Err(_) => return,
    };
    for name in [LOG_TABLE, KV_TABLE] {
        let _ = admin.drop_table(&TablePath::new(DATABASE, name), true).await;
    }
}

fn single_string(batches: &[RecordBatch], col: usize) -> DemoResult<String> {
    let values = collect_strings(batches, col)?;
    if values.len() != 1 {
        return Err(io::Error::other(format!(
            "expected one string value, got {}",
            values.len()
        ))
        .into());
    }
    Ok(values.into_iter().next().expect("checked one value exists"))
}

fn single_i64(batches: &[RecordBatch], col: usize) -> DemoResult<i64> {
    let mut values = Vec::new();
    for batch in batches {
        let arr = batch
            .column(col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| io::Error::other("expected int64 column"))?;
        values.extend(arr.values().iter().copied());
    }
    if values.len() != 1 {
        return Err(io::Error::other(format!(
            "expected one int64 value, got {}",
            values.len()
        ))
        .into());
    }
    Ok(values[0])
}

fn collect_i32(batches: &[RecordBatch], col: usize) -> DemoResult<Vec<i32>> {
    let mut values = Vec::new();
    for batch in batches {
        let arr = batch
            .column(col)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| io::Error::other("expected int32 column"))?;
        values.extend(arr.values().iter().copied());
    }
    Ok(values)
}

fn collect_strings(batches: &[RecordBatch], col: usize) -> DemoResult<Vec<String>> {
    let mut values = Vec::new();
    for batch in batches {
        let arr = batch
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| io::Error::other("expected string column"))?;
        for i in 0..arr.len() {
            values.push(arr.value(i).to_string());
        }
    }
    Ok(values)
}

fn render_explain(batches: &[RecordBatch]) -> String {
    let mut rendered = String::new();
    for batch in batches {
        for col in batch.columns() {
            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                for i in 0..arr.len() {
                    if arr.is_valid(i) {
                        rendered.push_str(arr.value(i));
                        rendered.push('\n');
                    }
                }
            }
        }
    }
    rendered
}
