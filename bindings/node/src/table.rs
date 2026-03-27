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
use crate::lookup::Lookuper;
use crate::metadata::{TableInfo, TablePath};
use crate::row::json_to_generic_row;
use crate::scanner::LogScanner;
use crate::upsert::UpsertWriter;
use crate::write_handle::WriteResultHandle;
use crate::TOKIO_RUNTIME;
use napi_derive::napi;
use std::sync::Arc;

#[napi]
pub struct FlussTable {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
    table_path: fcore::metadata::TablePath,
    has_pk: bool,
}

#[napi]
impl FlussTable {
    #[napi(getter)]
    pub fn table_path(&self) -> TablePath {
        TablePath::from_core(&self.table_path)
    }

    #[napi(getter)]
    pub fn has_primary_key(&self) -> bool {
        self.has_pk
    }

    #[napi]
    pub fn get_table_info(&self) -> TableInfo {
        TableInfo::from_core(self.table_info.clone())
    }

    #[napi]
    pub fn new_append(&self) -> napi::Result<TableAppend> {
        let fluss_table = fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );
        let table_append = fluss_table
            .new_append()
            .map_err(|e| from_core_error(&e))?;
        Ok(TableAppend {
            inner: table_append,
            table_info: self.table_info.clone(),
        })
    }

    #[napi]
    pub fn new_upsert(&self) -> napi::Result<TableUpsert> {
        if !self.has_pk {
            return Err(client_error(
                "Upsert is only supported for primary key tables",
            ));
        }
        let fluss_table = fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );
        let table_upsert = fluss_table
            .new_upsert()
            .map_err(|e| from_core_error(&e))?;
        Ok(TableUpsert {
            inner: table_upsert,
            table_info: self.table_info.clone(),
            target_columns: None,
        })
    }

    #[napi]
    pub fn new_lookup(&self) -> napi::Result<TableLookup> {
        if !self.has_pk {
            return Err(client_error(
                "Lookup is only supported for primary key tables",
            ));
        }
        Ok(TableLookup {
            connection: self.connection.clone(),
            metadata: self.metadata.clone(),
            table_info: self.table_info.clone(),
        })
    }

    #[napi]
    pub fn new_scan(&self) -> TableScan {
        TableScan {
            connection: self.connection.clone(),
            metadata: self.metadata.clone(),
            table_info: self.table_info.clone(),
            projected_columns: None,
        }
    }
}

impl FlussTable {
    pub fn new_table(
        connection: Arc<fcore::client::FlussConnection>,
        metadata: Arc<fcore::client::Metadata>,
        table_info: fcore::metadata::TableInfo,
        table_path: fcore::metadata::TablePath,
        has_pk: bool,
    ) -> Self {
        Self {
            connection,
            metadata,
            table_info,
            table_path,
            has_pk,
        }
    }
}

// --- TableAppend ---

#[napi]
pub struct TableAppend {
    inner: fcore::client::TableAppend,
    table_info: fcore::metadata::TableInfo,
}

#[napi]
impl TableAppend {
    #[napi]
    pub fn create_writer(&self) -> napi::Result<AppendWriter> {
        let writer = self
            .inner
            .create_writer()
            .map_err(|e| from_core_error(&e))?;
        Ok(AppendWriter {
            writer: Arc::new(writer),
            table_info: self.table_info.clone(),
        })
    }
}

// --- AppendWriter ---

#[napi]
pub struct AppendWriter {
    writer: Arc<fcore::client::AppendWriter>,
    table_info: fcore::metadata::TableInfo,
}

#[napi]
impl AppendWriter {
    #[napi]
    pub fn append(&self, row: serde_json::Value) -> napi::Result<WriteResultHandle> {
        let generic_row = json_to_generic_row(&row, &self.table_info)?;
        let result_future = self
            .writer
            .append(&generic_row)
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

// --- TableUpsert ---

#[napi]
pub struct TableUpsert {
    inner: fcore::client::TableUpsert,
    table_info: fcore::metadata::TableInfo,
    target_columns: Option<Vec<usize>>,
}

#[napi]
impl TableUpsert {
    #[napi]
    pub fn partial_update_by_name(&self, columns: Vec<String>) -> napi::Result<TableUpsert> {
        let new_inner = self
            .inner
            .partial_update_with_column_names(
                &columns.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )
            .map_err(|e| from_core_error(&e))?;

        // Resolve column names to indices
        let schema = self.table_info.get_schema();
        let all_columns = schema.columns();
        let mut indices = Vec::new();
        for name in &columns {
            let idx = all_columns
                .iter()
                .position(|c| c.name() == name)
                .ok_or_else(|| client_error(format!("Column not found: {name}")))?;
            indices.push(idx);
        }

        Ok(TableUpsert {
            inner: new_inner,
            table_info: self.table_info.clone(),
            target_columns: Some(indices),
        })
    }

    #[napi]
    pub fn partial_update_by_index(&self, indices: Vec<u32>) -> napi::Result<TableUpsert> {
        let usize_indices: Vec<usize> = indices.iter().map(|&i| i as usize).collect();
        let new_inner = self
            .inner
            .partial_update(Some(usize_indices.clone()))
            .map_err(|e| from_core_error(&e))?;

        Ok(TableUpsert {
            inner: new_inner,
            table_info: self.table_info.clone(),
            target_columns: Some(usize_indices),
        })
    }

    #[napi]
    pub fn create_writer(&self) -> napi::Result<UpsertWriter> {
        UpsertWriter::new(
            &self.inner,
            self.table_info.clone(),
            self.target_columns.clone(),
        )
    }
}

// --- TableLookup ---

#[napi]
pub struct TableLookup {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
}

#[napi]
impl TableLookup {
    #[napi]
    pub fn create_lookuper(&self) -> napi::Result<Lookuper> {
        Lookuper::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        )
    }
}

// --- TableScan ---

#[napi]
pub struct TableScan {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
    projected_columns: Option<Vec<usize>>,
}

#[napi]
impl TableScan {
    #[napi]
    pub fn project(&mut self, indices: Vec<u32>) -> &Self {
        self.projected_columns = Some(indices.into_iter().map(|i| i as usize).collect());
        self
    }

    #[napi]
    pub fn project_by_name(&mut self, names: Vec<String>) -> napi::Result<&Self> {
        let schema = self.table_info.get_schema();
        let columns = schema.columns();
        let mut indices = Vec::new();
        for name in &names {
            let idx = columns
                .iter()
                .position(|c| c.name() == name)
                .ok_or_else(|| client_error(format!("Column not found: {name}")))?;
            indices.push(idx);
        }
        self.projected_columns = Some(indices);
        Ok(self)
    }

    #[napi]
    pub fn create_log_scanner(&self) -> napi::Result<LogScanner> {
        let fluss_table = fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );
        let mut scan = fluss_table.new_scan();
        if let Some(cols) = &self.projected_columns {
            scan = scan.project(cols).map_err(|e| from_core_error(&e))?;
        }

        let core_scanner = scan
            .create_log_scanner()
            .map_err(|e| from_core_error(&e))?;

        // Determine projected row type
        let projected_row_type = if let Some(cols) = &self.projected_columns {
            let schema = self.table_info.get_schema();
            let all_columns = schema.columns();
            let fields: Vec<fcore::metadata::DataField> = cols
                .iter()
                .map(|&i| {
                    let col = &all_columns[i];
                    fcore::metadata::DataTypes::field(col.name(), col.data_type().clone())
                })
                .collect();
            fcore::metadata::RowType::new(fields)
        } else {
            self.table_info.get_schema().row_type().clone()
        };

        Ok(LogScanner::new(
            core_scanner,
            self.table_info.clone(),
            projected_row_type,
        ))
    }
}
