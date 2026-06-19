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

//! Append/log split execution: open one bucket as a `lake ++ log_tail` stream.
//!
//! The lake snapshot covers `[earliest, seam)` for the bucket; the Fluss log tail
//! covers `[seam, stop)`. Because the per-bucket seam is an exact cut, the union
//! is a plain concatenation — no row is read twice and no dedup is needed. This
//! is the append counterpart to the primary-key merge in [`crate::pk`].

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use fluss::client::FlussConnection;
use fluss::metadata::TablePath;

use crate::catalog::{get_table_at_snapshot, open_catalog};
use crate::config::LakeCatalogConfig;
use crate::error::{FlussLakeError, Result};
use crate::reader::RecordBatchStream;
use crate::reader::lake::read_lake_table;
use crate::reader::log::FlussLogTailReader;
use crate::union::{UnionPartition, union_append_partition};

/// Internal append/log execution spec for one logical read split (one bucket).
#[derive(Debug, Clone)]
pub(crate) struct AppendSplitSpec {
    pub partition_id: Option<i64>,
    pub bucket: i32,
    pub snapshot_id: i64,
    /// Inclusive log start (the lake seam).
    pub log_start_offset: i64,
    /// Exclusive log stop, frozen at plan time (`None` = read to latest at exec).
    pub log_stop_offset: Option<i64>,
}

/// Opens one append/log logical read split as a `lake ++ log_tail` stream.
///
/// The Paimon table is opened at `split.snapshot_id`, projected by column name
/// (which also drops lake system columns), then concatenated with the Fluss log
/// tail from the seam offset.
pub(crate) async fn open_append_split(
    connection: Arc<FlussConnection>,
    table_path: &TablePath,
    lake_catalog_properties: &HashMap<String, String>,
    projected_schema: SchemaRef,
    projected_column_names: Option<&[String]>,
    projected_column_indices: Option<Vec<usize>>,
    split: &AppendSplitSpec,
) -> Result<RecordBatchStream> {
    // Partitioned lake union is not supported yet: the lake read is filtered by
    // bucket only, so a partitioned table (where buckets repeat across
    // partitions) would mix partitions. Reject rather than return wrong rows.
    if split.partition_id.is_some() {
        return Err(FlussLakeError::Internal(
            "lake union read of partitioned tables is not yet supported".to_string(),
        ));
    }

    let catalog = open_catalog(&LakeCatalogConfig::from_catalog_properties(
        lake_catalog_properties,
    )?)
    .await?;
    let lake_table = get_table_at_snapshot(
        &catalog,
        table_path.database(),
        table_path.table(),
        split.snapshot_id,
    )
    .await?;
    // Restrict the lake read to this split's bucket so each execution partition
    // reads only its own lake data instead of the whole snapshot.
    let lake_stream =
        read_lake_table(&lake_table, projected_column_names, Some(split.bucket)).await?;

    let log_reader = FlussLogTailReader::new(
        connection,
        TablePath::new(table_path.database(), table_path.table()),
    );
    let partition = UnionPartition {
        partition_id: split.partition_id,
        bucket: split.bucket,
        log_start_offset: split.log_start_offset,
        log_stop_offset: split.log_stop_offset,
    };
    union_append_partition(
        lake_stream,
        &log_reader,
        &partition,
        projected_schema,
        projected_column_indices,
    )
    .await
}
