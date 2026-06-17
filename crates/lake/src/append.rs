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

//! High-level append-union kernel API.
//!
//! This is the M1-complete facade the connector consumes:
//! 1. plan the append union from a Fluss table + lake seam into one
//!    [`UnionScanPlan`] (one bucket => one execution partition);
//! 2. open one partition as a `lake ++ log_tail` Arrow stream.
//!
//! The kernel does not own engine concerns such as residual filters or final
//! `LIMIT`; it only resolves the per-bucket lake/log split and emits streams
//! aligned to the projected Fluss schema.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use fluss::client::FlussConnection;
use fluss::metadata::{Schema, TableInfo, TablePath};
use fluss::record::to_arrow_schema;

use crate::catalog::{get_table_at_snapshot, open_catalog};
use crate::config::LakeCatalogConfig;
use crate::error::{FlussLakeError, Result};
use crate::plan::UnionScanPlan;
use crate::reader::RecordBatchStream;
use crate::reader::lake::read_lake_table;
use crate::reader::log::FlussLogTailReader;
use crate::snapshot::LakeSeam;
use crate::union::{UnionPartition, union_append_partition};

/// Plans an append-table lake+log union scan for `table_info` using the given
/// lake seam.
///
/// The caller supplies the target buckets it wants to read (typically one per
/// Fluss/DataFusion execution partition) as `(partition_id, bucket)` pairs. The
/// planner attaches each target's seam offset; missing seams mean "no tiered
/// data for this bucket yet", so the log starts at offset 0 and the lake side
/// is empty for that bucket.
pub fn plan_append_union(
    table_info: &TableInfo,
    lake_catalog_properties: &std::collections::HashMap<String, String>,
    seam: &LakeSeam,
    projected_column_indices: Option<Vec<usize>>,
    targets: Vec<(Option<i64>, i32)>,
) -> Result<UnionScanPlan> {
    let full_schema = table_info.get_schema();
    let projected_schema = fluss_projected_arrow_schema(full_schema, projected_column_indices.as_deref())?;
    let projected_column_names = fluss_projected_column_names(full_schema, projected_column_indices.as_deref());

    let partitions = targets
        .into_iter()
        .map(|(partition_id, bucket)| UnionPartition {
            partition_id,
            bucket,
            log_start_offset: seam.seam_offset(partition_id, bucket).unwrap_or(0),
            log_stop_offset: None,
        })
        .collect();

    // Validate the catalog config early so the engine fails at plan time, not
    // when the first partition opens. The resulting Options are re-built on open.
    let _ = LakeCatalogConfig::from_catalog_properties(lake_catalog_properties)?;

    Ok(UnionScanPlan::new(
        partitions,
        projected_schema,
        projected_column_names,
        projected_column_indices,
    ))
}

/// Opens one planned append-union partition as a `lake ++ log_tail` stream.
///
/// The table is opened in Paimon at `seam.snapshot_id()`, projected by column
/// name, then concatenated with the Fluss log tail starting at the partition's
/// seam offset.
pub async fn open_append_partition(
    connection: Arc<FlussConnection>,
    table_info: &TableInfo,
    lake_catalog_properties: &std::collections::HashMap<String, String>,
    seam: &LakeSeam,
    plan: &UnionScanPlan,
    partition_idx: usize,
) -> Result<RecordBatchStream> {
    let partition = plan.partitions.get(partition_idx).ok_or_else(|| {
        FlussLakeError::Internal(format!("partition index {partition_idx} out of bounds"))
    })?;

    let catalog = open_catalog(&LakeCatalogConfig::from_catalog_properties(
        lake_catalog_properties,
    )?)
    .await?;
    let table_path = table_info.get_table_path();
    let lake_table = get_table_at_snapshot(
        &catalog,
        table_path.database(),
        table_path.table(),
        seam.snapshot_id(),
    )
    .await?;
    let lake_stream = read_lake_table(&lake_table, plan.projected_column_names.as_deref()).await?;

    let log_reader = FlussLogTailReader::new(connection, TablePath::new(table_path.database(), table_path.table()));
    union_append_partition(
        lake_stream,
        &log_reader,
        partition,
        plan.projected_schema.clone(),
        plan.projected_column_indices.clone(),
    )
    .await
}

fn fluss_projected_arrow_schema(
    schema: &Schema,
    projection: Option<&[usize]>,
) -> Result<SchemaRef> {
    if let Some(indices) = projection {
        let fields = indices
            .iter()
            .map(|&idx| {
                schema.columns().get(idx).cloned().ok_or_else(|| {
                    FlussLakeError::Internal(format!("projection index {idx} out of bounds"))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let projected = Schema::builder().with_columns(fields).build().map_err(|e| {
            FlussLakeError::Internal(format!("failed to build projected schema: {e}"))
        })?;
        Ok(to_arrow_schema(projected.row_type())?)
    } else {
        Ok(to_arrow_schema(schema.row_type())?)
    }
}

fn fluss_projected_column_names(schema: &Schema, projection: Option<&[usize]>) -> Option<Vec<String>> {
    projection.map(|indices| {
        indices
            .iter()
            .map(|&idx| schema.columns()[idx].name().to_string())
            .collect()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::{DataTypes, TableBucket};

    fn sample_table_info() -> TableInfo {
        let table_path = TablePath::new("db", "t");
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .build()
            .unwrap();
        TableInfo::new(
            table_path,
            1,
            1,
            schema,
            Vec::new(),
            Arc::from(Vec::<String>::new()),
            2,
            std::collections::HashMap::new(),
            std::collections::HashMap::new(),
            None,
            0,
            0,
        )
    }

    #[test]
    fn plans_targets_and_defaults_missing_seams_to_zero() {
        let table_info = sample_table_info();
        let mut offsets = std::collections::HashMap::new();
        offsets.insert(TableBucket::new(1, 0), 123);
        let seam = LakeSeam::from_lake_snapshot(&fluss::metadata::LakeSnapshot::new(7, offsets));
        let props = std::collections::HashMap::from([("warehouse".to_string(), "/tmp/wh".to_string())]);
        let plan = plan_append_union(&table_info, &props, &seam, None, vec![(None, 0), (None, 1)]).unwrap();

        assert_eq!(plan.partitions.len(), 2);
        assert_eq!(plan.partitions[0].log_start_offset, 123);
        assert_eq!(plan.partitions[1].log_start_offset, 0, "missing seam => start from earliest");
        assert_eq!(plan.projected_column_names, None);
    }

    #[test]
    fn projection_maps_indices_to_names_and_schema() {
        let table_info = sample_table_info();
        let seam = LakeSeam::from_lake_snapshot(&fluss::metadata::LakeSnapshot::new(
            1,
            std::collections::HashMap::new(),
        ));
        let props = std::collections::HashMap::from([("warehouse".to_string(), "/tmp/wh".to_string())]);
        let plan =
            plan_append_union(&table_info, &props, &seam, Some(vec![1]), vec![(None, 0)]).unwrap();
        assert_eq!(plan.projected_column_names, Some(vec!["name".to_string()]));
        assert_eq!(plan.projected_schema.fields().len(), 1);
        assert_eq!(plan.projected_schema.field(0).name(), "name");
    }
}
