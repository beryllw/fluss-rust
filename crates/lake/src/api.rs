// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under the
// Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
// License for the specific language governing permissions and limitations under
// the License.

//! Public `fluss-lake` read API.
//!
//! Public semantics intentionally follow:
//!
//! ```text
//! FlussLakeTable -> new_scan() -> FlussLakeScan -> plan() -> FlussLakeReadPlan
//!     -> new_read() -> FlussLakeRead -> read_split(...)
//! ```
//!
//! This keeps the Fluss-style `table -> scan` entry while making `plan()` an
//! explicit public boundary, following the same *value* as paimon-rust's
//! `scan -> plan -> read` layering: once-per-query planning, inspectable split
//! descriptors, and execute-many from a frozen plan.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use fluss::client::FlussConnection;
use fluss::metadata::{Schema, TableInfo, TablePath};
use fluss::rpc::message::OffsetSpec;
use futures::StreamExt;

use crate::append::{AppendSplitSpec, open_append_split};
use crate::config::LakeCatalogConfig;
use crate::error::{FlussLakeError, Result};
use crate::pk::{PkSplitSpec, open_pk_split};
use crate::reader::RecordBatchStream;
use crate::snapshot::LakeSeam;
use fluss::record::to_arrow_schema;

/// The internal execution task behind a public read split. append/log tables
/// stitch lake + log tail; primary-key tables merge lake current state with the
/// changelog tail. The public split descriptor never exposes which variant it is.
#[derive(Debug, Clone)]
enum ReadTask {
    Append(AppendSplitSpec),
    PrimaryKey(PkSplitSpec),
}

/// Public partition identity for a logical read split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlussLakePartitionIdentity {
    Unpartitioned,
    KeyValues(Vec<(String, String)>),
}

/// Batch-mode boundary selection for a lake read.
#[derive(Debug, Clone, Default)]
pub enum FlussLakeReadBoundaryMode {
    /// Read the latest readable snapshot. Current implementation falls back to
    /// the latest lake snapshot until a readable-snapshot RPC is wired in.
    #[default]
    LatestReadable,
    /// Read a specific lake snapshot id.
    SnapshotId(i64),
}

/// Public read mode. v1 only supports bounded batch reads.
#[derive(Debug, Clone)]
pub enum FlussLakeReadMode {
    Batch(FlussLakeReadBoundaryMode),
}

impl Default for FlussLakeReadMode {
    fn default() -> Self {
        Self::Batch(FlussLakeReadBoundaryMode::LatestReadable)
    }
}

/// Public scan options for `fluss-lake`.
#[derive(Debug, Clone, Default)]
pub struct FlussLakeReadOptions {
    pub batch_size: Option<usize>,
    pub mode: FlussLakeReadMode,
}

/// Public read filter. This is the final filter type; v1 only implements the
/// partition / bucket subset of it (a general value predicate is a future field
/// on this same type, not a rename).
#[derive(Debug, Clone, Default)]
pub struct FlussLakeFilter {
    pub partitions: Option<Vec<FlussLakePartitionFilter>>,
}

/// One partition entry: first select this partition, then select bucket ids
/// within it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlussLakePartitionFilter {
    pub partition: FlussLakePartitionIdentity,
    pub bucket_ids: Option<Vec<i32>>,
}

/// A Fluss table that is readable through the lake kernel.
#[derive(Clone)]
pub struct FlussLakeTable {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    schema: Schema,
    num_buckets: i32,
    partition_keys: Vec<String>,
    lake_catalog_properties: HashMap<String, String>,
}

impl FlussLakeTable {
    pub fn new(
        connection: Arc<FlussConnection>,
        table_path: TablePath,
        schema: Schema,
        num_buckets: i32,
        partition_keys: Vec<String>,
        lake_catalog_properties: HashMap<String, String>,
    ) -> Self {
        Self {
            connection,
            table_path,
            schema,
            num_buckets,
            partition_keys,
            lake_catalog_properties,
        }
    }

    /// Convenience constructor from Fluss `TableInfo`, extracting the lake
    /// catalog properties from the table config.
    pub fn try_from_table_info(
        connection: Arc<FlussConnection>,
        table_info: &TableInfo,
    ) -> Result<Self> {
        let lake_catalog_properties = table_info
            .get_table_config()
            .get_lake_catalog_properties()?
            .ok_or_else(|| {
                FlussLakeError::NotLakeReadable(format!(
                    "table {} has no table.datalake.* catalog properties",
                    table_info.get_table_path()
                ))
            })?;
        Ok(Self::new(
            connection,
            table_info.get_table_path().clone(),
            table_info.get_schema().clone(),
            table_info.get_num_buckets(),
            table_info.get_partition_keys().to_vec(),
            lake_catalog_properties,
        ))
    }

    pub fn new_scan(&self) -> FlussLakeScan {
        FlussLakeScan {
            connection: self.connection.clone(),
            table_path: self.table_path.clone(),
            schema: self.schema.clone(),
            num_buckets: self.num_buckets,
            partition_keys: self.partition_keys.clone(),
            lake_catalog_properties: self.lake_catalog_properties.clone(),
            projection: None,
            filter: None,
            options: FlussLakeReadOptions::default(),
        }
    }
}

/// A configurable scan request over a `FlussLakeTable`.
#[derive(Clone)]
pub struct FlussLakeScan {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    schema: Schema,
    num_buckets: i32,
    partition_keys: Vec<String>,
    lake_catalog_properties: HashMap<String, String>,
    projection: Option<Vec<usize>>,
    filter: Option<FlussLakeFilter>,
    options: FlussLakeReadOptions,
}

impl FlussLakeScan {
    pub fn with_projection(mut self, indices: Vec<usize>) -> Self {
        self.projection = Some(indices);
        self
    }

    pub fn with_filter(mut self, filter: FlussLakeFilter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_options(mut self, options: FlussLakeReadOptions) -> Self {
        self.options = options;
        self
    }

    pub async fn plan(&self) -> Result<FlussLakeReadPlan> {
        let is_pk = self.schema.primary_key().is_some();
        let seam = self.snapshot_boundary().await?;
        let targets = self.filter_targets()?;

        // Validate the catalog config early so planning fails fast, not on first read.
        let _ = LakeCatalogConfig::from_catalog_properties(&self.lake_catalog_properties)?;

        // Freeze the bounded-batch stop boundary at plan time: capture the latest
        // log offset per bucket now so the read is repeatable and does not absorb
        // rows appended between plan() and read_split(). v1 is non-partitioned, so
        // every target carries partition_id == None and one offset list suffices.
        let stop_offsets = self.frozen_stop_offsets(&targets).await?;

        let splits = targets
            .iter()
            .map(|&(partition_id, bucket)| {
                let log_start_offset = seam.seam_offset(partition_id, bucket).unwrap_or(0);
                let log_stop_offset = stop_offsets.get(&bucket).copied();
                let task = if is_pk {
                    ReadTask::PrimaryKey(PkSplitSpec {
                        partition_id,
                        bucket,
                        snapshot_id: seam.snapshot_id(),
                        log_start_offset,
                        // No tiered offset for an empty bucket => empty tail.
                        log_stop_offset: log_stop_offset.unwrap_or(log_start_offset),
                    })
                } else {
                    ReadTask::Append(AppendSplitSpec {
                        partition_id,
                        bucket,
                        snapshot_id: seam.snapshot_id(),
                        log_start_offset,
                        log_stop_offset,
                    })
                };
                FlussLakeReadSplit {
                    split_id: format!(
                        "{}:{}:{}",
                        self.table_path,
                        partition_id
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "-".to_string()),
                        bucket
                    ),
                    bucket_id: bucket,
                    partition: FlussLakePartitionIdentity::Unpartitioned,
                    estimated_rows: None,
                    estimated_size: None,
                    task,
                }
            })
            .collect();

        let (projected_schema, _names) =
            projected_schema_and_names(&self.schema, self.projection.as_deref())?;
        Ok(FlussLakeReadPlan {
            inner: Arc::new(FlussLakeReadPlanInner {
                schema: projected_schema,
                splits,
                mode: self.options.mode.clone(),
            }),
        })
    }

    /// Captures the exclusive latest log offset per target bucket at plan time,
    /// freezing the bounded-batch boundary. v1 only handles non-partitioned
    /// tables, so all targets share `partition_id == None`.
    async fn frozen_stop_offsets(
        &self,
        targets: &[(Option<i64>, i32)],
    ) -> Result<HashMap<i32, i64>> {
        if targets.is_empty() {
            return Ok(HashMap::new());
        }
        let buckets: Vec<i32> = targets.iter().map(|(_, b)| *b).collect();
        let admin = self.connection.get_admin()?;
        let latest = admin
            .list_offsets(&self.table_path, &buckets, OffsetSpec::Latest)
            .await?;
        Ok(latest)
    }

    pub fn new_read(&self) -> Result<FlussLakeRead> {
        Ok(FlussLakeRead {
            connection: self.connection.clone(),
            table_path: self.table_path.clone(),
            lake_catalog_properties: self.lake_catalog_properties.clone(),
            schema: self.schema.clone(),
            projection: self.projection.clone(),
        })
    }

    fn filter_targets(&self) -> Result<Vec<(Option<i64>, i32)>> {
        resolve_filter_targets(&self.filter, self.num_buckets, &self.partition_keys)
    }

    async fn snapshot_boundary(&self) -> Result<LakeSeam> {
        #[cfg(feature = "integration_tests")]
        if let Some(seam) = crate::test_overrides::get_test_lake_seam_override(&self.table_path) {
            return Ok(seam);
        }

        let admin = self.connection.get_admin()?;
        let snapshot = match self.options.mode {
            FlussLakeReadMode::Batch(FlussLakeReadBoundaryMode::LatestReadable) => {
                // TODO: switch to readable snapshot RPC when wired in.
                admin.get_latest_lake_snapshot(&self.table_path).await?
            }
            FlussLakeReadMode::Batch(FlussLakeReadBoundaryMode::SnapshotId(snapshot_id)) => {
                // TODO: support fetching a specific historical lake snapshot once
                // the Rust client exposes that RPC path.
                let latest = admin.get_latest_lake_snapshot(&self.table_path).await?;
                if latest.snapshot_id() != snapshot_id {
                    return Err(FlussLakeError::Internal(format!(
                        "snapshot mode SnapshotId({snapshot_id}) is not yet implemented in fluss-rust"
                    )));
                }
                latest
            }
        };
        Ok(LakeSeam::from_lake_snapshot(&snapshot))
    }
}

fn projected_schema_and_names(
    schema: &Schema,
    projection: Option<&[usize]>,
) -> Result<(SchemaRef, Option<Vec<String>>)> {
    match projection {
        Some(indices) => {
            let fields = indices
                .iter()
                .map(|&idx| {
                    schema.columns().get(idx).cloned().ok_or_else(|| {
                        FlussLakeError::Internal(format!("projection index {idx} out of bounds"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let projected = Schema::builder()
                .with_columns(fields)
                .build()
                .map_err(|e| {
                    FlussLakeError::Internal(format!("failed to build projected schema: {e}"))
                })?;
            let names = indices
                .iter()
                .map(|&idx| schema.columns()[idx].name().to_string())
                .collect();
            Ok((to_arrow_schema(projected.row_type())?, Some(names)))
        }
        None => Ok((to_arrow_schema(schema.row_type())?, None)),
    }
}

/// Resolves a [`FlussLakeFilter`] into the concrete `(partition_id, bucket)`
/// targets to read. v1 is non-partitioned only.
///
/// - no filter / `partitions = None`  -> all buckets
/// - `partitions = Some([])`          -> empty (no target)
/// - each entry's `bucket_ids = None` -> all buckets; `Some(ids)` -> those buckets
/// - any partitioned table or non-`Unpartitioned` identity -> error (v1)
fn resolve_filter_targets(
    filter: &Option<FlussLakeFilter>,
    num_buckets: i32,
    partition_keys: &[String],
) -> Result<Vec<(Option<i64>, i32)>> {
    if !partition_keys.is_empty() {
        return Err(FlussLakeError::Internal(
            "partitioned lake reads are not yet implemented".to_string(),
        ));
    }
    let all = || (0..num_buckets).map(|b| (None, b)).collect::<Vec<_>>();
    match filter.as_ref().and_then(|f| f.partitions.as_ref()) {
        None => Ok(all()),
        Some(entries) => {
            let mut out = Vec::new();
            for entry in entries {
                if entry.partition != FlussLakePartitionIdentity::Unpartitioned {
                    return Err(FlussLakeError::Internal(
                        "partitioned lake read filter is not yet implemented".to_string(),
                    ));
                }
                match &entry.bucket_ids {
                    None => out.extend((0..num_buckets).map(|b| (None, b))),
                    Some(ids) => out.extend(ids.iter().copied().map(|b| (None, b))),
                }
            }
            Ok(out)
        }
    }
}

struct FlussLakeReadPlanInner {
    schema: SchemaRef,
    splits: Vec<FlussLakeReadSplit>,
    mode: FlussLakeReadMode,
}

/// Public logical read split descriptor.
///
/// This is NOT a physical Paimon `DataSplit`. It is the kernel's stable
/// execution-unit descriptor exposed to engines and other consumers. Internally
/// one `FlussLakeReadSplit` may map to multiple lake `DataSplit`s plus a Fluss
/// log tail.
#[derive(Debug, Clone)]
pub struct FlussLakeReadSplit {
    pub split_id: String,
    pub bucket_id: i32,
    pub partition: FlussLakePartitionIdentity,
    pub estimated_rows: Option<usize>,
    pub estimated_size: Option<usize>,
    task: ReadTask,
}

/// A frozen, inspectable read plan.
#[derive(Clone)]
pub struct FlussLakeReadPlan {
    inner: Arc<FlussLakeReadPlanInner>,
}

impl FlussLakeReadPlan {
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema.clone()
    }

    pub fn split_count(&self) -> usize {
        self.inner.splits.len()
    }

    pub fn splits(&self) -> &[FlussLakeReadSplit] {
        &self.inner.splits
    }

    pub fn statistics(&self) -> FlussLakePlanStatistics {
        FlussLakePlanStatistics {
            split_count: self.inner.splits.len(),
            mode: self.inner.mode.clone(),
        }
    }
}

/// Lightweight plan statistics for engine/consumer visibility.
#[derive(Clone)]
pub struct FlussLakePlanStatistics {
    pub split_count: usize,
    pub mode: FlussLakeReadMode,
}

/// Read executor over a previously configured scan.
#[derive(Clone)]
pub struct FlussLakeRead {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    lake_catalog_properties: HashMap<String, String>,
    schema: Schema,
    projection: Option<Vec<usize>>,
}

impl FlussLakeRead {
    pub async fn read_split(&self, split: &FlussLakeReadSplit) -> Result<RecordBatchStream> {
        match &split.task {
            ReadTask::Append(spec) => {
                let (projected_schema, projected_column_names) =
                    projected_schema_and_names(&self.schema, self.projection.as_deref())?;
                open_append_split(
                    self.connection.clone(),
                    &self.table_path,
                    &self.lake_catalog_properties,
                    projected_schema,
                    projected_column_names.as_deref(),
                    self.projection.clone(),
                    spec,
                )
                .await
            }
            ReadTask::PrimaryKey(spec) => {
                open_pk_split(
                    self.connection.clone(),
                    &self.table_path,
                    &self.lake_catalog_properties,
                    &self.schema,
                    self.projection.clone(),
                    spec,
                )
                .await
            }
        }
    }

    pub async fn read_splits(&self, splits: &[FlussLakeReadSplit]) -> Result<RecordBatchStream> {
        if splits.is_empty() {
            return Ok(futures::stream::empty().boxed());
        }
        if splits.len() == 1 {
            return self.read_split(&splits[0]).await;
        }
        let mut streams = Vec::with_capacity(splits.len());
        for split in splits {
            streams.push(self.read_split(split).await?);
        }
        Ok(futures::stream::select_all(streams).boxed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pf(bucket_ids: Option<Vec<i32>>) -> FlussLakePartitionFilter {
        FlussLakePartitionFilter {
            partition: FlussLakePartitionIdentity::Unpartitioned,
            bucket_ids,
        }
    }

    #[test]
    fn no_filter_selects_all_buckets() {
        let targets = resolve_filter_targets(&None, 3, &[]).unwrap();
        assert_eq!(targets, vec![(None, 0), (None, 1), (None, 2)]);
    }

    #[test]
    fn filter_with_no_partitions_selects_all_buckets() {
        let filter = FlussLakeFilter { partitions: None };
        let targets = resolve_filter_targets(&Some(filter), 2, &[]).unwrap();
        assert_eq!(targets, vec![(None, 0), (None, 1)]);
    }

    #[test]
    fn empty_partitions_select_nothing() {
        let filter = FlussLakeFilter {
            partitions: Some(vec![]),
        };
        let targets = resolve_filter_targets(&Some(filter), 3, &[]).unwrap();
        assert!(targets.is_empty());
    }

    #[test]
    fn explicit_bucket_ids_select_those_buckets() {
        let filter = FlussLakeFilter {
            partitions: Some(vec![pf(Some(vec![2, 0]))]),
        };
        let targets = resolve_filter_targets(&Some(filter), 4, &[]).unwrap();
        assert_eq!(targets, vec![(None, 2), (None, 0)]);
    }

    #[test]
    fn entry_with_none_bucket_ids_selects_all_buckets() {
        let filter = FlussLakeFilter {
            partitions: Some(vec![pf(None)]),
        };
        let targets = resolve_filter_targets(&Some(filter), 2, &[]).unwrap();
        assert_eq!(targets, vec![(None, 0), (None, 1)]);
    }

    #[test]
    fn partitioned_table_is_rejected() {
        assert!(resolve_filter_targets(&None, 2, &["dt".to_string()]).is_err());
    }

    #[test]
    fn key_valued_partition_filter_is_rejected() {
        let filter = FlussLakeFilter {
            partitions: Some(vec![FlussLakePartitionFilter {
                partition: FlussLakePartitionIdentity::KeyValues(vec![(
                    "dt".to_string(),
                    "2024".to_string(),
                )]),
                bucket_ids: None,
            }]),
        };
        assert!(resolve_filter_targets(&Some(filter), 2, &[]).is_err());
    }
}
