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
//! `FlussLakeScan` is the shared, immutable configuration source for both
//! `plan()` and `new_read()` (mirroring Paimon's `ReadBuilder`). The public
//! split ([`FlussLakeReadSplit`]) is the only distributable, serializable value
//! object; it carries an opaque execution descriptor and no full scan config. In
//! a distributed setting the engine ships the serializable [`FlussLakeScanConfig`]
//! separately to the workers.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use fluss::client::FlussConnection;
use fluss::metadata::{Schema, TableInfo, TablePath};
use fluss::record::to_arrow_schema;
use fluss::rpc::message::OffsetSpec;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use crate::append::{AppendSplitSpec, open_append_split};
use crate::config::LakeCatalogConfig;
use crate::error::{FlussLakeError, Result};
use crate::pk::{PkSplitSpec, open_pk_split};
use crate::predicate::{FlussLakePredicate, apply_exact_filter, decompose};
use crate::reader::RecordBatchStream;
use crate::snapshot::LakeSeam;

/// Wire-format version of the opaque execution descriptor carried in a split.
const DESCRIPTOR_VERSION: u32 = 1;

/// The internal execution task behind a public read split. append/log tables
/// stitch lake + log tail; primary-key tables merge lake current state with the
/// changelog tail. The public split descriptor never exposes which variant it is.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ReadTask {
    Append(AppendSplitSpec),
    PrimaryKey(PkSplitSpec),
}

/// Public partition identity for a logical read split.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlussLakePartitionIdentity {
    Unpartitioned,
    KeyValues(Vec<(String, String)>),
}

/// Serializable scan configuration shared by `plan()` and `new_read()`.
///
/// Everything needed to interpret a split lives here; the runtime
/// [`FlussConnection`] is held separately on [`FlussLakeScan`] / [`FlussLakeRead`]
/// (it is not serializable). Engines ship this config to workers to reconstruct
/// an equivalent scan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlussLakeScanConfig {
    pub table_path: TablePath,
    pub schema: Schema,
    pub num_buckets: i32,
    pub partition_keys: Vec<String>,
    pub lake_catalog_properties: HashMap<String, String>,
    /// Output projection as column indices into the Fluss schema; `None` = all.
    pub projection: Option<Vec<usize>>,
    /// Unified predicate; `AlwaysTrue` means no filtering.
    pub filter: FlussLakePredicate,
    /// `RecordBatch` row-count hint; `None` = reader default.
    pub batch_size: Option<usize>,
    /// When true, read ONLY the lake snapshot (no Fluss log tail).
    pub lake_only: bool,
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

    /// Recommended entry: fetches table metadata and validates lake-readability.
    pub async fn open(connection: Arc<FlussConnection>, table_path: &TablePath) -> Result<Self> {
        let admin = connection.get_admin()?;
        let table_info = admin.get_table_info(table_path).await?;
        Self::try_from_table_info(connection, &table_info)
    }

    /// Low-level entry for engine adapters that already hold `TableInfo`.
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
            config: FlussLakeScanConfig {
                table_path: self.table_path.clone(),
                schema: self.schema.clone(),
                num_buckets: self.num_buckets,
                partition_keys: self.partition_keys.clone(),
                lake_catalog_properties: self.lake_catalog_properties.clone(),
                projection: None,
                filter: FlussLakePredicate::AlwaysTrue,
                batch_size: None,
                lake_only: false,
            },
        }
    }
}

/// A configurable scan request over a `FlussLakeTable`. `with_*` consume `self`
/// so a scan can be cloned and reused.
#[derive(Clone)]
pub struct FlussLakeScan {
    connection: Arc<FlussConnection>,
    config: FlussLakeScanConfig,
}

impl FlussLakeScan {
    /// Projects by column index into the Fluss schema.
    pub fn with_projection(mut self, indices: Vec<usize>) -> Self {
        self.config.projection = Some(indices);
        self
    }

    /// Projects by column name (resolved against the Fluss schema). Names not
    /// found resolve to an out-of-range index, surfaced as an error at `plan()`.
    pub fn with_projection_by_names(mut self, columns: Vec<String>) -> Self {
        let indices = columns
            .iter()
            .map(|name| {
                self.config
                    .schema
                    .columns()
                    .iter()
                    .position(|c| c.name() == name)
                    .unwrap_or(usize::MAX)
            })
            .collect();
        self.config.projection = Some(indices);
        self
    }

    /// Sets the unified predicate. The SDK auto-decomposes it into partition
    /// pruning and exact row filtering.
    pub fn with_filter(mut self, filter: FlussLakePredicate) -> Self {
        self.config.filter = filter;
        self
    }

    /// Sets the target `RecordBatch` row count.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.batch_size = Some(batch_size);
        self
    }

    /// Reads only the lake snapshot (Paimon current state), excluding the
    /// residual Fluss log tail.
    pub fn with_lake_only(mut self, lake_only: bool) -> Self {
        self.config.lake_only = lake_only;
        self
    }

    /// The shared, serializable scan configuration (for distributed dispatch).
    pub fn config(&self) -> &FlussLakeScanConfig {
        &self.config
    }

    pub async fn plan(&self) -> Result<FlussLakeReadPlan> {
        let cfg = &self.config;
        let is_pk = cfg.schema.primary_key().is_some();
        let seam = self.snapshot_boundary().await?;

        // Validate the catalog config early so planning fails fast, not on first read.
        let _ = LakeCatalogConfig::from_catalog_properties(&cfg.lake_catalog_properties)?;

        // Decompose the filter; partition conjuncts prune partitions at plan time.
        let decomposed = decompose(&cfg.filter, &cfg.partition_keys);
        let targets = self.resolve_targets(&decomposed.partition).await?;

        // Freeze the bounded-batch stop boundary now so the read is repeatable.
        // Lake-only mode has no log tail to bound, so each split's stop is pinned
        // to its start (an empty `[seam, seam)` tail).
        let stop_offsets = if cfg.lake_only {
            HashMap::new()
        } else {
            self.frozen_stop_offsets(&targets).await?
        };

        let mut splits = Vec::with_capacity(targets.len());
        for target in &targets {
            let key = (target.partition_id, target.bucket);
            let log_start_offset = seam
                .seam_offset(target.partition_id, target.bucket)
                .unwrap_or(0);
            let log_stop_offset = if cfg.lake_only {
                Some(log_start_offset)
            } else {
                stop_offsets.get(&key).copied()
            };
            let task = if is_pk {
                ReadTask::PrimaryKey(PkSplitSpec {
                    partition_id: target.partition_id,
                    partition_values: target.partition_values.clone(),
                    bucket: target.bucket,
                    snapshot_id: seam.snapshot_id(),
                    log_start_offset,
                    log_stop_offset: log_stop_offset.unwrap_or(log_start_offset),
                })
            } else {
                ReadTask::Append(AppendSplitSpec {
                    partition_id: target.partition_id,
                    partition_values: target.partition_values.clone(),
                    bucket: target.bucket,
                    snapshot_id: seam.snapshot_id(),
                    log_start_offset,
                    log_stop_offset,
                })
            };
            splits.push(FlussLakeReadSplit {
                split_id: format!(
                    "{}:{}:{}",
                    cfg.table_path,
                    target
                        .partition_id
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    target.bucket
                ),
                bucket_id: target.bucket,
                partition: target.identity.clone(),
                estimated_rows: None,
                estimated_size: None,
                descriptor_version: DESCRIPTOR_VERSION,
                execution_descriptor: encode_task(&task)?,
            });
        }

        let projected_schema = projected_schema(&cfg.schema, cfg.projection.as_deref())?;
        Ok(FlussLakeReadPlan {
            inner: Arc::new(FlussLakeReadPlanInner {
                schema: projected_schema,
                splits,
            }),
        })
    }

    pub fn new_read(&self) -> Result<FlussLakeRead> {
        Ok(FlussLakeRead {
            connection: self.connection.clone(),
            config: self.config.clone(),
        })
    }

    /// Resolves the concrete read targets after partition pruning.
    async fn resolve_targets(
        &self,
        partition_conjuncts: &[FlussLakePredicate],
    ) -> Result<Vec<ReadTarget>> {
        let cfg = &self.config;
        let buckets: Vec<i32> = (0..cfg.num_buckets).collect();
        if cfg.partition_keys.is_empty() {
            return Ok(buckets
                .into_iter()
                .map(|bucket| ReadTarget {
                    partition_id: None,
                    partition_name: None,
                    partition_values: Vec::new(),
                    identity: FlussLakePartitionIdentity::Unpartitioned,
                    bucket,
                })
                .collect());
        }

        let admin = self.connection.get_admin()?;
        let partitions = admin.list_partition_infos(&cfg.table_path).await?;
        let mut targets = Vec::new();
        for info in partitions {
            let spec = info.get_resolved_partition_spec();
            let values: Vec<(String, String)> = spec
                .get_partition_keys()
                .iter()
                .cloned()
                .zip(spec.get_partition_values().iter().cloned())
                .collect();
            if !crate::predicate::partition_matches(partition_conjuncts, &values) {
                continue;
            }
            let partition_id = Some(info.get_partition_id());
            let partition_name = Some(info.get_partition_name());
            for &bucket in &buckets {
                targets.push(ReadTarget {
                    partition_id,
                    partition_name: partition_name.clone(),
                    partition_values: values.clone(),
                    identity: FlussLakePartitionIdentity::KeyValues(values.clone()),
                    bucket,
                });
            }
        }
        Ok(targets)
    }

    /// Captures the exclusive latest log offset per target at plan time, keyed by
    /// `(partition_id, bucket)`. Partitioned tables query per partition.
    async fn frozen_stop_offsets(
        &self,
        targets: &[ReadTarget],
    ) -> Result<HashMap<(Option<i64>, i32), i64>> {
        if targets.is_empty() {
            return Ok(HashMap::new());
        }
        let admin = self.connection.get_admin()?;
        let cfg = &self.config;

        // Group buckets by partition so each partition needs one RPC.
        let mut by_partition: HashMap<(Option<i64>, Option<String>), Vec<i32>> = HashMap::new();
        for t in targets {
            by_partition
                .entry((t.partition_id, t.partition_name.clone()))
                .or_default()
                .push(t.bucket);
        }

        let mut out = HashMap::new();
        for ((partition_id, partition_name), buckets) in by_partition {
            let latest = match &partition_name {
                Some(name) => {
                    admin
                        .list_partition_offsets(
                            &cfg.table_path,
                            name,
                            &buckets,
                            OffsetSpec::Latest,
                        )
                        .await?
                }
                None => {
                    admin
                        .list_offsets(&cfg.table_path, &buckets, OffsetSpec::Latest)
                        .await?
                }
            };
            for (bucket, offset) in latest {
                out.insert((partition_id, bucket), offset);
            }
        }
        Ok(out)
    }

    async fn snapshot_boundary(&self) -> Result<LakeSeam> {
        #[cfg(feature = "integration_tests")]
        if let Some(seam) =
            crate::test_overrides::get_test_lake_seam_override(&self.config.table_path)
        {
            return Ok(seam);
        }

        let admin = self.connection.get_admin()?;
        // v1 always reads the latest readable snapshot; historical snapshot
        // selection (`with_snapshot_id`) is a future extension.
        let snapshot = admin
            .get_latest_lake_snapshot(&self.config.table_path)
            .await?;
        Ok(LakeSeam::from_lake_snapshot(&snapshot))
    }
}

/// One resolved `(partition, bucket)` read target.
struct ReadTarget {
    partition_id: Option<i64>,
    /// Canonical partition name for `list_partition_offsets`; `None` when the
    /// table is non-partitioned.
    partition_name: Option<String>,
    partition_values: Vec<(String, String)>,
    identity: FlussLakePartitionIdentity,
    bucket: i32,
}

fn encode_task(task: &ReadTask) -> Result<Vec<u8>> {
    serde_json::to_vec(task).map_err(|e| {
        FlussLakeError::Internal(format!("failed to encode split execution descriptor: {e}"))
    })
}

fn decode_task(bytes: &[u8]) -> Result<ReadTask> {
    serde_json::from_slice(bytes).map_err(|e| {
        FlussLakeError::Internal(format!("failed to decode split execution descriptor: {e}"))
    })
}

/// Builds the projected Arrow output schema for a projection (or the full schema
/// when `projection` is `None`).
fn projected_schema(schema: &Schema, projection: Option<&[usize]>) -> Result<SchemaRef> {
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
            Ok(to_arrow_schema(projected.row_type())?)
        }
        None => Ok(to_arrow_schema(schema.row_type())?),
    }
}

/// The output column names for a projection (used by the lake reader to project
/// by Fluss column name), or `None` for the full schema.
fn projected_names(schema: &Schema, projection: Option<&[usize]>) -> Result<Option<Vec<String>>> {
    match projection {
        Some(indices) => {
            let names = indices
                .iter()
                .map(|&idx| {
                    schema.columns().get(idx).map(|c| c.name().to_string()).ok_or_else(|| {
                        FlussLakeError::Internal(format!("projection index {idx} out of bounds"))
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Some(names))
        }
        None => Ok(None),
    }
}

/// Projects each batch to `output_names` by column NAME. The input batch may
/// carry a superset of columns (e.g. filter-only columns read for exact
/// filtering), so selection is by name rather than by full-schema index.
fn project_stream_by_names(
    stream: RecordBatchStream,
    output_schema: SchemaRef,
    output_names: Vec<String>,
) -> RecordBatchStream {
    stream
        .map(move |item| {
            let batch = item?;
            let columns = output_names
                .iter()
                .map(|name| {
                    let idx = batch.schema().index_of(name).map_err(|_| {
                        FlussLakeError::Internal(format!(
                            "output column `{name}` missing from filtered batch"
                        ))
                    })?;
                    Ok(batch.column(idx).clone())
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
        })
        .boxed()
}

/// Computes the read projection when a filter is present: the union of the
/// output projection and the filter's referenced columns (as indices into
/// `schema`), sorted ascending and deduplicated. A filter column absent from the
/// schema is skipped here; the exact filter surfaces it as a schema error.
fn read_projection_with_filter_columns(
    schema: &Schema,
    output: &[usize],
    filter: &FlussLakePredicate,
) -> Vec<usize> {
    let mut set: std::collections::BTreeSet<usize> = output.iter().copied().collect();
    for name in filter.referenced_columns() {
        if let Some(idx) = schema.columns().iter().position(|c| c.name() == name) {
            set.insert(idx);
        }
    }
    set.into_iter().collect()
}

struct FlussLakeReadPlanInner {
    schema: SchemaRef,
    splits: Vec<FlussLakeReadSplit>,
}

/// Public logical read split descriptor. Distributable, serializable, and
/// versioned; carries an opaque execution descriptor and no full scan config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlussLakeReadSplit {
    pub split_id: String,
    pub bucket_id: i32,
    pub partition: FlussLakePartitionIdentity,
    pub estimated_rows: Option<usize>,
    pub estimated_size: Option<usize>,
    pub descriptor_version: u32,
    /// Private, opaque, versioned execution descriptor. Not part of the stable
    /// human-facing surface; used by the reader to rebuild the internal task.
    execution_descriptor: Vec<u8>,
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
        // Aggregate split estimates: any `None` makes the plan estimate `None`.
        let split_count = self.inner.splits.len();
        let estimated_rows = self
            .inner
            .splits
            .iter()
            .map(|s| s.estimated_rows)
            .try_fold(0usize, |acc, r| r.map(|r| acc + r));
        let estimated_size = self
            .inner
            .splits
            .iter()
            .map(|s| s.estimated_size)
            .try_fold(0usize, |acc, r| r.map(|r| acc + r));
        FlussLakePlanStatistics {
            split_count,
            estimated_rows,
            estimated_size,
        }
    }
}

/// Plan-level statistics for engine cost estimation and parallelism decisions.
#[derive(Debug, Clone)]
pub struct FlussLakePlanStatistics {
    pub split_count: usize,
    pub estimated_rows: Option<usize>,
    pub estimated_size: Option<usize>,
}

/// Read executor over a previously configured scan. `Send + Sync`, so an
/// `Arc<FlussLakeRead>` can be shared and called concurrently across tasks.
#[derive(Clone)]
pub struct FlussLakeRead {
    connection: Arc<FlussConnection>,
    config: FlussLakeScanConfig,
}

impl FlussLakeRead {
    pub async fn read_split(&self, split: &FlussLakeReadSplit) -> Result<RecordBatchStream> {
        let cfg = &self.config;
        let task = decode_task(&split.execution_descriptor)?;
        let has_filter = !cfg.filter.is_always_true();

        // Without a filter, push the output projection straight down. With a
        // filter, the exact filter may reference columns outside the output
        // projection, so we read the UNION of the output columns and the filter's
        // columns (NOT the whole table), apply the exact filter, then project down
        // to the output columns. When the output projection is `None` (all
        // columns) the union is also all columns, so no extra reprojection runs.
        let read_projection: Option<Vec<usize>> = match (&cfg.projection, has_filter) {
            (proj, false) => proj.clone(),
            (None, true) => None,
            (Some(output), true) => {
                Some(read_projection_with_filter_columns(&cfg.schema, output, &cfg.filter))
            }
        };

        let stream = match &task {
            ReadTask::Append(spec) => {
                let schema = projected_schema(&cfg.schema, read_projection.as_deref())?;
                let names = projected_names(&cfg.schema, read_projection.as_deref())?;
                open_append_split(
                    self.connection.clone(),
                    &cfg.table_path,
                    &cfg.lake_catalog_properties,
                    schema,
                    names.as_deref(),
                    read_projection.clone(),
                    spec,
                )
                .await?
            }
            ReadTask::PrimaryKey(spec) => {
                open_pk_split(
                    self.connection.clone(),
                    &cfg.table_path,
                    &cfg.lake_catalog_properties,
                    &cfg.schema,
                    read_projection.clone(),
                    spec,
                )
                .await?
            }
        };

        if !has_filter {
            return Ok(stream);
        }

        // Apply the exact filter over the columns actually read, then project to
        // the output columns by name (the read batch may be a superset).
        let read_schema = projected_schema(&cfg.schema, read_projection.as_deref())?;
        let filtered = apply_exact_filter(stream, cfg.filter.clone(), read_schema);
        match &cfg.projection {
            // Read == output (all columns); no reprojection needed.
            None => Ok(filtered),
            Some(output) => {
                let output_schema = projected_schema(&cfg.schema, Some(output))?;
                let output_names = projected_names(&cfg.schema, Some(output))?
                    .expect("Some projection yields Some names");
                Ok(project_stream_by_names(filtered, output_schema, output_names))
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

    #[test]
    fn split_descriptor_round_trips() {
        let task = ReadTask::Append(AppendSplitSpec {
            partition_id: Some(7),
            partition_values: vec![("dt".to_string(), "2024".to_string())],
            bucket: 3,
            snapshot_id: 42,
            log_start_offset: 100,
            log_stop_offset: Some(200),
        });
        let bytes = encode_task(&task).unwrap();
        let decoded = decode_task(&bytes).unwrap();
        match decoded {
            ReadTask::Append(spec) => {
                assert_eq!(spec.partition_id, Some(7));
                assert_eq!(spec.bucket, 3);
                assert_eq!(spec.log_stop_offset, Some(200));
                assert_eq!(
                    spec.partition_values,
                    vec![("dt".to_string(), "2024".to_string())]
                );
            }
            _ => panic!("expected append task"),
        }
    }

    #[test]
    fn split_is_serializable() {
        let split = FlussLakeReadSplit {
            split_id: "db.t:-:0".to_string(),
            bucket_id: 0,
            partition: FlussLakePartitionIdentity::Unpartitioned,
            estimated_rows: None,
            estimated_size: None,
            descriptor_version: DESCRIPTOR_VERSION,
            execution_descriptor: vec![1, 2, 3],
        };
        let json = serde_json::to_string(&split).unwrap();
        let back: FlussLakeReadSplit = serde_json::from_str(&json).unwrap();
        assert_eq!(back.split_id, split.split_id);
        assert_eq!(back.descriptor_version, DESCRIPTOR_VERSION);
    }

    fn schema4() -> Schema {
        use fluss::metadata::DataTypes;
        Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .column("age", DataTypes::int())
            .column("dt", DataTypes::string())
            .build()
            .unwrap()
    }

    #[test]
    fn read_projection_unions_output_and_filter_columns() {
        use crate::predicate::FlussLakeLiteral;
        // output = [name(1)]; filter references age(2) -> read [1, 2].
        let filter = FlussLakePredicate::gt("age", FlussLakeLiteral::Int32(18));
        let read = read_projection_with_filter_columns(&schema4(), &[1], &filter);
        assert_eq!(read, vec![1, 2]);
    }

    #[test]
    fn read_projection_dedups_and_sorts() {
        use crate::predicate::FlussLakeLiteral;
        // output already includes age(2); filter on age adds nothing new; result
        // is sorted and deduplicated.
        let filter = FlussLakePredicate::gt("age", FlussLakeLiteral::Int32(18));
        let read = read_projection_with_filter_columns(&schema4(), &[2, 0], &filter);
        assert_eq!(read, vec![0, 2]);
    }
}
