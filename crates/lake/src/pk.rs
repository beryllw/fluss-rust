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

//! Primary-key union read: open one PK bucket as `lake_current_state` merged
//! with the Fluss changelog tail.
//!
//! The lake side is Paimon's deduplicated current state for the bucket; the log
//! side is the changelog tail from the seam offset to the frozen snapshot-end.
//! The merge ([`crate::reader::merge::pk_merge`]) overlays the bounded tail onto
//! the streamed lake state. v1 supports only the `deduplicate` merge engine and
//! rejects deletion-vector-enabled tables, where the Paimon Rust KV read path
//! cannot return a correct current state.

use std::collections::HashMap;
use std::sync::Arc;

use fluss::client::FlussConnection;
use fluss::metadata::{Schema, TablePath};
use fluss::record::to_arrow_schema;
use paimon::spec::{CoreOptions, MergeEngine};

use crate::catalog::{get_table_at_snapshot, open_catalog};
use crate::config::LakeCatalogConfig;
use crate::error::{FlussLakeError, Result};
use crate::reader::RecordBatchStream;
use crate::reader::changelog::read_pk_changelog_tail;
use crate::reader::lake::read_lake_table;
use crate::reader::merge::pk_merge;

/// Internal PK execution spec for one logical read split (one bucket).
#[derive(Debug, Clone)]
pub(crate) struct PkSplitSpec {
    pub partition_id: Option<i64>,
    pub bucket: i32,
    pub snapshot_id: i64,
    /// Inclusive log start (the lake seam).
    pub log_start_offset: i64,
    /// Exclusive log stop, frozen at plan time.
    pub log_stop_offset: i64,
}

/// Opens one PK logical read split as a merged `lake ++ changelog_tail` stream.
pub(crate) async fn open_pk_split(
    connection: Arc<FlussConnection>,
    table_path: &TablePath,
    lake_catalog_properties: &HashMap<String, String>,
    fluss_schema: &Schema,
    projection: Option<Vec<usize>>,
    spec: &PkSplitSpec,
) -> Result<RecordBatchStream> {
    // Partitioned PK union is not supported yet: the lake read is filtered by
    // bucket only, so a partitioned table would mix partitions.
    if spec.partition_id.is_some() {
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
        spec.snapshot_id,
    )
    .await?;

    // v1 supports only the deduplicate merge engine without deletion vectors; the
    // Paimon Rust KV read path cannot return a correct current state otherwise.
    validate_pk_merge_engine(lake_table.schema().options())?;

    // Lake side: full columns (PK columns required for the merge key), restricted
    // to this bucket. Projection is applied after the merge.
    let lake_stream = read_lake_table(&lake_table, None, Some(spec.bucket)).await?;

    let full_schema = to_arrow_schema(fluss_schema.row_type())?;
    let pk_indexes = fluss_schema.primary_key_indexes();

    let tail = read_pk_changelog_tail(
        connection,
        table_path,
        fluss_schema.row_type(),
        spec.partition_id,
        spec.bucket,
        spec.log_start_offset,
        spec.log_stop_offset,
    )
    .await?;

    pk_merge(
        lake_stream,
        tail.batch,
        tail.change_types,
        pk_indexes,
        full_schema,
        projection,
    )
}

/// v1 PK lake read supports only the deduplicate merge engine without deletion
/// vectors. Rejects anything else (partial-update / first-row / DV-enabled),
/// where the Paimon Rust KV read path cannot return a correct current state.
fn validate_pk_merge_engine(options: &HashMap<String, String>) -> Result<()> {
    let core_options = CoreOptions::new(options);
    if core_options.deletion_vectors_enabled() {
        return Err(FlussLakeError::Internal(
            "PK lake read of deletion-vector-enabled tables is not yet supported".to_string(),
        ));
    }
    match core_options.merge_engine()? {
        MergeEngine::Deduplicate => Ok(()),
        other => Err(FlussLakeError::Internal(format!(
            "PK lake read supports only the deduplicate merge engine; found {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_default_and_explicit_deduplicate() {
        // No merge-engine option => Paimon defaults to deduplicate.
        assert!(validate_pk_merge_engine(&HashMap::new()).is_ok());
        let opts = HashMap::from([("merge-engine".to_string(), "deduplicate".to_string())]);
        assert!(validate_pk_merge_engine(&opts).is_ok());
    }

    #[test]
    fn rejects_partial_update_and_first_row() {
        for engine in ["partial-update", "first-row"] {
            let opts = HashMap::from([("merge-engine".to_string(), engine.to_string())]);
            assert!(
                validate_pk_merge_engine(&opts).is_err(),
                "{engine} must be rejected"
            );
        }
    }

    #[test]
    fn rejects_deletion_vectors_enabled() {
        let opts = HashMap::from([("deletion-vectors.enabled".to_string(), "true".to_string())]);
        assert!(validate_pk_merge_engine(&opts).is_err());
    }
}
