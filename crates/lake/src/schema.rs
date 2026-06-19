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

//! Aligns lake (Paimon) record batches to the Fluss table's Arrow schema.
//!
//! A Fluss table tiered into Paimon stores the Fluss columns plus lake system
//! columns (`__bucket` / `__offset` / `__timestamp` for log tables), possibly in
//! a different column order. The log side, by contrast, produces exactly the
//! Fluss table schema. For the union the two sides must share an identical Arrow
//! schema, so the lake batches are realigned to the target (Fluss) schema:
//! columns are matched by name, reordered, and any extra system columns dropped.
//!
//! Projecting the Paimon read by the Fluss column names already does most of
//! this; [`align_batch_to`] is the explicit, validated realignment used as a
//! safety net and for the `SELECT *` path where no projection is pushed.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

use crate::error::{FlussLakeError, Result};

/// Reorders/selects `batch`'s columns to exactly match `target`, matching by
/// field name. Extra columns in `batch` (e.g. lake system columns) are dropped;
/// a target column absent from `batch`, or present with an incompatible data
/// type, is an error.
pub fn align_batch_to(batch: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch> {
    let source_schema = batch.schema();
    let mut columns = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let Some((idx, source_field)) = source_schema.column_with_name(field.name()) else {
            return Err(FlussLakeError::SchemaMismatch(format!(
                "column `{}` missing from lake batch",
                field.name()
            )));
        };
        if source_field.data_type() != field.data_type() {
            return Err(FlussLakeError::SchemaMismatch(format!(
                "column `{}` type mismatch: lake {:?} vs fluss {:?}",
                field.name(),
                source_field.data_type(),
                field.data_type()
            )));
        }
        columns.push(batch.column(idx).clone());
    }
    RecordBatch::try_new(Arc::clone(target), columns).map_err(FlussLakeError::from)
}

/// Whether `batch` already has exactly `target`'s fields in the same order (so
/// alignment can be skipped). Compares name + data type only.
pub fn matches_schema(batch: &RecordBatch, target: &SchemaRef) -> bool {
    let s = batch.schema();
    s.fields().len() == target.fields().len()
        && s.fields()
            .iter()
            .zip(target.fields())
            .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn target() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn reorders_and_drops_system_columns() {
        // lake batch: extra `__offset`, and columns in a different order.
        let lake_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("__offset", DataType::Int64, false),
            Field::new("id", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            lake_schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(arrow::array::Int64Array::from(vec![10, 11])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        let aligned = align_batch_to(&batch, &target()).unwrap();
        assert_eq!(aligned.schema().fields().len(), 2);
        assert_eq!(aligned.schema().field(0).name(), "id");
        assert_eq!(aligned.schema().field(1).name(), "name");
        let ids = aligned
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.values(), &[1, 2]);
    }

    #[test]
    fn errors_on_missing_column() {
        let lake_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(lake_schema, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let err = align_batch_to(&batch, &target()).unwrap_err();
        assert!(matches!(err, FlussLakeError::SchemaMismatch(_)));
    }

    #[test]
    fn errors_on_type_mismatch() {
        let lake_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false), // wrong type
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            lake_schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();
        let err = align_batch_to(&batch, &target()).unwrap_err();
        assert!(matches!(err, FlussLakeError::SchemaMismatch(_)));
    }

    #[test]
    fn matches_schema_detects_identity() {
        let batch = RecordBatch::try_new(
            target(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
            ],
        )
        .unwrap();
        assert!(matches_schema(&batch, &target()));
    }
}
