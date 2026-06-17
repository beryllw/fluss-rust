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

//! Reads the log (Fluss) side of a union: the residual log tail of one bucket,
//! from the lake seam offset up to a snapshot-end offset.
//!
//! This is the only part of the union that requires a live Fluss connection, so
//! it sits behind the [`LogTailReader`] trait. The union orchestration depends
//! on the trait, letting unit tests drive it with a fake while the real
//! [`FlussLogTailReader`] is exercised by an integration test against a cluster.

use async_trait::async_trait;

use crate::error::Result;
use crate::reader::RecordBatchStream;

/// Reads the residual log tail of a single bucket: the records from
/// `start_offset` (the lake seam) up to, but not including, `stop_offset`.
///
/// `partition_id` is `None` for a non-partitioned table. `stop_offset = None`
/// means "read to the latest offset captured at read time". `projection` is the
/// column indices into the Fluss table schema, or `None` for all columns.
#[async_trait]
pub trait LogTailReader: Send + Sync {
    async fn read_tail(
        &self,
        partition_id: Option<i64>,
        bucket: i32,
        start_offset: i64,
        stop_offset: Option<i64>,
        projection: Option<Vec<usize>>,
    ) -> Result<RecordBatchStream>;
}
