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

//! Per-partition readers that turn a union partition into an Arrow stream.
//!
//! - [`append`] concatenates the lake stream with the log-tail stream for an
//!   append (log) table.
//! - the lake and log source streams themselves are produced by the I/O glue
//!   (Paimon `to_arrow` and the Fluss log scanner respectively) and are kept
//!   behind the kernel's source abstraction.
//!
//! Primary-key cross-source merge (`merge`) lands in a later milestone.

pub mod append;
pub mod lake;

use arrow::array::RecordBatch;
use futures::stream::BoxStream;

use crate::error::Result;

/// A boxed, owned stream of Arrow record batches — the common currency between
/// the lake reader, the log reader, and the union output. Mirrors Paimon's
/// `ArrowRecordBatchStream` but carries the kernel's own error type.
pub type RecordBatchStream = BoxStream<'static, Result<RecordBatch>>;
