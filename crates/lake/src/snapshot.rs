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

//! The lake/log seam: where the lake snapshot ends and the log tail begins.
//!
//! A Fluss [`LakeSnapshot`] reports, per table bucket, the log offset up to
//! which data has been tiered into the lake. That offset is exactly the start
//! offset for the residual log read of that bucket. [`LakeSeam`] re-keys the
//! snapshot's `TableBucket -> offset` map onto the `(partition_id, bucket)`
//! shape the rest of the kernel (and the engine scan targets) use.
//!
//! The fetch itself (`admin.get_latest_lake_snapshot` / the future readable
//! variant) lives in the I/O layer; this module is the pure re-keying so it can
//! be unit-tested without a cluster.

use std::collections::HashMap;

use fluss::metadata::LakeSnapshot;

/// Per-bucket key into the seam map: `(partition_id, bucket)`, where
/// `partition_id` is `None` for a non-partitioned table.
pub type BucketKey = (Option<i64>, i32);

/// The resolved lake/log seam for one table: the lake snapshot id to read on the
/// lake side, plus the per-bucket log start offset for the log tail.
#[derive(Debug, Clone)]
pub struct LakeSeam {
    snapshot_id: i64,
    bucket_seams: HashMap<BucketKey, i64>,
}

impl LakeSeam {
    /// Re-keys a Fluss [`LakeSnapshot`] into `(partition_id, bucket) -> offset`.
    ///
    /// Buckets the snapshot omits (no tiered data / no log offset) simply do not
    /// appear; callers treat a missing seam as "read this bucket's log from the
    /// earliest offset" since nothing has been tiered for it yet.
    pub fn from_lake_snapshot(snapshot: &LakeSnapshot) -> Self {
        let bucket_seams = snapshot
            .table_buckets_offset()
            .iter()
            .map(|(tb, offset)| ((tb.partition_id(), tb.bucket_id()), *offset))
            .collect();
        Self {
            snapshot_id: snapshot.snapshot_id(),
            bucket_seams,
        }
    }

    /// The lake snapshot id to pin the Paimon read to.
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    /// The log start offset for `(partition_id, bucket)`, i.e. the first log
    /// offset NOT covered by the lake snapshot. `None` when the bucket has no
    /// tiered data yet.
    pub fn seam_offset(&self, partition_id: Option<i64>, bucket: i32) -> Option<i64> {
        self.bucket_seams.get(&(partition_id, bucket)).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::TableBucket;

    #[test]
    fn rekeys_partitioned_and_unpartitioned_buckets() {
        let table_id = 7;
        let mut offsets = HashMap::new();
        // non-partitioned bucket 0 -> seam 100
        offsets.insert(TableBucket::new(table_id, 0), 100);
        // partitioned (partition 5) bucket 1 -> seam 250
        offsets.insert(TableBucket::new_with_partition(table_id, Some(5), 1), 250);
        let snapshot = LakeSnapshot::new(42, offsets);

        let seam = LakeSeam::from_lake_snapshot(&snapshot);
        assert_eq!(seam.snapshot_id(), 42);
        assert_eq!(seam.seam_offset(None, 0), Some(100));
        assert_eq!(seam.seam_offset(Some(5), 1), Some(250));
        // a bucket with no tiered data has no seam
        assert_eq!(seam.seam_offset(None, 9), None);
    }
}
