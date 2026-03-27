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

/**
 * Integration tests for log (append-only) table operations.
 *
 * Mirrors the Python test/test_log_table.py and the Rust integration tests.
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import {
  startCluster,
  getConnection,
  getAdmin,
} from "../helpers/cluster";
import {
  FlussConnection,
  FlussAdmin,
  LogScanner,
  ScanRecord,
  Schema,
  DataTypes,
  TablePath,
  TableDescriptor,
  DatabaseDescriptor,
  OffsetSpec,
  EARLIEST_OFFSET,
} from "../../index.js";

let connection: FlussConnection;
let admin: FlussAdmin;

before(async () => {
  await startCluster();
  connection = await getConnection();
  admin = getAdmin(connection);
});

after(() => {
  if (connection) connection.close();
});

/**
 * Poll a LogScanner until expectedCount records are collected or timeout.
 */
async function pollRecords(scanner: LogScanner, expectedCount: number, timeoutMs = 10_000): Promise<ScanRecord[]> {
  const collected: ScanRecord[] = [];
  const deadline = Date.now() + timeoutMs;
  while (collected.length < expectedCount && Date.now() < deadline) {
    const scanRecords = await scanner.poll(5000);
    collected.push(...scanRecords.records());
  }
  return collected;
}

describe("Log Table - Append and Scan", () => {
  it("should append JSON rows and scan them back", async () => {
    const tablePath = new TablePath("fluss", "node_test_append_and_scan");
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("c1", DataTypes.int())
      .column("c2", DataTypes.string())
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .distributedBy(3, ["c1"])
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    const table = await connection.getTable(tablePath);
    const appendWriter = table.newAppend().createWriter();

    // Append first batch
    appendWriter.append({ c1: 1, c2: "a1" });
    appendWriter.append({ c1: 2, c2: "a2" });
    appendWriter.append({ c1: 3, c2: "a3" });

    // Append second batch
    appendWriter.append({ c1: 4, c2: "a4" });
    appendWriter.append({ c1: 5, c2: "a5" });
    appendWriter.append({ c1: 6, c2: "a6" });
    await appendWriter.flush();

    // Scan with log scanner
    const numBuckets = (await admin.getTableInfo(tablePath)).numBuckets;
    const scanner = table.newScan().createLogScanner();
    const bucketOffsets: Record<string, number> = {};
    for (let i = 0; i < numBuckets; i++) {
      bucketOffsets[String(i)] = EARLIEST_OFFSET;
    }
    await scanner.subscribeBuckets(bucketOffsets);

    const records = await pollRecords(scanner, 6);
    assert.strictEqual(records.length, 6, `Expected 6 records, got ${records.length}`);

    records.sort((a, b) => a.row.c1 - b.row.c1);

    const expectedC1 = [1, 2, 3, 4, 5, 6];
    const expectedC2 = ["a1", "a2", "a3", "a4", "a5", "a6"];
    for (let i = 0; i < records.length; i++) {
      assert.strictEqual(records[i].row.c1, expectedC1[i], `c1 mismatch at row ${i}`);
      assert.strictEqual(records[i].row.c2, expectedC2[i], `c2 mismatch at row ${i}`);
    }

    // Test ScanRecord properties
    for (const record of records) {
      assert.strictEqual(record.changeType, "+A");
      assert.ok(record.offset >= 0);
      assert.ok(record.timestamp >= 0);
      assert.ok(record.bucket);
    }

    // Test unsubscribe
    await scanner.unsubscribe(0);

    await admin.dropTable(tablePath, false);
  });

  it("should verify ScanRecords count and isEmpty", async () => {
    const tablePath = new TablePath("fluss", "node_test_scan_records_props");
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    const table = await connection.getTable(tablePath);
    const writer = table.newAppend().createWriter();
    writer.append({ id: 1, name: "Alice" });
    writer.append({ id: 2, name: "Bob" });
    await writer.flush();

    const numBuckets = (await admin.getTableInfo(tablePath)).numBuckets;
    const scanner = table.newScan().createLogScanner();
    const bucketOffsets: Record<string, number> = {};
    for (let i = 0; i < numBuckets; i++) {
      bucketOffsets[String(i)] = EARLIEST_OFFSET;
    }
    await scanner.subscribeBuckets(bucketOffsets);

    // Poll until we get records
    let totalCount = 0;
    const deadline = Date.now() + 10_000;
    while (totalCount < 2 && Date.now() < deadline) {
      const scanRecords = await scanner.poll(5000);
      if (!scanRecords.isEmpty) {
        assert.ok(scanRecords.count > 0);
        totalCount += scanRecords.count;
      }
    }
    assert.strictEqual(totalCount, 2);

    await admin.dropTable(tablePath, false);
  });
});

describe("Log Table - List Offsets", () => {
  it("should list earliest, latest, and timestamp-based offsets", async () => {
    const tablePath = new TablePath("fluss", "node_test_list_offsets");
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    await new Promise((r) => setTimeout(r, 2000)); // Wait for table init

    // Earliest offset for empty table
    let earliest = await admin.listOffsets(
      tablePath,
      [0],
      OffsetSpec.earliest()
    );
    assert.strictEqual(earliest["0"], 0);

    // Latest offset for empty table
    let latest = await admin.listOffsets(tablePath, [0], OffsetSpec.latest());
    assert.strictEqual(latest["0"], 0);

    const beforeAppendMs = Date.now();

    // Append some records
    const table = await connection.getTable(tablePath);
    const writer = table.newAppend().createWriter();
    writer.append({ id: 1, name: "alice" });
    writer.append({ id: 2, name: "bob" });
    writer.append({ id: 3, name: "charlie" });
    await writer.flush();

    await new Promise((r) => setTimeout(r, 1000));

    const afterAppendMs = Date.now();

    // Latest offset should be 3 after append
    latest = await admin.listOffsets(tablePath, [0], OffsetSpec.latest());
    assert.strictEqual(latest["0"], 3);

    // Earliest should still be 0
    earliest = await admin.listOffsets(tablePath, [0], OffsetSpec.earliest());
    assert.strictEqual(earliest["0"], 0);

    // Timestamp before append -> offset 0
    const tsBefore = await admin.listOffsets(
      tablePath,
      [0],
      OffsetSpec.timestamp(beforeAppendMs)
    );
    assert.strictEqual(tsBefore["0"], 0);

    await new Promise((r) => setTimeout(r, 1000));

    // Timestamp after append -> offset 3
    const tsAfter = await admin.listOffsets(
      tablePath,
      [0],
      OffsetSpec.timestamp(afterAppendMs)
    );
    assert.strictEqual(tsAfter["0"], 3);

    await admin.dropTable(tablePath, false);
  });
});

describe("Log Table - Column Projection", () => {
  it("should project columns by name", async () => {
    const tablePath = new TablePath("fluss", "node_test_project_by_name");
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("col_a", DataTypes.int())
      .column("col_b", DataTypes.string())
      .column("col_c", DataTypes.int())
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    const table = await connection.getTable(tablePath);
    const writer = table.newAppend().createWriter();
    writer.append({ col_a: 1, col_b: "x", col_c: 10 });
    writer.append({ col_a: 2, col_b: "y", col_c: 20 });
    writer.append({ col_a: 3, col_b: "z", col_c: 30 });
    await writer.flush();

    // Project by name: select col_b and col_c only
    const scan = table.newScan().projectByName(["col_b", "col_c"]);
    const scanner = scan.createLogScanner();
    await scanner.subscribe(0, 0);

    const records = await pollRecords(scanner, 3);
    assert.strictEqual(records.length, 3);

    records.sort((a, b) => a.row.col_c - b.row.col_c);
    for (let i = 0; i < records.length; i++) {
      assert.strictEqual(records[i].row.col_b, ["x", "y", "z"][i]);
      assert.strictEqual(records[i].row.col_c, [10, 20, 30][i]);
      // col_a should not be present
      assert.strictEqual(records[i].row.col_a, undefined);
    }

    await admin.dropTable(tablePath, false);
  });

  it("should project columns by index", async () => {
    const tablePath = new TablePath("fluss", "node_test_project_by_index");
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("col_a", DataTypes.int())
      .column("col_b", DataTypes.string())
      .column("col_c", DataTypes.int())
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    const table = await connection.getTable(tablePath);
    const writer = table.newAppend().createWriter();
    writer.append({ col_a: 1, col_b: "x", col_c: 10 });
    writer.append({ col_a: 2, col_b: "y", col_c: 20 });
    writer.append({ col_a: 3, col_b: "z", col_c: 30 });
    await writer.flush();

    // Project by indices [1, 0] -> (col_b, col_a)
    const scanner = table.newScan().project([1, 0]).createLogScanner();
    await scanner.subscribe(0, 0);

    const records = await pollRecords(scanner, 3);
    assert.strictEqual(records.length, 3);

    records.sort((a, b) => a.row.col_a - b.row.col_a);
    for (let i = 0; i < records.length; i++) {
      assert.strictEqual(records[i].row.col_b, ["x", "y", "z"][i]);
      assert.strictEqual(records[i].row.col_a, [1, 2, 3][i]);
      // col_c should not be present
      assert.strictEqual(records[i].row.col_c, undefined);
    }

    await admin.dropTable(tablePath, false);
  });
});

describe("Log Table - Partitioned Table", () => {
  it("should append and scan a partitioned log table", async () => {
    const tablePath = new TablePath("fluss", "node_test_partitioned_log");
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("region", DataTypes.string())
      .column("value", DataTypes.bigint())
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .partitionedBy(["region"])
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    // Create partitions
    for (const region of ["US", "EU"]) {
      await admin.createPartition(tablePath, { region }, true);
    }

    await new Promise((r) => setTimeout(r, 2000)); // Wait for partitions

    const table = await connection.getTable(tablePath);
    const writer = table.newAppend().createWriter();

    // Append rows to different partitions
    writer.append({ id: 1, region: "US", value: 100 });
    writer.append({ id: 2, region: "US", value: 200 });
    writer.append({ id: 3, region: "EU", value: 300 });
    writer.append({ id: 4, region: "EU", value: 400 });
    await writer.flush();

    // Verify partition offsets
    const usOffsets = await admin.listPartitionOffsets(
      tablePath,
      "US",
      [0],
      OffsetSpec.latest()
    );
    assert.strictEqual(usOffsets["0"], 2, "US partition should have 2 records");

    const euOffsets = await admin.listPartitionOffsets(
      tablePath,
      "EU",
      [0],
      OffsetSpec.latest()
    );
    assert.strictEqual(euOffsets["0"], 2, "EU partition should have 2 records");

    // Scan all partitions
    const scanner = table.newScan().createLogScanner();
    const partitionInfos = await admin.listPartitionInfos(tablePath);
    for (const p of partitionInfos) {
      await scanner.subscribePartition(p.partitionId, 0, 0);
    }

    const records = await pollRecords(scanner, 4);
    assert.strictEqual(records.length, 4);

    const collected = records
      .map((r) => [r.row.id, r.row.region, r.row.value])
      .sort((a, b) => (a[0] as number) - (b[0] as number));

    assert.deepStrictEqual(collected, [
      [1, "US", 100],
      [2, "US", 200],
      [3, "EU", 300],
      [4, "EU", 400],
    ]);

    // Test unsubscribePartition
    const euPartitionId = partitionInfos.find(
      (p) => p.partitionName === "EU"
    )!.partitionId;

    const scanner2 = table.newScan().createLogScanner();
    for (const p of partitionInfos) {
      await scanner2.subscribePartition(p.partitionId, 0, 0);
    }
    await scanner2.unsubscribePartition(euPartitionId, 0);

    const remaining = await pollRecords(scanner2, 2, 5000);
    assert.strictEqual(remaining.length, 2);
    assert.ok(remaining.every((r) => r.row.region === "US"));

    await admin.dropTable(tablePath, false);
  });
});
