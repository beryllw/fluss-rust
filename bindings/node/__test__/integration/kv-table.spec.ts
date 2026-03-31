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
 * Integration tests for KV (primary key) table operations.
 *
 * Mirrors the Python test/test_kv_table.py and the Rust integration tests.
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
  Schema,
  DataTypes,
  TablePath,
  TableDescriptor,
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

describe("KV Table - Upsert, Delete, and Lookup", () => {
  it("should perform complete CRUD lifecycle", async () => {
    const tablePath = new TablePath("fluss", "node_test_upsert_and_lookup");
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .column("age", DataTypes.bigint())
      .primaryKey(["id"])
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    const table = await connection.getTable(tablePath);
    const upsertWriter = table.newUpsert().createWriter();

    const testData = [
      { id: 1, name: "Verso", age: 32 },
      { id: 2, name: "Noco", age: 25 },
      { id: 3, name: "Esquie", age: 35 },
    ];

    // Upsert rows (fire-and-forget, then flush)
    for (const row of testData) {
      upsertWriter.upsert(row);
    }
    await upsertWriter.flush();

    // Lookup and verify
    const lookuper = table.newLookup().createLookuper();

    for (const { id, name, age } of testData) {
      const result = await lookuper.lookup({ id });
      assert.ok(result !== null, `Row with id=${id} should exist`);
      assert.strictEqual(result.id, id);
      assert.strictEqual(result.name, name);
      assert.strictEqual(result.age, age);
    }

    // Update record with id=1 (await acknowledgment)
    const handle = upsertWriter.upsert({ id: 1, name: "Verso", age: 33 });
    await handle.wait();

    let result = await lookuper.lookup({ id: 1 });
    assert.ok(result !== null);
    assert.strictEqual(result.age, 33);
    assert.strictEqual(result.name, "Verso");

    // Delete record with id=1
    const deleteHandle = upsertWriter.delete({ id: 1 });
    await deleteHandle.wait();

    result = await lookuper.lookup({ id: 1 });
    assert.strictEqual(result, null, "Record 1 should not exist after delete");

    // Verify other records still exist
    for (const id of [2, 3]) {
      result = await lookuper.lookup({ id });
      assert.ok(result !== null, `Record ${id} should still exist`);
    }

    // Lookup non-existent key
    result = await lookuper.lookup({ id: 999 });
    assert.strictEqual(result, null, "Non-existent key should return null");

    await admin.dropTable(tablePath, false);
  });
});

describe("KV Table - Composite Primary Keys", () => {
  it("should support multi-column primary keys", async () => {
    const tablePath = new TablePath("fluss", "node_test_composite_pk");
    await admin.dropTable(tablePath, true);

    // PK columns interleaved with non-PK column
    const schema = Schema.builder()
      .column("region", DataTypes.string())
      .column("score", DataTypes.bigint())
      .column("user_id", DataTypes.int())
      .primaryKey(["region", "user_id"])
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    const table = await connection.getTable(tablePath);
    const upsertWriter = table.newUpsert().createWriter();

    const testData = [
      { region: "US", user_id: 1, score: 100 },
      { region: "US", user_id: 2, score: 200 },
      { region: "EU", user_id: 1, score: 150 },
      { region: "EU", user_id: 2, score: 250 },
    ];

    for (const row of testData) {
      upsertWriter.upsert(row);
    }
    await upsertWriter.flush();

    const lookuper = table.newLookup().createLookuper();

    // Lookup (US, 1) -> score 100
    let result = await lookuper.lookup({ region: "US", user_id: 1 });
    assert.ok(result !== null);
    assert.strictEqual(result.score, 100);

    // Lookup (EU, 2) -> score 250
    result = await lookuper.lookup({ region: "EU", user_id: 2 });
    assert.ok(result !== null);
    assert.strictEqual(result.score, 250);

    // Update (US, 1)
    const handle = upsertWriter.upsert({
      region: "US",
      user_id: 1,
      score: 500,
    });
    await handle.wait();

    result = await lookuper.lookup({ region: "US", user_id: 1 });
    assert.ok(result !== null);
    assert.strictEqual(result.score, 500);

    await admin.dropTable(tablePath, false);
  });
});

describe("KV Table - Partial Update", () => {
  it("should partially update by column name", async () => {
    const tablePath = new TablePath("fluss", "node_test_partial_update");
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .column("age", DataTypes.bigint())
      .column("score", DataTypes.bigint())
      .primaryKey(["id"])
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    const table = await connection.getTable(tablePath);

    // Insert initial record
    const upsertWriter = table.newUpsert().createWriter();
    const handle = upsertWriter.upsert({
      id: 1,
      name: "Verso",
      age: 32,
      score: 6942,
    });
    await handle.wait();

    const lookuper = table.newLookup().createLookuper();
    let result = await lookuper.lookup({ id: 1 });
    assert.ok(result !== null);
    assert.strictEqual(result.name, "Verso");
    assert.strictEqual(result.score, 6942);

    // Partial update: only update score column
    const partialWriter = table
      .newUpsert()
      .partialUpdateByName(["id", "score"])
      .createWriter();
    const partialHandle = partialWriter.upsert({ id: 1, score: 420 });
    await partialHandle.wait();

    result = await lookuper.lookup({ id: 1 });
    assert.ok(result !== null);
    assert.strictEqual(result.id, 1);
    assert.strictEqual(result.name, "Verso", "name should remain unchanged");
    assert.strictEqual(result.age, 32, "age should remain unchanged");
    assert.strictEqual(result.score, 420, "score should be updated to 420");

    await admin.dropTable(tablePath, false);
  });

  it("should partially update by column index", async () => {
    const tablePath = new TablePath(
      "fluss",
      "node_test_partial_update_by_index"
    );
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .column("age", DataTypes.bigint())
      .column("score", DataTypes.bigint())
      .primaryKey(["id"])
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    const table = await connection.getTable(tablePath);
    const upsertWriter = table.newUpsert().createWriter();
    const handle = upsertWriter.upsert({
      id: 1,
      name: "Verso",
      age: 32,
      score: 6942,
    });
    await handle.wait();

    // Partial update by indices: columns 0=id (PK), 1=name
    const partialWriter = table
      .newUpsert()
      .partialUpdateByIndex([0, 1])
      .createWriter();
    const partialHandle = partialWriter.upsert({
      id: 1,
      name: "Verso Renamed",
    });
    await partialHandle.wait();

    const lookuper = table.newLookup().createLookuper();
    const result = await lookuper.lookup({ id: 1 });
    assert.ok(result !== null);
    assert.strictEqual(result.name, "Verso Renamed", "name should be updated");
    assert.strictEqual(result.score, 6942, "score should remain unchanged");

    await admin.dropTable(tablePath, false);
  });
});

describe("KV Table - Partitioned Table", () => {
  it("should upsert and lookup across partitions", async () => {
    const tablePath = new TablePath("fluss", "node_test_partitioned_kv");
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("region", DataTypes.string())
      .column("user_id", DataTypes.int())
      .column("name", DataTypes.string())
      .column("score", DataTypes.bigint())
      .primaryKey(["region", "user_id"])
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .partitionedBy(["region"])
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    // Create partitions
    for (const region of ["US", "EU", "APAC"]) {
      await admin.createPartition(tablePath, { region }, true);
    }

    const table = await connection.getTable(tablePath);
    const upsertWriter = table.newUpsert().createWriter();

    const testData = [
      { region: "US", user_id: 1, name: "Gustave", score: 100 },
      { region: "US", user_id: 2, name: "Lune", score: 200 },
      { region: "EU", user_id: 1, name: "Sciel", score: 150 },
      { region: "EU", user_id: 2, name: "Maelle", score: 250 },
      { region: "APAC", user_id: 1, name: "Noco", score: 300 },
    ];

    for (const row of testData) {
      upsertWriter.upsert(row);
    }
    await upsertWriter.flush();

    const lookuper = table.newLookup().createLookuper();

    // Verify all rows
    for (const { region, user_id, name, score } of testData) {
      const result = await lookuper.lookup({ region, user_id });
      assert.ok(result !== null, `Row (${region}, ${user_id}) should exist`);
      assert.strictEqual(result.region, region);
      assert.strictEqual(result.user_id, user_id);
      assert.strictEqual(result.name, name);
      assert.strictEqual(result.score, score);
    }

    // Update within a partition
    const handle = upsertWriter.upsert({
      region: "US",
      user_id: 1,
      name: "Gustave Updated",
      score: 999,
    });
    await handle.wait();

    let result = await lookuper.lookup({ region: "US", user_id: 1 });
    assert.ok(result !== null);
    assert.strictEqual(result.name, "Gustave Updated");
    assert.strictEqual(result.score, 999);

    // Lookup in non-existent partition
    result = await lookuper.lookup({ region: "UNKNOWN_REGION", user_id: 1 });
    assert.strictEqual(
      result,
      null,
      "Lookup in non-existent partition should return null"
    );

    // Delete within a partition
    const deleteHandle = upsertWriter.delete({
      region: "EU",
      user_id: 1,
    });
    await deleteHandle.wait();

    result = await lookuper.lookup({ region: "EU", user_id: 1 });
    assert.strictEqual(result, null, "Deleted record should not exist");

    // Sibling still exists
    result = await lookuper.lookup({ region: "EU", user_id: 2 });
    assert.ok(result !== null);
    assert.strictEqual(result.name, "Maelle");

    await admin.dropTable(tablePath, false);
  });
});

describe("KV Table - All Supported Data Types", () => {
  it("should round-trip all data types including nulls", async () => {
    const tablePath = new TablePath("fluss", "node_test_kv_all_datatypes");
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("pk_int", DataTypes.int())
      .column("col_boolean", DataTypes.boolean())
      .column("col_tinyint", DataTypes.tinyint())
      .column("col_smallint", DataTypes.smallint())
      .column("col_int", DataTypes.int())
      .column("col_bigint", DataTypes.bigint())
      .column("col_float", DataTypes.float())
      .column("col_double", DataTypes.double())
      .column("col_string", DataTypes.string())
      .column("col_decimal", DataTypes.decimal(10, 2))
      .column("col_date", DataTypes.date())
      .column("col_time", DataTypes.time())
      .column("col_timestamp", DataTypes.timestamp())
      .column("col_timestamp_ltz", DataTypes.timestampLtz())
      .column("col_bytes", DataTypes.bytes())
      .primaryKey(["pk_int"])
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    const table = await connection.getTable(tablePath);
    const upsertWriter = table.newUpsert().createWriter();

    // Row data with all types
    // Node.js representations:
    //   decimal -> string "123.45"
    //   date -> epoch days (int) or "YYYY-MM-DD" string
    //   time -> milliseconds since midnight (int)
    //   timestamp/timestampLtz -> epoch milliseconds (int)
    //   bytes -> array of byte values or UTF-8 string
    const rowData: Record<string, unknown> = {
      pk_int: 1,
      col_boolean: true,
      col_tinyint: 127,
      col_smallint: 32767,
      col_int: 2147483647,
      col_bigint: "9223372036854775807",
      col_float: 3.14,
      col_double: 2.718281828459045,
      col_string: "world of fluss node client",
      col_decimal: "123.45",
      col_date: "2026-01-23",
      col_time: 36827123, // 10:13:47.123 in ms since midnight
      col_timestamp: 1769166827123, // epoch ms
      col_timestamp_ltz: 1769166827123,
      col_bytes: "binary data",
    };

    const handle = upsertWriter.upsert(rowData);
    await handle.wait();

    const lookuper = table.newLookup().createLookuper();
    const result = await lookuper.lookup({ pk_int: 1 });
    assert.ok(result !== null, "Row should exist");

    assert.strictEqual(result.pk_int, 1);
    assert.strictEqual(result.col_boolean, true);
    assert.strictEqual(result.col_tinyint, 127);
    assert.strictEqual(result.col_smallint, 32767);
    assert.strictEqual(result.col_int, 2147483647);
    // BigInt may come back as number or string depending on magnitude
    assert.ok(
      result.col_bigint === 9223372036854775807 ||
        result.col_bigint === "9223372036854775807",
      "col_bigint should be max i64"
    );
    assert.ok(
      Math.abs(result.col_float - 3.14) < 0.001,
      "col_float should be ~3.14"
    );
    assert.ok(
      Math.abs(result.col_double - 2.718281828459045) < 1e-12,
      "col_double should be ~2.718"
    );
    assert.strictEqual(result.col_string, "world of fluss node client");
    assert.strictEqual(result.col_decimal, "123.45");
    // Date comes back as epoch days
    assert.strictEqual(typeof result.col_date, "number");
    // Time comes back as ms since midnight
    assert.strictEqual(typeof result.col_time, "number");
    assert.strictEqual(result.col_time, 36827123);
    // Timestamps come back as epoch ms
    assert.strictEqual(typeof result.col_timestamp, "number");
    assert.strictEqual(typeof result.col_timestamp_ltz, "number");

    // Test null values for all nullable columns
    const nullRow: Record<string, unknown> = { pk_int: 2 };
    for (const col of Object.keys(rowData)) {
      if (col !== "pk_int") {
        nullRow[col] = null;
      }
    }
    const nullHandle = upsertWriter.upsert(nullRow);
    await nullHandle.wait();

    const nullResult = await lookuper.lookup({ pk_int: 2 });
    assert.ok(nullResult !== null, "Row with nulls should exist");
    assert.strictEqual(nullResult.pk_int, 2);
    for (const col of Object.keys(rowData)) {
      if (col !== "pk_int") {
        assert.strictEqual(
          nullResult[col],
          null,
          `${col} should be null`
        );
      }
    }

    await admin.dropTable(tablePath, false);
  });
});

describe("KV Table - Write Semantics", () => {
  it("should support fire-and-forget and explicit wait", async () => {
    const tablePath = new TablePath("fluss", "node_test_write_semantics");
    await admin.dropTable(tablePath, true);

    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("value", DataTypes.string())
      .primaryKey(["id"])
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();
    await admin.createTable(tablePath, descriptor, false);

    const table = await connection.getTable(tablePath);
    const writer = table.newUpsert().createWriter();

    // Fire-and-forget (ignore handle)
    writer.upsert({ id: 1, value: "fire-and-forget" });
    await writer.flush();

    // Explicit wait per record
    const handle = writer.upsert({ id: 2, value: "explicit-wait" });
    await handle.wait();

    const lookuper = table.newLookup().createLookuper();

    const r1 = await lookuper.lookup({ id: 1 });
    assert.ok(r1 !== null);
    assert.strictEqual(r1.value, "fire-and-forget");

    const r2 = await lookuper.lookup({ id: 2 });
    assert.ok(r2 !== null);
    assert.strictEqual(r2.value, "explicit-wait");

    await admin.dropTable(tablePath, false);
  });
});
