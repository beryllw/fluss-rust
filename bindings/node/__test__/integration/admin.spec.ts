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
 * Integration tests for FlussAdmin operations.
 *
 * Mirrors the Python test/test_admin.py and the Rust integration tests.
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
  DatabaseDescriptor,
  OffsetSpec,
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

describe("Admin - Database operations", () => {
  it("should create, check, get info, and drop a database", async () => {
    const dbName = "node_test_create_database";

    // Cleanup prior failed run
    await admin.dropDatabase(dbName, true, true);

    assert.strictEqual(await admin.databaseExists(dbName), false);

    const descriptor = new DatabaseDescriptor(
      "test_db",
      { k1: "v1", k2: "v2" }
    );
    await admin.createDatabase(dbName, descriptor, false);

    assert.strictEqual(await admin.databaseExists(dbName), true);

    const dbInfo = await admin.getDatabaseInfo(dbName);
    assert.strictEqual(dbInfo.databaseName, dbName);

    await admin.dropDatabase(dbName, false, true);
    assert.strictEqual(await admin.databaseExists(dbName), false);
  });

  it("should list databases including default", async () => {
    const databases = await admin.listDatabases();
    assert.ok(databases.length > 0, "Expected at least one database");
    assert.ok(databases.includes("fluss"), "Expected 'fluss' default database");
  });

  it("should error on non-existent database operations", async () => {
    await assert.rejects(
      () => admin.getDatabaseInfo("node_no_such_db"),
      /error/i
    );

    await assert.rejects(
      () => admin.dropDatabase("node_no_such_db", false),
      /error/i
    );

    await assert.rejects(
      () => admin.listTables("node_no_such_db"),
      /error/i
    );
  });

  it("should error when creating duplicate database", async () => {
    const dbName = "node_test_error_db_already_exist";
    await admin.dropDatabase(dbName, true, true);
    await admin.createDatabase(dbName, undefined, false);

    await assert.rejects(
      () => admin.createDatabase(dbName, undefined, false),
      /error/i
    );

    // With ignore flag should succeed
    await admin.createDatabase(dbName, undefined, true);

    await admin.dropDatabase(dbName, true, true);
  });
});

describe("Admin - Table operations", () => {
  const dbName = "node_test_create_table_db";

  before(async () => {
    await admin.dropDatabase(dbName, true, true);
    await admin.createDatabase(
      dbName,
      new DatabaseDescriptor("Database for table tests"),
      false
    );
  });

  after(async () => {
    await admin.dropDatabase(dbName, true, true);
  });

  it("should create, check, list, get info, and drop a table", async () => {
    const tableName = "test_user_table";
    const tablePath = new TablePath(dbName, tableName);

    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .column("age", DataTypes.int())
      .column("email", DataTypes.string())
      .primaryKey(["id"])
      .build();

    const tableDescriptor = TableDescriptor.builder()
      .schema(schema)
      .distributedBy(3, ["id"])
      .comment("Test table for user data")
      .property("table.replication.factor", "1")
      .logFormat("arrow")
      .kvFormat("compacted")
      .build();

    await admin.createTable(tablePath, tableDescriptor, false);

    assert.strictEqual(await admin.tableExists(tablePath), true);

    const tables = await admin.listTables(dbName);
    assert.strictEqual(tables.length, 1);
    assert.ok(tables.includes(tableName));

    const tableInfo = await admin.getTableInfo(tablePath);
    assert.strictEqual(tableInfo.comment, "Test table for user data");
    assert.deepStrictEqual(tableInfo.getPrimaryKeys(), ["id"]);
    assert.strictEqual(tableInfo.numBuckets, 3);
    assert.deepStrictEqual(tableInfo.getBucketKeys(), ["id"]);
    assert.deepStrictEqual(tableInfo.getColumnNames(), [
      "id",
      "name",
      "age",
      "email",
    ]);
    assert.strictEqual(tableInfo.hasPrimaryKey(), true);
    assert.strictEqual(tableInfo.isPartitioned(), false);

    await admin.dropTable(tablePath, false);
    assert.strictEqual(await admin.tableExists(tablePath), false);
  });

  it("should error when creating duplicate table", async () => {
    const tablePath = new TablePath(dbName, "dup_table");
    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();

    await admin.createTable(tablePath, descriptor, false);

    await assert.rejects(
      () => admin.createTable(tablePath, descriptor, false),
      /error/i
    );

    // With ignore flag should succeed
    await admin.createTable(tablePath, descriptor, true);

    await admin.dropTable(tablePath, true);
  });

  it("should error on non-existent table", async () => {
    const tablePath = new TablePath("fluss", "node_no_such_table");

    await assert.rejects(
      () => admin.dropTable(tablePath, false),
      /error/i
    );

    // With ignore flag should succeed
    await admin.dropTable(tablePath, true);
  });
});

describe("Admin - Partition operations", () => {
  const dbName = "node_test_partition_apis_db";

  before(async () => {
    await admin.dropDatabase(dbName, true, true);
    await admin.createDatabase(
      dbName,
      new DatabaseDescriptor("Database for partition tests"),
      true
    );
  });

  after(async () => {
    await admin.dropDatabase(dbName, true, true);
  });

  it("should create, list, and drop partitions", async () => {
    const tablePath = new TablePath(dbName, "partitioned_table");

    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .column("dt", DataTypes.string())
      .column("region", DataTypes.string())
      .primaryKey(["id", "dt", "region"])
      .build();

    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .partitionedBy(["dt", "region"])
      .distributedBy(3, ["id"])
      .property("table.replication.factor", "1")
      .build();

    await admin.createTable(tablePath, descriptor, true);

    // Initially no partitions
    let partitions = await admin.listPartitionInfos(tablePath);
    assert.strictEqual(partitions.length, 0);

    // Create a partition
    await admin.createPartition(
      tablePath,
      { dt: "2024-01-15", region: "EMEA" },
      false
    );

    partitions = await admin.listPartitionInfos(tablePath);
    assert.strictEqual(partitions.length, 1);
    assert.strictEqual(partitions[0].partitionName, "2024-01-15$EMEA");

    // Drop the partition
    await admin.dropPartition(
      tablePath,
      { dt: "2024-01-15", region: "EMEA" },
      false
    );

    partitions = await admin.listPartitionInfos(tablePath);
    assert.strictEqual(partitions.length, 0);

    await admin.dropTable(tablePath, true);
  });

  it("should error on partition ops for non-partitioned table", async () => {
    const tablePath = new TablePath(dbName, "non_partitioned_table");
    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .build();
    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .property("table.replication.factor", "1")
      .build();

    await admin.createTable(tablePath, descriptor, false);

    await assert.rejects(
      () => admin.listPartitionInfos(tablePath),
      /error/i
    );

    await admin.dropTable(tablePath, true);
  });
});

describe("Admin - Server nodes", () => {
  it("should return coordinator and tablet servers", async () => {
    const nodes = await admin.getServerNodes();
    assert.ok(nodes.length > 0, "Expected at least one server node");

    const serverTypes = nodes.map((n) => n.serverType);
    assert.ok(
      serverTypes.includes("CoordinatorServer"),
      "Expected a coordinator server"
    );
    assert.ok(
      serverTypes.includes("TabletServer"),
      "Expected at least one tablet server"
    );

    for (const node of nodes) {
      assert.ok(node.host, "Server node host should not be empty");
      assert.ok(node.port > 0, "Server node port should be > 0");
      assert.ok(node.uid, "Server node uid should not be empty");
    }
  });
});
