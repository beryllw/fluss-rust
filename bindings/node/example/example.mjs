#!/usr/bin/env node

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
 * Comprehensive usage example for the fluss-node client.
 *
 * Prerequisites:
 *   1. Build the native binding: napi build --platform
 *   2. Start a Fluss cluster (Docker or external)
 *   3. Set FLUSS_BOOTSTRAP_SERVERS if not using default localhost:9223
 *
 * Run with: node example/example.mjs
 */

import {
  Config,
  FlussConnection,
  Schema,
  DataTypes,
  TablePath,
  TableDescriptor,
  DatabaseDescriptor,
  OffsetSpec,
  EARLIEST_OFFSET,
} from "../index.js";

const BOOTSTRAP_SERVERS =
  process.env.FLUSS_BOOTSTRAP_SERVERS || "127.0.0.1:9223";

async function main() {
  // ─────────────────────────────────────────────────
  // 1. Create Config and Connection
  // ─────────────────────────────────────────────────
  console.log("=== 1. Creating connection ===");

  // Config supports both dot-notation and camelCase keys
  const config = new Config({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
  });
  console.log(`  Bootstrap servers: ${config.bootstrapServers}`);

  const connection = await FlussConnection.create(config);
  const admin = connection.getAdmin();
  console.log("  Connected successfully.\n");

  // ─────────────────────────────────────────────────
  // 2. Database Management
  // ─────────────────────────────────────────────────
  console.log("=== 2. Database management ===");

  const dbName = "example_db";
  await admin.dropDatabase(dbName, true, true); // cleanup

  const dbDescriptor = new DatabaseDescriptor("Example database");
  await admin.createDatabase(dbName, dbDescriptor, false);
  console.log(`  Created database '${dbName}'`);

  const databases = await admin.listDatabases();
  console.log(`  Databases: ${databases.join(", ")}`);

  const exists = await admin.databaseExists(dbName);
  console.log(`  '${dbName}' exists: ${exists}\n`);

  // ─────────────────────────────────────────────────
  // 3. Log Table (append-only)
  // ─────────────────────────────────────────────────
  console.log("=== 3. Log table operations ===");

  const logTablePath = new TablePath(dbName, "events");
  const logSchema = Schema.builder()
    .column("event_id", DataTypes.int())
    .column("event_type", DataTypes.string())
    .column("payload", DataTypes.string())
    .build();

  const logDescriptor = TableDescriptor.builder()
    .schema(logSchema)
    .distributedBy(1)
    .comment("Event log table")
    .property("table.replication.factor", "1")
    .build();

  await admin.createTable(logTablePath, logDescriptor, false);
  console.log(`  Created log table '${logTablePath.toStringRepr()}'`);

  // Append rows
  const logTable = await connection.getTable(logTablePath);
  const appendWriter = logTable.newAppend().createWriter();

  appendWriter.append({ event_id: 1, event_type: "click", payload: '{"x": 100}' });
  appendWriter.append({ event_id: 2, event_type: "view", payload: '{"page": "/home"}' });
  appendWriter.append({ event_id: 3, event_type: "click", payload: '{"x": 200}' });
  await appendWriter.flush();
  console.log("  Appended 3 events.");

  // Scan records
  const scanner = logTable.newScan().createLogScanner();
  await scanner.subscribe(0, EARLIEST_OFFSET);

  let allRecords = [];
  const deadline = Date.now() + 10_000;
  while (allRecords.length < 3 && Date.now() < deadline) {
    const scanRecords = await scanner.poll(5000);
    allRecords.push(...scanRecords.records());
  }

  console.log(`  Scanned ${allRecords.length} records:`);
  for (const record of allRecords) {
    console.log(
      `    offset=${record.offset} type=${record.changeType} row=${JSON.stringify(record.row)}`
    );
  }

  // List offsets
  const latestOffsets = await admin.listOffsets(
    logTablePath,
    [0],
    OffsetSpec.latest()
  );
  console.log(`  Latest offsets: ${JSON.stringify(latestOffsets)}\n`);

  // ─────────────────────────────────────────────────
  // 4. KV Table (primary key)
  // ─────────────────────────────────────────────────
  console.log("=== 4. KV table operations ===");

  const kvTablePath = new TablePath(dbName, "users");
  const kvSchema = Schema.builder()
    .column("id", DataTypes.int())
    .column("name", DataTypes.string())
    .column("email", DataTypes.string())
    .column("score", DataTypes.bigint())
    .primaryKey(["id"])
    .build();

  const kvDescriptor = TableDescriptor.builder()
    .schema(kvSchema)
    .comment("User KV table")
    .property("table.replication.factor", "1")
    .build();

  await admin.createTable(kvTablePath, kvDescriptor, false);
  console.log(`  Created KV table '${kvTablePath.toStringRepr()}'`);

  const kvTable = await connection.getTable(kvTablePath);

  // Upsert rows
  const upsertWriter = kvTable.newUpsert().createWriter();
  upsertWriter.upsert({ id: 1, name: "Alice", email: "alice@example.com", score: 100 });
  upsertWriter.upsert({ id: 2, name: "Bob", email: "bob@example.com", score: 200 });
  upsertWriter.upsert({ id: 3, name: "Charlie", email: "charlie@example.com", score: 300 });
  await upsertWriter.flush();
  console.log("  Upserted 3 users.");

  // Lookup
  const lookuper = kvTable.newLookup().createLookuper();
  const alice = await lookuper.lookup({ id: 1 });
  console.log(`  Lookup id=1: ${JSON.stringify(alice)}`);

  // Update
  const updateHandle = upsertWriter.upsert({
    id: 1,
    name: "Alice",
    email: "alice@example.com",
    score: 150,
  });
  await updateHandle.wait();
  const aliceUpdated = await lookuper.lookup({ id: 1 });
  console.log(`  After update id=1: score=${aliceUpdated.score}`);

  // Delete
  const deleteHandle = upsertWriter.delete({ id: 3 });
  await deleteHandle.wait();
  const charlie = await lookuper.lookup({ id: 3 });
  console.log(`  After delete id=3: ${charlie === null ? "null (deleted)" : JSON.stringify(charlie)}`);

  // Partial update by name
  const partialWriter = kvTable
    .newUpsert()
    .partialUpdateByName(["id", "score"])
    .createWriter();
  const partialHandle = partialWriter.upsert({ id: 2, score: 999 });
  await partialHandle.wait();
  const bobUpdated = await lookuper.lookup({ id: 2 });
  console.log(`  After partial update id=2: name=${bobUpdated.name} score=${bobUpdated.score}\n`);

  // ─────────────────────────────────────────────────
  // 5. Column Projection
  // ─────────────────────────────────────────────────
  console.log("=== 5. Column projection ===");

  const projScanner = logTable
    .newScan()
    .projectByName(["event_type", "payload"])
    .createLogScanner();
  await projScanner.subscribe(0, EARLIEST_OFFSET);

  const projRecords = [];
  const projDeadline = Date.now() + 10_000;
  while (projRecords.length < 3 && Date.now() < projDeadline) {
    const sr = await projScanner.poll(5000);
    projRecords.push(...sr.records());
  }
  console.log(`  Projected scan (event_type, payload): ${projRecords.length} records`);
  for (const r of projRecords) {
    console.log(`    ${JSON.stringify(r.row)}`);
  }
  console.log();

  // ─────────────────────────────────────────────────
  // 6. Server Nodes
  // ─────────────────────────────────────────────────
  console.log("=== 6. Server nodes ===");
  const nodes = await admin.getServerNodes();
  for (const node of nodes) {
    console.log(
      `  ${node.serverType}: ${node.host}:${node.port} (id=${node.id})`
    );
  }
  console.log();

  // ─────────────────────────────────────────────────
  // 7. Cleanup
  // ─────────────────────────────────────────────────
  console.log("=== 7. Cleanup ===");
  await admin.dropDatabase(dbName, true, true);
  console.log(`  Dropped database '${dbName}'`);

  connection.close();
  console.log("  Connection closed.\n");

  console.log("Example completed successfully!");
}

main().catch((err) => {
  console.error("Example failed:", err);
  process.exit(1);
});
