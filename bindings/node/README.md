<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# fluss-node

Apache Fluss Node.js client built with [napi-rs](https://napi.rs/) native bindings.

## Features

- Native Rust performance via napi-rs
- Async/Promise-based API
- Log tables (append-only) and KV tables (primary key) support
- Column projection for selective scanning
- Partitioned table operations
- SASL/PLAIN authentication
- Full data type support (boolean, int, bigint, float, double, string, bytes, decimal, date, time, timestamp)

## Installation

```bash
npm install fluss-node
```

## Quick Start

```javascript
import {
  Config,
  FlussConnection,
  Schema,
  DataTypes,
  TablePath,
  TableDescriptor,
  EARLIEST_OFFSET,
} from "fluss-node";

// Connect to cluster
const config = new Config({ "bootstrap.servers": "localhost:9223" });
const connection = await FlussConnection.create(config);
const admin = connection.getAdmin();

// Create a log table
const tablePath = new TablePath("fluss", "events");
const schema = Schema.builder()
  .column("id", DataTypes.int())
  .column("message", DataTypes.string())
  .build();
const descriptor = TableDescriptor.builder().schema(schema).build();
await admin.createTable(tablePath, descriptor, true);

// Append rows
const table = await connection.getTable(tablePath);
const writer = table.newAppend().createWriter();
writer.append({ id: 1, message: "hello" });
writer.append({ id: 2, message: "world" });
await writer.flush();

// Scan records
const scanner = table.newScan().createLogScanner();
await scanner.subscribe(0, EARLIEST_OFFSET);
const records = await scanner.poll(5000);
for (const record of records.records()) {
  console.log(record.row); // { id: 1, message: "hello" }
}

connection.close();
```

## API Overview

### Config

```javascript
// Constructor with dot-notation or camelCase keys
const config = new Config({ "bootstrap.servers": "localhost:9223" });
const config = new Config({ bootstrapServers: "localhost:9223" });

// Factory from string-string map
const config = Config.fromProperties({ "bootstrap.servers": "localhost:9223" });

// Getter/setter properties
config.bootstrapServers;     // string
config.writerAcks;           // string
config.writerRetries;        // number
config.writerBatchSize;      // number
config.writerEnableIdempotence; // boolean
config.securityProtocol;     // string
config.connectTimeoutMs;     // number
```

### FlussConnection

```javascript
const connection = await FlussConnection.create(config);
const admin = connection.getAdmin();           // FlussAdmin
const table = await connection.getTable(tablePath); // FlussTable
connection.close();
```

### FlussAdmin

```javascript
// Database operations
await admin.createDatabase(name, descriptor?, ignoreIfExists?);
await admin.dropDatabase(name, ignoreIfNotExists?, cascade?);
await admin.listDatabases();           // string[]
await admin.databaseExists(name);      // boolean
await admin.getDatabaseInfo(name);     // DatabaseInfo

// Table operations
await admin.createTable(tablePath, descriptor, ignoreIfExists?);
await admin.dropTable(tablePath, ignoreIfNotExists?);
await admin.listTables(databaseName);  // string[]
await admin.tableExists(tablePath);    // boolean
await admin.getTableInfo(tablePath);   // TableInfo

// Partition operations
await admin.createPartition(tablePath, spec, ignoreIfExists?);
await admin.dropPartition(tablePath, spec, ignoreIfNotExists?);
await admin.listPartitionInfos(tablePath); // PartitionInfo[]

// Offset operations
await admin.listOffsets(tablePath, bucketIds, offsetSpec);
await admin.listPartitionOffsets(tablePath, partitionName, bucketIds, offsetSpec);

// Server info
await admin.getServerNodes();          // ServerNode[]
```

### Schema & TableDescriptor

```javascript
// Schema builder
const schema = Schema.builder()
  .column("id", DataTypes.int())
  .column("name", DataTypes.string())
  .primaryKey(["id"])
  .build();

schema.columnNames;  // string[]
schema.primaryKeys;  // string[]
schema.getColumns(); // [name, typeString][]

// TableDescriptor builder
const descriptor = TableDescriptor.builder()
  .schema(schema)
  .distributedBy(3, ["id"])    // bucket count, bucket keys
  .partitionedBy(["region"])   // partition keys
  .comment("my table")
  .property("table.replication.factor", "1")
  .logFormat("arrow")
  .kvFormat("compacted")
  .build();
```

### FlussTable

```javascript
const table = await connection.getTable(tablePath);
table.tablePath;       // TablePath
table.hasPrimaryKey;   // boolean
table.getTableInfo();  // TableInfo

// Append (log tables)
const appendWriter = table.newAppend().createWriter();
const handle = appendWriter.append({ id: 1, name: "Alice" });
await handle.wait();   // optional: wait for ack
await appendWriter.flush();

// Upsert (KV tables)
const upsertWriter = table.newUpsert().createWriter();
upsertWriter.upsert({ id: 1, name: "Alice" });
upsertWriter.delete({ id: 1 });
await upsertWriter.flush();

// Partial update
const partialWriter = table.newUpsert()
  .partialUpdateByName(["id", "score"])  // or .partialUpdateByIndex([0, 3])
  .createWriter();
partialWriter.upsert({ id: 1, score: 100 });

// Lookup (KV tables)
const lookuper = table.newLookup().createLookuper();
const row = await lookuper.lookup({ id: 1 }); // JSON object or null

// Scan
const scanner = table.newScan()
  .projectByName(["id", "name"])  // optional column projection
  .createLogScanner();
await scanner.subscribe(0, EARLIEST_OFFSET);
const scanRecords = await scanner.poll(5000);
scanRecords.count;      // number
scanRecords.isEmpty;    // boolean
scanRecords.records();  // ScanRecord[]
```

### Data Type Mapping

| Fluss Type | JavaScript Type | Notes |
|-----------|----------------|-------|
| BOOLEAN | `boolean` | |
| TINYINT | `number` | -128 to 127 |
| SMALLINT | `number` | -32768 to 32767 |
| INT | `number` | |
| BIGINT | `number` or `string` | String for values > Number.MAX_SAFE_INTEGER |
| FLOAT | `number` | |
| DOUBLE | `number` | |
| STRING | `string` | |
| BYTES | `number[]` or `string` | Byte array or UTF-8 string |
| DECIMAL | `string` | e.g. "123.45" |
| DATE | `number` or `string` | Epoch days (read) / "YYYY-MM-DD" or epoch days (write) |
| TIME | `number` | Milliseconds since midnight |
| TIMESTAMP | `number` | Epoch milliseconds |
| TIMESTAMP_LTZ | `number` | Epoch milliseconds |

### Error Handling

All async operations throw on failure. Use standard try/catch:

```javascript
try {
  await admin.getTableInfo(new TablePath("fluss", "nonexistent"));
} catch (err) {
  console.error("Operation failed:", err.message);
}
```

### SASL Authentication

```javascript
const config = new Config({
  "bootstrap.servers": "localhost:9123",
  "security.protocol": "sasl",
  "security.sasl.mechanism": "PLAIN",
  "security.sasl.username": "admin",
  "security.sasl.password": "admin-secret",
});
```

## Example

See [example/example.mjs](example/example.mjs) for a comprehensive usage demonstration.

## License

Apache License 2.0
