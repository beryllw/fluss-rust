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

Apache Fluss Node.js 客户端，基于 [napi-rs](https://napi.rs/) 原生绑定构建。

## 特性

- 通过 napi-rs 实现原生 Rust 性能
- 基于 Async/Promise 的 API
- 支持日志表（Log Tables，仅追加）和 KV 表（KV Tables，主键表）
- 支持列投影进行选择性扫描
- 支持分区表操作
- 支持 SASL/PLAIN 认证
- 完整的数据类型支持（boolean、int、bigint、float、double、string、bytes、decimal、date、time、timestamp）

## 安装

```bash
npm install fluss-node
```

## 快速开始

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

// 连接到集群
const config = new Config({ "bootstrap.servers": "localhost:9223" });
const connection = await FlussConnection.create(config);
const admin = connection.getAdmin();

// 创建日志表
const tablePath = new TablePath("fluss", "events");
const schema = Schema.builder()
  .column("id", DataTypes.int())
  .column("message", DataTypes.string())
  .build();
const descriptor = TableDescriptor.builder().schema(schema).build();
await admin.createTable(tablePath, descriptor, true);

// 追加行数据
const table = await connection.getTable(tablePath);
const writer = table.newAppend().createWriter();
writer.append({ id: 1, message: "hello" });
writer.append({ id: 2, message: "world" });
await writer.flush();

// 扫描记录
const scanner = table.newScan().createLogScanner();
await scanner.subscribe(0, EARLIEST_OFFSET);
const records = await scanner.poll(5000);
for (const record of records.records()) {
  console.log(record.row); // { id: 1, message: "hello" }
}

connection.close();
```

## API 概览

### Config（配置）

```javascript
// 构造函数支持点分隔符或驼峰式键名
const config = new Config({ "bootstrap.servers": "localhost:9223" });
const config = new Config({ bootstrapServers: "localhost:9223" });

// 从字符串-字符串映射创建
const config = Config.fromProperties({ "bootstrap.servers": "localhost:9223" });

// Getter/setter 属性
config.bootstrapServers;     // string
config.writerAcks;           // string
config.writerRetries;        // number
config.writerBatchSize;      // number
config.writerEnableIdempotence; // boolean
config.securityProtocol;     // string
config.connectTimeoutMs;     // number
```

### FlussConnection（连接）

```javascript
const connection = await FlussConnection.create(config);
const admin = connection.getAdmin();           // FlussAdmin
const table = await connection.getTable(tablePath); // FlussTable
connection.close();
```

### FlussAdmin（管理）

```javascript
// 数据库操作
await admin.createDatabase(name, descriptor?, ignoreIfExists?);
await admin.dropDatabase(name, ignoreIfNotExists?, cascade?);
await admin.listDatabases();           // string[]
await admin.databaseExists(name);      // boolean
await admin.getDatabaseInfo(name);     // DatabaseInfo

// 表操作
await admin.createTable(tablePath, descriptor, ignoreIfExists?);
await admin.dropTable(tablePath, ignoreIfNotExists?);
await admin.listTables(databaseName);  // string[]
await admin.tableExists(tablePath);    // boolean
await admin.getTableInfo(tablePath);   // TableInfo

// 分区操作
await admin.createPartition(tablePath, spec, ignoreIfExists?);
await admin.dropPartition(tablePath, spec, ignoreIfNotExists?);
await admin.listPartitionInfos(tablePath); // PartitionInfo[]

// 偏移量操作
await admin.listOffsets(tablePath, bucketIds, offsetSpec);
await admin.listPartitionOffsets(tablePath, partitionName, bucketIds, offsetSpec);

// 服务器信息
await admin.getServerNodes();          // ServerNode[]
```

### Schema & TableDescriptor（模式和表描述）

```javascript
// Schema 构建器
const schema = Schema.builder()
  .column("id", DataTypes.int())
  .column("name", DataTypes.string())
  .primaryKey(["id"])
  .build();

schema.columnNames;  // string[]
schema.primaryKeys;  // string[]
schema.getColumns(); // [name, typeString][]

// TableDescriptor 构建器
const descriptor = TableDescriptor.builder()
  .schema(schema)
  .distributedBy(3, ["id"])    // bucket 数量, bucket 键
  .partitionedBy(["region"])   // 分区键
  .comment("我的表")
  .property("table.replication.factor", "1")
  .logFormat("arrow")
  .kvFormat("compacted")
  .build();
```

### FlussTable（表操作）

```javascript
const table = await connection.getTable(tablePath);
table.tablePath;       // TablePath
table.hasPrimaryKey;   // boolean
table.getTableInfo();  // TableInfo

// 追加操作（日志表）
const appendWriter = table.newAppend().createWriter();
const handle = appendWriter.append({ id: 1, name: "Alice" });
await handle.wait();   // 可选：等待确认
await appendWriter.flush();

// Upsert 操作（KV 表）
const upsertWriter = table.newUpsert().createWriter();
upsertWriter.upsert({ id: 1, name: "Alice" });
upsertWriter.delete({ id: 1 });
await upsertWriter.flush();

// 部分更新
const partialWriter = table.newUpsert()
  .partialUpdateByName(["id", "score"])  // 或 .partialUpdateByIndex([0, 3])
  .createWriter();
partialWriter.upsert({ id: 1, score: 100 });

// Lookup 查询（KV 表）
const lookuper = table.newLookup().createLookuper();
const row = await lookuper.lookup({ id: 1 }); // JSON 对象或 null

// 扫描
const scanner = table.newScan()
  .projectByName(["id", "name"])  // 可选列投影
  .createLogScanner();
await scanner.subscribe(0, EARLIEST_OFFSET);
const scanRecords = await scanner.poll(5000);
scanRecords.count;      // number
scanRecords.isEmpty;    // boolean
scanRecords.records();  // ScanRecord[]
```

### 数据类型映射

| Fluss 类型 | JavaScript 类型 | 说明 |
|-----------|----------------|------|
| BOOLEAN | `boolean` | |
| TINYINT | `number` | -128 到 127 |
| SMALLINT | `number` | -32768 到 32767 |
| INT | `number` | |
| BIGINT | `number` 或 `string` | 大于 Number.MAX_SAFE_INTEGER 的值使用字符串 |
| FLOAT | `number` | |
| DOUBLE | `number` | |
| STRING | `string` | |
| BYTES | `number[]` 或 `string` | 字节数组或 UTF-8 字符串 |
| DECIMAL | `string` | 例如 "123.45" |
| DATE | `number` 或 `string` | 读取时为纪元天数，写入时支持 "YYYY-MM-DD" 或纪元天数 |
| TIME | `number` | 午夜以来的毫秒数 |
| TIMESTAMP | `number` | 纪元毫秒数 |
| TIMESTAMP_LTZ | `number` | 纪元毫秒数 |

### 错误处理

所有异步操作失败时会抛出异常。使用标准的 try/catch：

```javascript
try {
  await admin.getTableInfo(new TablePath("fluss", "nonexistent"));
} catch (err) {
  console.error("操作失败:", err.message);
}
```

### SASL 认证

```javascript
const config = new Config({
  "bootstrap.servers": "localhost:9123",
  "security.protocol": "sasl",
  "security.sasl.mechanism": "PLAIN",
  "security.sasl.username": "admin",
  "security.sasl.password": "admin-secret",
});
```

## 示例

完整的使用示例请参见 [example/example.mjs](example/example.mjs)。

## 许可证

Apache License 2.0
