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
 * Unit tests for metadata types (no cluster required).
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  TablePath,
  DataTypes,
  DataType,
  OffsetSpec,
  Schema,
  TableDescriptor,
  DatabaseDescriptor,
  ChangeType,
} from "../../index.js";

describe("TablePath", () => {
  it("should store database and table names", () => {
    const tp = new TablePath("my_db", "my_table");
    assert.strictEqual(tp.databaseName, "my_db");
    assert.strictEqual(tp.tableName, "my_table");
  });

  it("should format as db.table via toStringRepr()", () => {
    const tp = new TablePath("default", "users");
    assert.strictEqual(tp.toStringRepr(), "default.users");
  });
});

describe("DataTypes", () => {
  const typeTests: [string, () => DataType][] = [
    ["boolean", () => DataTypes.boolean()],
    ["tinyint", () => DataTypes.tinyint()],
    ["smallint", () => DataTypes.smallint()],
    ["int", () => DataTypes.int()],
    ["bigint", () => DataTypes.bigint()],
    ["float", () => DataTypes.float()],
    ["double", () => DataTypes.double()],
    ["string", () => DataTypes.string()],
    ["bytes", () => DataTypes.bytes()],
    ["binary(10)", () => DataTypes.binary(10)],
    ["char(5)", () => DataTypes.char(5)],
    ["decimal(10,2)", () => DataTypes.decimal(10, 2)],
    ["date", () => DataTypes.date()],
    ["time", () => DataTypes.time()],
    ["time(3)", () => DataTypes.timeWithPrecision(3)],
    ["timestamp", () => DataTypes.timestamp()],
    ["timestamp(6)", () => DataTypes.timestampWithPrecision(6)],
    ["timestampLtz", () => DataTypes.timestampLtz()],
    ["timestampLtz(3)", () => DataTypes.timestampLtzWithPrecision(3)],
  ];

  for (const [label, factory] of typeTests) {
    it(`should create ${label} data type`, () => {
      const dt = factory();
      assert.ok(dt, `${label} should not be null`);
      const repr = dt.toStringRepr();
      assert.ok(repr.length > 0, `${label} toStringRepr should not be empty`);
    });
  }
});

describe("OffsetSpec", () => {
  it("should create earliest offset spec", () => {
    const spec = OffsetSpec.earliest();
    assert.ok(spec);
  });

  it("should create latest offset spec", () => {
    const spec = OffsetSpec.latest();
    assert.ok(spec);
  });

  it("should create timestamp offset spec", () => {
    const spec = OffsetSpec.timestamp(1700000000000);
    assert.ok(spec);
  });
});

describe("TableDescriptor", () => {
  it("should build a table descriptor with schema", () => {
    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .primaryKey(["id"])
      .build();

    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .distributedBy(3, ["id"])
      .comment("test table")
      .property("table.replication.factor", "1")
      .build();

    assert.ok(descriptor);
  });

  it("should build a partitioned table descriptor", () => {
    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("region", DataTypes.string())
      .column("value", DataTypes.bigint())
      .primaryKey(["id", "region"])
      .build();

    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .partitionedBy(["region"])
      .distributedBy(3, ["id"])
      .build();

    assert.ok(descriptor);
  });

  it("should build with log and kv format", () => {
    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .primaryKey(["id"])
      .build();

    const descriptor = TableDescriptor.builder()
      .schema(schema)
      .logFormat("arrow")
      .kvFormat("compacted")
      .build();

    assert.ok(descriptor);
  });

  it("should throw if schema is not set", () => {
    assert.throws(
      () => TableDescriptor.builder().build(),
      /Schema is required/
    );
  });
});

describe("DatabaseDescriptor", () => {
  it("should create with comment", () => {
    const dd = new DatabaseDescriptor("my comment");
    assert.strictEqual(dd.comment, "my comment");
  });

  it("should create with comment and custom properties", () => {
    const dd = new DatabaseDescriptor("test db", { k1: "v1", k2: "v2" });
    assert.strictEqual(dd.comment, "test db");
  });

  it("should create without arguments", () => {
    const dd = new DatabaseDescriptor();
    assert.ok(dd);
  });
});

describe("ChangeType", () => {
  it("should have correct enum values", () => {
    assert.strictEqual(ChangeType.AppendOnly, 0);
    assert.strictEqual(ChangeType.Insert, 1);
    assert.strictEqual(ChangeType.UpdateBefore, 2);
    assert.strictEqual(ChangeType.UpdateAfter, 3);
    assert.strictEqual(ChangeType.Delete, 4);
  });
});
