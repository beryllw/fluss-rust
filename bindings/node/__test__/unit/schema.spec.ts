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
 * Unit tests for Schema and SchemaBuilder (no cluster required).
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { Schema, DataTypes } from "../../index.js";

describe("Schema", () => {
  it("should build a schema with columns and primary key", () => {
    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .primaryKey(["id"])
      .build();

    assert.deepStrictEqual(schema.columnNames, ["id", "name"]);
    assert.deepStrictEqual(schema.primaryKeys, ["id"]);
  });

  it("should build a schema without primary key", () => {
    const schema = Schema.builder()
      .column("c1", DataTypes.int())
      .column("c2", DataTypes.string())
      .build();

    assert.deepStrictEqual(schema.columnNames, ["c1", "c2"]);
    assert.deepStrictEqual(schema.primaryKeys, []);
  });

  it("should support multiple data types", () => {
    const schema = Schema.builder()
      .column("col_bool", DataTypes.boolean())
      .column("col_tinyint", DataTypes.tinyint())
      .column("col_smallint", DataTypes.smallint())
      .column("col_int", DataTypes.int())
      .column("col_bigint", DataTypes.bigint())
      .column("col_float", DataTypes.float())
      .column("col_double", DataTypes.double())
      .column("col_string", DataTypes.string())
      .column("col_bytes", DataTypes.bytes())
      .build();

    assert.strictEqual(schema.columnNames.length, 9);
    assert.deepStrictEqual(schema.primaryKeys, []);
  });

  it("should return columns as [name, type] pairs via getColumns()", () => {
    const schema = Schema.builder()
      .column("id", DataTypes.int())
      .column("name", DataTypes.string())
      .primaryKey(["id"])
      .build();

    const columns = schema.getColumns();
    assert.strictEqual(columns.length, 2);
    assert.strictEqual(columns[0][0], "id");
    assert.strictEqual(columns[1][0], "name");
    // Type strings should be non-empty
    assert.ok(columns[0][1].length > 0);
    assert.ok(columns[1][1].length > 0);
  });

  it("should support composite primary keys", () => {
    const schema = Schema.builder()
      .column("region", DataTypes.string())
      .column("user_id", DataTypes.int())
      .column("score", DataTypes.bigint())
      .primaryKey(["region", "user_id"])
      .build();

    assert.deepStrictEqual(schema.primaryKeys, ["region", "user_id"]);
    assert.deepStrictEqual(schema.columnNames, ["region", "user_id", "score"]);
  });

  it("should support advanced data types", () => {
    const schema = Schema.builder()
      .column("col_decimal", DataTypes.decimal(10, 2))
      .column("col_date", DataTypes.date())
      .column("col_time", DataTypes.time())
      .column("col_timestamp", DataTypes.timestamp())
      .column("col_timestamp_ltz", DataTypes.timestampLtz())
      .column("col_char", DataTypes.char(10))
      .column("col_binary", DataTypes.binary(20))
      .build();

    assert.strictEqual(schema.columnNames.length, 7);
  });
});
