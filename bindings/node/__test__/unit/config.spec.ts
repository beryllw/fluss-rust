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
 * Unit tests for Config class (no cluster required).
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { Config } from "../../index.js";

describe("Config", () => {
  it("should create with dot-notation properties", () => {
    const config = new Config({ "bootstrap.servers": "localhost:9123" });
    assert.strictEqual(config.bootstrapServers, "localhost:9123");
  });

  it("should create with camelCase properties", () => {
    const config = new Config({ bootstrapServers: "localhost:9123" });
    assert.strictEqual(config.bootstrapServers, "localhost:9123");
  });

  it("should create from properties map", () => {
    const config = Config.fromProperties({
      "bootstrap.servers": "localhost:9123",
      "writer.acks": "all",
    });
    assert.strictEqual(config.bootstrapServers, "localhost:9123");
    assert.strictEqual(config.writerAcks, "all");
  });

  it("should support getter/setter for bootstrapServers", () => {
    const config = new Config();
    config.bootstrapServers = "host1:9090,host2:9090";
    assert.strictEqual(config.bootstrapServers, "host1:9090,host2:9090");
  });

  it("should support getter/setter for writerAcks", () => {
    const config = new Config();
    config.writerAcks = "all";
    assert.strictEqual(config.writerAcks, "all");
  });

  it("should support getter/setter for writerRetries", () => {
    const config = new Config({ "writer.retries": 5 });
    assert.strictEqual(config.writerRetries, 5);
    config.writerRetries = 10;
    assert.strictEqual(config.writerRetries, 10);
  });

  it("should support getter/setter for writerBatchSize", () => {
    const config = new Config({ writerBatchSize: 32768 });
    assert.strictEqual(config.writerBatchSize, 32768);
  });

  it("should support getter/setter for writerEnableIdempotence", () => {
    const config = new Config({ writerEnableIdempotence: true });
    assert.strictEqual(config.writerEnableIdempotence, true);
  });

  it("should support getter/setter for securityProtocol", () => {
    const config = new Config({ securityProtocol: "sasl" });
    assert.strictEqual(config.securityProtocol, "sasl");
  });

  it("should support getter/setter for connectTimeoutMs", () => {
    const config = new Config({ connectTimeoutMs: 5000 });
    assert.strictEqual(config.connectTimeoutMs, 5000);
  });

  it("should support SASL properties via constructor", () => {
    const config = new Config({
      "security.protocol": "sasl",
      "security.sasl.mechanism": "PLAIN",
      "security.sasl.username": "admin",
      "security.sasl.password": "admin-secret",
    });
    assert.strictEqual(config.securityProtocol, "sasl");
  });

  it("should throw on unknown property key", () => {
    assert.throws(
      () => new Config({ "totally.unknown.key": "value" }),
      /Unknown property/
    );
  });

  it("should accept multiple properties in constructor", () => {
    const config = new Config({
      "bootstrap.servers": "localhost:9123",
      "writer.acks": "all",
      "writer.retries": 3,
      "writer.batch-size": 16384,
      "writer.enable-idempotence": true,
    });
    assert.strictEqual(config.bootstrapServers, "localhost:9123");
    assert.strictEqual(config.writerAcks, "all");
    assert.strictEqual(config.writerRetries, 3);
    assert.strictEqual(config.writerBatchSize, 16384);
    assert.strictEqual(config.writerEnableIdempotence, true);
  });
});
