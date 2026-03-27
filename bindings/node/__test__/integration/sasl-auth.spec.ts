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
 * Integration tests for SASL/PLAIN authentication.
 *
 * Mirrors the Python test/test_sasl_auth.py and the Rust integration tests.
 */

import { describe, it, before } from "node:test";
import assert from "node:assert/strict";
import {
  startCluster,
  getSaslBootstrapServers,
  getBootstrapServers,
} from "../helpers/cluster";
import {
  Config,
  FlussConnection,
  DatabaseDescriptor,
} from "../../index.js";

let saslBootstrapServers: string;
let plaintextBootstrapServers: string;

before(async () => {
  await startCluster();
  saslBootstrapServers = getSaslBootstrapServers();
  plaintextBootstrapServers = getBootstrapServers();
});

describe("SASL Authentication", () => {
  it("should connect with valid admin credentials", async () => {
    const config = new Config({
      "bootstrap.servers": saslBootstrapServers,
      "security.protocol": "sasl",
      "security.sasl.mechanism": "PLAIN",
      "security.sasl.username": "admin",
      "security.sasl.password": "admin-secret",
    });
    const conn = await FlussConnection.create(config);
    const admin = conn.getAdmin();

    const dbName = "node_sasl_test_valid_db";
    const descriptor = new DatabaseDescriptor("created via SASL auth");
    await admin.createDatabase(dbName, descriptor, true);

    assert.strictEqual(await admin.databaseExists(dbName), true);

    // Cleanup
    await admin.dropDatabase(dbName, true, true);
    conn.close();
  });

  it("should connect with second user (alice)", async () => {
    const config = new Config({
      "bootstrap.servers": saslBootstrapServers,
      "security.protocol": "sasl",
      "security.sasl.mechanism": "PLAIN",
      "security.sasl.username": "alice",
      "security.sasl.password": "alice-secret",
    });
    const conn = await FlussConnection.create(config);
    const admin = conn.getAdmin();

    // Basic operation to confirm functional connection
    assert.strictEqual(
      await admin.databaseExists("some_nonexistent_db_alice"),
      false
    );
    conn.close();
  });

  it("should reject wrong password", async () => {
    const config = new Config({
      "bootstrap.servers": saslBootstrapServers,
      "security.protocol": "sasl",
      "security.sasl.mechanism": "PLAIN",
      "security.sasl.username": "admin",
      "security.sasl.password": "wrong-password",
    });
    await assert.rejects(
      () => FlussConnection.create(config),
      /error/i
    );
  });

  it("should reject unknown user", async () => {
    const config = new Config({
      "bootstrap.servers": saslBootstrapServers,
      "security.protocol": "sasl",
      "security.sasl.mechanism": "PLAIN",
      "security.sasl.username": "nonexistent_user",
      "security.sasl.password": "some-password",
    });
    await assert.rejects(
      () => FlussConnection.create(config),
      /error/i
    );
  });

  it("should fail when SASL client connects to plaintext server", async () => {
    const config = new Config({
      "bootstrap.servers": plaintextBootstrapServers,
      "security.protocol": "sasl",
      "security.sasl.mechanism": "PLAIN",
      "security.sasl.username": "admin",
      "security.sasl.password": "admin-secret",
    });
    await assert.rejects(() => FlussConnection.create(config));
  });
});
