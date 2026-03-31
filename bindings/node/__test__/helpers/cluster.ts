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
 * Shared test infrastructure for Fluss Node.js integration tests.
 *
 * If FLUSS_BOOTSTRAP_SERVERS is set, tests connect to an existing cluster.
 * Otherwise, a Fluss cluster is started automatically via testcontainers.
 *
 * Mirrors the Python binding's test/conftest.py.
 */

import net from "node:net";
import { execSync } from "node:child_process";
import { Config, FlussConnection, FlussAdmin } from "../../index.js";

const FLUSS_IMAGE = "apache/fluss";
const FLUSS_VERSION = "0.9.0-incubating";

const BOOTSTRAP_SERVERS_ENV: string | undefined = process.env.FLUSS_BOOTSTRAP_SERVERS;

// Detect the container runtime binary. Shell aliases (e.g. docker → podman)
// are not available to execSync, so we need to find the correct binary.
// testcontainers talks to whatever daemon it detects; we must use the same CLI.
const DOCKER_CMD = (() => {
  try {
    execSync("podman version", { stdio: "pipe" });
    return "podman";
  } catch {
    return "docker";
  }
})();

// Container / network names
const NETWORK_NAME = "fluss-node-test-network";
const ZOOKEEPER_NAME = "zookeeper-node-test";
const COORDINATOR_NAME = "coordinator-server-node-test";
const TABLET_SERVER_NAME = "tablet-server-node-test";

// Fixed host ports (must match across workers)
const COORDINATOR_PORT = 9123;
const TABLET_SERVER_PORT = 9124;
const PLAIN_CLIENT_PORT = 9223;
const PLAIN_CLIENT_TABLET_PORT = 9224;

const ALL_PORTS = [
  COORDINATOR_PORT,
  TABLET_SERVER_PORT,
  PLAIN_CLIENT_PORT,
  PLAIN_CLIENT_TABLET_PORT,
];

// Module-level state
let clusterStarted = false;
let clusterPromise: Promise<void> | null = null;

/**
 * Wait for a TCP port to become available.
 */
function waitForPort(host: string, port: number, timeoutMs = 60_000): Promise<boolean> {
  return new Promise((resolve) => {
    const deadline = Date.now() + timeoutMs;
    const tryConnect = () => {
      if (Date.now() > deadline) {
        resolve(false);
        return;
      }
      const sock = new net.Socket();
      sock.setTimeout(1000);
      sock.once("connect", () => {
        sock.destroy();
        resolve(true);
      });
      sock.once("error", () => {
        sock.destroy();
        setTimeout(tryConnect, 1000);
      });
      sock.once("timeout", () => {
        sock.destroy();
        setTimeout(tryConnect, 1000);
      });
      sock.connect(port, host);
    };
    tryConnect();
  });
}

/**
 * Wait for all cluster ports to become available.
 */
async function allPortsReady(timeoutMs = 60_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  for (const port of ALL_PORTS) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) return false;
    const ready = await waitForPort("localhost", port, remaining);
    if (!ready) return false;
  }
  return true;
}

function runCmd(cmd: string): number {
  try {
    execSync(cmd, { stdio: "pipe" });
    return 0;
  } catch {
    return 1;
  }
}

/**
 * Start the Fluss Docker cluster via testcontainers.
 *
 * If another process already started the cluster (detected via port check),
 * reuses it. If container creation fails (name conflict), waits for the
 * other process's cluster to become ready.
 */
async function startClusterDocker(): Promise<void> {
  // Reuse cluster started by another process or previous run
  if (await waitForPort("localhost", PLAIN_CLIENT_PORT, 1000)) {
    console.log("Reusing existing cluster via port check.");
    return;
  }

  // Disable Ryuk reaper — we handle container cleanup ourselves in
  // stopClusterDocker(). Ryuk can fail to start on some Docker setups.
  process.env.TESTCONTAINERS_RYUK_DISABLED = "true";

  const { GenericContainer, Network } = await import("testcontainers");

  console.log("Starting Fluss cluster via testcontainers...");

  // Create the network via testcontainers API so it uses the same Docker
  // client that will later create the containers (avoids Podman socket
  // mismatch where CLI-created networks are invisible to the API client).
  // Ignore "already exists" errors from a previous unclean shutdown.
  try {
    await new Network({ nextUuid: () => NETWORK_NAME }).start();
  } catch {
    // Network may already exist — that's fine, containers will attach to it.
  }

  const saslJaas =
    "org.apache.fluss.security.auth.sasl.plain.PlainLoginModule required" +
    ' user_admin="admin-secret" user_alice="alice-secret";';

  const coordinatorProps = [
    `zookeeper.address: ${ZOOKEEPER_NAME}:2181`,
    `bind.listeners: INTERNAL://${COORDINATOR_NAME}:0, CLIENT://${COORDINATOR_NAME}:9123, PLAIN_CLIENT://${COORDINATOR_NAME}:9223`,
    "advertised.listeners: CLIENT://localhost:9123, PLAIN_CLIENT://localhost:9223",
    "internal.listener.name: INTERNAL",
    "security.protocol.map: CLIENT:sasl",
    "security.sasl.enabled.mechanisms: plain",
    `security.sasl.plain.jaas.config: ${saslJaas}`,
    "netty.server.num-network-threads: 1",
    "netty.server.num-worker-threads: 3",
  ].join("\n");

  const tabletProps = [
    `zookeeper.address: ${ZOOKEEPER_NAME}:2181`,
    `bind.listeners: INTERNAL://${TABLET_SERVER_NAME}:0, CLIENT://${TABLET_SERVER_NAME}:9123, PLAIN_CLIENT://${TABLET_SERVER_NAME}:9223`,
    "advertised.listeners: CLIENT://localhost:9124, PLAIN_CLIENT://localhost:9224",
    "internal.listener.name: INTERNAL",
    "security.protocol.map: CLIENT:sasl",
    "security.sasl.enabled.mechanisms: plain",
    `security.sasl.plain.jaas.config: ${saslJaas}`,
    "tablet-server.id: 0",
    "netty.server.num-network-threads: 1",
    "netty.server.num-worker-threads: 3",
  ].join("\n");

  try {
    // Start Zookeeper
    await new GenericContainer("zookeeper:3.9.2")
      .withName(ZOOKEEPER_NAME)
      .withNetworkMode(NETWORK_NAME)
      .start();

    // Start Coordinator
    await new GenericContainer(`${FLUSS_IMAGE}:${FLUSS_VERSION}`)
      .withName(COORDINATOR_NAME)
      .withNetworkMode(NETWORK_NAME)
      .withExposedPorts(
        { container: 9123, host: COORDINATOR_PORT },
        { container: 9223, host: PLAIN_CLIENT_PORT }
      )
      .withCommand(["coordinatorServer"])
      .withEnvironment({ FLUSS_PROPERTIES: coordinatorProps })
      .start();

    // Start TabletServer
    await new GenericContainer(`${FLUSS_IMAGE}:${FLUSS_VERSION}`)
      .withName(TABLET_SERVER_NAME)
      .withNetworkMode(NETWORK_NAME)
      .withExposedPorts(
        { container: 9123, host: TABLET_SERVER_PORT },
        { container: 9223, host: PLAIN_CLIENT_TABLET_PORT }
      )
      .withCommand(["tabletServer"])
      .withEnvironment({ FLUSS_PROPERTIES: tabletProps })
      .start();
  } catch (e: unknown) {
    // Another process may have started containers with the same names.
    const message = e instanceof Error ? e.message : String(e);
    console.log(
      `Container start failed (${message}), waiting for cluster from another process...`
    );
    if (await allPortsReady()) {
      return;
    }
    throw e;
  }

  if (!(await allPortsReady())) {
    throw new Error("Cluster listeners did not become ready");
  }

  console.log("Fluss cluster started successfully.");
}

function stopClusterDocker(): void {
  for (const name of [TABLET_SERVER_NAME, COORDINATOR_NAME, ZOOKEEPER_NAME]) {
    runCmd(`${DOCKER_CMD} rm -f ${name}`);
  }
  runCmd(`${DOCKER_CMD} network rm ${NETWORK_NAME}`);
}

/**
 * Start the Fluss cluster. Uses existing cluster if FLUSS_BOOTSTRAP_SERVERS
 * is set, otherwise starts Docker containers.
 */
export async function startCluster(): Promise<void> {
  if (clusterStarted) return;
  if (!clusterPromise) {
    clusterPromise = (async () => {
      if (!BOOTSTRAP_SERVERS_ENV) {
        await startClusterDocker();
      }
      clusterStarted = true;
    })();
  }
  return clusterPromise;
}

/**
 * Stop the Fluss cluster (only stops Docker containers if we started them).
 */
export function stopCluster(): void {
  if (BOOTSTRAP_SERVERS_ENV) return;
  stopClusterDocker();
  clusterStarted = false;
}

/**
 * Get the plaintext bootstrap servers address.
 */
export function getBootstrapServers(): string {
  return BOOTSTRAP_SERVERS_ENV || `127.0.0.1:${PLAIN_CLIENT_PORT}`;
}

/**
 * Get the SASL bootstrap servers address.
 */
export function getSaslBootstrapServers(): string {
  return (
    process.env.FLUSS_SASL_BOOTSTRAP_SERVERS ||
    BOOTSTRAP_SERVERS_ENV ||
    `127.0.0.1:${COORDINATOR_PORT}`
  );
}

/**
 * Connect to the cluster with retries until it's fully ready.
 * Waits until both coordinator and at least one tablet server are available.
 */
export async function connectWithRetry(bootstrapServers: string, timeoutMs = 60_000): Promise<FlussConnection> {
  const config = new Config({ "bootstrap.servers": bootstrapServers });
  const deadline = Date.now() + timeoutMs;
  let lastErr: Error | null = null;

  while (Date.now() < deadline) {
    let conn: FlussConnection | null = null;
    try {
      conn = await FlussConnection.create(config);
      const admin = conn.getAdmin();
      const nodes = await admin.getServerNodes();
      if (nodes.some((n) => n.serverType === "TabletServer")) {
        return conn;
      }
      lastErr = new Error("No TabletServer available yet");
    } catch (e: unknown) {
      lastErr = e instanceof Error ? e : new Error(String(e));
    }
    if (conn) conn.close();
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error(`Could not connect to cluster after ${timeoutMs}ms: ${lastErr}`);
}

/**
 * Get a connected FlussConnection for the plaintext listener.
 */
export async function getConnection(): Promise<FlussConnection> {
  return connectWithRetry(getBootstrapServers());
}

/**
 * Get a FlussAdmin from a connection.
 */
export function getAdmin(connection: FlussConnection): FlussAdmin {
  return connection.getAdmin();
}
