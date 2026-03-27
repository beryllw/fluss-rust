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

# Development Guide

## Prerequisites

- **Node.js** >= 18
- **Rust** toolchain (stable, 1.70+)
- **Docker** (for integration tests)

## Install Dependencies

```bash
npm install
```

## Build

```bash
# Debug build (faster compilation)
napi build --platform

# Release build (optimized)
napi build --platform --release
```

## Run Tests

### Unit Tests (no Docker required)

Unit tests verify Schema, Config, and metadata types without a running cluster:

```bash
npm run test:unit
```

### Integration Tests (Docker required)

Integration tests automatically start a Fluss cluster via Docker (Zookeeper + Coordinator + TabletServer):

```bash
npm run test:integration
```

### All Tests

```bash
npm test
```

### Using an Existing Cluster

Skip Docker and connect to an already-running cluster:

```bash
FLUSS_BOOTSTRAP_SERVERS=localhost:9223 npm run test:integration
```

For SASL tests, also set:

```bash
FLUSS_SASL_BOOTSTRAP_SERVERS=localhost:9123 FLUSS_BOOTSTRAP_SERVERS=localhost:9223 npm test
```

## Run Example

```bash
# With Docker cluster running or external cluster
FLUSS_BOOTSTRAP_SERVERS=localhost:9223 npm run example
```

## Project Structure

```
bindings/node/
├── src/                          # Rust source (napi-rs bindings)
│   ├── lib.rs                    # Module declarations, TOKIO_RUNTIME
│   ├── admin.rs                  # FlussAdmin class
│   ├── config.rs                 # Config class
│   ├── connection.rs             # FlussConnection class
│   ├── error.rs                  # Error conversion
│   ├── lookup.rs                 # Lookuper class
│   ├── metadata.rs               # Schema, TableDescriptor, DataTypes, etc.
│   ├── row.rs                    # JSON <-> Datum conversion
│   ├── scanner.rs                # LogScanner, ScanRecords
│   ├── table.rs                  # FlussTable, TableAppend, TableUpsert, etc.
│   ├── upsert.rs                 # UpsertWriter class
│   └── write_handle.rs           # WriteResultHandle class
├── __test__/
│   ├── helpers/
│   │   └── cluster.mjs           # Docker cluster management
│   ├── unit/
│   │   ├── schema.spec.mjs       # Schema builder tests
│   │   ├── config.spec.mjs       # Config tests
│   │   └── metadata.spec.mjs     # Metadata type tests
│   └── integration/
│       ├── admin.spec.mjs        # Admin operations tests
│       ├── log-table.spec.mjs    # Log table append/scan tests
│       ├── kv-table.spec.mjs     # KV table CRUD tests
│       └── sasl-auth.spec.mjs    # SASL authentication tests
├── example/
│   └── example.mjs               # Comprehensive usage example
├── Cargo.toml                    # Rust dependencies
├── package.json                  # Node.js config
├── tsconfig.json                 # TypeScript config (type checking only)
├── build.rs                      # napi-rs build script
├── README.md                     # User documentation
└── DEVELOPMENT.md                # This file
```
