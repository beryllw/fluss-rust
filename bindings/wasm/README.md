# Apache Fluss WASM Bindings

WebAssembly bindings for Apache Fluss (Incubating) client, enabling Node.js and browser applications to interact with Fluss clusters.

## Installation

```bash
npm install @apache/fluss-wasm
```

## Usage

### Basic Example

```javascript
import { FlussClient } from '@apache/fluss-wasm';

async function main() {
  // Connect to Fluss cluster
  const client = await FlussClient.new('localhost:9123');

  // Get admin interface
  const admin = client.getAdmin();

  // List databases
  const databases = await admin.listDatabases();
  console.log('Databases:', databases);

  // Get a table
  const table = await client.getTable('fluss', 'users');

  // Write data (log table)
  const writer = table.newAppend();
  // ... write data

  // Close client
  client.close();
}

main().catch(console.error);
```

### Using Config Object

```javascript
import { FlussClient, WasmConfig } from '@apache/fluss-wasm';

const config = new WasmConfig('localhost:9123')
  .withRequestTimeout(30000)
  .withMaxRequestSize(10485760);

const client = await FlussClient.new(config);
```

## API Reference

### FlussClient

Main entry point for connecting to a Fluss cluster.

#### `FlussClient.new(config)`

Create a new client connection.

- `config`: String (bootstrap servers) or `WasmConfig` object

#### `client.getAdmin()`

Get the admin interface for managing databases and tables.

#### `client.getTable(database, table)`

Get a handle to a specific table.

#### `client.close()`

Close the client and release resources.

### FlussAdmin

Admin interface for cluster management.

- `createDatabase(name)`: Create a new database
- `listDatabases()`: List all databases
- `databaseExists(name)`: Check if a database exists
- `dropDatabase(name, ifExists?)`: Drop a database
- `listTables(database)`: List tables in a database
- `tableExists(database, table)`: Check if a table exists
- `getTableInfo(database, table)`: Get table information

### FlussTable

Represents a Fluss table for data operations.

#### Log Tables (Append + Scan)

```javascript
const table = await client.getTable('fluss', 'events');

// Append data
const writer = table.newAppend();
writer.append(row);
await writer.flush();

// Scan data
const scanner = table.newScan();
await scanner.subscribe(0, OffsetSpec.earliest());
const records = await scanner.poll(1000);
```

#### Primary Key Tables (Upsert + Lookup)

```javascript
const table = await client.getTable('fluss', 'users');

// Upsert data
const writer = table.newUpsert();
writer.upsert(row);
await writer.flush();

// Lookup data
const lookuper = table.newLookup();
const result = await lookuper.lookup(key);
```

## Building from Source

### Prerequisites

- Rust 1.85+
- [wasm-pack](https://rustwasm.github.io/wasm-pack/installer/)

```bash
# Install wasm-pack
curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh

# Build for Node.js
cd bindings/wasm
npm run build

# Build for web browsers
npm run build:web
```

## Development

```bash
# Run tests
npm test

# Clean build artifacts
npm run clean
```

## License

Apache-2.0

## Links

- [Apache Fluss Documentation](https://fluss.apache.org/)
- [GitHub Repository](https://github.com/apache/fluss-rust)
- [npm Package](https://www.npmjs.com/package/@apache/fluss-wasm)
