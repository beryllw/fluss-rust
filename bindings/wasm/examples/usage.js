// Example usage of fluss-wasm in Node.js

import { FlussClient, WasmConfig } from './pkg/fluss_wasm.js';

async function exampleBasic() {
  console.log('=== Basic Usage Example ===');

  // Connect to Fluss cluster
  const client = await FlussClient.new('localhost:9123');
  console.log('Connected to Fluss cluster');

  // Get admin interface
  const admin = client.getAdmin();

  // List databases
  const databases = await admin.listDatabases();
  console.log('Databases:', databases);

  // Create a new database
  await admin.createDatabase('test_db');
  console.log('Created database: test_db');

  // Get a table
  const table = await client.getTable('fluss', 'users');
  console.log('Got table:', table.tableName);

  // Close client
  client.close();
  console.log('Client closed');
}

async function exampleLogTable() {
  console.log('=== Log Table Example ===');

  const client = await FlussClient.new('localhost:9123');
  const table = await client.getTable('fluss', 'events');

  // Create append writer
  const writer = table.newAppend();

  // Append some data (you need to create proper row objects)
  // const row = { ts: Date.now(), message: 'hello' };
  // writer.append(row);
  // await writer.flush();

  // Create scanner
  const scanner = table.newScan();
  // await scanner.subscribe(0, OffsetSpec.earliest());
  // const records = await scanner.poll(1000);

  client.close();
}

async function examplePrimaryKeyTable() {
  console.log('=== Primary Key Table Example ===');

  const client = await FlussClient.new('localhost:9123');
  const table = await client.getTable('fluss', 'users');

  // Create upsert writer
  const writer = table.newUpsert();

  // Upsert some data
  // const row = { id: 1, name: 'Alice', age: 30 };
  // writer.upsert(row);
  // await writer.flush();

  // Create lookuper
  const lookuper = table.newLookup();
  // const key = { id: 1 };
  // const result = await lookuper.lookup(key);

  client.close();
}

async function exampleWithConfig() {
  console.log('=== Config Example ===');

  const config = new WasmConfig('localhost:9123')
    .withRequestTimeout(30000)
    .withMaxRequestSize(10485760);

  const client = await FlussClient.new(config);
  console.log('Connected with custom config');

  client.close();
}

// Run examples
async function main() {
  try {
    // Uncomment to run examples:
    // await exampleBasic();
    // await exampleLogTable();
    // await examplePrimaryKeyTable();
    // await exampleWithConfig();

    console.log('Examples loaded. Uncomment the functions you want to run.');
  } catch (error) {
    console.error('Error:', error);
  }
}

main();
