# fluss-datafusion

> For the Chinese version, see [README.zh.md](./README.zh.md).

`fluss-datafusion` is a **stateless** DataFusion integration crate with a single
responsibility: turning `SQL` into access to Apache Fluss tables. It exposes
Fluss KV and Log tables to DataFusion as `CatalogProvider` / `SchemaProvider` /
`TableProvider`, and supplies custom `ExecutionPlan`s for the supported query
forms, so you can query Fluss directly with `ctx.sql(...)`.

> This crate is unaware of caller identity, protocol, session variables, or auth
> mode, and it does not know whether it is running inside a gateway. The
> dependency direction is strictly one-way: `fluss client/core -> fluss-datafusion`,
> never the reverse.

## Current capabilities (Phase 1)

- List databases / tables, and fetch a table's schema and table type.
- **KV tables**: full-primary-key equality predicate pushed down as a point
  lookup, for both single and composite primary keys.
- **Log tables**: bounded scan with `LIMIT`, supporting projection pushdown and
  multi-bucket tables (one bucket per parallel partition, per-bucket last-N with
  a final cross-bucket `LIMIT`; no cross-bucket order guarantee).
- Fluss-schema-to-Arrow-schema and Fluss-row-to-`RecordBatch` conversion.
- A usage model of a shared installer plus per-session `register_catalog(...)`.
- Unsupported query forms **fail conservatively** (raise an explicit error)
  rather than silently degrading into a misleading full-table scan.

Phase 1 **excludes**: PostgreSQL / MySQL compatibility objects, REST/gRPC
interfaces, session or operation lifecycle, principal routing, multi-cluster,
SQL writes (DML), and gateway auth / audit.

## Usage

The overall model is "one shared installer plus a per-session install":

1. Construct one shared `FlussDatafusion` from a `FlussConnection` (stateless; it
   holds no metadata cache).
2. Each SQL session creates its own `SessionContext`.
3. The session installs the Fluss catalog into that context via
   `register_catalog(...)`.
4. From then on, just call `ctx.sql(...)` as usual.

```rust,no_run
use std::sync::Arc;

use datafusion::execution::context::SessionContext;
use fluss::client::FlussConnection;
use fluss::config::Config;

use fluss_datafusion::{FlussDatafusion, FlussDatafusionOptions, RegisterCatalogOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1) Connect to Fluss.
    let mut config = Config::default();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();
    let connection = Arc::new(FlussConnection::new(config).await?);

    // 2) Build the shared installer (construct once, reuse across sessions; metadata stays live).
    let fd = FlussDatafusion::new(connection, FlussDatafusionOptions::default()).await?;

    // 3) Each session creates its own context and installs the catalog.
    let ctx = SessionContext::new();
    fd.register_catalog(&ctx, "fluss", RegisterCatalogOptions::default())
        .await?;

    // 4) Query normally. Table names look like <catalog>.<database>.<table>.
    let df = ctx
        .sql("SELECT id, name, age FROM fluss.my_db.users WHERE id = 2")
        .await?;
    df.show().await?;

    Ok(())
}
```

`FlussDatafusion` is `Arc`-shared and stateless: construct it once and reuse it
across many `SessionContext`s. The catalog is fully live — it holds no cached
database/table listing, so every listing/table call goes back to Fluss and DDL is
visible in the same session immediately.

## Supported query forms

| Table type | Supported form | Notes |
|---|---|---|
| KV (primary-key table) | `WHERE <full primary key> = <value>` (composite keys use `AND` equality on every column) | Pushed down as a point lookup; a hit returns 1 row, a miss returns 0 rows (no error) |
| Log table | `... LIMIT n` | Bounded scan; projection pushdown; multi-bucket (one bucket per parallel partition); each bucket keeps its **last** `n` rows (matching Fluss `LimitBatchScanner` semantics), with a final cross-bucket `LIMIT` applied; no cross-bucket order guarantee |

### Forms that fail conservatively

The following fail outright at **planning time** instead of degrading into a
full-table scan:

| Query | Error |
|---|---|
| A KV table with a predicate on a non-primary-key column, e.g. `WHERE name = 'x'` | `unsupported query pattern: ...` |
| A Log table without `LIMIT`, e.g. `SELECT * FROM log_t` | `LIMIT required: ...` |

You can confirm the custom execution plan with `EXPLAIN`: a KV point lookup
shows `FlussKvLookupExec`, and a Log bounded scan shows `FlussLogScanExec`.

## Configuration

`FlussDatafusionOptions` and `RegisterCatalogOptions` are both currently empty
placeholders, reserved for future options. The catalog is fully live with no
cache to tune, so there is nothing to configure today.

## Metadata visibility

The catalog is **fully live, with zero snapshot and no cache**: every catalog
listing/table call (`schema_names()`, `schema()`, `table_names()`,
`table_exist()`, `table()`) fetches fresh from Fluss. This matches SQL catalog
semantics — DDL is visible in the same `SessionContext` immediately:

- A table created **after** `register_catalog` is queryable in the same session,
  with no need to re-register.
- A dropped table disappears from that same session's listing and `table()`
  resolution right away.
- A table's schema/meta is re-read on every access, so schema changes are
  reflected immediately.

DataFusion's `CatalogProvider` / `SchemaProvider` listing methods are
**synchronous**, whereas Fluss's metadata interface is async. To stay live, the
crate bridges the two: the synchronous callbacks run the async source call to
completion via a small `block_on` helper (`src/runtime.rs`), spawning a short-lived
thread when already inside a tokio runtime to avoid the nested-runtime panic. The
known, accepted cost is **one admin RPC per catalog call** (plus the thread hop on
the synchronous paths). This is intentional for this phase; no caching is layered
on top.

## Error model

The public error type `FlussDatafusionError` (with the alias `Result<T>`) clearly
distinguishes the various unsupported/failure cases: `DatabaseNotFound` /
`TableNotFound` / `UnsupportedQueryPattern` / `LimitRequired` / `SchemaMismatch` /
`TypeConversion` / `FlussClient` / `Internal`. It is bridged into DataFusion's
`DataFusionError::External`, so it bubbles up normally through the standard
DataFusion paths (such as `ctx.sql(...).collect()`). The type deliberately does
**not** encode any PostgreSQL / gateway concepts.

## Testing and features

| Feature | Purpose | Containers needed |
|---|---|---|
| (default) | Unit tests: schema mapping, `ScalarValue`-to-key conversion, predicate recognition, pushdown decisions, and error mapping | No |
| `integration_tests` | Integration tests against a real Fluss cluster: e2e (real SQL against the real backend) plus live-metadata visibility | Yes (Docker / podman) |

Common commands:

```bash
# Unit tests (fast, the default CI gate)
cargo test -p fluss-datafusion

# Real-cluster e2e (requires a working container runtime)
cargo test -p fluss-datafusion --features integration_tests -- e2e
```

There are only two test layers: unit tests (no cluster required) plus
real-cluster integration tests. There is no fake/fixture mock layer, so there is
no risk of "cluster-free assertions" and "real assertions" drifting apart from
each other; the integration tests verify the full catalog / execution / pushdown
contract directly against the real backend (`setup.rs` provides shared table
creation and seeding).

## Architecture and boundaries

```
+-----------------------------+
|       SessionContext        |   one per SQL session
+--------------+--------------+
               | register_catalog(...)
               v
+-----------------------------+
|       FlussDatafusion       |   shared installer (Arc, stateless)
|     MetadataLoader (live)   |   no cache; every call hits Fluss
+--------------+--------------+
               | FlussSource (internal trait, crate boundary)
               v
+-----------------------------+
|       RealFlussSource       |   wraps Arc<FlussConnection>
+--------------+--------------+
               |
               v
        Fluss client / core
```

Module responsibilities:

- `metadata/`: shared live metadata loading (no cache).
- `catalog/`: `CatalogProvider` / `SchemaProvider`.
- `table/`: `TableProvider` and predicate recognition.
- `execution/`: custom `ExecutionPlan`s (`FlussKvLookupExec` / `FlussLogScanExec`).
- `types/`: Fluss <-> Arrow type bridging.
- `install.rs`: shared installer / registration entry point.

This contains **no** server-side code, protocol adapters, `pg_catalog`, REST
handlers, auth, session registration, or operation-lifecycle logic.
