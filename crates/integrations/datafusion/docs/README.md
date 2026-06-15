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
- **Log tables**: bounded scan with `LIMIT`, supporting projection pushdown.
- Fluss-schema-to-Arrow-schema and Fluss-row-to-`RecordBatch` conversion.
- A usage model of a shared installer plus per-session `register_catalog(...)`.
- Unsupported query forms **fail conservatively** (raise an explicit error)
  rather than silently degrading into a misleading full-table scan.

Phase 1 **excludes**: PostgreSQL / MySQL compatibility objects, REST/gRPC
interfaces, session or operation lifecycle, principal routing, multi-cluster,
SQL writes (DML), and gateway auth / audit.

## Usage

The overall model is "one shared installer plus a per-session install":

1. Construct one shared `FlussDatafusion` from a `FlussConnection` (it holds the
   metadata cache internally).
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

    // 2) Build the shared installer (construct once, reuse the metadata cache across sessions).
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
across many `SessionContext`s. Repeated `register_catalog` calls (including on a
freshly created context) reuse the cached database/table listing and do not
re-issue remote metadata requests.

## Supported query forms

| Table type | Supported form | Notes |
|---|---|---|
| KV (primary-key table) | `WHERE <full primary key> = <value>` (composite keys use `AND` equality on every column) | Pushed down as a point lookup; a hit returns 1 row, a miss returns 0 rows (no error) |
| Log table | `... LIMIT n` | Bounded scan; projection pushdown; keeps the **last** `n` rows (matching Fluss `LimitBatchScanner` semantics) |

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

`FlussDatafusionOptions`:

| Field | Default | Meaning |
|---|---|---|
| `metadata_cache_ttl` | `300s` | TTL for the shared metadata cache; the database/table listing is not re-fetched during this window |

`RegisterCatalogOptions` is currently an empty placeholder, reserved for future
per-catalog options.

## Metadata visibility and known limitations (open for discussion)

> This is a **known open question**. The current implementation deliberately
> keeps snapshot semantics; the fix is to be discussed later.

DataFusion's `CatalogProvider` / `SchemaProvider` listing methods
(`schema_names()`, `table_names()`, `table_exist()`) are **synchronous**, whereas
Fluss's metadata interface is async. There are only two ways to obtain an async
result inside a synchronous context: put the data into memory ahead of time (a
snapshot), or `block_on` and wait for the remote response inside the synchronous
context (which requires working around tokio's nested runtime and pays the cost
of a blocking RPC plus thread creation on every listing). This crate chooses the
**snapshot** approach, trading for nanosecond-level synchronous reads and a clean
implementation.

This yields two layers of snapshot semantics:

| Layer | Freeze point | Behavior |
|---|---|---|
| Provider snapshot | At the moment of `register_catalog`, the database/table names are frozen into a `Vec<String>` | The provider **never goes back to the source** during its `SessionContext` lifetime; it does not refresh even after the cache TTL expires |
| Cache TTL (`metadata_cache_ttl`, default 300s) | On the next `build_catalog_provider` / the first load of a given table's meta | Determines whether to go back to the source when rebuilding the provider or loading a table schema |

**Current visibility contract:**

- A catalog's database/table listing is a snapshot as of the `register_catalog`
  moment.
- Tables created **after** registration are **invisible** to that
  `SessionContext` (`table()` returns `None` for a name not in the snapshot,
  without going back to the source), independent of the cache TTL. To see new
  tables, call `register_catalog` again.
- A table's schema/meta is cached on first access according to the TTL; schema
  changes within the TTL are not reflected immediately.

**Fix directions under discussion** (not yet implemented, noted for memory):

- Document the contract without changing code — if consumers already re-register
  per request / per session, snapshot staleness is barely a problem.
- Provide explicit `invalidate()` / `refresh()` primitives on `FlussDatafusion`
  that consumers call after DDL as needed — keeping synchronous reads fast and
  making refresh event-driven.
- Change the provider to peek the cache synchronously rather than freeze a `Vec`
  (a partial fix; the synchronous methods still cannot go back to the source on
  their own when the cache is empty or expired).
- Query live via `block_on` (always fresh, but at the cost of a blocking RPC plus
  the nested-runtime thread hack on every listing).

The choice depends on the consumer's access pattern (re-register per request vs.
long-lived session) and the source of DDL (whether it flows through this crate /
gateway). This is left to be decided later in light of the gateway's actual
usage.

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
| (default) | Unit tests: schema mapping, `ScalarValue`-to-key conversion, predicate recognition, pushdown decisions, error mapping, and the metadata cache's TTL / hit-reuse logic | No |
| `integration_tests` | Integration tests against a real Fluss cluster: e2e (real SQL against the real backend) | Yes (Docker / podman) |

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
|    MetadataLoader + Cache   |   reuses metadata across sessions
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

- `metadata/`: shared metadata loading and caching.
- `catalog/`: `CatalogProvider` / `SchemaProvider`.
- `table/`: `TableProvider` and predicate recognition.
- `execution/`: custom `ExecutionPlan`s (`FlussKvLookupExec` / `FlussLogScanExec`).
- `types/`: Fluss <-> Arrow type bridging.
- `install.rs`: shared installer / registration entry point.

This contains **no** server-side code, protocol adapters, `pg_catalog`, REST
handlers, auth, session registration, or operation-lifecycle logic.
