# fluss-datafusion design document

> For the Chinese version, see [design.zh.md](./design.zh.md).

This document describes the design and internal structure of `fluss-datafusion`,
aimed at maintainers of the crate and at the future `fluss-gateway` integration
work that will consume it. For usage and the public API, see the [`README.md`](./README.md)
in the same directory.

## Overview

`fluss-datafusion` is a **stateless** Rust library crate that exposes Fluss data
to DataFusion through catalog / schema / table / execution-plan integration, so
callers can query Fluss tables directly with `ctx.sql(...)`.

The design is deliberately narrow:

- make the SQL read path of `fluss-gateway` possible;
- keep the crate independently reusable outside the gateway;
- reuse the existing Fluss Rust client rather than inventing a gateway-specific
  backend abstraction.

## Goals and scope

### Currently supported

1. Database and table discovery (list / get schema / table type).
2. Full-primary-key equality predicate SQL pushdown for KV tables, pushed down as
   a point lookup.
3. Bounded scan for log tables, which must carry a `LIMIT`, with projection
   pushdown.
4. Reuse of the Fluss-to-Arrow schema and row conversion.
5. DataFusion integration via `CatalogProvider` / `SchemaProvider` /
   `TableProvider` / custom `ExecutionPlan`s.
6. A usage model of a shared installer plus per-session `register_catalog(...)`.

### Non-goals

Deliberately excluded (to avoid over-promising SQL semantics that are not yet
implemented):

- PostgreSQL / MySQL protocols or compatibility objects (such as `pg_catalog`).
- session / user / auth / multi-cluster awareness.
- SQL DML writes.
- direct-access REST / gRPC APIs.
- KV prefix-scan pushdown and batch-lookup optimizations.
- offset pseudo-columns and offset-predicate pushdown.
- complex filter / join / aggregate / sort pushdown.

## Architecture and dependency direction

The dependency direction is strictly one-way: `fluss client/core -> fluss-datafusion`,
never the reverse. The crate is unaware of caller identity, protocol, session
variables, or auth mode, and it does not know whether it is running inside a
gateway.

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

The core boundary is the internal trait `FlussSource`: inside the crate, Fluss is
accessed only through it. Neither `metadata/loader.rs` nor `execution/*`
**directly holds** a `FlussConnection` / `FlussAdmin` / `Lookuper` / scanner.
`FlussSource` is an internal test seam (`pub(crate)`), not a public gateway
backend abstraction, and must not be loaded with session / protocol / auth
concepts.

## Crate layout

```text
crates/integrations/datafusion/
  Cargo.toml
  docs/
    README.md          # usage and public API
    design.md          # this file
  src/
    lib.rs             # public exports
    config.rs          # FlussDatafusionOptions / RegisterCatalogOptions
    error.rs           # FlussDatafusionError / Result
    install.rs         # FlussDatafusion: new / register_catalog
    backend/
      mod.rs           # FlussSource trait (pub(crate)) + shared aliases
      real.rs          # production implementation, wraps FlussConnection
    metadata/
      mod.rs
      cache.rs         # shared, RwLock-guarded, per-entry TTL
      loader.rs        # depends only on FlussSource, fronts the cache
    catalog/
      mod.rs
      provider.rs      # FlussCatalogProvider (sync schema_names / schema)
      schema.rs        # FlussSchemaProvider (async table() lazy loading)
      register.rs      # build_catalog_provider
    table/
      mod.rs
      predicate.rs     # KV predicate recognition (analyze_kv_filters)
      kv.rs            # FlussKvTableProvider
      log.rs           # FlussLogTableProvider
    execution/
      mod.rs
      lookup.rs        # FlussKvLookupExec
      log_scan.rs      # FlussLogScanExec
      stream.rs        # stream adapter with cooperative cancellation
    types/
      mod.rs
      scalar.rs        # ScalarValue -> Fluss KeyValue
      record_batch.rs  # projection column trimming
  tests/
    test_fluss_datafusion.rs
    integration/
      mod.rs
      utils.rs         # shared table-name constants + SQL-path helper
      setup.rs         # integration_tests: real-cluster bootstrap + table creation/seeding
      e2e.rs           # integration_tests: end-to-end SQL against the real backend
```

> Note: the crate lives at `crates/integrations/datafusion/` (the package name is
> `fluss-datafusion`). The `integrations/` directory level groups other
> integrations that may be added in the future (such as other query engines /
> formats).

## Public API

The public surface is deliberately minimal and is exported entirely from
`lib.rs`:

- `FlussDatafusion` (`install.rs`) — the shared installer.
- `FlussDatafusionOptions` / `RegisterCatalogOptions` (`config.rs`).
- `FlussDatafusionError` / `Result` (`error.rs`).

```rust
pub struct FlussDatafusionOptions {
    pub metadata_cache_ttl: std::time::Duration,   // default 300s
}

pub struct RegisterCatalogOptions {}               // placeholder, reserved for per-catalog options

impl FlussDatafusion {
    pub async fn new(
        connection: Arc<fluss::client::FlussConnection>,
        options: FlussDatafusionOptions,
    ) -> Result<Self>;

    pub async fn register_catalog(
        &self,
        ctx: &datafusion::execution::context::SessionContext,
        catalog_name: &str,
        options: RegisterCatalogOptions,
    ) -> Result<()>;
}
```

`new()` takes a concrete `Arc<FlussConnection>`, but internally wraps it
immediately into an `Arc<dyn FlussSource>`, after which the crate no longer
depends on `FlussConnection`. `FlussSource` is a purely `pub(crate)` internal
seam; it exposes no test-injection entry point. Integration tests always go
through the `new()` production path against a real cluster.

## Internal module responsibilities

### `backend/` (internal access seam)

- The `FlussSource` trait (`pub(crate)`) covers only the atomic operations Phase 1
  actually uses: metadata listing/fetching, KV full-primary-key
  `lookup(key) -> RecordBatch`, and log bounded `scan -> Vec<RecordBatch>`.
- `real.rs`: the sole implementation, wrapping `FlussConnection` / `FlussAdmin` /
  lookuper / scanner, reusing Fluss's existing Arrow helpers (such as
  `LookupResult::to_record_batch`) rather than reimplementing Arrow assembly.
- The seam returns the crate's own slim `FlussTableMeta` (schema + primary_keys +
  num_buckets, etc.), decoupling upper layers from `fluss::metadata::TableInfo`
  and carrying only the fields catalog wiring, predicate analysis, and execution
  truly need.

### `metadata/`

- `cache.rs`: a shared, `RwLock`-guarded snapshot (db/table listing + per-table
  `TableEntry{meta, arrow_schema}`), with a per-entry `Instant`-based TTL. **The
  lock guard is released immediately after reading/cloning, and is never held
  across an `.await`.**
- `loader.rs`: `MetadataLoader` depends only on `SharedFlussSource` and fronts the
  cache. The database/table listing is a single whole-cluster read shared across
  all `SessionContext`s; per-table meta + Arrow schema is lazily loaded and
  cached on the first `table()` call. The Arrow schema is converted via
  `fluss::record::to_arrow_schema(meta.schema.row_type())`.

### `catalog/`

- `provider.rs`: `FlussCatalogProvider` maps a Fluss database to a schema. The
  synchronous `schema_names()` / `schema()` are served directly from the snapshot
  pre-built at `register_catalog` time, with no async needed.
- `schema.rs`: `FlussSchemaProvider`'s synchronous `table_names()` comes from the
  registration-time snapshot; its async `table()` branches on
  `meta.has_primary_key()` to return either `FlussKvTableProvider` (KV) or
  `FlussLogTableProvider` (log).
- `register.rs`: `build_catalog_provider` assembles the provider tree from the
  shared (cache-fronted) listing snapshot, and takes effect when
  `register_catalog` calls `ctx.register_catalog(...)`.

> The sync/async bridge uses a "snapshot at registration time" instead of a
> `block_on` inside a synchronous callback, thereby avoiding the nested-runtime
> thread hack. The cost is the snapshot semantics of listing visibility; see the
> README's "Metadata visibility and known limitations".

### `table/`

- `predicate.rs`: `analyze_kv_filters` recognizes the top-level AND conjuncts that
  DataFusion has split apart as a full-primary-key equality, producing
  `LookupKey`s in primary-key order; it explicitly rejects partial PKs, non-PK
  columns, `IN`, ranges, column-to-column comparisons, duplicate/missing PKs, and
  empty filters, all as `UnsupportedQueryPattern`.
- `kv.rs`: `FlussKvTableProvider`'s `supports_filters_pushdown` marks only PK
  equality filters as `Exact` (consumed, so no `FilterExec` is generated) and
  everything else as `Unsupported`; `scan()` builds the lookup plan on a match
  (correctly passing the projection), otherwise raises `UnsupportedQueryPattern`
  directly and never silently degrades to a full scan.
- `log.rs`: `FlussLogTableProvider` cleanly returns `LimitRequired` when `limit`
  is `None`; its `supports_filters_pushdown` is always `Unsupported` (residual
  filters are left to the upper `FilterExec`); the projection is pushed down to
  `FlussSource::log_scan`. A full identity projection from `SELECT *` is
  normalized to `None`.

### `execution/`

- `lookup.rs`: `FlussKvLookupExec`, a single-partition leaf `ExecutionPlan` whose
  `execute()` calls `source.lookup` asynchronously through `single_batch_stream`
  to produce 0/1 rows; it implements `DisplayAs`, so `EXPLAIN` shows
  `FlussKvLookupExec`.
- `log_scan.rs`: `FlussLogScanExec`, a single-partition leaf whose `execute()`
  calls `source.log_scan(projection, limit)` asynchronously through
  `bounded_batches_stream`; `EXPLAIN` shows `FlussLogScanExec`.
- `stream.rs`: adapts the lookup / scan output into a DataFusion stream
  (`futures::stream::once` / Vec versions); dropping it is cooperative
  cancellation.
- An asymmetry: `log_scan` is already projected on the `FlussSource` side, so the
  log path does **not** project a second time; the KV lookup returns all columns
  and is then trimmed by `project_batch` in `types/record_batch.rs`.

### `types/`

- `scalar.rs`: `scalar_to_key_value` strictly converts a `ScalarValue` (`Boolean`
  / `Int8/16/32/64` / `Utf8`) into a `KeyValue`; NULL and other types explicitly
  return `TypeConversion`.
- `record_batch.rs`: `project_batch` centralizes per-projection column trimming
  (`None` passes through).
- There is no `types/schema.rs`: the Fluss->Arrow mapping reuses Fluss's
  `to_arrow_schema` directly, with no de-duplication benefit.

## Query semantics

### KV tables

Only **full-primary-key equality** predicates are supported (`pk1 = v1 AND pk2 = v2 ...`,
covering every PK column). A hit returns 1 row, a miss returns 0 rows (no error).
Partial keys / prefixes / ranges / non-key filters / `IN` all fail cleanly with
`UnsupportedQueryPattern` and never silently degrade to a full scan.

### Log tables

Locked-in decisions:

1. `LIMIT` is mandatory; its absence is `LimitRequired`.
2. Projection pushdown is supported.
3. No offset pseudo-column is exposed and offset predicates are not supported.
4. The underlying Fluss `LimitBatchScanner` keeps the **last** `limit` rows
   (last-N), not the first-N starting from the earliest offset.
5. Phase 1 supports only a single-bucket bounded scan; multi-bucket tables fail
   cleanly with `UnsupportedQueryPattern` to avoid silently dropping rows.
6. `ORDER BY` pushdown is out of scope; cross-bucket global row order is not
   guaranteed.

## Error model

`error.rs` defines the crate-local error `FlussDatafusionError` (with the alias
`Result<T>`), which clearly distinguishes the various unsupported/failure cases:

```rust
pub enum FlussDatafusionError {
    DatabaseNotFound(String),
    TableNotFound(String),
    UnsupportedQueryPattern(String),
    LimitRequired(String),
    SchemaMismatch(String),
    TypeConversion(String),
    FlussClient(String),
    Internal(String),
}
```

It bubbles up cleanly at the plan or collect stage via
`From<FlussDatafusionError> for DataFusionError` (`External`). The `From`
implementations map fluss / arrow errors to `FlussClient` / `SchemaMismatch`
respectively. The type deliberately does **not** encode any PostgreSQL / gateway
concepts.

## Testing strategy

A single real-cluster layering: there is no fake/fixture mock layer, avoiding two
copies of "cluster-free assertions" and "real assertions" drifting apart. There
are two layers:

| Layer | Feature | Containers | Purpose |
|---|---|---|---|
| Unit tests | default | No | schema mapping, `ScalarValue`-to-key conversion, predicate recognition, pushdown decisions, error mapping, and the metadata cache's TTL / hit-reuse logic |
| Integration tests (e2e) | `integration_tests` | Yes | real SQL against the real backend (`FlussDatafusion::new` -> `RealFlussSource`), covering catalog listing, KV point lookup (single/composite PK, missing key, and unsupported forms such as non-PK / partial PK / `IN` / no filter), log bounded scan and projection, `EXPLAIN` custom plans, and the Arrow schema exposed by `TableProvider` |

The integration tests assert sequentially against a single cluster inside one
test function: standing up a Fluss cluster is expensive, while these assertions
are all read-only and mutually independent, so they share one cluster to lower
the cost (`setup.rs` provides shared table creation and seeding). The per-call
cache hit/invalidate is a white-box mechanism covered by `metadata::cache`'s unit
tests, with no need to re-verify it against the real backend.

Verification commands:

```bash
cargo check --workspace
cargo test -p fluss-datafusion                               # unit
cargo test -p fluss-datafusion --features integration_tests  # containers required, real cluster
```

Boundary constraint (aligned with CLAUDE.md): `FlussSource` is an internal seam,
kept `pub(crate)`, not part of the public API, and not shaped into a gateway-style
backend abstraction.

## Interfacing with fluss-gateway

This crate is responsible only for `SQL -> Fluss table access`. On top of it,
`fluss-gateway` should:

1. create one shared `FlussDatafusion` per cluster/proxy connection;
2. call `register_catalog(&ctx, "fluss", ...)` when a SQL session builds a new
   `SessionContext`;
3. layer gateway-specific SQL environment setup on top of the real Fluss catalog;
4. keep `pg_catalog`, session variables, auth, and timeout / cancel semantics in
   the gateway, not here.
