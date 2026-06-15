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
    runtime.rs         # sync<->async bridge for synchronous catalog callbacks
    backend/
      mod.rs           # FlussSource trait (pub(crate)) + shared aliases
      real.rs          # production implementation, wraps FlussConnection
    metadata/
      mod.rs
      loader.rs        # live pass-through over FlussSource; no cache
    catalog/
      mod.rs
      provider.rs      # FlussCatalogProvider (live sync schema_names / schema)
      schema.rs        # FlussSchemaProvider (live table_names; async table())
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
      live_metadata.rs # integration_tests: live post-DDL visibility (no cache)
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
pub struct FlussDatafusionOptions {}               // placeholder; the catalog is live, nothing to tune

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

- The `FlussSource` trait (`pub(crate)`) covers only the atomic operations the
  crate actually uses: metadata listing/fetching, KV full-primary-key
  `lookup(key) -> RecordBatch`, partition listing (`list_partitions`), and log
  bounded `log_scan(partition_id, bucket, projection, limit) -> Vec<RecordBatch>`.
  `log_scan` takes an `Option<i64>` partition id (`None` for a non-partitioned
  table) so the scanner can target a partition-qualified bucket.
- `real.rs`: the sole implementation, wrapping `FlussConnection` / `FlussAdmin` /
  lookuper / scanner, reusing Fluss's existing Arrow helpers (such as
  `LookupResult::to_record_batch`) rather than reimplementing Arrow assembly.
  `list_partitions` maps `admin.list_partition_infos` to the crate's
  `FlussPartition` (partition id + partition-key/value string pairs); `log_scan`
  builds the bucket via `TableBucket::new_with_partition`.
- The seam returns the crate's own slim `FlussTableMeta` (schema + primary_keys +
  num_buckets + partition_keys, etc.), decoupling upper layers from
  `fluss::metadata::TableInfo` and carrying only the fields catalog wiring,
  predicate analysis, and execution truly need.

### `metadata/`

- `loader.rs`: `MetadataLoader` depends only on `SharedFlussSource` and holds no
  cache. `databases()` reads the whole-cluster db/table listing live every call;
  `table_entry()` re-reads per-table meta + Arrow schema live every call. The
  Arrow schema is converted via
  `fluss::record::to_arrow_schema(meta.schema.row_type())`. The loader stays the
  home for that derivation so callers do not re-implement the mapping.

### `runtime.rs`

- The sync<->async bridge for DataFusion's synchronous catalog callbacks.
  `block_on_with_runtime` drives an async source call to completion from a
  synchronous callback; when already inside a tokio runtime it spawns a
  short-lived thread that `block_on`s a global runtime handle, avoiding the
  "cannot block within a runtime" panic. A single lazily-built global runtime
  backs the fallback so no runtime is created per call.

### `catalog/`

- `provider.rs`: `FlussCatalogProvider` holds only the shared loader. The
  synchronous `schema_names()` lists databases live (via `block_on_with_runtime`);
  `schema()` checks the database exists live and returns `None` for an unknown
  name (mirroring paimon's `DatabaseNotExist -> None`), otherwise hands back a
  `FlussSchemaProvider`.
- `schema.rs`: `FlussSchemaProvider` holds the database name + loader. Its
  synchronous `table_names()` / `table_exist()` list tables live; its async
  `table()` loads the entry live, maps `TableNotFound -> Ok(None)`, and branches on
  `meta.has_primary_key()` to return either `FlussKvTableProvider` (KV) or
  `FlussLogTableProvider` (log).
- `register.rs`: `build_catalog_provider` simply wraps the loader in a
  `FlussCatalogProvider`; it does no pre-listing, so registration is cheap and
  listings never go stale.

> The sync/async bridge uses a live `block_on` inside the synchronous callbacks
> (via `runtime.rs`) instead of a registration-time snapshot. This makes the
> catalog fully live — DDL is visible in the same session immediately — at the
> known, accepted cost of one admin RPC (plus a thread hop) per catalog call. See
> the README's "Metadata visibility".

### `table/`

- `predicate.rs`: `analyze_kv_filters` recognizes the top-level AND conjuncts that
  DataFusion has split apart as a full-primary-key equality, producing
  `LookupKey`s in primary-key order; it explicitly rejects partial PKs, non-PK
  columns, `IN`, ranges, column-to-column comparisons, duplicate/missing PKs, and
  empty filters, all as `UnsupportedQueryPattern`. `analyze_partition_filters`
  extracts equality bindings on partition columns only (best-effort: a literal
  that cannot be rendered to the partition-value string is skipped, never an
  error), and `is_partition_equality` classifies a single partition-column
  equality for the log table's pushdown decision.
- `kv.rs`: `FlussKvTableProvider`'s `supports_filters_pushdown` marks only PK
  equality filters as `Exact` (consumed, so no `FilterExec` is generated) and
  everything else as `Unsupported`; `scan()` builds the lookup plan on a match
  (correctly passing the projection), otherwise raises `UnsupportedQueryPattern`
  directly and never silently degrades to a full scan.
- `log.rs`: `FlussLogTableProvider` cleanly returns `LimitRequired` when `limit`
  is `None`; the projection is pushed down to `FlussSource::log_scan`, and a full
  identity projection from `SELECT *` is normalized to `None`. For a
  non-partitioned table `supports_filters_pushdown` is always `Unsupported`. For a
  partitioned table it reports partition-column equality as `Inexact` (so a
  residual `FilterExec` still re-applies the predicate — pruning is best-effort)
  and everything else as `Unsupported`. `scan()` computes the scan targets
  (`Vec<(Option<i64>, i32)>` of partition id + bucket): non-partitioned tables
  emit one `(None, bucket)` per bucket; partitioned tables list partitions, keep
  those matching every equality binding (no bindings => keep all), and emit the
  cross product of kept partitions and buckets. An empty target set (no partition
  matched, or a partitioned table with zero partitions) yields an `EmptyExec` with
  the projected schema rather than a 0-partition scan.

### `execution/`

- `lookup.rs`: `FlussKvLookupExec`, a single-partition leaf `ExecutionPlan` whose
  `execute()` calls `source.lookup` asynchronously through `single_batch_stream`
  to produce 0/1 rows; it implements `DisplayAs`, so `EXPLAIN` shows
  `FlussKvLookupExec`.
- `log_scan.rs`: `FlussLogScanExec`, a leaf that reports one DataFusion partition
  per scan target (a `(partition_id, bucket)` pair); `execute(partition)` scans
  `targets[partition]` via `source.log_scan(partition_id, bucket, projection, limit)`
  asynchronously through `bounded_batches_stream`; `EXPLAIN` shows
  `FlussLogScanExec`.
- `stream.rs`: adapts the lookup / scan output into a DataFusion stream
  (`futures::stream::once` / Vec versions); dropping it is cooperative
  cancellation.
- An asymmetry: `log_scan` is already projected on the `FlussSource` side, so the
  log path does **not** project a second time; the KV lookup returns all columns
  and is then trimmed by `project_batch` in `types/record_batch.rs`.

### `types/`

- `scalar.rs`: `scalar_to_key_value` strictly converts a `ScalarValue` (`Boolean`
  / `Int8/16/32/64` / `Utf8`) into a `KeyValue`; NULL and other types explicitly
  return `TypeConversion`. `scalar_to_partition_string` renders the same supported
  literal types to the Fluss partition-value string form for equality pruning,
  with the same NULL/unsupported-type rejection.
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

Partitioned KV tables need no extra handling: for a partitioned PK table the
partition columns are part of the primary key, so the full-PK equality already
binds them. The Fluss `Lookuper` resolves the partition id from the lookup key
row, so the existing point-lookup path targets the single owning partition
automatically.

### Log tables

Locked-in decisions:

1. `LIMIT` is mandatory; its absence is `LimitRequired`.
2. Projection pushdown is supported.
3. No offset pseudo-column is exposed and offset predicates are not supported.
4. The underlying Fluss `LimitBatchScanner` keeps the **last** `limit` rows
   (last-N), not the first-N starting from the earliest offset.
5. Multi-bucket bounded scan is supported: one bucket maps to one DataFusion
   partition (`FlussLogScanExec` reports `num_buckets` partitions, read in
   parallel). Each bucket independently returns its own last-`limit` rows
   (per-bucket last-N), and DataFusion applies a final cross-bucket `LIMIT` above
   the scan, so the merged result is capped at exactly `limit` rows. There is no
   global cross-bucket last-N coordination.
6. `ORDER BY` pushdown is out of scope; cross-bucket global row order is not
   guaranteed.
7. Partition pruning is supported for partitioned log tables, equality-only.
   Equality predicates on partition columns prune the scan to partitions matching
   every bound partition-key value that was provided; full equality on every
   partition key is NOT required. A partition predicate is never required, so
   with no partition predicate all partitions are scanned. The scan targets are
   the cross product of the kept partitions and the buckets, and each target is
   one DataFusion partition (partition x bucket => DataFusion partitions). Pushdown
   is `Inexact`, so DataFusion still layers a `FilterExec` that re-applies the
   predicate: pruning is a best-effort optimization and correctness never depends
   on it. A predicate that matches no partition yields zero rows via `EmptyExec`.

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
| Unit tests | default | No | schema mapping, `ScalarValue`-to-key conversion, predicate recognition, pushdown decisions, and error mapping |
| Integration tests | `integration_tests` | Yes | real SQL against the real backend (`FlussDatafusion::new` -> `RealFlussSource`), covering catalog listing, KV point lookup (single/composite PK, missing key, and unsupported forms such as non-PK / partial PK / `IN` / no filter), log bounded scan and projection, partition pruning, `EXPLAIN` custom plans, the Arrow schema exposed by `TableProvider`, and the live no-cache contract (`register_catalog`-after DDL is visible in the same session, and drops disappear from listing / `table()` resolution) |

The integration tests assert sequentially against a single cluster inside one
test function: standing up a Fluss cluster is expensive, while these assertions
are all read-only and mutually independent, so they share one cluster to lower
the cost (`setup.rs` provides shared table creation and seeding). Because the
catalog holds no cache, there is no white-box cache mechanism to unit-test;
live-visibility behaviour is verified end to end against the real backend.

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
