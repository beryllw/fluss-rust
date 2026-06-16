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
2. KV (primary-keyed) tables support four read shapes in precedence order: a
   full-primary-key equality predicate pushed down as a point lookup, a bucket-key
   prefix equality (plus all partition keys when partitioned) pushed down as a
   prefix lookup, a bounded `LIMIT` scan when neither equality shape is present,
   and a full-table scan (no filter / no `LIMIT`, e.g. `SELECT *`) via a
   changelog merge to current state. The full-table scan's completeness is bounded
   by changelog retention.
3. Log tables support two scan shapes: a head-first first-N scan when a `LIMIT`
   is present, or a finite earliest->latest snapshot scan when no `LIMIT` is
   present, both with projection pushdown.
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
- batch-lookup optimizations.
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
      log_scan.rs      # FlussLogScanExec (bounded scan; label per log/KV)
      kv_full_scan.rs  # FlussKvFullScanExec (KV full-table scan; changelog merge)
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
  `lookup(key) -> RecordBatch`, partition listing (`list_partitions`), KV bounded
  `bounded_scan(partition_id, bucket, projection, limit) -> Vec<RecordBatch>`,
  log snapshot `log_scan(partition_id, bucket, projection, row_limit) -> Vec<RecordBatch>`,
  and KV full scan `kv_full_scan(partition_id, bucket, projection) -> Vec<RecordBatch>`.
  `bounded_scan` remains the tail-oriented `LimitBatchScanner` path for KV only.
  `log_scan` is log-specific: it reads earliest->latest, with `Some(n)` meaning
  first-N and `None` meaning a full finite snapshot to the latest offsets captured
  at query start. `kv_full_scan` is KV-only: it merges one bucket's CDC changelog
  (earliest->latest at call time) into its current state via the client's
  `collect_kv_current_state_batch`, returning the merged rows (no kv-snapshot RPC,
  no RocksDB). All take an `Option<i64>` partition id (`None` for a
  non-partitioned table) so the scanner can target a partition-qualified bucket.
- `real.rs`: the sole implementation, wrapping `FlussConnection` / `FlussAdmin` /
  lookuper / scanner, reusing Fluss's existing Arrow helpers (such as
  `LookupResult::to_record_batch`) rather than reimplementing Arrow assembly.
  `list_partitions` maps `admin.list_partition_infos` to the crate's
  `FlussPartition` (partition id + partition-key/value string pairs); both scan
  methods build the bucket via `TableBucket::new_with_partition`. `log_scan`
  captures `Earliest` and `Latest` offsets once per target at query start,
  subscribes a `RecordBatchLogScanner` at the earliest offset, and polls until it
  either reaches the captured latest offset or satisfies the requested first-N cap.
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
- Correctness note: this private runtime is **not** a connection-ownership
  runtime. The upstream `fluss-rs` fix made `FlussConnection` runtime-agnostic by
  turning the connection into a full actor: socket creation/registration, reads,
  and writes are all pinned to one dedicated I/O runtime inside `fluss-rs`, and
  callers interact with it only through runtime-agnostic channels. Therefore the
  catalog bridge's private runtime is no longer a deadlock risk; it only affects
  the cost model (one thread hop / `block_on`), not connection correctness.
- Operational implication: if a host wants Fluss socket I/O to share its own
  long-lived runtime (for thread budgeting or observability), it should inject
  that runtime when constructing the underlying `FlussConnection` via
  `FlussConnection::new_with_io_handle(...)`; `fluss-datafusion` itself does not
  own or configure the Fluss I/O runtime.
- Known upstream limitation (recorded, not fixed here): `fluss-rs` currently uses
  an unbounded outbound mpsc for the connection actor. There is no evidence of
  problematic accumulation today, but under a stalled socket plus many concurrent
  callers that queue could grow without bound; treat this as an upstream
  observability / backpressure follow-up rather than a `fluss-datafusion`
  correctness issue.

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
- `kv.rs`: `FlussKvTableProvider` serves four read shapes in precedence order.
  `scan()` first tries `analyze_kv_filters`: a COMPLETE PK equality builds the
  point-lookup plan (passing the projection); else a complete bucket-key prefix
  equality builds the prefix-lookup plan; else it computes the `(partition,
  bucket)` targets (partition pruning exactly like the log provider) and, if a
  `LIMIT` is present, runs a bounded KV scan reusing `FlussLogScanExec` (display
  name `FlussKvScanExec`); with NO `LIMIT` it runs a full-table scan via
  `FlussKvFullScanExec`, which merges each target bucket's CDC changelog into its
  current state (changelog-only; no kv-snapshot RPC, no RocksDB). The full-table
  scan's completeness is bounded by changelog retention. An empty target set
  yields an `EmptyExec`. `supports_filters_pushdown` is precedence-aware so it
  stays sound: a filter is `Exact` (consumed, no `FilterExec`) ONLY when the full
  set forms a complete PK equality (point lookup) or a complete bucket-key prefix
  equality (prefix lookup) that the matching plan will apply; on the bounded-scan
  / full-scan path nothing is `Exact` — partition equality is `Inexact` (drives
  pruning, residual `FilterExec` re-applies) for a partitioned table and
  everything else is `Unsupported`. This prevents the unsound case where a partial
  PK equality would be marked `Exact` and silently dropped.
- `log.rs`: `FlussLogTableProvider` no longer requires `LIMIT`. The projection is
  pushed down to the log source scan, and a full identity projection from
  `SELECT *` is normalized to `None`. For a non-partitioned table
  `supports_filters_pushdown` is always `Unsupported`. For a partitioned table it
  reports partition-column equality as `Inexact` (so a residual `FilterExec`
  still re-applies the predicate — pruning is best-effort) and everything else as
  `Unsupported`. `scan()` computes the scan targets (`Vec<(Option<i64>, i32)>` of
  partition id + bucket): non-partitioned tables emit one `(None, bucket)` per
  bucket; partitioned tables list partitions, keep those matching every equality
  binding (no bindings => keep all), and emit the cross product of kept partitions
  and buckets. An empty target set (no partition matched, or a partitioned table
  with zero partitions) yields an `EmptyExec` with the projected schema rather
  than a 0-partition scan. `LIMIT` now means head-first first-N; no `LIMIT` means
  a finite earliest->latest snapshot.

### `execution/`

- `lookup.rs`: `FlussKvLookupExec`, a single-partition leaf `ExecutionPlan` whose
  `execute()` calls `source.lookup` asynchronously through `single_batch_stream`
  to produce 0/1 rows; it implements `DisplayAs`, so `EXPLAIN` shows
  `FlussKvLookupExec`.
- `log_scan.rs`: `FlussLogScanExec`, a leaf that reports one DataFusion partition
  per scan target (a `(partition_id, bucket)` pair). In log mode,
  `execute(partition)` calls `source.log_scan(partition_id, bucket, projection, row_limit)`
  asynchronously through `bounded_batches_stream`; in KV mode it still calls
  `source.bounded_scan(...)`. The same plan shape backs both log and KV scans; its
  `EXPLAIN` label is a `plan_name` field — the log provider passes
  `FlussLogScanExec`, the KV provider passes `FlussKvScanExec`. Debug/display text
  now shows `limit=<n>` or `limit=None/full`.
- `kv_full_scan.rs`: `FlussKvFullScanExec`, a leaf that reports one DataFusion
  partition per `(partition_id, bucket)` target. `execute(partition)` calls
  `source.kv_full_scan(partition_id, bucket, projection)` asynchronously through
  `bounded_batches_stream`, merging that bucket's CDC changelog into its current
  state. It is distinct from `FlussLogScanExec` (which backs log snapshot scans
  and KV bounded `LIMIT` scans); `EXPLAIN` shows `FlussKvFullScanExec`.
- `stream.rs`: adapts the lookup / scan output into a DataFusion stream
  (`futures::stream::once` / Vec versions); dropping it is cooperative
  cancellation.
- An asymmetry: source-side scans are already projected on the `FlussSource`
  side, so the scan path does **not** project a second time; the KV point lookup
  returns all columns and is then trimmed by `project_batch` in
  `types/record_batch.rs`.

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

A KV (primary-keyed) table supports four read shapes, in precedence order:

1. **Point lookup** — a **full-primary-key equality** predicate
   (`pk1 = v1 AND pk2 = v2 ...`, covering every PK column). A hit returns 1 row, a
   miss returns 0 rows (no error).
2. **Prefix lookup** — a **complete bucket-key prefix equality**. For a
   non-partitioned table this is equality on every bucket key; for a partitioned
   table it is equality on every partition column plus every bucket key. The
   bucket key must be a STRICT prefix of the physical primary key (the PK with the
   partition columns removed), matching the Fluss client's `lookup_by(...)`
   validation. This shape returns all rows whose primary key starts with that
   prefix and is shown in `EXPLAIN` as `FlussKvPrefixLookupExec`.
3. **Bounded scan** — any other predicate shape combined with a `LIMIT`. This runs
   the same bounded-scan machinery as the log table (per-bucket last-`limit` rows,
   with DataFusion applying the final cross-target `LIMIT`), shown in `EXPLAIN` as
   `FlussKvScanExec`. For a partitioned table, partition-column equality prunes the
   scanned partitions (`Inexact`, with a residual `FilterExec`).
4. **Full scan** — any other predicate shape with NO `LIMIT` (e.g. a bare
   `SELECT *`, a partial key, or a non-key filter). This merges each target
   bucket's CDC changelog into its current state (changelog-only: no kv-snapshot
   RPC, no RocksDB), shown in `EXPLAIN` as `FlussKvFullScanExec`. It uses the same
   `(partition, bucket)` target computation as the bounded scan, so partition-column
   equality prunes the scanned partitions; an empty target set yields an
   `EmptyExec`. The merge applies `+I`/`+U`/`+A` as upserts and `-D` as deletes
   (ignoring `-U`) per primary key, since a KV table's rows for a given PK live in
   exactly one bucket.

   The full scan's completeness is bounded by changelog retention: rows whose
   changelog has aged out of the retained window (compacted into kv snapshots and
   dropped from the log) are not reflected. This is acceptable for the current
   feature; full historical completeness would require a snapshot-backed reader.

Partitioned KV tables need no extra handling for the point lookup: for a
partitioned PK table the partition columns are part of the primary key, so the
full-PK equality already binds them. The Fluss `Lookuper` resolves the partition
id from the lookup key row, so the point-lookup path targets the single owning
partition automatically. For a prefix lookup, the partition columns must be part
of the `lookup_by(...)` column list and the lookup key row so the client can
resolve the owning partition before routing the bucket-key prefix request.

### Log tables

Locked-in decisions:

1. `LIMIT` is optional. With `LIMIT n`, log tables return the first `n` rows from
   the earliest offset forward (head-first first-N). Without `LIMIT`, they return
   a finite earliest->latest snapshot.
2. Projection pushdown is supported.
3. No offset pseudo-column is exposed and offset predicates are not supported.
4. The source captures the latest offset ONCE at query start for each scan
   target, subscribes at the earliest offset, and polls until it reaches that
   finite snapshot boundary.
5. Multi-bucket snapshot scan is supported: one bucket maps to one DataFusion
   partition (`FlussLogScanExec` reports `num_buckets` partitions, read in
   parallel). With `LIMIT`, each target independently applies its own first-N cap,
   and DataFusion applies a final cross-bucket `LIMIT` above the scan, so the
   merged result is capped at exactly `limit` rows. There is no global cross-bucket
   first-N coordination.
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
2. construct the underlying `FlussConnection` with a **single long-lived I/O
   runtime** (prefer `FlussConnection::new_with_io_handle(...)` when the gateway
   wants to supply its own runtime; otherwise `FlussConnection::new(...)` falls
   back to `fluss-rs`'s process-global default);
3. call `register_catalog(&ctx, "fluss", ...)` when a SQL session builds a new
   `SessionContext`;
4. layer gateway-specific SQL environment setup on top of the real Fluss catalog;
5. keep `pg_catalog`, session variables, auth, and timeout / cancel semantics in
   the gateway, not here.
