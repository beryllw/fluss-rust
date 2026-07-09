# fluss-datafusion design

> For the Chinese version, see [design.zh.md](./design.zh.md).
>
> This document uses a FIP-style structure to describe the current `fluss-datafusion`
> module design, its public contract, the code paths that already exist today, and
> the convergence work that is still ahead. It is intentionally grounded in the
> current implementation rather than describing an idealized future gateway.

For usage and quickstart, see [`README.md`](./README.md).
For the formal FIP-style proposal of this module, see [`FIP.md`](./FIP.md) and
[`FIP.en.md`](./FIP.en.md).
For the lake-kernel boundary specific to lake-enabled tables, also see
[`fip-lake-integration.md`](./fip-lake-integration.md).

## Motivation

`fluss-datafusion` exists to expose Fluss tables to Apache DataFusion through the
standard DataFusion extension points:

- `CatalogProvider`
- `SchemaProvider`
- `TableProvider`
- custom `ExecutionPlan`s

This solves a concrete problem in the Rust-side Fluss ecosystem: SQL-oriented
consumers want to query Fluss tables with `ctx.sql(...)`, but Fluss itself
natively exposes client-side KV and log read APIs rather than a DataFusion
catalog.

The module has to bridge three different read families:

1. **non-lake KV tables**
   - point lookup by complete primary-key equality;
   - prefix lookup by complete bucket-key prefix equality;
   - bounded scan only when a `LIMIT` is present.
2. **non-lake log tables**
   - finite earliest-to-latest snapshot scan;
   - optional `LIMIT` with head-first first-N semantics.
3. **lake-enabled tables**
   - unified read over lake snapshot plus residual Fluss log tail;
   - lake-only read via `<table>$lake`;
   - direct KV fast paths for eligible primary-key queries.

The key design pressure is to keep this crate **thin and reusable**:

- it should be a library, not a protocol server;
- it should not own session/auth/protocol concerns;
- it should not duplicate lake+log union semantics that belong in `fluss-lake`;
- it should not hide metadata freshness behind an internal cache.

In other words, `fluss-datafusion` is the DataFusion adapter layer for Fluss,
not a second storage kernel and not yet a full query gateway.

## Public Interfaces

The public surface of the crate is intentionally small.

### Rust API

The crate currently exports only:

- `FlussDatafusion`
- `FlussDatafusionOptions`
- `RegisterCatalogOptions`
- `FlussDatafusionError`
- `Result`

The operational API is:

```rust
pub struct FlussDatafusionOptions {
    pub lake_storage_options: std::collections::HashMap<String, String>,
}

pub struct RegisterCatalogOptions {}

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

    pub fn swap_connection(
        &self,
        new: Arc<fluss::client::FlussConnection>,
    ) -> Result<()>;
}
```

Important public behavior:

- `new(...)` builds a shared, stateless installer over a live Fluss connection.
- `register_catalog(...)` installs a fully live Fluss catalog into a
  `SessionContext`.
- `swap_connection(...)` hot-swaps the underlying connection without requiring
  sessions to re-register the catalog.

### SQL-visible contract

The SQL-facing contract exposed through DataFusion includes:

- table naming as `<catalog>.<database>.<table>`;
- `<table>$options` as a key/value metadata table;
- `<table>$lake` as a lake-only read shape under the `lake` feature;
- `EXPLAIN` visibility of custom physical plans.

Today the main user-visible execution-plan names are:

- `FlussKvLookupExec`
- `FlussKvPrefixLookupExec`
- `FlussKvScanExec`
- `FlussLogScanExec`
- `FlussUnionScanExec`

### Feature-gated behavior

The crate's behavior materially changes with features:

- default build: core DataFusion integration without `fluss-lake` dependency;
- `lake`: enables lake-enabled table routing and `<table>$lake`;
- `integration_tests`: enables the real-cluster integration suites and test-only
  lake seam helpers re-exported from `fluss-lake`.

## Proposed Changes

This section describes the intended module contract and the behavior already
implemented today.

### 1. Scope and positioning

`fluss-datafusion` is defined as a **stateless DataFusion adapter over Fluss**.

Its responsibilities are:

- register live Fluss catalogs into DataFusion sessions;
- load table metadata and Arrow schema on demand;
- recognize supported SQL predicate shapes;
- route reads to KV fast paths, bounded scans, log snapshot scans, or the
  `fluss-lake` kernel;
- adapt Fluss and `fluss-lake` output streams into DataFusion
  `ExecutionPlan`s.

It is explicitly **not** responsible for:

- PostgreSQL / Flight SQL / REST / gRPC protocol frontends;
- session variables, user identity, or authentication;
- SQL writes or DML;
- generic backend abstraction for arbitrary sources;
- lake seam interpretation, append stitching, or PK merge logic.

The dependency direction is therefore one-way:

```text
+-----------------------------+
|       SessionContext        |
+--------------+--------------+
               |
               v
+-----------------------------+
|       FlussDatafusion       |
|   live catalog installer    |
+--------------+--------------+
               |
               v
+-----------------------------+
|        MetadataLoader       |
|         FlussSource         |
+--------------+--------------+
               |
               v
+-----------------------------+
|       RealFlussSource       |
|   Fluss client / fluss-lake |
+-----------------------------+
```

### 2. Architecture overview

The crate is organized around four layers.

#### 2.1 Installer and lifecycle layer

- `src/install.rs`
- `src/config.rs`
- `src/runtime.rs`

This layer owns:

- the shared `FlussDatafusion` installer;
- session-level `register_catalog(...)`;
- connection hot-swap;
- sync/async bridging required by DataFusion's synchronous catalog callbacks.

A single `FlussDatafusion` instance is intended to be reused across many
`SessionContext`s.

#### 2.2 Metadata and catalog layer

- `src/metadata/loader.rs`
- `src/catalog/provider.rs`
- `src/catalog/schema.rs`
- `src/catalog/register.rs`

This layer maps Fluss metadata into DataFusion's catalog model:

- one DataFusion catalog contains Fluss databases as schemas;
- one Fluss database maps to one `SchemaProvider`;
- one Fluss table dynamically maps to one `TableProvider`.

A deliberate design choice is that metadata is **live and uncached**:

- `schema_names()` lists databases live;
- `table_names()` lists tables live;
- `table()` reloads per-table metadata and Arrow schema every time;
- DDL after `register_catalog(...)` is visible in the same session.

#### 2.3 Table-provider routing layer

- `src/table/kv.rs`
- `src/table/log.rs`
- `src/table/union.rs`
- `src/table/predicate.rs`

This layer contains the read-shape recognition logic.

It is where the crate decides whether a query should use:

- point lookup;
- prefix lookup;
- bounded KV scan;
- finite log snapshot scan;
- lake+log unified read;
- lake-only read.

#### 2.4 Physical execution layer

- `src/execution/lookup.rs`
- `src/execution/prefix_lookup.rs`
- `src/execution/log_scan.rs`
- `src/execution/union_scan.rs`
- `src/execution/stream.rs`

This layer exposes DataFusion `ExecutionPlan`s that wrap Fluss or
`fluss-lake` reads and adapt them into `SendableRecordBatchStream`s.

### 3. Public API and lifecycle model

The intended lifecycle is:

1. caller constructs `FlussConnection`;
2. caller constructs one shared `FlussDatafusion` installer;
3. each SQL session creates its own `SessionContext`;
4. caller invokes `register_catalog(&ctx, "fluss", ...)`;
5. queries execute through the live catalog.

Important operational properties already implemented today:

- the installer is session-agnostic and reusable;
- the catalog provider does not snapshot metadata at registration time;
- `swap_connection(...)` allows already-registered sessions to transparently use
  a new connection on subsequent accesses.

### 4. Catalog, schema, and table mapping

The current table-resolution logic is implemented in
`src/catalog/schema.rs`.

Routing precedence is:

1. `<table>$options`
2. `<table>$lake` under `lake`
3. `<table>$lake$<system>` -> explicit unsupported error
4. normal table name -> provider selected by metadata

For normal tables, the provider routing is:

| Table shape | Provider |
| --- | --- |
| lake-enabled Paimon table, `lake` feature on | `FlussUnionTableProvider` |
| non-lake primary-key table | `FlussKvTableProvider` |
| non-lake append/log table | `FlussLogTableProvider` |

This precedence matters: a lake-enabled PK table must not go directly to the
non-lake KV provider, because a Fluss-only KV read would miss rows already
tiered into lake storage.

#### `<table>$options`

`<table>$options` is a formal user-visible introspection surface.
It exposes key/value metadata such as:

- table id;
- schema id;
- primary keys;
- bucket keys;
- partition keys;
- datalake enablement and format;
- lake catalog properties.

Secret-like values are redacted before exposure.

#### `<table>$lake`

`<table>$lake` is a formal lake-only read shape.
It means:

- read only the Paimon lake snapshot;
- skip the residual Fluss log tail;
- skip KV fast-path routing even for primary-key tables.

### 5. Query routing and planner behavior

This is the most important part of the module contract.

#### 5.1 Non-lake KV tables

`FlussKvTableProvider` supports the following precedence order.

| Query shape | Route | Notes |
| --- | --- | --- |
| complete primary-key equality | `FlussKvLookupExec` | exact point lookup |
| complete bucket-key prefix equality | `FlussKvPrefixLookupExec` | exact prefix lookup |
| any other shape with `LIMIT` | `FlussKvScanExec` | bounded scan over computed scan targets |
| any other shape without `LIMIT` | unsupported | non-lake KV full current-state scan is not exposed |

Important current semantics:

- complete primary-key equality means every PK column is bound by equality;
- complete bucket-key prefix equality means every required prefix column is
  bound, including partition keys when the table is partitioned;
- unsupported shapes fail conservatively rather than silently degrading to a
  full scan.

#### 5.2 Non-lake log tables

`FlussLogTableProvider` surfaces a finite snapshot scan.

| Query shape | Route | Notes |
| --- | --- | --- |
| `LIMIT n` present | `FlussLogScanExec` | head-first first-N from earliest offset |
| no `LIMIT` | `FlussLogScanExec` | full earliest-to-latest snapshot captured at query start |

Important current semantics:

- `LIMIT` is optional in the current implementation;
- projection is pushed down to the source scan;
- partition equality can prune partitions, but pushdown is `Inexact` and a
  residual `FilterExec` remains above the scan;
- no cross-bucket global ordering guarantee is provided.

#### 5.3 Lake-enabled tables

Lake-enabled tables route through `FlussUnionTableProvider`, which is a thin
adapter over `fluss-lake`.

Current precedence is:

| Query shape | Route | Notes |
| --- | --- | --- |
| PK table + full primary-key equality | `FlussKvLookupExec` | served directly from Fluss current state |
| PK table + complete bucket-key prefix equality | `FlussKvPrefixLookupExec` | served directly from Fluss current state |
| `<table>$lake` | lake-only `fluss-lake` path | no residual log tail |
| any other lake-enabled read | `FlussUnionScanExec` | unified lake snapshot + residual Fluss tail |

Important current behavior:

- KV fast paths are enabled only when `!lake_only` and the table has primary
  keys;
- the union path does not currently push value filters down into the kernel;
- residual filters remain above the scan;
- `num_buckets == 0` returns `EmptyExec`.

### 6. Filter pushdown contract

The filter-pushdown contract is intentionally conservative.

#### KV provider

`FlussKvTableProvider::supports_filters_pushdown(...)` is precedence-aware and
sound:

- `Exact` only when the query truly becomes a point lookup or prefix lookup;
- partition equality may be `Inexact` on scan paths;
- everything else is `Unsupported`.

This prevents unsound cases where DataFusion would drop a `FilterExec` for a
predicate that the underlying source is not actually applying.

#### Log provider

`FlussLogTableProvider` reports:

- `Unsupported` for all filters on non-partitioned tables;
- `Inexact` for partition equality on partitioned tables;
- `Unsupported` otherwise.

#### Lake provider

`FlussUnionTableProvider` reports:

- `Exact` only for KV fast-path predicates;
- `Unsupported` for all filters on the normal union path.

This means unified lake reads currently rely on DataFusion residual filtering
for correctness.

### 7. Execution behavior

The physical execution layer consists of four main plan families.

#### `FlussKvLookupExec`

- one logical lookup;
- returns 0 or 1 row;
- trims projection after fetch.

#### `FlussKvPrefixLookupExec`

- prefix lookup over Fluss KV state;
- returns all matching rows.

#### `FlussLogScanExec`

- one DataFusion partition per `(partition_id, bucket)` target;
- reused for log snapshot scans and bounded KV scans;
- in log mode, reads earliest-to-latest or first-N;
- in KV bounded-scan mode, reads the per-bucket bounded scan path.

#### `FlussUnionScanExec`

- one DataFusion partition per `FlussLakeReadSplit`;
- wraps opaque `FlussLakeReadPlan` + `FlussLakeRead`;
- does not know about seams, physical lake splits, append stitching, or PK
  merge internals.

### 8. Lake integration boundary

The intended responsibility split is:

| Layer | Responsibility |
| --- | --- |
| `fluss-rust` | metadata, KV reads, log reads, Arrow helpers |
| `fluss-lake` | seam resolution, snapshot planning, append stitch, PK merge, logical splits |
| `fluss-datafusion` | predicate recognition, route selection, DataFusion wrapping, config injection |

The DataFusion module should therefore remain a **thin adapter** above
`fluss-lake`.

This is already largely true in code today:

- `src/table/union.rs` builds `FlussLakeTable` and `FlussLakeScan`;
- it calls `plan()` and `new_read()` on the kernel;
- `src/execution/union_scan.rs` executes public `FlussLakeReadSplit`s;
- the connector itself never interprets seam DTOs or performs PK merge.

### 9. Configuration contract

The current meaningful installer-level configuration is:

- `FlussDatafusionOptions.lake_storage_options`

This exists because server metadata may not retain complete lake storage
credentials. The adapter therefore supports caller-side reinjection of storage
properties just before opening the lake catalog.

Merge rule:

- server-derived `table.datalake.*` properties provide the base configuration;
- caller-supplied `lake_storage_options` are merged over them;
- **caller wins** on key conflicts.

This is intentionally an adapter concern rather than a lake semantic concern.

`RegisterCatalogOptions` is currently reserved and carries no behavior.

### 10. Error model

The crate-local error type is `FlussDatafusionError` with the main variants:

- `DatabaseNotFound`
- `TableNotFound`
- `UnsupportedQueryPattern`
- `LimitRequired`
- `SchemaMismatch`
- `TypeConversion`
- `FlussClient`
- `Internal`

Design principles:

- fail conservatively on unsupported SQL shapes;
- keep storage/client failures explicit;
- bridge cleanly into `DataFusionError::External`.

Note that `LimitRequired` remains part of the error taxonomy even though log
scans no longer require `LIMIT` in the current implementation.

### 11. Current constraints and convergence items

The implementation is already useful, but several parts still reflect a
transitional or convergence state.

#### 11.1 Transitional naming

Current names still use `Union` terminology:

- `FlussUnionTableProvider`
- `FlussUnionScanExec`

Semantically these are already the lake adapter path. A future rename to
`FlussLake*` would improve clarity without changing behavior.

#### 11.2 Lake path limit pushdown is not wired

`FlussUnionTableProvider::scan(...)` currently ignores SQL `LIMIT` for the
kernel path, so DataFusion applies the limit above the lake scan.
This preserves correctness but may over-read.

#### 11.3 Lake path filter pushdown is intentionally narrow

The normal union path does not yet provide value-filter pushdown.
Residual filtering remains a DataFusion responsibility.

#### 11.4 Real-source dependency on the lake path

The lake path currently downcasts to `RealFlussSource` in order to obtain the
production Fluss connection and caller-supplied lake storage options. This is a
pragmatic production boundary, but it also shows that the lake path is not yet
fully abstracted behind a broader source trait.

#### 11.5 Feature-gated packaging

Lake support is behind the `lake` feature. That is the right current packaging
boundary, but it means the module contract must explicitly document the feature
flag rather than imply that unified lake reads are always available.

#### 11.6 Upstream `fluss-lake` capability gaps remain visible here

The DataFusion adapter inherits current `fluss-lake` limits, including:

- partitioned lake reads still being a convergence area;
- historical snapshot selection not yet being the primary supported path;
- thin plan statistics and incomplete limit/predicate pushdown in the kernel.

## Compatibility, Deprecation, and Migration Plan

This design does not require Fluss server-side wire or storage changes.

Compatibility notes:

- existing Fluss clusters do not need a server change to use this crate;
- the module remains additive for callers that choose to adopt it;
- lake behavior is additive and feature-gated;
- `<table>$lake` documents an already-existing read shape rather than adding a
  new incompatible mode.

Migration expectations:

- no data migration is required;
- no SQL migration tooling is required;
- future internal renames from `FlussUnion*` to `FlussLake*` should be done in
  lockstep with `EXPLAIN` expectations and docs.

## Test Plan

The test strategy is intentionally grounded in real behavior rather than only
mocked unit tests.

### Unit tests

Current unit coverage includes:

- predicate recognition and pushdown classification;
- projection normalization;
- scalar-to-key conversion;
- metadata-property merging and redaction behavior.

### Integration tests against a real Fluss cluster

The main integration suites verify:

- catalog listing and `information_schema` visibility;
- live post-DDL metadata visibility without re-registration;
- KV point lookup and prefix lookup behavior;
- unsupported-query failures for non-lake KV paths;
- bounded and full snapshot log reads;
- partition pruning for partitioned log and KV scans;
- hot connection swap behavior;
- lake-enabled unified reads through both injected-seam and real-tiering paths;
- `$options` and `$lake` behavior;
- `EXPLAIN` showing the expected custom plans.

### Verification commands

```bash
cargo check --workspace
cargo test -p fluss-datafusion
cargo test -p fluss-datafusion --features integration_tests
```

## Rejected Alternatives

### 1. Make `fluss-datafusion` a full gateway backend abstraction

Rejected because protocol/auth/session concerns would pollute the engine adapter
boundary and reduce standalone reuse.

### 2. Reimplement lake+log union semantics in the DataFusion layer

Rejected because those semantics belong in `fluss-lake` and should be reusable
across Arrow engines.

### 3. Hide metadata behind a catalog cache

Rejected for the current design because same-session DDL visibility is a more
important contract than reducing admin RPCs.

### 4. Route all lake-enabled PK reads through `fluss-lake`

Rejected because exact PK and eligible prefix queries can be served directly from
Fluss current state more efficiently.

### 5. Silently degrade unsupported predicates into full scans

Rejected because correctness and predictable behavior are more important than
pretending every SQL shape is supported.

## Future Work and Open Questions

The main next-step topics are:

1. rename `FlussUnion*` internals to `FlussLake*`;
2. add limit propagation to the lake scan path;
3. broaden pushdown only when it can be done soundly;
4. reduce the `RealFlussSource`-specific downcast boundary on lake reads;
5. converge with `fluss-lake` as partitioned lake reads and richer statistics
   mature;
6. keep this crate library-scoped even if a higher-level Fluss gateway later
   consumes it.

The key constraint for all future work is unchanged: `fluss-datafusion` should
continue to be the DataFusion adapter for Fluss, not the semantic owner of the
lake storage model.