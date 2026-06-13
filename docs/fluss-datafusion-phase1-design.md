# fluss-datafusion Phase 1 Design and Task Breakdown

## Audience

- Maintainers implementing `fluss-datafusion` inside `fluss-rust`
- Future `fluss-gateway` integration work that will consume this crate

## Goal

Add a new stateless Rust library crate that exposes Fluss data to DataFusion through catalog, schema, table, and execution-plan integrations.

Phase 1 is intentionally narrow:

- make `fluss-gateway` SQL read path possible
- keep the crate reusable outside gateway
- reuse the existing Fluss Rust client instead of inventing a gateway-specific backend abstraction

## Scope

Phase 1 must support:

1. database and table discovery
2. KV table SQL pushdown for complete primary-key equality predicates
3. log table bounded scan with `LIMIT` required
4. Fluss-to-Arrow schema and row conversion reuse
5. DataFusion integration through:
   - `CatalogProvider`
   - `SchemaProvider`
   - `TableProvider`
   - custom `ExecutionPlan`
6. integration tests against a real Fluss test cluster

## Non-goals

Phase 1 does not include:

- PostgreSQL compatibility objects such as `pg_catalog`
- session, user, auth, or multi-cluster awareness
- SQL DML writes
- direct read/write REST APIs
- MySQL or PostgreSQL protocol compatibility
- prefix scan pushdown
- batch lookup optimization
- offset pseudo columns
- offset predicate pushdown
- complex filter, join, aggregate, or sort pushdown

## Placement Decision

`fluss-datafusion` should live at:

```text
crates/fluss-datafusion/
```

It should not live at:

```text
integration/fluss-datafusion/
```

Reasoning:

1. Current workspace convention is that reusable Rust crates live under `crates/`.
2. Current `integration` usage in this repository is test-only, for example `crates/fluss/tests/integration/*`.
3. `fluss-datafusion` is a first-class reusable library crate, not a test harness.
4. Keeping it separate from `crates/fluss` prevents DataFusion dependencies and planning logic from leaking into the core client crate.

Relevant repository references:

- `Cargo.toml:29-31`
- `DEVELOPMENT.md:75-95`
- `README.md:63-69`

## Existing Building Blocks to Reuse

The repository already has most of the lower-level pieces needed by Phase 1.

### Metadata APIs

Use the existing admin APIs in `crates/fluss/src/client/admin.rs`:

- `FlussAdmin::list_databases`
- `FlussAdmin::list_tables`
- `FlussAdmin::get_table_info`
- `FlussAdmin::get_table_schema`

These cover Phase 1 metadata discovery without adding new Fluss RPC abstractions.

### KV point lookup APIs

Use the existing table lookup path in `crates/fluss/src/client/table/lookup.rs`:

- `TableLookup::create_lookuper`
- `Lookuper::lookup`
- `LookupResult::to_record_batch`

This is the natural execution backend for complete primary-key equality pushdown.

### Log bounded read APIs

There are two existing bounded read directions:

1. `crates/fluss/src/client/table/batch_scanner.rs`
   - useful for one-shot bounded reads
   - currently does not expose explicit `start_offset`
2. `crates/fluss/src/client/table/reader.rs`
   - `RecordBatchLogReader::new_until_offsets`
   - better fit when explicit stop-offset semantics are needed

Phase 1 log execution should prefer the path that preserves the narrowest, clearest SQL contract.

### Arrow conversion helpers

Reuse the existing helpers in `crates/fluss/src/record/arrow.rs`:

- `to_arrow_schema`
- `from_arrow_field`
- `RowAppendRecordBatchBuilder`

Do not reimplement Fluss-to-Arrow mapping from scratch.

### Test infrastructure

Reuse the support crate:

- `crates/fluss-test-cluster`

Follow the patterns from existing integration tests:

- `crates/fluss/tests/test_fluss.rs`
- `crates/fluss/tests/integration/utils.rs`
- `crates/fluss/tests/integration/kv_table.rs`
- `crates/fluss/tests/integration/log_table.rs`
- `crates/fluss/tests/integration/record_batch_log_reader.rs`

Note that `crates/fluss/tests/integration/utils.rs` is not a reusable library. New tests in `crates/fluss-datafusion` must create their own local `tests/integration/utils.rs`, likely by porting the minimal helper patterns while depending on `fluss-test-cluster`.

## Proposed Crate Layout

```text
Cargo.toml

docs/
  fluss-datafusion-phase1-design.md

crates/
  fluss-datafusion/
    Cargo.toml
    src/
      lib.rs
      config.rs
      error.rs
      install.rs

      metadata/
        mod.rs
        cache.rs
        loader.rs

      catalog/
        mod.rs
        provider.rs
        schema.rs
        register.rs

      table/
        mod.rs
        predicate.rs
        kv.rs
        log.rs

      execution/
        mod.rs
        lookup.rs
        log_scan.rs
        stream.rs

      types/
        mod.rs
        schema.rs
        scalar.rs
        record_batch.rs

    tests/
      test_fluss_datafusion.rs
      integration/
        utils.rs
        catalog.rs
        kv_lookup.rs
        log_scan.rs
        explain.rs
```

## Public API Shape

Phase 1 public API should stay small.

### `src/lib.rs`

Export only the public entry points:

- `FlussDatafusion`
- `FlussDatafusionOptions`
- `RegisterCatalogOptions`
- `FlussDatafusionError`
- crate-local `Result<T>` alias if needed

### `src/config.rs`

Define:

```rust
pub struct FlussDatafusionOptions {
    pub metadata_cache_ttl: Option<std::time::Duration>,
    pub table_cache_capacity: usize,
}

pub struct RegisterCatalogOptions {}
```

Keep options minimal in Phase 1. Do not introduce session-specific knobs here.

### `src/install.rs`

Define the main entry point:

```rust
pub struct FlussDatafusion {
    inner: std::sync::Arc<Inner>,
}

impl FlussDatafusion {
    pub async fn new(
        connection: std::sync::Arc<fluss::client::FlussConnection>,
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

Phase 1 should not expose gateway-only abstractions.

## Internal Module Responsibilities

### `src/metadata/cache.rs`

Responsibility:

- shared cache for databases, tables, and table metadata
- cache entries reused across sessions and `SessionContext`s
- no gateway session state

Expected cached objects:

- database names
- table names per database
- `TableInfo` per table
- latest schema info per table when needed

### `src/metadata/loader.rs`

Responsibility:

- async loading using `FlussAdmin`
- bridge from DataFusion sync-facing trait methods to cached data
- enforce lazy loading instead of preloading the full cluster

Key design rule:

- sync DataFusion trait entry points should read from the shared cache
- async refresh paths should live behind the cache loader
- avoid per-query full metadata scans

### `src/catalog/provider.rs`

Responsibility:

- implement the top-level DataFusion `CatalogProvider`
- expose Fluss databases as schemas

### `src/catalog/schema.rs`

Responsibility:

- implement the DataFusion `SchemaProvider`
- expose Fluss tables inside one database
- construct either KV or log `TableProvider`

### `src/catalog/register.rs`

Responsibility:

- create the catalog provider graph
- register it into a `SessionContext`
- keep registration logic out of `lib.rs`

### `src/table/predicate.rs`

Responsibility:

- inspect DataFusion filter expressions
- detect complete primary-key equality predicates for KV tables
- classify unsupported patterns clearly

Phase 1 accepted KV pattern:

- `pk1 = value AND pk2 = value ...` for the full primary key

Phase 1 rejected KV patterns:

- partial primary key filters
- `IN (...)`
- non-primary-key filters
- prefix patterns
- range scans

### `src/table/kv.rs`

Responsibility:

- implement KV `TableProvider`
- advertise supported filter pushdown
- build a lookup execution plan when filters fully match the primary key

Important rule:

- when the query does not match the supported KV pattern, do not silently fall back to a full scan
- return a clear unsupported-query error instead

### `src/table/log.rs`

Responsibility:

- implement log `TableProvider`
- require `LIMIT`
- build the bounded log execution plan

Important rule:

- log tables without `LIMIT` must fail with `LimitRequired`

### `src/execution/lookup.rs`

Responsibility:

- implement the DataFusion `ExecutionPlan` for KV point lookup
- execute through `Lookuper::lookup`
- emit a `SendableRecordBatchStream`

### `src/execution/log_scan.rs`

Responsibility:

- implement the DataFusion `ExecutionPlan` for bounded log reads
- reuse existing Fluss scan/reader building blocks
- emit a `SendableRecordBatchStream`

### `src/execution/stream.rs`

Responsibility:

- adapt lookup or scan outputs into DataFusion streams
- keep drop behavior simple and cooperative

### `src/types/schema.rs`

Responsibility:

- centralize schema conversion glue that is specific to DataFusion integration
- reuse `fluss::record::arrow::to_arrow_schema` whenever possible

### `src/types/scalar.rs`

Responsibility:

- convert DataFusion `ScalarValue` into the Fluss row/key representation needed by lookup execution
- validate types strictly and fail loudly on unsupported conversions

### `src/types/record_batch.rs`

Responsibility:

- adapt existing Fluss batch results into DataFusion-friendly batch outputs
- avoid duplicating low-level Arrow assembly logic already present in `fluss`

### `src/error.rs`

Define a crate-specific error model, for example:

```rust
pub enum FlussDatafusionError {
    DatabaseNotFound,
    TableNotFound,
    UnsupportedQueryPattern,
    LimitRequired,
    SchemaMismatch,
    TypeConversion,
    MetadataLoad,
    FlussClient,
    Internal,
}
```

The crate must not encode PostgreSQL- or gateway-specific errors.

## Phase 1 Query Semantics

## KV tables

Phase 1 KV support is intentionally strict.

Supported:

- full primary-key equality predicates only

Unsupported:

- partial-key scans
- prefix scans
- range scans
- non-key filter pushdown
- hidden fallback full scans

Recommendation:

- fail clearly for unsupported KV SQL instead of pretending to support more than is truly implemented

## Log tables

Phase 1 log support must be conservative.

Locked decisions for Phase 1:

1. `LIMIT` is mandatory.
2. Projection pushdown is supported.
3. Offset pseudo columns are not exposed.
4. Offset predicates are not supported.
5. Bucket-local order follows existing Fluss log read semantics.
6. Cross-bucket global row order is unspecified unless a future version implements stronger ordering semantics.

Recommended initial behavior:

- read from earliest available offsets
- bound the read by the SQL `LIMIT`
- keep the contract narrow and document that `ORDER BY` pushdown is out of scope

This keeps the crate aligned with existing Fluss client capabilities and avoids overpromising SQL semantics that are not yet implemented.

## Dependency Plan

### Root `Cargo.toml`

Update the workspace root file:

- add `crates/fluss-datafusion` to `[workspace].members`
- add `datafusion` to `[workspace.dependencies]`
- add any directly needed supporting crates there if the workspace wants to share versions

### `crates/fluss-datafusion/Cargo.toml`

Expected dependencies:

- `fluss = { workspace = true }`
- `arrow = { workspace = true }`
- `tokio = { workspace = true }`
- `serde` and `serde_json` only if needed
- `datafusion`
- `async-trait` only if truly needed
- small utility crates only when justified

Guideline:

- keep the new crate dependency surface narrow
- do not pull gateway or PostgreSQL compatibility crates into this crate

## Testing Strategy

### Unit tests

Colocate focused unit tests with the modules they validate.

Required coverage:

- predicate recognition
- `ScalarValue` conversion
- schema mapping glue
- pushdown decision behavior
- error mapping behavior

### Integration tests

Create:

- `crates/fluss-datafusion/tests/test_fluss_datafusion.rs`
- `crates/fluss-datafusion/tests/integration/*`

Suggested pattern:

- mirror `crates/fluss/tests/test_fluss.rs`
- gate cluster-backed integration tests behind an `integration_tests` feature
- build a local `tests/integration/utils.rs` on top of `fluss-test-cluster`

Integration cases required in Phase 1:

1. `catalog.rs`
   - registering a catalog exposes databases and tables
2. `kv_lookup.rs`
   - SQL with complete primary-key equality returns the expected row
   - composite primary-key equality also works
   - unsupported predicates fail clearly
3. `log_scan.rs`
   - log table queries require `LIMIT`
   - bounded log scan returns rows
   - projection pushdown works
4. `explain.rs`
   - `EXPLAIN` shows the custom execution plan name
   - unsupported plans are not misrepresented as pushed down

### Validation commands

At minimum, Phase 1 work should pass:

```bash
cargo check --workspace
cargo test -p fluss-datafusion
cargo test -p fluss-datafusion --features integration_tests
```

If integration tests are slow or environment-sensitive, keep the feature gate explicit.

## File-Level Implementation Tasks

## Task 1: workspace and crate skeleton

Goal:

- introduce the new crate cleanly into the workspace

Files:

- `Cargo.toml`
- `crates/fluss-datafusion/Cargo.toml`
- `crates/fluss-datafusion/src/lib.rs`
- `crates/fluss-datafusion/src/config.rs`
- `crates/fluss-datafusion/src/error.rs`
- `crates/fluss-datafusion/src/install.rs`

Deliverables:

- workspace member added
- dependency line added
- crate builds
- public API shape compiles even if internals are stubbed

Validation:

- `cargo check -p fluss-datafusion`

Status:

- Completed. Added `crates/fluss-datafusion` as a workspace member with a compiling public API skeleton (`FlussDatafusion`, `FlussDatafusionOptions`, `RegisterCatalogOptions`, `FlussDatafusionError`).
- Pinned `datafusion = 51.0.0` (aligns with workspace `arrow 57`) and bumped workspace `rust-version` to `1.88` to satisfy DataFusion's MSRV.
- `register_catalog()` is intentionally a failing stub until Task 2 wires real catalog registration.
- Verified with `cargo check -p fluss-datafusion` and `cargo check --workspace`. No Task 1 tests added; substantive behavior arrives in Task 2/5.

## Task 2: metadata cache and registration path

Goal:

- make `register_catalog()` create a real Fluss catalog tree backed by shared metadata

Files:

- `crates/fluss-datafusion/src/metadata/mod.rs`
- `crates/fluss-datafusion/src/metadata/cache.rs`
- `crates/fluss-datafusion/src/metadata/loader.rs`
- `crates/fluss-datafusion/src/catalog/mod.rs`
- `crates/fluss-datafusion/src/catalog/provider.rs`
- `crates/fluss-datafusion/src/catalog/schema.rs`
- `crates/fluss-datafusion/src/catalog/register.rs`

Deliverables:

- catalog registration works against a `SessionContext`
- database and table listing use shared metadata state
- no per-session full metadata warmup

Validation:

- integration test for catalog registration and listing

## Task 3: KV predicate analysis and lookup execution

Goal:

- support the narrow KV pushdown path for complete primary-key equality

Files:

- `crates/fluss-datafusion/src/table/mod.rs`
- `crates/fluss-datafusion/src/table/predicate.rs`
- `crates/fluss-datafusion/src/table/kv.rs`
- `crates/fluss-datafusion/src/execution/mod.rs`
- `crates/fluss-datafusion/src/execution/lookup.rs`
- `crates/fluss-datafusion/src/execution/stream.rs`
- `crates/fluss-datafusion/src/types/mod.rs`
- `crates/fluss-datafusion/src/types/scalar.rs`
- `crates/fluss-datafusion/src/types/record_batch.rs`

Deliverables:

- full primary-key equality is recognized
- lookup execution runs through existing Fluss lookup APIs
- unsupported KV SQL returns a clear error

Validation:

- unit tests for predicate matching and scalar conversion
- integration tests for single-key and composite-key SQL lookups
- `EXPLAIN` shows the custom lookup plan

## Task 4: log bounded scan execution

Goal:

- support the narrow log read path with `LIMIT` required

Files:

- `crates/fluss-datafusion/src/table/log.rs`
- `crates/fluss-datafusion/src/execution/log_scan.rs`
- `crates/fluss-datafusion/src/types/schema.rs`
- `crates/fluss-datafusion/src/types/record_batch.rs`

Deliverables:

- log `TableProvider` requires `LIMIT`
- bounded execution emits `RecordBatch` stream output
- projection pushdown works

Validation:

- integration tests for log query with `LIMIT`
- integration tests for missing-`LIMIT` error
- `EXPLAIN` shows the custom log scan plan

## Task 5: crate-local integration test harness

Goal:

- make the new crate independently testable with a real cluster

Files:

- `crates/fluss-datafusion/tests/test_fluss_datafusion.rs`
- `crates/fluss-datafusion/tests/integration/utils.rs`
- `crates/fluss-datafusion/tests/integration/catalog.rs`
- `crates/fluss-datafusion/tests/integration/kv_lookup.rs`
- `crates/fluss-datafusion/tests/integration/log_scan.rs`
- `crates/fluss-datafusion/tests/integration/explain.rs`

Deliverables:

- feature-gated integration test entrypoint
- local helper utilities built on `fluss-test-cluster`
- end-to-end test coverage for the supported Phase 1 SQL paths

Validation:

- `cargo test -p fluss-datafusion --features integration_tests`

## Recommended Execution Order for Sub-agents

Use sequential sub-agent work in this order:

1. Task 1: workspace and crate skeleton
2. Task 2: metadata cache and registration path
3. Task 3: KV predicate analysis and lookup execution
4. Task 4: log bounded scan execution
5. Task 5: crate-local integration test harness

Why this order:

- Task 2 depends on the crate existing
- Task 3 and Task 4 depend on catalog and table plumbing
- Task 5 is easiest to finalize once real behaviors exist, even if test scaffolding can begin earlier

## Out-of-repo Follow-up

This document only covers `fluss-rust` work.

After Phase 1 lands here, `fluss-gateway` follow-up should:

1. create one shared `FlussDatafusion` per cluster/proxy connection
2. call `register_catalog(&ctx, "fluss", ...)` when a SQL session builds a fresh `SessionContext`
3. layer gateway-specific SQL environment setup on top of the real Fluss catalog
4. keep `pg_catalog`, session variables, auth, timeout, and cancel semantics in gateway, not here

## Summary

Phase 1 should add a new reusable crate at `crates/fluss-datafusion/`, not under `integration/`.

The implementation should stay narrow:

- shared metadata-backed catalog registration
- KV full-primary-key equality pushdown
- log bounded scan with `LIMIT` required
- real-cluster integration tests

This keeps the crate aligned with the existing `fluss` client architecture and gives later `fluss-gateway` work a stable installer-style integration point.
