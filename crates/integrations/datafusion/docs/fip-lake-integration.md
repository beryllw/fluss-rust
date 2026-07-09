# FIP: `fluss-datafusion` lake interaction and adapter boundary

> For the Chinese version, see [fip-lake-integration.zh.md](./fip-lake-integration.zh.md).

|                   |-----------|
| ----------------- | --------- |
| Discussion thread | TBD       |
| Vote thread       | TBD       |
| ISSUE             | TBD       |
| Release           | TBD       |

# Motivation

`fluss-datafusion` already has a working lake-enabled table path, but the role of the crate needs to be made explicit before the implementation grows further.

The key design pressure is that `fluss-datafusion` should not become a second place that re-implements lake+log union semantics. Those semantics belong in `fluss-lake`. The DataFusion integration should stay a thin adapter that:

1. recognizes when a table should use the lake kernel;
2. preserves a small number of explicit Fluss KV fast paths for primary-key tables;
3. delegates all remaining lake-enabled reads to `fluss-lake`;
4. maps kernel splits to DataFusion execution partitions.

Without a clear FIP, there is a risk that future changes reintroduce seam handling, lake split regrouping, append stitching, or PK merge logic inside the connector itself. That would duplicate semantics, make other Arrow engines harder to support, and blur the intended dependency direction.

A second motivation is naming and scope clarity. The current implementation still uses `FlussUnionTableProvider` and `FlussUnionScanExec`, even though the real behavior is now “DataFusion adapter over `fluss-lake`”. This FIP records the intended steady state.

# Public Interfaces

This proposal is primarily about the behavioral and adapter contract of the existing lake-enabled DataFusion path. It does not introduce a large new end-user Rust API in `fluss-datafusion`; instead it clarifies the user-visible and maintainer-visible contract of the current path.

The public contract covered by this FIP is:

1. **Lake-enabled SQL read routing**
   - For a lake-enabled table, the connector may choose among:
     - KV point lookup fast path;
     - KV prefix lookup fast path;
     - normal lake+log union read through `fluss-lake`;
     - lake-only read through `<table>$lake`.

2. **User-visible read shapes**
   - `<table>` means the unified view: lake snapshot plus residual Fluss log tail.
   - `<table>$lake` means lake-only snapshot read.

3. **Engine boundary contract with `fluss-lake`**
   - `FlussLakeTable`
   - `FlussLakeScan`
   - `FlussLakeReadPlan`
   - `FlussLakeRead`
   - `FlussLakeReadSplit`

4. **Execution partition mapping**
   - One `FlussLakeReadSplit` maps to one DataFusion partition.

5. **Pushdown contract for the current implementation**
   - PK point/prefix lookup predicates may be consumed exactly by fast paths.
   - The lake union path does not currently provide value-filter pushdown; residual filtering remains above the scan.

No compatibility promise is made for current internal transitional names such as `FlussUnionTableProvider` and `FlussUnionScanExec`; those are implementation details and may be renamed as part of convergence.

# Proposed Changes

## 1. Fix the role of `fluss-datafusion`

For lake-enabled tables, `fluss-datafusion` is defined as a **thin adapter** over `fluss-lake`.

The responsibility split is:

- `fluss-rust`
  - table metadata;
  - lake snapshot / seam RPCs;
  - Fluss log reading;
  - Arrow conversion helpers.
- `fluss-lake`
  - bounded read boundary freezing;
  - logical split planning;
  - append/log stitching;
  - PK merge;
  - lake-only read semantics.
- `fluss-datafusion`
  - SQL predicate recognition;
  - fast-path routing for eligible PK reads;
  - construction of `FlussLakeTable` / `FlussLakeScan`;
  - wrapping `FlussLakeReadPlan` and `FlussLakeRead` as DataFusion execution plans.

`fluss-datafusion` must not own:

- seam interpretation;
- Paimon `DataSplit` planning;
- append/log stitch behavior;
- PK merge behavior;
- physical lake/log task graph design.

## 2. Lock the routing matrix

For lake-enabled tables, `TableProvider::scan()` follows this precedence order:

| Query shape | Route | Why |
| --- | --- | --- |
| PK table + full primary-key equality | `FlussKvLookupExec` | Current state already lives in Fluss KV; no need to open lake |
| PK table + complete bucket-key prefix equality | `FlussKvPrefixLookupExec` | Prefix lookup can still be served directly from Fluss current state |
| `<table>$lake` | `fluss-lake` with `lake_only` | Caller explicitly asks for the lake snapshot only |
| Any other lake-enabled read | `fluss-lake` union read | Kernel owns seam, snapshot, append stitch, and PK merge semantics |

Current implementation note: the `fluss-lake` union path is currently stable only for **non-partitioned** lake tables. Public API shape already exists for partitioned tables, but the kernel still rejects true partitioned lake reads.

Additional constraints:

- fast paths are enabled only when `!lake_only && !primary_keys.is_empty()`;
- the union path currently marks all filters as `Unsupported`, leaving residual `FilterExec` above the scan;
- `num_buckets == 0` returns `EmptyExec` directly.

## 3. Lock the planning and execution flow

### `TableProvider::scan()`

For the lake path, the connector does the following:

1. normalize the projection;
2. try PK point lookup fast path;
3. try PK prefix lookup fast path;
4. validate that the source is the production `RealFlussSource`;
5. merge caller-supplied lake storage options over server-derived `table.datalake.*` properties, with **caller wins** semantics;
6. construct `FlussLakeTable`;
7. create `FlussLakeScan`, applying projection and `lake_only` when needed;
8. call `plan()` to obtain `FlussLakeReadPlan`;
9. call `new_read()` to obtain `FlussLakeRead`;
10. return a DataFusion execution plan wrapping `plan + read`.

### `ExecutionPlan::execute(i)`

At execution time:

1. choose `plan.splits()[i]`;
2. call `read.read_split(split)`;
3. adapt the kernel `RecordBatchStream` into DataFusion’s `SendableRecordBatchStream`;
4. let the remaining DataFusion operators handle residual filters, limits, and other plan nodes.

This intentionally keeps DataFusion ignorant of:

- seam DTOs;
- physical Paimon `DataSplit`s;
- append-vs-PK internal task shapes;
- log start/stop offset semantics.

## 4. Formalize `<table>$lake`

The lake-only read shape is a first-class contract, not an implementation accident.

- `<table>` returns the unified lake snapshot + residual Fluss tail view.
- `<table>$lake` returns only the Paimon lake snapshot.
- PK fast paths are disabled for `<table>$lake`, because the caller explicitly wants the lake side rather than Fluss current state.

This proposal treats `lake_only` as a stable adapter-level behavior that the connector may rely on.

## 5. Keep configuration injection at the adapter boundary

The connector has one important deployment-facing responsibility: restoring lake storage connection properties that may not remain fully present in table metadata at query time.

The contract is:

- server-derived `table.datalake.*` properties provide the logical catalog configuration;
- caller-supplied storage options may add or override connection properties such as credentials, endpoint, and region;
- merging is **caller wins**.

This belongs in the adapter because it is transport/configuration glue needed to open the lake catalog, not part of the lake read semantics themselves.

## 6. Record the convergence plan

### Phase 1: documentation convergence

- define `fluss-datafusion` as a lake-kernel adapter;
- document the routing matrix;
- document the `<table>` vs `<table>$lake` read shapes.

### Phase 2: naming convergence

Rename the transitional internal names:

- `FlussUnionTableProvider` -> `FlussLakeTableProvider`
- `FlussUnionScanExec` -> `FlussLakeScanExec`

Requirements:

- no behavioral change;
- `EXPLAIN` output updated consistently;
- tests and maintainership docs updated together.

### Phase 3: narrower long-term dependency surface

As `fluss-lake` hardens its split contract, `fluss-datafusion` should continue shrinking toward:

- predicate recognition;
- execution-plan wrapping;
- configuration injection.

# Compatibility, Deprecation, and Migration Plan

- **Impact on existing users:** no immediate SQL surface break is required. The proposal primarily documents and stabilizes existing behavior.
- **Behavioral caveat:** the lake union path remains effectively limited to non-partitioned lake tables until `fluss-lake` implements partitioned reads.
- **Internal renames:** renaming `FlussUnion*` types to `FlussLake*` is expected to be maintainership-facing only. The change should be phased in together with `EXPLAIN` string updates and test updates.
- **`<table>$lake` behavior:** this is additive documentation of an already-existing mode, not a new incompatible behavior.
- **Migration tooling:** none is expected.
- **Removal timing:** the old `Union` internal names can be removed once code, tests, and docs have all converged on the `Lake` naming.

# Test Plan

We are primarily interested in end-to-end behavior, not only unit details.

Existing and required validation includes:

- `cargo test -p fluss-datafusion`
- `cargo test -p fluss-datafusion --features integration_tests`

Key regression scenarios:

1. a lake-enabled append/log table queried as `<table>` returns the unified lake + log result;
2. a lake-enabled PK table with full PK equality uses `FlussKvLookupExec`;
3. a lake-enabled PK table with complete bucket-key prefix equality uses `FlussKvPrefixLookupExec`;
4. a non-fast-path lake-enabled read uses the lake scan execution plan;
5. `<table>$lake` returns only the lake snapshot and does not use PK fast paths;
6. caller-supplied storage options correctly override endpoint/credential values when opening the lake catalog;
7. `EXPLAIN` reflects the selected fast path or lake scan path.

# Rejected Alternatives

## 1. Keep lake+log union semantics inside `fluss-datafusion`

Rejected because it duplicates semantics that should be reusable by other Arrow engines and makes the connector own too much storage-specific logic.

## 2. Route all lake-enabled PK reads through `fluss-lake`

Rejected because full-PK equality and complete bucket-key prefix equality already have efficient Fluss current-state fast paths. For those shapes, opening lake is unnecessary overhead.

## 3. Treat `<table>$lake` as an internal-only implementation detail

Rejected because it is already a distinct user-visible read shape and should therefore be documented as such.

## 4. Expose seam and physical split details to DataFusion

Rejected because it would couple the engine adapter to kernel internals, making both reuse and future evolution harder.
