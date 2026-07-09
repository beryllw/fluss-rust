# FIP-XX: Apache Fluss DataFusion Integration Module

| Field | Content |
|------|------|
| **Author** | TBD |
| **Discussion thread** | TBD |
| **Vote thread** | TBD |
| **ISSUE** | TBD |
| **Release** | TBD |

Please keep discussion on the mailing list rather than in document comments.

## 1. Motivation

Apache Fluss already provides client-side read capabilities for KV tables, log tables, and lake-enabled tables. However, Rust-side SQL consumers still lack a formal, reusable, and stable DataFusion integration layer.

The goal of `fluss-datafusion` is not to become a second query gateway. Its purpose is to expose Fluss tables through native DataFusion concepts so callers can query Fluss directly through `SessionContext` and `ctx.sql(...)` without re-implementing:

- catalog / schema / table registration;
- Fluss-schema to Arrow-schema mapping;
- predicate recognition for primary-key lookups and prefix lookups;
- finite log snapshot scanning;
- unified access to lake-enabled tables.

The module must cover three read families at the same time:

1. **Non-lake KV tables**
   - point lookup by complete primary-key equality;
   - prefix lookup by complete bucket-key prefix equality;
   - bounded scan when a `LIMIT` is present.
2. **Non-lake log tables**
   - finite earliest-to-latest snapshot scan;
   - optional `LIMIT` with head-first first-N semantics.
3. **Lake-enabled tables**
   - unified reads through the normal table name `<table>`;
   - lake-only reads through `<table>$lake`;
   - direct KV fast paths for eligible primary-key queries.

The central goal of this FIP is to define `fluss-datafusion` as a **formal, stable DataFusion adapter**:

- it is a library, not a protocol server;
- it owns DataFusion integration, not session / auth / protocol concerns;
- it reuses `fluss-rust` and `fluss-lake` rather than duplicating lake-read semantics;
- it adopts a live metadata model rather than an internal metadata cache.

## 2. Public Interfaces

This FIP defines and stabilizes the following public interfaces and user-visible contracts.

### 2.1 Rust API

Public types:

- `FlussDatafusion`
- `FlussDatafusionOptions`
- `RegisterCatalogOptions`
- `FlussDatafusionError`
- `Result`

The key public interfaces can be expressed in Rust as follows:

```rust
pub struct FlussDatafusionOptions {
    pub lake_storage_options: std::collections::HashMap<String, String>,
}

pub struct RegisterCatalogOptions {}

pub struct FlussDatafusion;

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

Their formal meanings are:

- `new(...)`: create a shared installer over a live `FlussConnection`;
- `register_catalog(...)`: install the Fluss catalog into a `SessionContext`;
- `swap_connection(...)`: hot-swap the underlying connection without requiring the catalog to be re-registered;
- `FlussDatafusionOptions`: carry installer-level options, currently centered on lake storage injection;
- `RegisterCatalogOptions`: reserve the extension point for future catalog-install options.

### 2.2 SQL-visible interfaces

This module formally exposes the following SQL-facing interfaces:

- table paths of the form `<catalog>.<database>.<table>`;
- a metadata table `<table>$options`;
- a lake-only read shape `<table>$lake`;
- custom physical plans visible through `EXPLAIN`.

The key SQL-visible interface content is:

1. **Normal table path**
   - form: `<catalog>.<database>.<table>`
   - example: `fluss.sales.orders`

2. **`<table>$options`**
   - form: `<catalog>.<database>.<table>$options`
   - output schema:

```text
key: Utf8
value: Utf8
```

   - typical content includes `table.id`, `schema.id`, `num.buckets`, `primary.keys`, `bucket.keys`, `partition.keys`, `datalake.enabled`, `datalake.format`, and redacted datalake properties.

3. **`<table>$lake`**
   - form: `<catalog>.<database>.<table>$lake`
   - semantics: read only the lake snapshot, with no residual Fluss log tail.

4. **`EXPLAIN`-visible plan names**
   - the formal plan names currently visible include:
     - `FlussKvLookupExec`
     - `FlussKvPrefixLookupExec`
     - `FlussKvScanExec`
     - `FlussLogScanExec`
     - `FlussUnionScanExec`

### 2.3 Configuration interface

The current formal configuration surface is:

- `FlussDatafusionOptions.lake_storage_options`

Its interface content is:

```rust
pub struct FlussDatafusionOptions {
    pub lake_storage_options: std::collections::HashMap<String, String>,
}
```

This option is used to add or override lake storage connection parameters before opening the lake catalog, for example:

- `s3.endpoint`
- `s3.access-key`
- `s3.secret-key`
- `s3.region`

The merge rule is **caller wins**, meaning:

- server-derived `table.datalake.*` properties provide the base configuration;
- caller-supplied `lake_storage_options` are merged over them;
- conflicting keys are resolved in favor of the caller.

### 2.4 DataFusion integration interfaces

Beyond the public Rust API, the module also defines a formal integration contract through DataFusion's standard extension points:

- `CatalogProvider`
- `SchemaProvider`
- `TableProvider`
- `ExecutionPlan`

Their roles in this module are:

- `FlussCatalogProvider`: expose database-level catalog structure;
- `FlussSchemaProvider`: expose table listing and table resolution;
- `FlussKvTableProvider` / `FlussLogTableProvider` / `FlussUnionTableProvider`: expose the read path appropriate for each table kind;
- custom `ExecutionPlan`s: adapt lower-layer reads into executable DataFusion physical plans.

The key `TableProvider` interface content in this module can be summarized as:

```rust
trait TableProvider {
    fn schema(&self) -> SchemaRef;
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>>;
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}
```

Its formal meaning is:

- `schema()`: expose the Arrow schema of the table to DataFusion;
- `supports_filters_pushdown(...)`: explicitly declare which predicates are exact pushdowns, inexact pushdowns, or unsupported;
- `scan(...)`: produce the final physical read plan from projection / filters / limit.

## 3. Proposed Changes

### 3.1 Module positioning and boundary

`fluss-datafusion` is defined as Fluss's **DataFusion integration module**. It sits above `fluss-rust` and `fluss-lake`, and is responsible for connecting Fluss tables to DataFusion without taking ownership of higher-layer protocol semantics.

Its responsibilities include:

- registering the Fluss catalog;
- loading metadata and Arrow schema on demand;
- recognizing supported SQL predicate shapes;
- routing queries to KV fast paths, bounded scans, finite log scans, or the lake read path;
- adapting lower-layer results into DataFusion `ExecutionPlan`s and `RecordBatchStream`s.

Its non-goals include:

- PostgreSQL / Flight SQL / REST / gRPC protocol adapters;
- session, authentication, tenant, or multi-cluster routing;
- SQL DML / DDL execution;
- re-implementing lake seam handling, append stitching, or PK merge logic;
- becoming the gateway itself.

### 3.2 Overall architecture

```text
+-------------------------+
| SessionContext          |
+------------+------------+
             |
             v
+-------------------------+
| FlussDatafusion         |
| register_catalog(...)   |
+------------+------------+
             |
             v
+-------------------------+
| FlussCatalogProvider    |
| FlussSchemaProvider     |
+------------+------------+
             |
             v
+-------------------------+
| KV / Log / Lake Tables  |
| Predicate Routing       |
+------------+------------+
             |
             v
+-------------------------+
| Custom Execution Plans  |
| RecordBatch Streams     |
+------------+------------+
             |
             v
+-------------------------+
| fluss-rust              |
| fluss-lake              |
+-------------------------+
```

This architecture establishes three formal principles:

1. **Each SQL session owns its own `SessionContext`.**
2. **One `FlussDatafusion` installer can be reused across many sessions.**
3. **Core table semantics remain owned by `fluss-rust` and `fluss-lake`, while this module performs engine adaptation.**

### 3.3 Lifecycle and metadata visibility

The formal usage model is:

1. create one shared `FlussDatafusion`;
2. create one `SessionContext` per SQL session;
3. call `register_catalog(...)` to install the catalog;
4. query Fluss tables with `ctx.sql(...)`.

This FIP fixes the following metadata contract:

- the catalog is a **live catalog**;
- `schema_names()`, `table_names()`, `table()`, and related calls fetch live metadata from Fluss;
- DDL performed after `register_catalog(...)` is immediately visible in the same `SessionContext`;
- the module does not introduce a metadata cache.

### 3.4 Catalog / schema / table mapping

The module maps Fluss metadata into the DataFusion catalog model as follows:

- one DataFusion catalog contains Fluss databases;
- one Fluss database maps to one `SchemaProvider`;
- one Fluss table maps to one `TableProvider`.

The key provider interfaces can be summarized as:

```rust
trait CatalogProvider {
    fn schema_names(&self) -> Vec<String>;
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>>;
}

trait SchemaProvider {
    fn table_names(&self) -> Vec<String>;
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>>;
}
```

Their formal meaning in this module is:

- `schema_names()`: list Fluss databases live;
- `schema(name)`: resolve one Fluss database into a `SchemaProvider`;
- `table_names()`: list tables in that database live;
- `table(name)`: dynamically route to the KV / Log / Lake `TableProvider` according to metadata.

Table resolution follows the formal rules below:

| Input name | Route |
|---|---|
| `<table>$options` | metadata key/value table |
| `<table>$lake` | lake-only table provider |
| normal lake-enabled table | lake read provider |
| normal non-lake KV table | KV table provider |
| normal non-lake log table | log table provider |

This means:

- `<table>$options` is a formal introspection surface;
- `<table>$lake` is a formal lake-only read shape, not merely an internal debug hook;
- a lake-enabled primary-key table must first enter the lake route, and only then may use an internal KV fast path when appropriate.

### 3.5 Query routing matrix

#### 3.5.1 Non-lake KV tables

Non-lake KV tables support the following routing matrix:

| Query shape | Route |
|---|---|
| complete primary-key equality | point lookup |
| complete bucket-key prefix equality | prefix lookup |
| any other shape with `LIMIT` | bounded KV scan |
| any other shape without `LIMIT` | explicit rejection |

This defines the formal product behavior: non-lake KV tables do not promise a generic current-state full scan for arbitrary SQL. Instead, the module supports a narrow set of shapes that are both correct and operationally clear.

#### 3.5.2 Non-lake log tables

Non-lake log tables support the following routes:

| Query shape | Route |
|---|---|
| `LIMIT n` | first-N finite snapshot scan |
| no `LIMIT` | earliest-to-latest finite snapshot scan |

This path supports:

- projection pushdown;
- partition-equality pruning;
- parallel multi-bucket scans.

This path does not promise:

- global cross-bucket ordering;
- general value predicate pushdown;
- offset pseudo-column semantics.

#### 3.5.3 Lake-enabled tables

Lake-enabled tables formally expose two user-visible read shapes:

1. `<table>`: unified view
2. `<table>$lake`: lake-only view

The unified view means:

- for append/log tables: lake snapshot plus the residual Fluss log tail;
- for primary-key tables: current-view semantics for callers.

For lake-enabled primary-key tables, the adapter may directly use a KV fast path for the following query shapes:

- complete primary-key equality;
- complete bucket-key prefix equality.

This is an execution optimization only. It does not change the table-level meaning of `<table>`, which remains the unified readable view.

### 3.6 Physical execution contract

The module exposes formal execution paths through custom DataFusion physical plans, including:

- KV point lookup plan;
- KV prefix lookup plan;
- KV bounded scan plan;
- log snapshot scan plan;
- lake read plan.

This FIP fixes the following execution rules:

- one KV/log `(partition, bucket)` target maps to one DataFusion partition;
- one `FlussLakeReadSplit` maps to one DataFusion partition;
- when projection is already applied by the source, it is not re-applied;
- residual filters, limits, joins, aggregates, and similar operators remain the responsibility of upper DataFusion layers.

### 3.7 Formal boundary with `fluss-lake`

This FIP explicitly defines the responsibility boundary between `fluss-datafusion` and `fluss-lake`:

| Module | Responsibility |
|---|---|
| `fluss-rust` | metadata, KV reads, log reads, Arrow helpers |
| `fluss-lake` | seam resolution, snapshot planning, append stitch, PK merge, logical splits |
| `fluss-datafusion` | predicate recognition, route selection, DataFusion wrapping, config injection |

Therefore `fluss-datafusion` must not duplicate:

- seam interpretation;
- Paimon split planning;
- append stitching;
- PK merge.

Those semantics belong formally to `fluss-lake`.

### 3.8 Filter pushdown contract

The formal filter-pushdown strategy is: **provide exact pushdown only for shapes that can be proven sound**.

Concretely:

- complete PK equality may be pushed down exactly;
- complete prefix equality may be pushed down exactly;
- partition equality may be used as `Inexact` pushdown for pruning;
- the normal union read path keeps residual filtering in DataFusion.

This definition prioritizes correctness over the appearance of broader SQL support.

### 3.9 Error model

The public error type `FlussDatafusionError` remains the stable module-level error model for:

- missing metadata objects;
- unsupported query shapes;
- schema/type conversion mismatch;
- Fluss client failures;
- internal failures.

All such errors bubble through standard DataFusion error paths, without introducing gateway-protocol-specific error semantics.

## 4. Compatibility, Deprecation, and Migration Plan

This FIP defines an additive module / stabilized module design. It does not require incompatible changes to existing Fluss server protocols or storage formats.

Compatibility impact:

- **Fluss server:** no required changes;
- **Fluss storage formats:** no required changes;
- **existing clients:** no migration required;
- **lake-enabled tables:** continue to rely on unified read semantics owned by `fluss-lake`;
- **callers:** adopt the crate as needed, with no impact on users who do not use it.

Migration plan:

- no data migration is required;
- no schema migration is required;
- no compatibility shims or migration tools are required.

## 5. Test Plan

This FIP focuses on system-level semantic validation rather than only local unit behavior.

### 5.1 Unit tests

Should cover:

- predicate recognition;
- pushdown classification;
- projection normalization;
- Fluss-to-Arrow type conversion;
- metadata property redaction;
- lake storage option merge behavior.

### 5.2 Real-cluster integration tests

Should cover:

- catalog listing and `information_schema` exposure;
- immediate DDL visibility in the same session;
- KV point lookup and prefix lookup;
- conservative failure for unsupported KV queries;
- log-table reads both with and without `LIMIT`;
- partition pruning;
- connection swap while keeping the catalog usable.

### 5.3 Lake-enabled end-to-end tests

Should cover:

- unified reads through `<table>`;
- lake-only reads through `<table>$lake`;
- fast-path reads for lake-enabled primary-key tables;
- caller-supplied storage options when opening the lake catalog;
- full-stack verification against real tiering.

### 5.4 Verification commands

```bash
cargo check --workspace
cargo test -p fluss-datafusion
cargo test -p fluss-datafusion --features integration_tests
```

## 6. Rejected Alternatives

### 6.1 Make `fluss-datafusion` the query gateway itself

Rejected because protocol, authentication, and session concerns are not the same layer as engine adaptation. Combining them would weaken reuse and module boundaries.

### 6.2 Re-implement unified lake+log semantics inside the DataFusion module

Rejected because that would duplicate the responsibility of `fluss-lake` and would make the lake read contract harder to reuse across Arrow engines.

### 6.3 Add a metadata cache for faster catalog access

Rejected because the module prioritizes live metadata semantics. Immediate DDL visibility in the same session is more important than reducing catalog RPCs.

### 6.4 Force all lake-enabled primary-key queries through the lake kernel

Rejected because complete PK equality and complete prefix equality can be served more efficiently from Fluss current state without changing the final unified read semantics.

### 6.5 Silently degrade unsupported query shapes into full scans

Rejected because that would create a misleading appearance of broader SQL support and would weaken correctness and performance boundaries. This module chooses explicit failure instead.