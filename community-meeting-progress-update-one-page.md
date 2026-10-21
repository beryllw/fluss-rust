# Fluss Rust Progress Update

This is a short progress update, not a final decision.

## Overall

The work is now converging around three parts:

- **Lake kernel**: a shared lake + log unified read layer
- **DataFusion integration**: a thin SQL adapter on top of Fluss
- **Gateway**: the external access layer, with SQL and REST paths

We already have working prototype paths in all three areas.

## Lake Kernel + DataFusion

The main direction is to keep lake-specific read logic in one shared Rust layer and keep the DataFusion layer thin.

**What already works**
- unified read prototypes for append/log tables
- unified read prototypes for primary-key tables
- lake-only read support
- Fluss tables exposed through DataFusion SQL
- integration tests, including tiering scenarios

**Main gaps**
- partitioned lake tables are not fully supported yet
- historical snapshot support is still incomplete
- the planning and split contract still needs hardening

## Gateway

### SQL path

The SQL path is a PostgreSQL-compatible query entry point.

**Supports today**
- PostgreSQL wire protocol
- simple query and extended query
- read-only SELECT
- SET / SHOW
- basic transaction-style compatibility commands
- query cancellation

### REST path

The REST path is mainly for direct write and metadata access.

**Supports today**
- write to KV tables
- write to log tables
- delete for KV tables
- list databases and tables
- get table information
- JSON input
- Arrow IPC input
- OTLP over HTTP ingestion

**Main gap**
- direct read over REST is not fully implemented yet

## Closing

Overall, the project has moved from design ideas to working prototypes.

The next step is to harden the contracts, fill the remaining gaps, and integrate these parts into a cleaner long-term architecture.
