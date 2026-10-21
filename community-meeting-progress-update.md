# Fluss Rust Progress Update

## Status

This is a short progress update, not a final decision.

The goal is to share:
- what already works
- what the current direction is
- what still needs to be done

## Overall Direction

The work is now converging around three parts:

- **Lake kernel**: a shared lake + log unified read layer
- **DataFusion integration**: a thin SQL adapter on top of Fluss
- **Gateway**: the external access layer, with SQL and REST paths

We already have working prototype paths in all three areas.

## Lake Kernel and DataFusion

The main direction is:
- move lake-specific read logic into one shared Rust layer
- keep the DataFusion layer thin
- avoid re-implementing the same logic in multiple engines

### What already works

- unified read prototypes for append/log tables
- unified read prototypes for primary-key tables
- lake-only read support
- Fluss tables exposed through DataFusion SQL
- integration tests, including tiering-related scenarios

### Main gaps

- partitioned lake tables are not fully supported yet
- historical snapshot support is still incomplete
- the planning and split contract still needs hardening

### Key point

The lake work has moved from design into a real prototype, and DataFusion is already connected on top of it.

## Gateway Progress

Gateway can be explained in two parts: SQL and REST.

### SQL path

The SQL path is a PostgreSQL-compatible query entry point.

**What it supports today**
- PostgreSQL wire protocol
- simple query
- extended query
- read-only SELECT
- SET and SHOW
- basic transaction-style compatibility commands
- query cancellation

**Current boundary**
- mainly read-oriented
- not full PostgreSQL compatibility
- SQL write is not the focus right now

### REST path

The REST path is mainly for direct write and metadata access.

**What it supports today**
- write to KV tables
- write to log tables
- delete for KV tables
- list databases
- list tables
- get table information
- JSON input
- Arrow IPC input
- OTLP over HTTP ingestion

**Current boundary**
- direct read over REST is not fully implemented yet

### Key point

Gateway already has useful capabilities today: SQL for query access, and REST for direct write and metadata access.

## Closing

Overall, the project has moved from design ideas to working prototypes.

- the lake kernel is becoming the shared place for unified read semantics
- the DataFusion integration is already usable as a thin SQL layer
- the gateway already supports meaningful SQL and REST capabilities

The next step is to harden the contracts, fill the remaining gaps, and integrate these parts into a cleaner long-term architecture.
