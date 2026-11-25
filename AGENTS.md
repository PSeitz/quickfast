# Quickfast repository instructions

Rust source code is under `quickwit/` (internal crates keep their `quickwit-*` names; the CLI binary is `quickfast`).

## Project
Quickfast is a fork of Quickwit: a cloud-native distributed search engine for observability data (logs, traces) with simplified file-based ingest (`ingesthree`), standalone compaction, and plain Tokio services.

## Repository layout

- `quickwit/`: Rust workspace and REST API tests
- `docs/internals/`: architecture, verification, and design documentation
- `config/`: runtime configurations
- `Makefile`: repository-level development commands
- `docker-compose.yml`: development test services

## Working principles

- Keep changes focused on the requested task.
- Do not add fallback behavior that hides failures in production paths.
- Do not work around incorrect behavior by weakening or manipulating tests.
- Treat unexpected states as errors or violated invariants; do not silently
  pretend they succeeded.
- Update relevant documentation when changing public behavior, configuration,
  protocols, or architecture.

## Testing

Repository-level commands include:

```bash
make docker-compose-up
make test-all
make docker-compose-down
make typos
```

# Rust workspace (`quickwit/`)

## Architecture Overview

The workspace contains 30 Rust crates.

### Key Layers

**Protocol & Types** — `quickwit-proto` defines gRPC service contracts and message types via protobuf (`tonic_prost_build`) with explicit service traits and client/server adapters.

**Ingest, Indexing & Compaction** — Services run as plain async Tokio tasks with explicit shutdown:

- `quickwit-ingest` (`ingesthree`) validates and batches incoming documents into compressed staged files on object storage and registers them in the metastore.
- `quickwit-indexing` claims staged files from the metastore, builds and uploads Tantivy splits, and publishes them atomically. Thin `Source → Ingest API` forwarders support Kafka, SQS, and GCP Pub/Sub using native broker offsets/acks.
- `quickwit-compaction` plans and executes split merges on standalone compactor nodes.
- `quickwit-janitor` runs garbage collection, retention policies, delete tasks, and completed-file cleanup.

**Search** — `quickwit-search` implements a root-leaf pattern: root servers parse queries and coordinate, leaf servers search their assigned splits in parallel, leaf results are merged at root.

**Storage** — `quickwit-storage` abstracts cloud storage (S3, Azure, GCS, local file, RAM) behind a `Storage` trait.

**Metastore** — `quickwit-metastore` manages index metadata, split metadata, and staged-file leases with file-backed (dev) and PostgreSQL (production) backends.

**Cluster** — `quickwit-cluster` uses Chitchat gossip protocol for cluster membership and node readiness.

**API Surface** — `quickwit-serve` hosts REST and gRPC endpoints over the same service traits, plus serves the embedded React UI.

### Core Crates

| Crate | Purpose |
|-------|---------|
| `quickwit-cli` | CLI entry point (`quickfast` binary) |
| `quickwit-serve` | REST/gRPC server and node lifecycle |
| `quickwit-search` | Distributed search orchestration |
| `quickwit-ingest` | File-based ingestion (`ingesthree`) and write queue |
| `quickwit-indexing` | Staged-file indexing, packaging, and pull-source forwarders |
| `quickwit-compaction` | Standalone compaction planner and compactor service |
| `quickwit-janitor` | GC, retention, delete tasks, and completed-file cleanup |
| `quickwit-metastore` | Index, split, and staged-file metadata storage |
| `quickwit-storage` | Multi-cloud storage abstraction |
| `quickwit-config` | Configuration parsing/validation |
| `quickwit-doc-mapper` | Index schema and document mapping |
| `quickwit-query` | Query DSL parsing (ES-compatible) |
| `quickwit-cluster` | Cluster membership (Chitchat) |
| `quickwit-proto` | Protobuf definitions and gRPC traits |
| `quickwit-common` | Shared utilities and `run_cpu_intensive` |
| `quickwit-metrics` | Metrics declarations and macros |
| `quickwit-lambda-server` | AWS Lambda leaf search handler |
| `quickwit-lambda-client` | Lambda invocation with auto-deployment |

`quickwit-common` contains shared utilities for rate-limited logging, reading from environment variables, and more. It also contains `run_cpu_intensive`, which should be used to run CPU-intensive work from Tokio tasks.

When the client is unlikely to match on an error, you can rely on the crate-level error or `anyhow::Error`. If you need to introduce a new error type, use `thiserror`.

### Design Patterns

- **Trait-based services**: `SearchService`, `MetastoreService`, etc. — enables mocking and multiple implementations
- **Feature gates**: Cloud backends (`azure`, `gcs`), pull-source forwarders (`kafka`, `sqs`, `gcp-pubsub`), `postgres` metastore
- **Metrics**: `std::sync::LazyLock` statics with `quickwit_metrics::*` macros and `quickwit_common::metrics::*` helpers

### Key Dependencies

- **Tantivy**: Search engine library (with `quickwit` feature flag)
- **Tonic/Prost**: gRPC framework and protobuf
- **Tokio**: Async runtime
- **SQLx**: PostgreSQL metastore

## Quickwit Dependencies

When adding a new dependency, update licenses by running `make update-licenses`.
Prefer referring to the crate in the workspace and keep features minimal.

In other words, prefer:

```toml
zip = { workspace = true, default-features = false, features = ["deflate"] }
```

over:

```toml
zip = "2"
```

Run `cargo check` after editing `Cargo.toml` to update `Cargo.lock`.

## Coding Style

- Avoid single-letter variable names except for indices (`i`, `j`, `k`).
- Document all "hidden contracts" (implicit assumptions, invariants, preconditions).
- Try to avoid deep nesting. In particular, prefer early returns.
- Avoid abusing iterator chaining with complex constructs like `.transpose()`.
- Write type names explicitly when it aids readability.

## Checklist After Making Changes

**MUST**:

- [ ] Run `make fmt`.
- [ ] Keep new files under 500 lines (split by responsibility if larger).
- [ ] Ensure tests pass (see below).
- [ ] Update documentation for new public behavior, configuration, protocols, or architecture.

### Testing

- Single crate test: `cargo nextest run -p quickwit-search my_test_name`
- `make test-all` — starts Docker services (LocalStack S3, PostgreSQL, Pub/Sub emulator) and runs the full test suite with `cargo nextest run --all-features --profile ci` (the `ci` profile in `quickwit/.config/nextest.toml` retries failing tests twice).
- Docker services: `make docker-compose-up` / `make docker-compose-down` (subset: `DOCKER_SERVICES=kafka,postgres`).
- Integration tests are under `rest-api-tests`; compile and run the `quickfast` binary (`cargo build -p quickwit-cli`) to have an instance available to test against.

