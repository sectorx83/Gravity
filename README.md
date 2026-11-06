# Gravity

[![Gravity CI](https://github.com/sectorx83/Gravity/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/sectorx83/Gravity/actions/workflows/ci.yml)

**Phase 1 status: Complete.** The local runtime foundation passes formatting, Clippy, all 20 unit tests, documentation generation, and the CLI demonstration in GitHub Actions.

Gravity is an adaptive computational runtime designed to change the topology of a running computational system while preserving required state and correctness.

Its central research problem is safe live topology transformation: adding, removing, splitting, replacing, and reconnecting computational cells without restarting the entire application.

Gravity is not an AI-agent framework, workflow dashboard, or general-purpose orchestrator. Its core abstraction is a mutable computational graph whose cells have explicit interfaces, state, and lifecycle rules.

## Phase 1 — Gravity Core

Phase 1 establishes the local runtime foundation:

- Rust workspace with a core runtime and CLI
- Explicit cell registration, startup, shutdown, and failure handling
- Deterministic directed message graph
- Bounded message queues and per-step delivery limits
- Transactional cell processing with state rollback on failure
- In-memory runtime snapshots and transactional restoration
- Graph inspection and repeatable tests

The runtime is intentionally single-threaded and single-machine in this phase. Its checkpoints are in-memory snapshots, not durable distributed checkpoints.

## Phase 2 — Gravity Evolution Engine

Phase 2 enables safe structural changes while execution continues:

- Runtime topology mutation transactions
- Work partitioning contracts
- Consistent cell snapshots
- Morphological State Transfer (MST)
- Shadow validation of candidate topology
- Validated routing cutover
- Rollback and cell regeneration
- Measured adaptation

The required demonstration starts with Input -> Processor A -> Output, creates Processor B under load, partitions eligible work, transfers required state, validates the candidate graph, and switches routing without restarting the application or losing accepted work.

## Existing systems we study

- [Apache Flink](https://flink.apache.org/) — consistent checkpoints, savepoints, stateful stream processing, and keyed-state rescaling.
- [Ray](https://www.ray.io/) — stateful actors, task execution, resource-aware scheduling, and retry semantics.
- [Akka](https://akka.io/) — actor supervision and explicit failure decisions.
- [Erlang/OTP](https://www.erlang.org/) — process lifecycle, supervision trees, and restart strategies.
- [Apache Spark](https://spark.apache.org/) — resilient distributed computation and execution graphs.
- [Dask](https://www.dask.org/) — task graphs and parallel scheduling.
- [Temporal](https://temporal.io/) — durable execution and recovery.

See [docs/comparisons.md](docs/comparisons.md) for the technical comparison. Gravity will reuse proven techniques where appropriate and will not claim novelty for capabilities already demonstrated by these projects.

## Engineering principles

1. **Continuity:** preserve accepted work and required state across supported operations.
2. **Controlled evolution:** transformations must pass validation and respect explicit safety constraints.
3. **Measured improvement:** a new topology must preserve correctness and demonstrate an objective benefit.
4. **Explicit limits:** workloads that cannot be partitioned safely must not be split automatically.
5. **Honest guarantees:** in-memory rollback is not a substitute for durable distributed transactions.

## Build and test

Install a stable Rust toolchain, then run:

    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace --all-targets
    cargo run -p gravity-cli -- demo

## Project structure

    crates/
      gravity-core/    Cell runtime, graph, queue, snapshots, and recovery
      gravity-cli/     Command-line demonstration
    docs/
      cell-contract.md
      comparisons.md
      phase-1.md
      phase-2.md

## License

MIT. See [LICENSE](LICENSE).
