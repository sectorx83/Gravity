# Gravity

Gravity is an adaptive computational runtime designed to change the topology of a running computational system while preserving required state and correctness.

Its central research problem is safe live topology transformation: adding, removing, splitting, replacing, and reconnecting computational cells without restarting the entire application.

Gravity is not an AI-agent framework, workflow dashboard, or general-purpose orchestrator. Its core abstraction is a mutable computational graph whose cells have explicit interfaces, state, and lifecycle rules.

## Phase 1 — Gravity Core

Build a reliable local runtime for independent computational cells: cell lifecycle, typed messages, a directed graph, bounded execution, state foundations, and correctness tests.

## Phase 2 — Gravity Evolution Engine

Enable safe structural changes while execution continues: topology mutations, work partitioning, Morphological State Transfer (MST), validated routing cutover, rollback, regeneration, and measured adaptation.

Phase 2 is not complete until a running workload can expand from one processing cell to two, transfer work and required state safely, change routing, and demonstrate correctness without restarting the application.

## Existing systems we will study

- [Apache Flink](https://flink.apache.org/) — dataflow execution, checkpoints, savepoints, and state redistribution.
- [Ray](https://www.ray.io/) — distributed tasks, actors, and resource-aware execution.
- [Akka](https://akka.io/) — actor lifecycle, supervision, and distributed stateful components.
- [Erlang/OTP](https://www.erlang.org/) — lightweight processes, supervision trees, and fault recovery.
- [Apache Spark](https://spark.apache.org/) — parallel execution and resilient distributed processing.
- [Dask](https://www.dask.org/) — task graphs and parallel scheduling.
- [Temporal](https://temporal.io/) — durable execution and recovery.

See docs/comparisons.md. We will reuse proven techniques where appropriate and will not claim novelty for capabilities already demonstrated by these projects.

## Engineering principles

1. Continuity: preserve accepted work and required state across supported transformations.
2. Controlled evolution: transformations must pass validation and respect explicit safety constraints.
3. Measured improvement: a new topology must preserve correctness and demonstrate an objective benefit.
4. Explicit limits: workloads that cannot be partitioned safely must not be split automatically.

## Current status

The repository is at the beginning of Phase 1. The initial implementation targets a single machine and a small, inspectable runtime. Distributed execution and automatic adaptation are out of scope until the local execution and state model are reliable.

## Build

Install a stable Rust toolchain, then run:

    cargo test --workspace
    cargo run -p gravity-cli -- demo

## License

MIT. See LICENSE.
