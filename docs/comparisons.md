# Architecture comparison register

This document is maintained throughout Gravity development. It distinguishes established capabilities from Gravity's research goals. A capability is not novel merely because Gravity uses different terminology.

## Apache Flink

Established techniques: operator/dataflow graphs, checkpoints and savepoints, consistent state snapshots, rescaling operators, and keyed-state redistribution.

Gravity implication: Flink is a primary reference for consistent state and rescaling. Study its snapshot and redistribution protocols. Gravity's target is a more general runtime-level topology mutation model, not a replacement for Flink's stream-processing semantics.

Reference: https://flink.apache.org/

## Ray

Established techniques: tasks, stateful actors, distributed scheduling, resource-aware placement, actor lifecycle, and recovery.

Gravity implication: Ray is a reference for independent execution units and scheduling. Gravity's cells need explicit lifecycle and state-transfer contracts.

Reference: https://www.ray.io/

## Akka

Established techniques: actor model, supervision, clustering, sharding, and stateful distributed components.

Gravity implication: Akka informs cell isolation, supervision, and failure handling. Actor restart must not be confused with safe migration of in-flight work.

Reference: https://akka.io/

## Erlang/OTP

Established techniques: lightweight processes, supervision trees, fault isolation, and controlled code upgrades.

Gravity implication: OTP is a key reference for fault containment and recovery. Gravity should adopt explicit supervision policies before automatic adaptation.

Reference: https://www.erlang.org/

## Apache Spark

Established techniques: distributed data processing, parallel execution, resilient computation, and DAG-based scheduling.

Gravity implication: Spark informs execution planning and resilience, but Gravity's first milestone is a local mutable runtime, not a distributed analytics engine.

Reference: https://spark.apache.org/

## Dask

Established techniques: task graphs, dependency scheduling, and parallel/distributed execution.

Gravity implication: Dask is useful for graph scheduling and dependency semantics. Gravity should keep graph structure explicit and inspectable.

Reference: https://www.dask.org/

## Temporal

Established techniques: durable workflow history, replay, and recovery after process failures.

Gravity implication: Temporal informs durable progress and recovery. Gravity's state model must distinguish durable accepted work from transient execution state.

Reference: https://temporal.io/

## Current distinction and open research

Gravity's intended contribution is the combination of runtime-owned mutable topology, explicit cell state and migration contracts, validated topology cutover, rollback when a structural transformation fails, and measurable correctness/performance criteria.

These are research goals, not claims that no existing system has any of these features. Phase 2 must include direct comparative experiments before novelty claims are made.
