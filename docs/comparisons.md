# Architecture comparison register

Gravity studies established systems as engineering references. The purpose is to understand which problems they already solve, reuse proven techniques, and define the remaining research gap without relabeling existing features as new inventions.

## Apache Flink

**Established capabilities**

Flink checkpoints capture operator state together with stream positions for consistent recovery. Its keyed state is partitioned into key groups, which form the atomic unit for redistributing keyed state when a job is rescaled.

**What Gravity learns**

- State and the position of work must be captured consistently.
- Redistribution requires an explicit partitioning model.
- Rescaling must preserve the relationship between input partitions and state.

**Important difference today**

Gravity Phase 1 snapshots are local in-memory copies of cell state, topology, and its pending queue. They are not durable distributed checkpoints, do not coordinate multiple workers, and do not implement Flink's keyed-state redistribution.

References:
- https://flink.apache.org/
- https://github.com/apache/flink/blob/master/docs/content/docs/ops/state/checkpoints.md
- https://github.com/apache/flink/blob/master/docs/content/docs/concepts/stateful-stream-processing.md
- https://github.com/apache/flink/blob/master/docs/content/docs/ops/state/savepoints.md

## Ray

**Established capabilities**

Ray provides stateful actors, resource-aware scheduling, actor restart controls, and configurable task retry semantics. Its documentation explicitly warns that retried actor methods may execute more than once and that critical application state needs application-managed checkpointing.

**What Gravity learns**

- A stateful execution unit needs a clear lifecycle and recovery contract.
- Retry semantics must be explicit; a retry can duplicate work.
- Scheduling decisions must account for declared resource requirements.

**Important difference today**

Gravity cells are in-process Rust objects executed synchronously. Gravity has no distributed scheduler, remote actors, resource placement, or network transport.

References:
- https://docs.ray.io/en/latest/ray-core/actors.html
- https://docs.ray.io/en/latest/ray-core/fault_tolerance/actors.html
- https://docs.ray.io/en/latest/ray-core/scheduling/index.html

## Akka

**Established capabilities**

Akka supervision gives a parent explicit failure decisions such as resume, restart, stop, or escalation. Its documentation also calls out message-delivery implications: a message that fails during actor processing is not automatically put back into the mailbox.

**What Gravity learns**

- Failure policy belongs in an explicit runtime contract.
- Retaining or retrying a failed message must be a deliberate semantic choice.
- Restarting stateful components requires a defined state-recovery strategy.

**Important difference today**

Gravity currently implements a single local rollback policy for cell-local snapshot state and retains the failed message. It does not implement actor hierarchies or configurable supervision strategies.

References:
- https://doc.akka.io/libraries/akka-core/current/typed/fault-tolerance.html
- https://doc.akka.io/libraries/akka-core/2.9/general/supervision.html

## Erlang/OTP

**Established capabilities**

OTP supervisors start, stop, and monitor child processes. They support restart strategies such as one-for-one, one-for-all, and rest-for-one, with restart-intensity limits.

**What Gravity learns**

- Startup and shutdown order are part of system correctness.
- Recovery needs bounded restart policies to avoid endless failure loops.
- Failure containment should follow declared dependency relationships.

**Important difference today**

Gravity Phase 1 provides deterministic cell startup/shutdown hooks and cleanup on startup failure. It does not yet provide a supervision tree, automatic restarts, restart intensity windows, or distributed process monitoring.

References:
- https://www.erlang.org/doc/system/sup_princ.html
- https://www.erlang.org/docs/27/system/design_principles.html

## Apache Spark

**Established capabilities**

Spark provides distributed data processing and resilient execution abstractions, including DAG-based scheduling and recovery of computation.

**What Gravity learns**

Study how execution graphs, partitioning, and recomputation interact. Gravity's first release is intentionally not a distributed analytics engine.

Reference: https://spark.apache.org/

## Dask

**Established capabilities**

Dask exposes task graphs and parallel/distributed scheduling for Python workloads.

**What Gravity learns**

Keep graph dependencies explicit and inspectable. Study graph execution and scheduling behavior without assuming a graph alone provides safe state migration.

Reference: https://www.dask.org/

## Temporal

**Established capabilities**

Temporal provides durable workflow execution with persisted history and recovery across worker or process restarts.

**What Gravity learns**

Distinguish transient in-memory execution from durable accepted work. Durable progress needs an explicit persistence and replay model.

Reference: https://temporal.io/

## Gravity's open research target

Gravity's intended contribution is not simply adding or restarting workers. Its Phase 2 research target is a runtime-owned mutable computational topology with:

- Explicit state-transfer contracts between old and new cell arrangements.
- Validated migration of in-flight work and cell state.
- Candidate topology validation before traffic is redirected.
- A controlled routing cutover with rollback.
- Measured evidence that the evolved topology improves a defined objective without breaking correctness.

These remain research goals. The Phase 1 implementation is a local correctness foundation, not evidence that the Phase 2 problem has already been solved.
