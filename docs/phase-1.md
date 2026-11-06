# Phase 1 — Gravity Core

## Objective

Build a correct, inspectable, single-machine runtime that executes independent cells through an explicit directed graph.

## Work sequence

1. Workspace and crate boundaries
2. Cell interface and lifecycle
3. Message representation and routing
4. Directed graph validation
5. Bounded execution and scheduler semantics
6. Cell state and snapshot contract
7. Failure handling and recovery foundations
8. CLI demonstrations and integration tests
9. Comparative review against Flink, Ray, Akka, Erlang/OTP, Spark, Dask, and Temporal

## Initial implementation

- gravity-core owns cells, messages, graph connections, and bounded execution.
- gravity-cli provides a runnable local demonstration.
- The runtime is single-threaded at this stage. Parallel scheduling is not claimed until concurrency semantics and tests exist.

## Exit criteria

- [ ] Cells have explicit identities and runtime interfaces.
- [ ] Graph connections reject unknown cells and duplicate cell identities.
- [ ] Messages route through connected cells.
- [ ] Execution is bounded and reports pending work when the limit is reached.
- [ ] Cell failures are attributed to the responsible cell.
- [ ] State and snapshot semantics are specified and tested.
- [ ] Recovery behavior is tested.
- [ ] CLI demonstration runs successfully.
- [ ] Workspace tests and formatting pass in CI.
- [ ] Comparison notes document relevant established approaches.

Phase 1 is not complete merely because the project compiles. State semantics, failure behavior, and repeatable tests are required.
