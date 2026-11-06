# Phase 1 — Gravity Core

## Objective

Build a correct, inspectable, single-machine runtime that executes independent computational cells through an explicit directed graph.

## Delivered foundation

- Rust workspace split into gravity-core and gravity-cli.
- Stable cell identities and deterministic graph inspection.
- Explicit cell startup and shutdown hooks.
- Startup rollback when a cell fails to initialize.
- Reverse-order shutdown with failed-state reporting.
- Directed message routing with deterministic fan-out.
- Bounded pending-message queues and per-step delivery limits.
- Per-message state snapshots and rollback on processing or routing failure.
- Runtime checkpoints containing cell state, graph topology, and pending messages.
- Transactional checkpoint restoration with rollback if a cell rejects restored state.
- Typed runtime errors and execution reports.
- CLI demonstration and GitHub Actions validation.

## Runtime guarantees

1. The runtime processes one message at a time in a deterministic order.
2. A cell's state is snapshotted before processing.
3. If processing fails, the runtime restores the cell state and puts the original message back at the front of the queue.
4. If routing would exceed configured limits, the runtime restores the cell state and retains the original message.
5. A runtime snapshot captures all cell snapshots, the exact graph topology, and the pending queue.
6. A snapshot cannot be restored against a different graph or cell set.
7. Checkpoint restoration captures pre-restore state and rolls back attempted cells if a restore operation fails.
8. Topology configuration is frozen while the runtime is running. Live topology mutation is reserved for Phase 2.

The rollback guarantee covers state represented by a cell's snapshot. Cells must not perform irreversible external side effects during process; the runtime cannot undo an HTTP request, external database write, or action in another system.

## Deliberate Phase 1 limits

- Single process and single machine.
- Single-threaded execution.
- In-memory snapshots only; no durable checkpoint storage.
- Opaque cell-state bytes; no cross-version schema migration.
- No automatic retries, restart supervision policy, or dead-letter subsystem.
- No live topology changes while running.
- No distributed scheduling or network transport.

These are explicit boundaries, not hidden claims of capability. Phase 2 will build on the local correctness model.

## Verification

The automated suite covers graph validation, routing, bounded execution, lifecycle ordering, startup cleanup, message retention, processing rollback, snapshot restoration, topology mismatch, restore rollback, queue limits, delivery limits, and state inspection.

The GitHub Actions workflow checks formatting, Clippy with warnings denied, workspace tests, documentation generation, and the CLI demonstration.

## Exit criteria

- [x] Cells have explicit identities and runtime interfaces.
- [x] Cell startup and shutdown lifecycle is explicit and tested.
- [x] Graph connections reject unknown cells and duplicate cell identities.
- [x] Messages route through connected cells.
- [x] Execution is bounded and reports pending work when the limit is reached.
- [x] Cell failures are attributed to the responsible cell.
- [x] State and snapshot semantics are specified and tested.
- [x] Recovery and rollback behavior is tested.
- [x] CLI demonstration is included in CI.
- [x] Workspace formatting, lint, tests, and documentation are included in CI.
- [x] Comparison notes document relevant established approaches.

## Verified completion

**Phase 1 is complete.** GitHub Actions run [#3](https://github.com/sectorx83/Gravity/actions/runs/42233827812) passed on commit `7c1903e05533ee84f4dc6150d63a675bd1049960`.

- Rust formatting: passed
- Clippy with warnings denied: passed
- Unit tests: 20 passed, 0 failed
- Documentation generation: passed
- CLI demonstration: passed; 3 messages processed, 2 deliveries routed, 0 pending

This completes the local runtime foundation. It does not claim Phase 2 live topology evolution, durable distributed checkpoints, or distributed execution.
