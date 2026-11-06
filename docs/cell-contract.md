# Cell contract

A Gravity cell is a runtime-owned computation unit. The runtime controls when it starts, which messages it receives, how its outputs are routed, and when it stops.

## Interface

A cell implements:

- `process(message)`: consume one message and return zero or more output messages.
- `on_start()`: acquire resources and initialize runtime-local state.
- `on_stop()`: release resources.
- `snapshot()`: encode all mutable state required to resume processing.
- `restore(state)`: restore a previously captured state.

Stateless cells may use the default empty snapshot and restore implementation.

## Lifecycle

Cells start in ascending cell-ID order. If startup fails, the runtime calls `on_stop` on every attempted cell in reverse order, including the cell whose startup failed, so partially acquired resources can be cleaned up.

Cells stop in reverse cell-ID order. Shutdown attempts every cell even if one stop hook fails. A failed cleanup moves the runtime into a failed lifecycle state; it cannot silently resume.

Topology configuration is mutable only before startup or after a clean stop. The running graph is frozen in Phase 1.

## Processing and rollback

Before processing each message, Gravity snapshots the target cell. If processing fails, Gravity restores that snapshot and retains the original message at the front of the queue.

Before routing outputs, Gravity checks both the per-step delivery limit and the pending-queue limit. If either limit would be exceeded, it restores the cell's pre-message state and retains the original message.

This is a local in-memory transaction boundary. It does not undo external side effects. Cell implementations must therefore avoid irreversible I/O in `process`, or make such operations idempotent and coordinate them outside this guarantee.

## Snapshot contract

A cell snapshot must:

- Include every mutable value that affects future outputs.
- Be deterministic for an unchanged state.
- Be accepted by the same compatible cell implementation.
- Reject malformed or incompatible state without leaving the cell partially modified where possible.

The runtime checkpoint additionally stores the exact topology and pending message queue. Restoration rejects a different cell set or graph. It captures current cell states first and attempts rollback if any target restore fails.

Phase 1 snapshots are in-memory objects. They are not serialized, persisted, or portable between program versions. State schema versioning and migration are Phase 2 work.

## Message and delivery semantics

The runtime processes one queued message at a time. Outputs are copied to each outgoing edge in deterministic cell-ID order. A cell with no outgoing edges consumes its message and its returned outputs are not routed.

The runtime retains a failed message for explicit caller-controlled retry. It does not retry indefinitely, deduplicate message IDs, or promise exactly-once effects outside cell-local snapshot state.

## Resource bounds

The runtime limits pending queued messages and the number of routed deliveries produced by a single cell invocation. The execution step limit bounds work performed by one `run_until_idle` call. These controls prevent unbounded graph cycles and fan-out from running without a caller-visible limit.
