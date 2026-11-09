# Phase 2 — Gravity Evolution Engine

## Objective

Change a running computational graph while preserving accepted work, required state, and observable correctness.

## Work sequence

1. Runtime topology mutation transactions
2. Work partitioning contracts
3. Consistent cell snapshots
4. Morphological State Transfer (MST)
5. Shadow validation of candidate topology
6. Atomic routing cutover
7. Rollback to the previous topology
8. Cell regeneration
9. Measured adaptation policies

## Required demonstration

Start with Input -> Processor A -> Output.

Under a controlled workload:
1. Detect that Processor A is the bottleneck.
2. Create Processor B without restarting the application.
3. Partition eligible work between A and B.
4. Transfer required state using explicit migration logic.
5. Validate the candidate graph and outputs.
6. Switch routing to the new graph.
7. Continue processing accepted work.
8. Roll back if validation fails.

## Safety requirements

- Never split a workload without a declared partitioning contract.
- Never discard accepted work during a transition.
- Define duplicate-delivery and retry semantics explicitly.
- Validate state compatibility before cutover.
- Keep the old topology recoverable until the new topology is committed.
- Measure correctness and performance; throughput alone is insufficient.

## Automated verification

Run the complete workspace checks:

    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace --all-targets
    cargo run -p gravity-cli -- evolve-demo

The integration suite exercises live add/reconnect, state transfer, work partitioning, cell removal and pending-work rerouting. It also injects snapshot, partition, migration, validation, and lifecycle failures and verifies that the previous topology and accepted queue remain intact. The CLI demonstration performs a live Input -> Processor A -> Output evolution, adds Processor B, partitions queued work, transfers state, validates the candidate, and continues processing without restarting.

## Exit criteria

Phase 2 is complete when the automated workspace checks and the live evolution demonstration pass on main. The runtime remains deliberately single-threaded and in-memory: this phase does not claim distributed consensus, durable cross-process transactions, or automatic partitioning of workloads without an explicit partitioner.
