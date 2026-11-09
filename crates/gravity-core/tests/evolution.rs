use gravity_core::{
    AdaptationMetrics, AdaptationPolicy, Cell, CellError, EvolutionError, Message, Runtime,
    RuntimeState,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

struct Pass;
impl Cell for Pass {
    fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
        Ok(vec![message])
    }
}

struct Counter(u64);
impl Cell for Counter {
    fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
        self.0 += 1;
        Ok(vec![message])
    }

    fn snapshot(&self) -> Result<Vec<u8>, CellError> {
        Ok(self.0.to_be_bytes().to_vec())
    }

    fn restore(&mut self, state: &[u8]) -> Result<(), CellError> {
        let bytes: [u8; 8] = state
            .try_into()
            .map_err(|_| CellError::new("expected an eight-byte counter state"))?;
        self.0 = u64::from_be_bytes(bytes);
        Ok(())
    }
}

struct RejectRestore;
impl Cell for RejectRestore {
    fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
        Ok(vec![message])
    }

    fn restore(&mut self, _state: &[u8]) -> Result<(), CellError> {
        Err(CellError::new("injected state restore failure"))
    }
}

struct RecordingOutput(Arc<Mutex<Vec<Message>>>);
impl Cell for RecordingOutput {
    fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
        self.0.lock().expect("output lock poisoned").push(message);
        Ok(Vec::new())
    }
}

#[test]
fn live_evolution_migrates_state_partitions_work_and_keeps_runtime_running() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new();
    runtime.add_cell("input", Pass).unwrap();
    runtime.add_cell("processor-a", Counter(4)).unwrap();
    runtime
        .add_cell("output", RecordingOutput(Arc::clone(&output)))
        .unwrap();
    runtime.connect("input", "processor-a").unwrap();
    runtime.connect("processor-a", "output").unwrap();
    runtime.start().unwrap();
    runtime
        .inject("processor-a", Message::new(1, b"accepted".to_vec()))
        .unwrap();

    let mut tx = runtime.topology_transaction();
    tx.add_cell("processor-b", Counter(0)).unwrap();
    tx.disconnect("input", "processor-a").unwrap();
    tx.connect("input", "processor-b").unwrap();
    tx.connect("processor-b", "output").unwrap();
    tx.partition_pending("processor-a", |_message: &Message| {
        Ok(vec![
            ("processor-a".to_owned(), Message::new(1, b"left".to_vec())),
            ("processor-b".to_owned(), Message::new(2, b"right".to_vec())),
        ])
    });
    tx.transfer_state("processor-a", "processor-b", |state| Ok(state.to_vec()));

    let report = runtime.evolve(tx, |candidate| {
        if !candidate
            .edges
            .get("input")
            .is_some_and(|targets| targets.contains("processor-b"))
        {
            return Err("replacement processor is not connected".into());
        }
        Ok(())
    }).unwrap();

    assert_eq!(runtime.state(), RuntimeState::Running);
    assert_eq!(report.cells_after, 4);
    assert_eq!(report.pending_before, 1);
    assert_eq!(report.pending_after, 2);
    assert_eq!(report.partitioned_messages, 1);
    assert_eq!(report.migrated_destinations, 1);
    assert_eq!(
        runtime.snapshot().unwrap().cell_state("processor-b"),
        Some(4_u64.to_be_bytes().as_slice())
    );

    let run = runtime.run_until_idle(16).unwrap();
    assert_eq!(run.pending_messages, 0);
    assert_eq!(output.lock().unwrap().len(), 2);
}

#[test]
fn removing_a_cell_routes_accepted_work_to_its_replacement() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new();
    runtime.add_cell("input", Pass).unwrap();
    runtime.add_cell("processor-a", Pass).unwrap();
    runtime
        .add_cell("output", RecordingOutput(Arc::clone(&output)))
        .unwrap();
    runtime.connect("input", "processor-a").unwrap();
    runtime.connect("processor-a", "output").unwrap();
    runtime.start().unwrap();
    runtime
        .inject("processor-a", Message::new(7, b"preserve-me".to_vec()))
        .unwrap();

    let mut tx = runtime.topology_transaction();
    tx.remove_cell("processor-a").unwrap();
    tx.add_cell("processor-b", Pass).unwrap();
    tx.connect("input", "processor-b").unwrap();
    tx.connect("processor-b", "output").unwrap();
    tx.route_pending("processor-a", "processor-b");

    let report = runtime.evolve(tx, |_| Ok(())).unwrap();
    assert_eq!(report.cells_before, 3);
    assert_eq!(report.cells_after, 3);
    assert_eq!(runtime.pending_messages(), 1);
    runtime.run_until_idle(8).unwrap();

    let received = output.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].id, 7);
    assert_eq!(received[0].payload, b"preserve-me");
}

#[test]
fn failed_state_migration_leaves_live_topology_and_queue_untouched() {
    let mut runtime = Runtime::new();
    runtime.add_cell("source", Counter(9)).unwrap();
    runtime.start().unwrap();
    runtime
        .inject("source", Message::new(3, b"still-queued".to_vec()))
        .unwrap();

    let mut tx = runtime.topology_transaction();
    tx.add_cell("destination", RejectRestore).unwrap();
    tx.transfer_state("source", "destination", |state| Ok(state.to_vec()));

    assert!(matches!(
        runtime.evolve(tx, |_| Ok(())),
        Err(EvolutionError::MigrationFailed { .. })
    ));
    assert_eq!(runtime.state(), RuntimeState::Running);
    assert_eq!(runtime.cell_count(), 1);
    assert!(!runtime.contains_cell("destination"));
    assert_eq!(runtime.pending_messages(), 1);
    assert_eq!(
        runtime.snapshot().unwrap().cell_state("source"),
        Some(9_u64.to_be_bytes().as_slice())
    );
}

#[test]
fn adaptation_rejects_impossible_or_inconsistent_metrics() {
    let policy = AdaptationPolicy::default();
    let invalid = AdaptationMetrics {
        window_micros: 1_000_000,
        processed_messages: 100,
        queue_high_water: 101,
        queue_capacity: 100,
        per_cell_processed: BTreeMap::from([("processor".to_owned(), 80)]),
    };
    assert!(policy.evaluate(&invalid).is_none());

    let inconsistent = AdaptationMetrics {
        window_micros: 1_000_000,
        processed_messages: 100,
        queue_high_water: 90,
        queue_capacity: 100,
        per_cell_processed: BTreeMap::from([
            ("processor-a".to_owned(), 80),
            ("processor-b".to_owned(), 30),
        ]),
    };
    assert!(policy.evaluate(&inconsistent).is_none());
}

struct FailStop(Arc<Mutex<usize>>);
impl Cell for FailStop {
    fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
        Ok(vec![message])
    }

    fn on_start(&mut self) -> Result<(), CellError> {
        *self.0.lock().expect("start counter lock poisoned") += 1;
        Ok(())
    }

    fn on_stop(&mut self) -> Result<(), CellError> {
        Err(CellError::new("injected stop failure"))
    }
}

struct FailStart(Arc<Mutex<usize>>);
impl Cell for FailStart {
    fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
        Ok(vec![message])
    }

    fn on_start(&mut self) -> Result<(), CellError> {
        Err(CellError::new("injected start failure"))
    }

    fn on_stop(&mut self) -> Result<(), CellError> {
        *self.0.lock().expect("cleanup counter lock poisoned") += 1;
        Ok(())
    }
}

struct NoSnapshot;
impl Cell for NoSnapshot {
    fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
        Ok(vec![message])
    }

    fn snapshot(&self) -> Result<Vec<u8>, CellError> {
        Err(CellError::new("injected snapshot failure"))
    }
}

#[test]
fn failed_removal_restarts_the_cell_and_preserves_the_old_topology() {
    let starts = Arc::new(Mutex::new(0));
    let mut runtime = Runtime::new();
    runtime
        .add_cell("processor", FailStop(Arc::clone(&starts)))
        .unwrap();
    runtime.start().unwrap();

    let mut tx = runtime.topology_transaction();
    tx.remove_cell("processor").unwrap();

    assert!(matches!(
        runtime.evolve(tx, |_| Ok(())),
        Err(EvolutionError::LifecycleFailed { .. })
    ));
    assert_eq!(*starts.lock().unwrap(), 2);
    assert_eq!(runtime.state(), RuntimeState::Running);
    assert!(runtime.contains_cell("processor"));
    assert_eq!(runtime.cell_count(), 1);
}

#[test]
fn failed_new_cell_start_runs_cleanup_without_committing_topology() {
    let cleanups = Arc::new(Mutex::new(0));
    let mut runtime = Runtime::new();
    runtime.add_cell("existing", Pass).unwrap();
    runtime.start().unwrap();

    let mut tx = runtime.topology_transaction();
    tx.add_cell("unstable", FailStart(Arc::clone(&cleanups)))
        .unwrap();

    assert!(matches!(
        runtime.evolve(tx, |_| Ok(())),
        Err(EvolutionError::LifecycleFailed { .. })
    ));
    assert_eq!(*cleanups.lock().unwrap(), 1);
    assert_eq!(runtime.state(), RuntimeState::Running);
    assert_eq!(runtime.cell_count(), 1);
    assert!(!runtime.contains_cell("unstable"));
}

#[test]
fn snapshot_failure_aborts_evolution_without_losing_queued_work() {
    let mut runtime = Runtime::new();
    runtime.add_cell("unsnapshotable", NoSnapshot).unwrap();
    runtime.start().unwrap();
    runtime
        .inject("unsnapshotable", Message::new(11, b"queued".to_vec()))
        .unwrap();

    let tx = runtime.topology_transaction();
    assert!(matches!(
        runtime.evolve(tx, |_| Ok(())),
        Err(EvolutionError::Runtime(_))
    ));
    assert_eq!(runtime.state(), RuntimeState::Running);
    assert_eq!(runtime.pending_messages(), 1);
}

#[test]
fn partition_failure_aborts_evolution_without_losing_queued_work() {
    let mut runtime = Runtime::new();
    runtime.add_cell("processor", Pass).unwrap();
    runtime.start().unwrap();
    runtime
        .inject("processor", Message::new(12, b"queued".to_vec()))
        .unwrap();

    let mut tx = runtime.topology_transaction();
    tx.partition_pending("processor", |_message: &Message| {
        Err(CellError::new("injected partition failure"))
    });

    assert!(matches!(
        runtime.evolve(tx, |_| Ok(())),
        Err(EvolutionError::PartitionFailed { .. })
    ));
    assert_eq!(runtime.state(), RuntimeState::Running);
    assert_eq!(runtime.pending_messages(), 1);
}
