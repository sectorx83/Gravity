use super::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub struct CandidateTopology<'a> {
    pub cells: &'a BTreeSet<CellId>,
    pub edges: &'a BTreeMap<CellId, BTreeSet<CellId>>,
    pub cell_states: &'a BTreeMap<CellId, Vec<u8>>,
    pub pending: &'a VecDeque<(CellId, Message)>,
}

pub trait WorkPartitioner: Send + Sync {
    fn partition(&self, message: &Message) -> Result<Vec<(CellId, Message)>, CellError>;
}
impl<F> WorkPartitioner for F
where F: Fn(&Message) -> Result<Vec<(CellId, Message)>, CellError> + Send + Sync {
    fn partition(&self, message: &Message) -> Result<Vec<(CellId, Message)>, CellError> { self(message) }
}

type StateTransform = Box<dyn Fn(&[u8]) -> Result<Vec<u8>, CellError> + Send + Sync>;
struct StateTransfer { source: CellId, destination: CellId, transform: StateTransform }

pub struct TopologyTransaction {
    cells: BTreeSet<CellId>,
    edges: BTreeMap<CellId, BTreeSet<CellId>>,
    additions: BTreeMap<CellId, Box<dyn Cell>>,
    removals: BTreeSet<CellId>,
    routes: BTreeMap<CellId, CellId>,
    partitioners: BTreeMap<CellId, Box<dyn WorkPartitioner>>,
    transfers: Vec<StateTransfer>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum EvolutionError {
    Runtime(RuntimeError),
    InvalidCandidate(String),
    MigrationFailed { source: CellId, destination: CellId, message: String },
    PartitionFailed { cell: CellId, message: String },
    ValidationFailed(String),
    LifecycleFailed { cell: CellId, message: String },
    RollbackFailed { cell: CellId, message: String },
}
impl std::fmt::Display for EvolutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(e) => write!(f, "runtime error: {e}"),
            Self::InvalidCandidate(s) => write!(f, "invalid candidate: {s}"),
            Self::MigrationFailed { source, destination, message } => write!(f, "migration {source} -> {destination} failed: {message}"),
            Self::PartitionFailed { cell, message } => write!(f, "partitioning for {cell} failed: {message}"),
            Self::ValidationFailed(s) => write!(f, "shadow validation failed: {s}"),
            Self::LifecycleFailed { cell, message } => write!(f, "lifecycle operation for {cell} failed: {message}"),
            Self::RollbackFailed { cell, message } => write!(f, "rollback for {cell} failed: {message}"),
        }
    }
}
impl std::error::Error for EvolutionError {}
impl From<RuntimeError> for EvolutionError { fn from(e: RuntimeError) -> Self { Self::Runtime(e) } }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvolutionReport {
    pub cells_before: usize,
    pub cells_after: usize,
    pub edges_before: usize,
    pub edges_after: usize,
    pub pending_before: usize,
    pub pending_after: usize,
    pub migrated_destinations: usize,
    pub partitioned_messages: usize,
}

impl TopologyTransaction {
    fn new(runtime: &Runtime) -> Self {
        Self {
            cells: runtime.cells.keys().cloned().collect(),
            edges: runtime.edges.clone(),
            additions: BTreeMap::new(),
            removals: BTreeSet::new(),
            routes: BTreeMap::new(),
            partitioners: BTreeMap::new(),
            transfers: Vec::new(),
        }
    }
    pub fn add_cell(&mut self, id: impl Into<CellId>, cell: impl Cell + 'static) -> Result<(), EvolutionError> {
        let id = id.into();
        if id.trim().is_empty() || !self.cells.insert(id.clone()) {
            return Err(EvolutionError::InvalidCandidate(format!("empty or duplicate cell ID: {id}")));
        }
        self.edges.insert(id.clone(), BTreeSet::new());
        self.additions.insert(id, Box::new(cell));
        Ok(())
    }
    pub fn remove_cell(&mut self, id: &str) -> Result<(), EvolutionError> {
        if !self.cells.remove(id) { return Err(EvolutionError::InvalidCandidate(format!("unknown cell: {id}"))); }
        self.removals.insert(id.to_owned());
        self.additions.remove(id);
        self.edges.remove(id);
        for targets in self.edges.values_mut() { targets.remove(id); }
        Ok(())
    }
    pub fn connect(&mut self, from: &str, to: &str) -> Result<(), EvolutionError> {
        if !self.cells.contains(from) || !self.cells.contains(to) {
            return Err(EvolutionError::InvalidCandidate(format!("unknown edge endpoint: {from} -> {to}")));
        }
        self.edges.get_mut(from).expect("candidate adjacency exists").insert(to.to_owned());
        Ok(())
    }
    pub fn disconnect(&mut self, from: &str, to: &str) -> Result<bool, EvolutionError> {
        if !self.cells.contains(from) || !self.cells.contains(to) {
            return Err(EvolutionError::InvalidCandidate(format!("unknown edge endpoint: {from} -> {to}")));
        }
        Ok(self.edges.get_mut(from).expect("candidate adjacency exists").remove(to))
    }
    pub fn route_pending(&mut self, from: impl Into<CellId>, to: impl Into<CellId>) {
        self.routes.insert(from.into(), to.into());
    }
    pub fn partition_pending(&mut self, cell: impl Into<CellId>, partitioner: impl WorkPartitioner + 'static) {
        self.partitioners.insert(cell.into(), Box::new(partitioner));
    }
    pub fn transfer_state(
        &mut self, source: impl Into<CellId>, destination: impl Into<CellId>,
        transform: impl Fn(&[u8]) -> Result<Vec<u8>, CellError> + Send + Sync + 'static,
    ) {
        self.transfers.push(StateTransfer { source: source.into(), destination: destination.into(), transform: Box::new(transform) });
    }
}

impl Runtime {
    pub fn topology_transaction(&self) -> TopologyTransaction { TopologyTransaction::new(self) }

    pub fn evolve(
        &mut self,
        mut tx: TopologyTransaction,
        validator: impl FnOnce(&CandidateTopology<'_>) -> Result<(), String>,
    ) -> Result<EvolutionReport, EvolutionError> {
        self.ensure_running()?;
        let old_states = self.capture_cell_states()?;
        let old_queue = self.queue.clone();
        let old_edges = self.edges.clone();
        let cells_before = self.cells.len();
        let edges_before = old_edges.values().map(BTreeSet::len).sum();
        let pending_before = old_queue.len();

        if tx.edges.len() != tx.cells.len() || tx.cells.iter().any(|id| !tx.edges.contains_key(id))
            || tx.edges.iter().any(|(from, tos)| !tx.cells.contains(from) || tos.iter().any(|to| !tx.cells.contains(to))) {
            return Err(EvolutionError::InvalidCandidate("graph contains unknown cells or incomplete adjacency".into()));
        }

        let mut states = old_states.clone();
        for id in &tx.removals { states.remove(id); }
        for (id, cell) in &tx.additions {
            states.insert(id.clone(), cell.snapshot().map_err(|e| EvolutionError::MigrationFailed {
                source: id.clone(), destination: id.clone(), message: e.to_string()
            })?);
        }
        for transfer in &tx.transfers {
            let source = old_states.get(&transfer.source).or_else(|| states.get(&transfer.source))
                .ok_or_else(|| EvolutionError::InvalidCandidate(format!("unknown migration source: {}", transfer.source)))?;
            if !tx.cells.contains(&transfer.destination) {
                return Err(EvolutionError::InvalidCandidate(format!("unknown migration destination: {}", transfer.destination)));
            }
            let migrated = (transfer.transform)(source).map_err(|e| EvolutionError::MigrationFailed {
                source: transfer.source.clone(), destination: transfer.destination.clone(), message: e.to_string()
            })?;
            states.insert(transfer.destination.clone(), migrated);
        }

        let mut queue = VecDeque::new();
        let mut partitioned = 0;
        for (target, message) in &old_queue {
            if let Some(partitioner) = tx.partitioners.get(target) {
                let parts = partitioner.partition(message).map_err(|e| EvolutionError::PartitionFailed { cell: target.clone(), message: e.to_string() })?;
                partitioned += 1;
                for (destination, part) in parts {
                    if !tx.cells.contains(&destination) { return Err(EvolutionError::InvalidCandidate(format!("partition target does not exist: {destination}"))); }
                    queue.push_back((destination, part));
                }
            } else if tx.cells.contains(target) {
                queue.push_back((target.clone(), message.clone()));
            } else if let Some(destination) = tx.routes.get(target) {
                if !tx.cells.contains(destination) { return Err(EvolutionError::InvalidCandidate(format!("pending route target does not exist: {destination}"))); }
                queue.push_back((destination.clone(), message.clone()));
            } else {
                return Err(EvolutionError::InvalidCandidate(format!("pending work for removed cell {target} has no route")));
            }
        }
        if queue.len() > self.config.max_pending_messages {
            return Err(EvolutionError::Runtime(RuntimeError::QueueCapacityExceeded {
                limit: self.config.max_pending_messages, attempted: queue.len()
            }));
        }

        validator(&CandidateTopology { cells: &tx.cells, edges: &tx.edges, cell_states: &states, pending: &queue })
            .map_err(EvolutionError::ValidationFailed)?;

        let mut started = Vec::new();
        for id in tx.additions.keys().cloned().collect::<Vec<_>>() {
            let result = tx.additions.get_mut(&id).expect("staged cell exists").on_start();
            if let Err(error) = result {
                if let Err(rollback) = tx.additions.get_mut(&id).expect("staged cell exists").on_stop() {
                    self.state = RuntimeState::Failed;
                    return Err(EvolutionError::RollbackFailed { cell: id, message: rollback.to_string() });
                }
                for started_id in started.iter().rev() {
                    if let Err(rollback) = tx.additions.get_mut(started_id).expect("staged cell exists").on_stop() {
                        self.state = RuntimeState::Failed;
                        return Err(EvolutionError::RollbackFailed { cell: started_id.clone(), message: rollback.to_string() });
                    }
                }
                return Err(EvolutionError::LifecycleFailed { cell: id, message: error.to_string() });
            }
            started.push(id);
        }

        let mut restored_existing = Vec::new();
        let mut migrated_destinations = BTreeSet::new();
        for transfer in &tx.transfers {
            let desired = states.get(&transfer.destination).expect("migrated state exists");
            let result = if let Some(cell) = tx.additions.get_mut(&transfer.destination) {
                cell.restore(desired)
            } else if let Some(cell) = self.cells.get_mut(&transfer.destination) {
                cell.restore(desired)
            } else {
                Err(CellError::new("destination cell is not installed"))
            };
            if let Err(error) = result {
                for id in restored_existing.iter().rev() {
                    if let (Some(cell), Some(state)) = (self.cells.get_mut(id), old_states.get(id)) {
                        if let Err(rollback) = cell.restore(state) {
                            self.state = RuntimeState::Failed;
                            return Err(EvolutionError::RollbackFailed { cell: id.clone(), message: rollback.to_string() });
                        }
                    }
                }
                for id in started.iter().rev() {
                    if let Err(rollback) = tx.additions.get_mut(id).expect("staged cell exists").on_stop() {
                        self.state = RuntimeState::Failed;
                        return Err(EvolutionError::RollbackFailed { cell: id.clone(), message: rollback.to_string() });
                    }
                }
                return Err(EvolutionError::MigrationFailed {
                    source: transfer.source.clone(), destination: transfer.destination.clone(), message: error.to_string()
                });
            }
            migrated_destinations.insert(transfer.destination.clone());
            if self.cells.contains_key(&transfer.destination) {
                restored_existing.push(transfer.destination.clone());
            }
        }

        let mut stopped = Vec::new();
        for id in &tx.removals {
            if let Some(cell) = self.cells.get_mut(id) {
                if let Err(error) = cell.on_stop() {
                    if let Err(rollback) = cell.on_start() {
                        self.state = RuntimeState::Failed;
                        return Err(EvolutionError::RollbackFailed { cell: id.clone(), message: rollback.to_string() });
                    }
                    for stopped_id in stopped.iter().rev() {
                        if let Err(rollback) = self.cells.get_mut(stopped_id).expect("removed cell still present").on_start() {
                            self.state = RuntimeState::Failed;
                            return Err(EvolutionError::RollbackFailed { cell: stopped_id.clone(), message: rollback.to_string() });
                        }
                    }
                    for restored_id in restored.iter().rev() {
                        if let Err(rollback) = self.cells.get_mut(restored_id).expect("existing cell remains").restore(old_states.get(restored_id).expect("rollback snapshot exists")) {
                            self.state = RuntimeState::Failed;
                            return Err(EvolutionError::RollbackFailed { cell: restored_id.clone(), message: rollback.to_string() });
                        }
                    }
                    for started_id in started.iter().rev() {
                        if let Err(rollback) = tx.additions.get_mut(started_id).expect("staged cell exists").on_stop() {
                            self.state = RuntimeState::Failed;
                            return Err(EvolutionError::RollbackFailed { cell: started_id.clone(), message: rollback.to_string() });
                        }
                    }
                    return Err(EvolutionError::LifecycleFailed { cell: id.clone(), message: error.to_string() });
                }
                stopped.push(id.clone());
            }
        }

        for id in &tx.removals { self.cells.remove(id); }
        for (id, cell) in tx.additions { self.cells.insert(id, cell); }
        self.edges = tx.edges;
        self.queue = queue;

        Ok(EvolutionReport {
            cells_before, cells_after: self.cells.len(), edges_before,
            edges_after: self.edges.values().map(BTreeSet::len).sum(),
            pending_before, pending_after: self.queue.len(),
            migrated_destinations: migrated_destinations.len(), partitioned_messages: partitioned,
        })
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdaptationMetrics {
    pub window_micros: u64,
    pub processed_messages: u64,
    pub queue_high_water: usize,
    pub queue_capacity: usize,
    pub per_cell_processed: BTreeMap<CellId, u64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdaptationRecommendation {
    pub cell: CellId,
    pub reason: String,
    pub throughput_per_second: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdaptationPolicy {
    pub minimum_samples: u64,
    pub queue_pressure_percent: u8,
    pub hot_cell_share_percent: u8,
}
impl Default for AdaptationPolicy {
    fn default() -> Self { Self { minimum_samples: 100, queue_pressure_percent: 75, hot_cell_share_percent: 70 } }
}
impl AdaptationPolicy {
    /// Returns a recommendation only when measured queue pressure and per-cell
    /// workload concentration both cross configured thresholds. It never mutates topology.
    pub fn evaluate(&self, metrics: &AdaptationMetrics) -> Option<AdaptationRecommendation> {
        if metrics.processed_messages < self.minimum_samples || metrics.window_micros == 0
            || metrics.queue_capacity == 0 || metrics.per_cell_processed.is_empty()
            || self.queue_pressure_percent > 100 || self.hot_cell_share_percent > 100
            || metrics.queue_high_water > metrics.queue_capacity
            || metrics.per_cell_processed.values().any(|count| *count > metrics.processed_messages)
            || metrics.per_cell_processed.values().copied().fold(0_u64, u64::saturating_add) > metrics.processed_messages {
            return None;
        }
        let pressure = metrics.queue_high_water.saturating_mul(100) / metrics.queue_capacity;
        if pressure < usize::from(self.queue_pressure_percent) { return None; }
        let (cell, count) = metrics.per_cell_processed.iter().max_by_key(|(_, count)| *count)?;
        let share = count.saturating_mul(100) / metrics.processed_messages;
        if share < u64::from(self.hot_cell_share_percent) { return None; }
        Some(AdaptationRecommendation {
            cell: cell.clone(),
            reason: format!("queue pressure {pressure}% and cell share {share}% exceed policy thresholds"),
            throughput_per_second: metrics.processed_messages.saturating_mul(1_000_000) / metrics.window_micros,
        })
    }
}

#[cfg(test)]
mod evolution_tests {
    use super::*;

    struct Pass;
    impl Cell for Pass {
        fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> { Ok(vec![message]) }
    }
    struct Counter(u64);
    impl Cell for Counter {
        fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> { self.0 += 1; Ok(vec![message]) }
        fn snapshot(&self) -> Result<Vec<u8>, CellError> { Ok(self.0.to_be_bytes().to_vec()) }
        fn restore(&mut self, state: &[u8]) -> Result<(), CellError> {
            let bytes: [u8; 8] = state.try_into().map_err(|_| CellError::new("expected u64 state"))?;
            self.0 = u64::from_be_bytes(bytes); Ok(())
        }
    }

    #[test]
    fn evolves_running_graph_and_preserves_queued_work() {
        let mut runtime = Runtime::new();
        runtime.add_cell("input", Pass).unwrap();
        runtime.add_cell("processor-a", Counter(4)).unwrap();
        runtime.add_cell("output", Pass).unwrap();
        runtime.connect("input", "processor-a").unwrap();
        runtime.connect("processor-a", "output").unwrap();
        runtime.start().unwrap();
        runtime.inject("processor-a", Message::new(1, b"accepted".to_vec())).unwrap();

        let mut tx = runtime.topology_transaction();
        tx.add_cell("processor-b", Counter(0)).unwrap();
        tx.connect("input", "processor-b").unwrap();
        tx.disconnect("input", "processor-a").unwrap();
        tx.connect("processor-b", "output").unwrap();
        tx.partition_pending("processor-a", |_message: &Message| Ok(vec![
            ("processor-a".to_owned(), Message::new(1, b"left".to_vec())),
            ("processor-b".to_owned(), Message::new(2, b"right".to_vec())),
        ]));
        tx.transfer_state("processor-a", "processor-b", |state| Ok(state.to_vec()));

        let report = runtime.evolve(tx, |candidate| {
            if !candidate.edges.get("input").is_some_and(|targets| targets.contains("processor-b")) {
                return Err("new processor is not connected".into());
            }
            Ok(())
        }).unwrap();
        assert_eq!(runtime.state(), RuntimeState::Running);
        assert_eq!(report.cells_after, 4);
        assert_eq!(runtime.pending_messages(), 2);
        assert_eq!(runtime.snapshot().unwrap().cell_state("processor-b"), Some(4u64.to_be_bytes().as_slice()));
    }

    #[test]
    fn rejects_shadow_validation_without_mutating_live_topology() {
        let mut runtime = Runtime::new();
        runtime.add_cell("a", Pass).unwrap();
        runtime.start().unwrap();
        let mut tx = runtime.topology_transaction();
        tx.add_cell("b", Pass).unwrap();
        assert!(matches!(runtime.evolve(tx, |_| Err("injected validation failure".into())), Err(EvolutionError::ValidationFailed(_))));
        assert_eq!(runtime.cell_count(), 1);
        assert_eq!(runtime.state(), RuntimeState::Running);
    }

    #[test]
    fn adaptation_requires_measured_pressure_and_hot_cell_share() {
        let policy = AdaptationPolicy::default();
        let metrics = AdaptationMetrics {
            window_micros: 1_000_000, processed_messages: 1000, queue_high_water: 90, queue_capacity: 100,
            per_cell_processed: [("processor".to_owned(), 800)].into_iter().collect(),
        };
        assert_eq!(policy.evaluate(&metrics).unwrap().cell, "processor");
    }
}
