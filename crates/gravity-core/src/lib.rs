use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::error::Error;
use std::fmt;

pub type CellId = String;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub id: u64,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn new(id: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            id,
            payload: payload.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellError {
    message: String,
}

impl CellError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for CellError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for CellError {}

/// A unit of computation owned and invoked by a Gravity runtime.
///
/// A cell snapshot must contain all mutable state that affects future
/// processing. Process must not perform irreversible external side effects:
/// the runtime can restore cell-local state, but cannot undo a network request,
/// filesystem write, or action in another system.
pub trait Cell: Send {
    fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError>;

    /// Called when the runtime starts. Implementations should release partially
    /// acquired resources from on_stop if startup returns an error.
    fn on_start(&mut self) -> Result<(), CellError> {
        Ok(())
    }

    /// Called in reverse cell-ID order when the runtime stops.
    fn on_stop(&mut self) -> Result<(), CellError> {
        Ok(())
    }

    /// Capture the cell's complete restorable state.
    /// Stateless cells can use the default empty snapshot.
    fn snapshot(&self) -> Result<Vec<u8>, CellError> {
        Ok(Vec::new())
    }

    /// Restore state previously returned by snapshot.
    /// Stateless cells accept only the default empty snapshot.
    fn restore(&mut self, state: &[u8]) -> Result<(), CellError> {
        if state.is_empty() {
            Ok(())
        } else {
            Err(CellError::new(
                "stateless cell cannot restore a non-empty snapshot",
            ))
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub max_pending_messages: usize,
    pub max_deliveries_per_step: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeState {
    Configuring,
    Running,
    Stopped,
    Failed,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_pending_messages: 65_536,
            max_deliveries_per_step: 16_384,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunReport {
    pub processed_messages: usize,
    pub routed_messages: usize,
    pub pending_messages: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeSnapshot {
    topology: BTreeMap<CellId, BTreeSet<CellId>>,
    cell_states: BTreeMap<CellId, Vec<u8>>,
    pending: VecDeque<(CellId, Message)>,
}

impl RuntimeSnapshot {
    pub fn cell_count(&self) -> usize {
        self.cell_states.len()
    }

    pub fn pending_messages(&self) -> usize {
        self.pending.len()
    }

    pub fn cell_state(&self, cell_id: &str) -> Option<&[u8]> {
        self.cell_states.get(cell_id).map(Vec::as_slice)
    }

    pub fn topology(&self) -> &BTreeMap<CellId, BTreeSet<CellId>> {
        &self.topology
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum RuntimeError {
    InvalidCellId,
    RuntimeNotRunning,
    RuntimeRunning,
    RuntimeFailed,
    CellStartFailed {
        cell: CellId,
        source: CellError,
    },
    CellStartRollbackFailed {
        cell: CellId,
        start_error: CellError,
        rollback_error: CellError,
    },
    CellStopFailed {
        cell: CellId,
        source: CellError,
    },
    DuplicateCell(CellId),
    UnknownCell(CellId),
    QueueCapacityExceeded {
        limit: usize,
        attempted: usize,
    },
    DeliveryLimitExceeded {
        limit: usize,
        attempted: usize,
    },
    StepLimitExceeded {
        limit: usize,
        remaining: usize,
    },
    CellSnapshotFailed {
        cell: CellId,
        source: CellError,
    },
    CellFailed {
        cell: CellId,
        source: CellError,
    },
    CellRollbackFailed {
        cell: CellId,
        process_error: CellError,
        rollback_error: CellError,
    },
    TopologyMismatch,
    SnapshotCellSetMismatch,
    CellRestoreFailed {
        cell: CellId,
        source: CellError,
    },
    RestoreRollbackFailed {
        cell: CellId,
        restore_error: CellError,
        rollback_error: CellError,
    },
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCellId => formatter.write_str("cell ID cannot be empty"),
            Self::RuntimeNotRunning => formatter.write_str("runtime is not running"),
            Self::RuntimeRunning => formatter.write_str("topology cannot change while runtime is running"),
            Self::RuntimeFailed => formatter.write_str("runtime is in a failed lifecycle state"),
            Self::CellStartFailed { cell, source } => {
                write!(formatter, "could not start cell {cell}: {source}")
            }
            Self::CellStartRollbackFailed { cell, start_error, rollback_error } => write!(
                formatter,
                "cell startup failed ({start_error}) and cleanup of cell {cell} also failed ({rollback_error})"
            ),
            Self::CellStopFailed { cell, source } => {
                write!(formatter, "could not stop cell {cell}: {source}")
            }
            Self::DuplicateCell(id) => write!(formatter, "cell already exists: {id}"),
            Self::UnknownCell(id) => write!(formatter, "unknown cell: {id}"),
            Self::QueueCapacityExceeded { limit, attempted } => write!(
                formatter,
                "pending message limit {limit} would be exceeded ({attempted})"
            ),
            Self::DeliveryLimitExceeded { limit, attempted } => write!(
                formatter,
                "per-step delivery limit {limit} would be exceeded ({attempted})"
            ),
            Self::StepLimitExceeded { limit, remaining } => write!(
                formatter,
                "execution step limit {limit} reached with {remaining} messages pending"
            ),
            Self::CellSnapshotFailed { cell, source } => {
                write!(formatter, "could not snapshot cell {cell}: {source}")
            }
            Self::CellFailed { cell, source } => {
                write!(formatter, "cell {cell} failed: {source}")
            }
            Self::CellRollbackFailed { cell, process_error, rollback_error } => write!(
                formatter,
                "cell {cell} failed ({process_error}) and state rollback also failed ({rollback_error})"
            ),
            Self::TopologyMismatch => {
                formatter.write_str("snapshot topology does not match the current runtime")
            }
            Self::SnapshotCellSetMismatch => {
                formatter.write_str("snapshot cell set does not match the current runtime")
            }
            Self::CellRestoreFailed { cell, source } => {
                write!(formatter, "could not restore cell {cell}: {source}")
            }
            Self::RestoreRollbackFailed { cell, restore_error, rollback_error } => write!(
                formatter,
                "cell restore failed ({restore_error}) and rollback of cell {cell} also failed ({rollback_error})"
            ),
        }
    }
}

impl Error for RuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CellSnapshotFailed { source, .. }
            | Self::CellFailed { source, .. }
            | Self::CellRestoreFailed { source, .. }
            | Self::CellStartFailed { source, .. }
            | Self::CellStopFailed { source, .. } => Some(source),
            Self::CellRollbackFailed { process_error, .. } => Some(process_error),
            Self::CellStartRollbackFailed { start_error, .. } => Some(start_error),
            Self::RestoreRollbackFailed { restore_error, .. } => Some(restore_error),
            _ => None,
        }
    }
}

pub struct Runtime {
    cells: BTreeMap<CellId, Box<dyn Cell>>,
    edges: BTreeMap<CellId, BTreeSet<CellId>>,
    queue: VecDeque<(CellId, Message)>,
    config: RuntimeConfig,
    state: RuntimeState,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    pub fn new() -> Self {
        Self::with_config(RuntimeConfig::default())
    }

    pub fn with_config(config: RuntimeConfig) -> Self {
        Self {
            cells: BTreeMap::new(),
            edges: BTreeMap::new(),
            queue: VecDeque::new(),
            config,
            state: RuntimeState::Configuring,
        }
    }

    pub fn state(&self) -> RuntimeState {
        self.state
    }

    pub fn start(&mut self) -> Result<(), RuntimeError> {
        match self.state {
            RuntimeState::Running => return Ok(()),
            RuntimeState::Failed => return Err(RuntimeError::RuntimeFailed),
            RuntimeState::Configuring | RuntimeState::Stopped => {}
        }

        let mut attempted_cells = Vec::new();
        for cell_id in self.cells.keys().cloned().collect::<Vec<_>>() {
            attempted_cells.push(cell_id.clone());
            let result = self
                .cells
                .get_mut(&cell_id)
                .expect("cell remains registered during startup")
                .on_start();

            if let Err(start_error) = result {
                let mut cleanup_failure = None;
                for rollback_id in attempted_cells.iter().rev() {
                    let rollback_result = self
                        .cells
                        .get_mut(rollback_id)
                        .expect("started cell remains registered")
                        .on_stop();
                    if let Err(rollback_error) = rollback_result {
                        if cleanup_failure.is_none() {
                            cleanup_failure = Some((rollback_id.clone(), rollback_error));
                        }
                    }
                }

                if let Some((rollback_cell, rollback_error)) = cleanup_failure {
                    self.state = RuntimeState::Failed;
                    return Err(RuntimeError::CellStartRollbackFailed {
                        cell: rollback_cell,
                        start_error,
                        rollback_error,
                    });
                }

                return Err(RuntimeError::CellStartFailed {
                    cell: cell_id,
                    source: start_error,
                });
            }
        }

        self.state = RuntimeState::Running;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<(), RuntimeError> {
        if self.state != RuntimeState::Running {
            return Err(RuntimeError::RuntimeNotRunning);
        }

        let mut first_error = None;
        for cell_id in self.cells.keys().rev().cloned().collect::<Vec<_>>() {
            let result = self
                .cells
                .get_mut(&cell_id)
                .expect("cell remains registered during shutdown")
                .on_stop();
            if let Err(source) = result {
                if first_error.is_none() {
                    first_error = Some((cell_id, source));
                }
            }
        }

        if let Some((cell, source)) = first_error {
            self.state = RuntimeState::Failed;
            return Err(RuntimeError::CellStopFailed { cell, source });
        }

        self.state = RuntimeState::Stopped;
        Ok(())
    }

    fn ensure_topology_mutable(&self) -> Result<(), RuntimeError> {
        match self.state {
            RuntimeState::Configuring | RuntimeState::Stopped => Ok(()),
            RuntimeState::Running => Err(RuntimeError::RuntimeRunning),
            RuntimeState::Failed => Err(RuntimeError::RuntimeFailed),
        }
    }

    fn ensure_running(&self) -> Result<(), RuntimeError> {
        match self.state {
            RuntimeState::Running => Ok(()),
            RuntimeState::Failed => Err(RuntimeError::RuntimeFailed),
            RuntimeState::Configuring | RuntimeState::Stopped => {
                Err(RuntimeError::RuntimeNotRunning)
            }
        }
    }

    pub fn config(&self) -> RuntimeConfig {
        self.config
    }

    pub fn add_cell(
        &mut self,
        id: impl Into<CellId>,
        cell: impl Cell + 'static,
    ) -> Result<(), RuntimeError> {
        self.ensure_topology_mutable()?;
        let id = id.into();
        if id.trim().is_empty() {
            return Err(RuntimeError::InvalidCellId);
        }
        if self.cells.contains_key(&id) {
            return Err(RuntimeError::DuplicateCell(id));
        }
        self.cells.insert(id.clone(), Box::new(cell));
        self.edges.insert(id, BTreeSet::new());
        Ok(())
    }

    pub fn connect(&mut self, from: &str, to: &str) -> Result<(), RuntimeError> {
        self.ensure_topology_mutable()?;
        if !self.cells.contains_key(from) {
            return Err(RuntimeError::UnknownCell(from.to_owned()));
        }
        if !self.cells.contains_key(to) {
            return Err(RuntimeError::UnknownCell(to.to_owned()));
        }
        self.edges
            .get_mut(from)
            .expect("every registered cell has an adjacency entry")
            .insert(to.to_owned());
        Ok(())
    }

    pub fn disconnect(&mut self, from: &str, to: &str) -> Result<bool, RuntimeError> {
        self.ensure_topology_mutable()?;
        if !self.cells.contains_key(from) {
            return Err(RuntimeError::UnknownCell(from.to_owned()));
        }
        if !self.cells.contains_key(to) {
            return Err(RuntimeError::UnknownCell(to.to_owned()));
        }
        Ok(self
            .edges
            .get_mut(from)
            .expect("every registered cell has an adjacency entry")
            .remove(to))
    }

    pub fn inject(&mut self, target: &str, message: Message) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        if !self.cells.contains_key(target) {
            return Err(RuntimeError::UnknownCell(target.to_owned()));
        }
        if self.queue.len() >= self.config.max_pending_messages {
            return Err(RuntimeError::QueueCapacityExceeded {
                limit: self.config.max_pending_messages,
                attempted: self.queue.len().saturating_add(1),
            });
        }
        self.queue.push_back((target.to_owned(), message));
        Ok(())
    }

    pub fn cell_count(&self) -> usize {
        self.cells.len()
    }

    pub fn contains_cell(&self, id: &str) -> bool {
        self.cells.contains_key(id)
    }

    pub fn cell_ids(&self) -> impl Iterator<Item = &str> {
        self.cells.keys().map(String::as_str)
    }

    pub fn topology(&self) -> &BTreeMap<CellId, BTreeSet<CellId>> {
        &self.edges
    }

    pub fn pending_messages(&self) -> usize {
        self.queue.len()
    }

    pub fn snapshot(&self) -> Result<RuntimeSnapshot, RuntimeError> {
        let cell_states = self.capture_cell_states()?;
        Ok(RuntimeSnapshot {
            topology: self.edges.clone(),
            cell_states,
            pending: self.queue.clone(),
        })
    }

    /// Restore a checkpoint only when its topology and cell identities match.
    /// If a cell rejects its state, attempted cells are rolled back and the
    /// pending-message queue remains unchanged.
    pub fn restore(&mut self, snapshot: &RuntimeSnapshot) -> Result<(), RuntimeError> {
        if self.edges != snapshot.topology {
            return Err(RuntimeError::TopologyMismatch);
        }
        if self.cells.len() != snapshot.cell_states.len()
            || self
                .cells
                .keys()
                .any(|id| !snapshot.cell_states.contains_key(id))
        {
            return Err(RuntimeError::SnapshotCellSetMismatch);
        }
        if snapshot.pending.len() > self.config.max_pending_messages {
            return Err(RuntimeError::QueueCapacityExceeded {
                limit: self.config.max_pending_messages,
                attempted: snapshot.pending.len(),
            });
        }
        for (cell_id, _) in &snapshot.pending {
            if !self.cells.contains_key(cell_id) {
                return Err(RuntimeError::UnknownCell(cell_id.clone()));
            }
        }

        let previous_states = self.capture_cell_states()?;
        let mut attempted_cells = Vec::new();

        for (cell_id, desired_state) in &snapshot.cell_states {
            attempted_cells.push(cell_id.clone());
            let result = self
                .cells
                .get_mut(cell_id)
                .expect("snapshot cell set was validated")
                .restore(desired_state);

            if let Err(restore_error) = result {
                for rollback_id in attempted_cells.iter().rev() {
                    let previous_state = previous_states
                        .get(rollback_id)
                        .expect("pre-restore snapshot contains every registered cell");
                    let rollback_result = self
                        .cells
                        .get_mut(rollback_id)
                        .expect("rollback cell remains registered")
                        .restore(previous_state);

                    if let Err(rollback_error) = rollback_result {
                        self.state = RuntimeState::Failed;
                        return Err(RuntimeError::RestoreRollbackFailed {
                            cell: rollback_id.clone(),
                            restore_error,
                            rollback_error,
                        });
                    }
                }
                return Err(RuntimeError::CellRestoreFailed {
                    cell: cell_id.clone(),
                    source: restore_error,
                });
            }
        }

        self.queue.clone_from(&snapshot.pending);
        Ok(())
    }

    pub fn run_until_idle(&mut self, step_limit: usize) -> Result<RunReport, RuntimeError> {
        self.ensure_running()?;
        let mut processed_messages = 0;
        let mut routed_messages = 0;

        while processed_messages < step_limit {
            let Some((cell_id, message)) = self.queue.pop_front() else {
                return Ok(RunReport {
                    processed_messages,
                    routed_messages,
                    pending_messages: 0,
                });
            };

            let before_state = match self
                .cells
                .get(&cell_id)
                .expect("queued messages always reference registered cells")
                .snapshot()
            {
                Ok(state) => state,
                Err(source) => {
                    self.queue.push_front((cell_id.clone(), message));
                    return Err(RuntimeError::CellSnapshotFailed {
                        cell: cell_id,
                        source,
                    });
                }
            };

            let outputs = match self
                .cells
                .get_mut(&cell_id)
                .expect("queued messages always reference registered cells")
                .process(message.clone())
            {
                Ok(outputs) => outputs,
                Err(process_error) => {
                    let rollback_result = self
                        .cells
                        .get_mut(&cell_id)
                        .expect("queued messages always reference registered cells")
                        .restore(&before_state);
                    self.queue.push_front((cell_id.clone(), message));
                    return match rollback_result {
                        Ok(()) => Err(RuntimeError::CellFailed {
                            cell: cell_id,
                            source: process_error,
                        }),
                        Err(rollback_error) => {
                            self.state = RuntimeState::Failed;
                            Err(RuntimeError::CellRollbackFailed {
                                cell: cell_id,
                                process_error,
                                rollback_error,
                            })
                        }
                    };
                }
            };

            let targets = self
                .edges
                .get(&cell_id)
                .expect("every registered cell has an adjacency entry")
                .clone();
            let deliveries = outputs.len().saturating_mul(targets.len());

            if deliveries > self.config.max_deliveries_per_step {
                let rollback_result = self
                    .cells
                    .get_mut(&cell_id)
                    .expect("queued messages always reference registered cells")
                    .restore(&before_state);
                self.queue.push_front((cell_id.clone(), message));

                if let Err(rollback_error) = rollback_result {
                    self.state = RuntimeState::Failed;
                    return Err(RuntimeError::CellRollbackFailed {
                        cell: cell_id,
                        process_error: CellError::new(format!(
                            "delivery count {deliveries} exceeds configured limit {}",
                            self.config.max_deliveries_per_step
                        )),
                        rollback_error,
                    });
                }
                return Err(RuntimeError::DeliveryLimitExceeded {
                    limit: self.config.max_deliveries_per_step,
                    attempted: deliveries,
                });
            }

            let attempted_pending = self.queue.len().saturating_add(deliveries);
            if attempted_pending > self.config.max_pending_messages {
                let rollback_result = self
                    .cells
                    .get_mut(&cell_id)
                    .expect("queued messages always reference registered cells")
                    .restore(&before_state);
                self.queue.push_front((cell_id.clone(), message));

                if let Err(rollback_error) = rollback_result {
                    self.state = RuntimeState::Failed;
                    return Err(RuntimeError::CellRollbackFailed {
                        cell: cell_id,
                        process_error: CellError::new(format!(
                            "pending message count {attempted_pending} exceeds configured limit {}",
                            self.config.max_pending_messages
                        )),
                        rollback_error,
                    });
                }
                return Err(RuntimeError::QueueCapacityExceeded {
                    limit: self.config.max_pending_messages,
                    attempted: attempted_pending,
                });
            }

            for output in outputs {
                for target in &targets {
                    self.queue.push_back((target.clone(), output.clone()));
                    routed_messages += 1;
                }
            }
            processed_messages += 1;
        }

        if self.queue.is_empty() {
            Ok(RunReport {
                processed_messages,
                routed_messages,
                pending_messages: 0,
            })
        } else {
            Err(RuntimeError::StepLimitExceeded {
                limit: step_limit,
                remaining: self.queue.len(),
            })
        }
    }

    fn capture_cell_states(&self) -> Result<BTreeMap<CellId, Vec<u8>>, RuntimeError> {
        let mut states = BTreeMap::new();
        for (cell_id, cell) in &self.cells {
            let state = cell
                .snapshot()
                .map_err(|source| RuntimeError::CellSnapshotFailed {
                    cell: cell_id.clone(),
                    source,
                })?;
            states.insert(cell_id.clone(), state);
        }
        Ok(states)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Uppercase;
    impl Cell for Uppercase {
        fn process(&mut self, mut message: Message) -> Result<Vec<Message>, CellError> {
            message.payload.make_ascii_uppercase();
            Ok(vec![message])
        }
    }

    struct Duplicate;
    impl Cell for Duplicate {
        fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
            Ok(vec![message.clone(), message])
        }
    }

    struct Sink;
    impl Cell for Sink {
        fn process(&mut self, _message: Message) -> Result<Vec<Message>, CellError> {
            Ok(Vec::new())
        }
    }

    #[derive(Default)]
    struct Counter {
        value: u64,
    }
    impl Cell for Counter {
        fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
            self.value += 1;
            Ok(vec![message])
        }
        fn snapshot(&self) -> Result<Vec<u8>, CellError> {
            Ok(self.value.to_be_bytes().to_vec())
        }
        fn restore(&mut self, state: &[u8]) -> Result<(), CellError> {
            let bytes: [u8; 8] = state
                .try_into()
                .map_err(|_| CellError::new("counter snapshot must contain 8 bytes"))?;
            self.value = u64::from_be_bytes(bytes);
            Ok(())
        }
    }

    struct MutateThenFail {
        value: u64,
    }
    impl Cell for MutateThenFail {
        fn process(&mut self, _message: Message) -> Result<Vec<Message>, CellError> {
            self.value += 1;
            Err(CellError::new("injected processing failure"))
        }
        fn snapshot(&self) -> Result<Vec<u8>, CellError> {
            Ok(self.value.to_be_bytes().to_vec())
        }
        fn restore(&mut self, state: &[u8]) -> Result<(), CellError> {
            let bytes: [u8; 8] = state
                .try_into()
                .map_err(|_| CellError::new("counter snapshot must contain 8 bytes"))?;
            self.value = u64::from_be_bytes(bytes);
            Ok(())
        }
    }

    struct CannotRollback {
        value: u64,
    }

    impl Cell for CannotRollback {
        fn process(&mut self, _message: Message) -> Result<Vec<Message>, CellError> {
            self.value += 1;
            Err(CellError::new("processing failed"))
        }

        fn snapshot(&self) -> Result<Vec<u8>, CellError> {
            Ok(self.value.to_be_bytes().to_vec())
        }

        fn restore(&mut self, _state: &[u8]) -> Result<(), CellError> {
            Err(CellError::new("rollback unavailable"))
        }
    }

    struct RejectRestore {
        value: u64,
    }
    impl Cell for RejectRestore {
        fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
            Ok(vec![message])
        }
        fn snapshot(&self) -> Result<Vec<u8>, CellError> {
            Ok(self.value.to_be_bytes().to_vec())
        }
        fn restore(&mut self, state: &[u8]) -> Result<(), CellError> {
            let bytes: [u8; 8] = state
                .try_into()
                .map_err(|_| CellError::new("snapshot must contain 8 bytes"))?;
            let value = u64::from_be_bytes(bytes);
            self.value = value;
            if value == 99 {
                return Err(CellError::new("test restore rejection"));
            }
            Ok(())
        }
    }

    use std::sync::{Arc, Mutex};

    struct LifecycleProbe {
        name: &'static str,
        events: Arc<Mutex<Vec<String>>>,
        fail_start: bool,
    }

    impl Cell for LifecycleProbe {
        fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
            Ok(vec![message])
        }

        fn on_start(&mut self) -> Result<(), CellError> {
            self.events
                .lock()
                .unwrap()
                .push(format!("start:{}", self.name));
            if self.fail_start {
                return Err(CellError::new("injected start failure"));
            }
            Ok(())
        }

        fn on_stop(&mut self) -> Result<(), CellError> {
            self.events
                .lock()
                .unwrap()
                .push(format!("stop:{}", self.name));
            Ok(())
        }
    }

    #[test]
    fn lifecycle_starts_in_order_and_stops_in_reverse_order() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut runtime = Runtime::new();
        runtime
            .add_cell(
                "b",
                LifecycleProbe {
                    name: "b",
                    events: Arc::clone(&events),
                    fail_start: false,
                },
            )
            .unwrap();
        runtime
            .add_cell(
                "a",
                LifecycleProbe {
                    name: "a",
                    events: Arc::clone(&events),
                    fail_start: false,
                },
            )
            .unwrap();

        runtime.start().unwrap();
        assert_eq!(runtime.state(), RuntimeState::Running);
        runtime.stop().unwrap();
        assert_eq!(runtime.state(), RuntimeState::Stopped);
        assert_eq!(
            *events.lock().unwrap(),
            vec!["start:a", "start:b", "stop:b", "stop:a"]
        );
    }

    #[test]
    fn failed_start_cleans_up_attempted_cells() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut runtime = Runtime::new();
        runtime
            .add_cell(
                "a",
                LifecycleProbe {
                    name: "a",
                    events: Arc::clone(&events),
                    fail_start: false,
                },
            )
            .unwrap();
        runtime
            .add_cell(
                "b",
                LifecycleProbe {
                    name: "b",
                    events: Arc::clone(&events),
                    fail_start: true,
                },
            )
            .unwrap();

        assert!(matches!(
            runtime.start(),
            Err(RuntimeError::CellStartFailed { cell, .. }) if cell == "b"
        ));
        assert_eq!(runtime.state(), RuntimeState::Configuring);
        assert_eq!(
            *events.lock().unwrap(),
            vec!["start:a", "start:b", "stop:b", "stop:a"]
        );
    }

    #[test]
    fn stop_preserves_queued_work_for_a_later_restart() {
        let mut runtime = Runtime::new();
        runtime.add_cell("counter", Counter::default()).unwrap();
        runtime.start().unwrap();
        runtime
            .inject("counter", Message::new(1, b"waiting".to_vec()))
            .unwrap();
        runtime.stop().unwrap();
        assert_eq!(runtime.state(), RuntimeState::Stopped);
        assert_eq!(runtime.pending_messages(), 1);
        assert_eq!(
            runtime.run_until_idle(1).unwrap_err(),
            RuntimeError::RuntimeNotRunning
        );
        runtime.start().unwrap();
        runtime.run_until_idle(1).unwrap();
        assert_eq!(runtime.pending_messages(), 0);
        runtime.stop().unwrap();
    }

    #[test]
    fn topology_is_frozen_while_runtime_is_running() {
        let mut runtime = Runtime::new();
        runtime.add_cell("a", Uppercase).unwrap();
        runtime.add_cell("b", Sink).unwrap();
        runtime.start().unwrap();
        assert_eq!(
            runtime.connect("a", "b").unwrap_err(),
            RuntimeError::RuntimeRunning
        );
        runtime.stop().unwrap();
        runtime.connect("a", "b").unwrap();
    }

    fn encoded_counter(value: u64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    #[test]
    fn routes_messages_through_connected_cells() {
        let mut runtime = Runtime::new();
        runtime.add_cell("processor", Uppercase).unwrap();
        runtime.add_cell("output", Sink).unwrap();
        runtime.connect("processor", "output").unwrap();
        runtime.start().unwrap();
        runtime
            .inject("processor", Message::new(7, b"gravity".to_vec()))
            .unwrap();
        let report = runtime.run_until_idle(8).unwrap();
        assert_eq!(report.processed_messages, 2);
        assert_eq!(report.routed_messages, 1);
        assert_eq!(report.pending_messages, 0);
    }

    #[test]
    fn rejects_duplicate_and_empty_cell_ids() {
        let mut runtime = Runtime::new();
        runtime.add_cell("cell-a", Uppercase).unwrap();
        assert_eq!(
            runtime.add_cell("cell-a", Uppercase).unwrap_err(),
            RuntimeError::DuplicateCell("cell-a".to_owned())
        );
        assert_eq!(
            runtime.add_cell("   ", Uppercase).unwrap_err(),
            RuntimeError::InvalidCellId
        );
    }

    #[test]
    fn rejects_connections_to_unknown_cells() {
        let mut runtime = Runtime::new();
        runtime.add_cell("known", Uppercase).unwrap();
        assert_eq!(
            runtime.connect("known", "missing").unwrap_err(),
            RuntimeError::UnknownCell("missing".to_owned())
        );
        assert_eq!(
            runtime.connect("missing", "known").unwrap_err(),
            RuntimeError::UnknownCell("missing".to_owned())
        );
    }

    #[test]
    fn connections_are_idempotent_and_can_be_removed() {
        let mut runtime = Runtime::new();
        runtime.add_cell("a", Uppercase).unwrap();
        runtime.add_cell("b", Sink).unwrap();
        runtime.connect("a", "b").unwrap();
        runtime.connect("a", "b").unwrap();
        assert_eq!(runtime.topology()["a"].len(), 1);
        assert!(runtime.disconnect("a", "b").unwrap());
        assert!(!runtime.disconnect("a", "b").unwrap());
        assert!(runtime.topology()["a"].is_empty());
    }

    #[test]
    fn enforces_execution_step_limit_without_dropping_work() {
        let mut runtime = Runtime::new();
        runtime.add_cell("duplicator", Duplicate).unwrap();
        runtime.connect("duplicator", "duplicator").unwrap();
        runtime.start().unwrap();
        runtime
            .inject("duplicator", Message::new(1, b"x".to_vec()))
            .unwrap();
        assert_eq!(
            runtime.run_until_idle(4).unwrap_err(),
            RuntimeError::StepLimitExceeded {
                limit: 4,
                remaining: 5,
            }
        );
        assert_eq!(runtime.pending_messages(), 5);
    }

    #[test]
    fn restores_cell_state_and_message_when_processing_fails() {
        let mut runtime = Runtime::new();
        runtime
            .add_cell("mutator", MutateThenFail { value: 12 })
            .unwrap();
        runtime.start().unwrap();
        runtime
            .inject("mutator", Message::new(9, b"retry".to_vec()))
            .unwrap();
        assert!(matches!(
            runtime.run_until_idle(1),
            Err(RuntimeError::CellFailed { .. })
        ));
        let snapshot = runtime.snapshot().unwrap();
        assert_eq!(snapshot.cell_states["mutator"], encoded_counter(12));
        assert_eq!(snapshot.pending_messages(), 1);
    }

    #[test]
    fn failed_processing_rollback_quarantines_the_runtime() {
        let mut runtime = Runtime::new();
        runtime
            .add_cell("unstable", CannotRollback { value: 0 })
            .unwrap();
        runtime.start().unwrap();
        runtime
            .inject("unstable", Message::new(1, b"work".to_vec()))
            .unwrap();

        assert!(matches!(
            runtime.run_until_idle(1),
            Err(RuntimeError::CellRollbackFailed { .. })
        ));
        assert_eq!(runtime.state(), RuntimeState::Failed);
        assert_eq!(runtime.pending_messages(), 1);
        assert_eq!(
            runtime
                .inject("unstable", Message::new(2, b"more".to_vec()))
                .unwrap_err(),
            RuntimeError::RuntimeFailed
        );
    }

    #[test]
    fn checkpoint_restores_cell_state_and_pending_messages() {
        let mut runtime = Runtime::new();
        runtime.add_cell("counter", Counter::default()).unwrap();
        runtime.start().unwrap();
        runtime
            .inject("counter", Message::new(1, b"first".to_vec()))
            .unwrap();
        let checkpoint = runtime.snapshot().unwrap();
        runtime.run_until_idle(1).unwrap();
        assert_eq!(
            runtime.snapshot().unwrap().cell_states["counter"],
            encoded_counter(1)
        );
        assert_eq!(runtime.pending_messages(), 0);
        runtime.restore(&checkpoint).unwrap();
        assert_eq!(runtime.pending_messages(), 1);
        assert_eq!(
            runtime.snapshot().unwrap().cell_states["counter"],
            encoded_counter(0)
        );
        runtime.run_until_idle(1).unwrap();
        assert_eq!(
            runtime.snapshot().unwrap().cell_states["counter"],
            encoded_counter(1)
        );
    }

    #[test]
    fn refuses_checkpoint_restore_after_topology_changes() {
        let mut runtime = Runtime::new();
        runtime.add_cell("a", Uppercase).unwrap();
        runtime.add_cell("b", Sink).unwrap();
        let checkpoint = runtime.snapshot().unwrap();
        runtime.connect("a", "b").unwrap();
        assert_eq!(
            runtime.restore(&checkpoint).unwrap_err(),
            RuntimeError::TopologyMismatch
        );
    }

    #[test]
    fn rolls_back_partial_checkpoint_restore_failure() {
        let mut runtime = Runtime::new();
        runtime.add_cell("a", RejectRestore { value: 1 }).unwrap();
        runtime.add_cell("b", RejectRestore { value: 2 }).unwrap();
        let mut checkpoint = runtime.snapshot().unwrap();
        checkpoint
            .cell_states
            .insert("a".to_owned(), encoded_counter(10));
        checkpoint
            .cell_states
            .insert("b".to_owned(), encoded_counter(99));
        assert!(matches!(
            runtime.restore(&checkpoint),
            Err(RuntimeError::CellRestoreFailed { .. })
        ));
        let after = runtime.snapshot().unwrap();
        assert_eq!(after.cell_states["a"], encoded_counter(1));
        assert_eq!(after.cell_states["b"], encoded_counter(2));
    }

    #[test]
    fn enforces_pending_queue_capacity_on_injection() {
        let mut runtime = Runtime::with_config(RuntimeConfig {
            max_pending_messages: 1,
            max_deliveries_per_step: 8,
        });
        runtime.add_cell("sink", Sink).unwrap();
        runtime.start().unwrap();
        runtime
            .inject("sink", Message::new(1, b"one".to_vec()))
            .unwrap();
        assert_eq!(
            runtime
                .inject("sink", Message::new(2, b"two".to_vec()))
                .unwrap_err(),
            RuntimeError::QueueCapacityExceeded {
                limit: 1,
                attempted: 2,
            }
        );
        assert_eq!(runtime.pending_messages(), 1);
    }

    #[test]
    fn queue_overflow_rolls_back_cell_and_preserves_input() {
        let mut runtime = Runtime::with_config(RuntimeConfig {
            max_pending_messages: 2,
            max_deliveries_per_step: 8,
        });
        runtime.add_cell("counter", Counter::default()).unwrap();
        runtime.add_cell("sink-a", Sink).unwrap();
        runtime.add_cell("sink-b", Sink).unwrap();
        runtime.connect("counter", "sink-a").unwrap();
        runtime.connect("counter", "sink-b").unwrap();
        runtime.start().unwrap();
        runtime
            .inject("counter", Message::new(1, b"work".to_vec()))
            .unwrap();
        runtime.start().unwrap();
        runtime
            .inject("sink-a", Message::new(2, b"queued".to_vec()))
            .unwrap();
        assert_eq!(
            runtime.run_until_idle(1).unwrap_err(),
            RuntimeError::QueueCapacityExceeded {
                limit: 2,
                attempted: 3,
            }
        );
        let snapshot = runtime.snapshot().unwrap();
        assert_eq!(snapshot.cell_states["counter"], encoded_counter(0));
        assert_eq!(snapshot.pending_messages(), 2);
    }

    #[test]
    fn delivery_limit_rolls_back_cell_and_preserves_input() {
        let mut runtime = Runtime::with_config(RuntimeConfig {
            max_pending_messages: 16,
            max_deliveries_per_step: 1,
        });
        runtime.add_cell("counter", Counter::default()).unwrap();
        runtime.add_cell("sink-a", Sink).unwrap();
        runtime.add_cell("sink-b", Sink).unwrap();
        runtime.connect("counter", "sink-a").unwrap();
        runtime.connect("counter", "sink-b").unwrap();
        runtime.start().unwrap();
        runtime
            .inject("counter", Message::new(1, b"work".to_vec()))
            .unwrap();
        assert_eq!(
            runtime.run_until_idle(1).unwrap_err(),
            RuntimeError::DeliveryLimitExceeded {
                limit: 1,
                attempted: 2,
            }
        );
        let snapshot = runtime.snapshot().unwrap();
        assert_eq!(snapshot.cell_states["counter"], encoded_counter(0));
        assert_eq!(snapshot.pending_messages(), 1);
    }

    #[test]
    fn snapshot_rejects_cell_state_capture_failure() {
        struct Unsnapshotable;
        impl Cell for Unsnapshotable {
            fn process(&mut self, _message: Message) -> Result<Vec<Message>, CellError> {
                Ok(Vec::new())
            }
            fn snapshot(&self) -> Result<Vec<u8>, CellError> {
                Err(CellError::new("snapshot unavailable"))
            }
        }
        let mut runtime = Runtime::new();
        runtime.add_cell("bad", Unsnapshotable).unwrap();
        assert!(matches!(
            runtime.snapshot(),
            Err(RuntimeError::CellSnapshotFailed { .. })
        ));
    }

    #[test]
    fn reports_idle_runtime_with_no_work() {
        let mut runtime = Runtime::new();
        runtime.start().unwrap();
        let report = runtime.run_until_idle(0).unwrap();
        assert_eq!(report.processed_messages, 0);
        assert_eq!(report.routed_messages, 0);
        assert_eq!(report.pending_messages, 0);
    }

    #[test]
    fn runtime_exposes_stable_cell_and_topology_inspection() {
        let mut runtime = Runtime::new();
        runtime.add_cell("input", Uppercase).unwrap();
        runtime.add_cell("output", Sink).unwrap();
        runtime.connect("input", "output").unwrap();
        assert_eq!(runtime.cell_count(), 2);
        assert!(runtime.contains_cell("input"));
        assert!(!runtime.contains_cell("missing"));
        assert_eq!(
            runtime.cell_ids().collect::<Vec<_>>(),
            vec!["input", "output"]
        );
        assert_eq!(
            runtime.topology()["input"]
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["output"]
        );
    }
}


mod evolution;
pub use evolution::*;
