use std::collections::{BTreeMap, VecDeque};
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
        Self { id, payload: payload.into() }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellError {
    message: String,
}

impl CellError {
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

impl fmt::Display for CellError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for CellError {}

pub trait Cell: Send {
    fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum RuntimeError {
    DuplicateCell(CellId),
    UnknownCell(CellId),
    StepLimitExceeded { limit: usize, remaining: usize },
    CellFailed { cell: CellId, source: CellError },
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateCell(id) => write!(f, "cell already exists: {id}"),
            Self::UnknownCell(id) => write!(f, "unknown cell: {id}"),
            Self::StepLimitExceeded { limit, remaining } => write!(
                f, "execution step limit {limit} reached with {remaining} messages pending"
            ),
            Self::CellFailed { cell, source } => write!(f, "cell {cell} failed: {source}"),
        }
    }
}

impl Error for RuntimeError {}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunReport {
    pub processed_messages: usize,
    pub pending_messages: usize,
}

pub struct Runtime {
    cells: BTreeMap<CellId, Box<dyn Cell>>,
    edges: BTreeMap<CellId, Vec<CellId>>,
    queue: VecDeque<(CellId, Message)>,
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    pub fn new() -> Self {
        Self {
            cells: BTreeMap::new(),
            edges: BTreeMap::new(),
            queue: VecDeque::new(),
        }
    }

    pub fn add_cell(
        &mut self,
        id: impl Into<CellId>,
        cell: impl Cell + 'static,
    ) -> Result<(), RuntimeError> {
        let id = id.into();
        if self.cells.contains_key(&id) {
            return Err(RuntimeError::DuplicateCell(id));
        }
        self.cells.insert(id.clone(), Box::new(cell));
        self.edges.entry(id).or_default();
        Ok(())
    }

    pub fn connect(&mut self, from: &str, to: &str) -> Result<(), RuntimeError> {
        if !self.cells.contains_key(from) {
            return Err(RuntimeError::UnknownCell(from.to_owned()));
        }
        if !self.cells.contains_key(to) {
            return Err(RuntimeError::UnknownCell(to.to_owned()));
        }
        let targets = self.edges.entry(from.to_owned()).or_default();
        if !targets.iter().any(|target| target == to) {
            targets.push(to.to_owned());
        }
        Ok(())
    }

    pub fn inject(&mut self, target: &str, message: Message) -> Result<(), RuntimeError> {
        if !self.cells.contains_key(target) {
            return Err(RuntimeError::UnknownCell(target.to_owned()));
        }
        self.queue.push_back((target.to_owned(), message));
        Ok(())
    }

    pub fn pending_messages(&self) -> usize {
        self.queue.len()
    }

    pub fn cell_count(&self) -> usize {
        self.cells.len()
    }

    pub fn run_until_idle(&mut self, step_limit: usize) -> Result<RunReport, RuntimeError> {
        let mut processed = 0;
        while processed < step_limit {
            let Some((cell_id, message)) = self.queue.pop_front() else {
                return Ok(RunReport {
                    processed_messages: processed,
                    pending_messages: 0,
                });
            };

            let outputs = {
                let cell = self.cells.get_mut(&cell_id)
                    .expect("queued messages always reference registered cells");
                match cell.process(message.clone()) {
                    Ok(outputs) => outputs,
                    Err(source) => {
                        self.queue.push_front((cell_id.clone(), message));
                        return Err(RuntimeError::CellFailed { cell: cell_id, source });
                    }
                }
            };

            processed += 1;
            if let Some(targets) = self.edges.get(&cell_id) {
                for output in outputs {
                    for target in targets {
                        self.queue.push_back((target.clone(), output.clone()));
                    }
                }
            }
        }

        if self.queue.is_empty() {
            Ok(RunReport { processed_messages: processed, pending_messages: 0 })
        } else {
            Err(RuntimeError::StepLimitExceeded {
                limit: step_limit,
                remaining: self.queue.len(),
            })
        }
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

    struct Failing;
    impl Cell for Failing {
        fn process(&mut self, _message: Message) -> Result<Vec<Message>, CellError> {
            Err(CellError::new("injected failure"))
        }
    }

    #[test]
    fn routes_messages_through_connected_cells() {
        let mut runtime = Runtime::new();
        runtime.add_cell("processor", Uppercase).unwrap();
        runtime.add_cell("output", Sink).unwrap();
        runtime.connect("processor", "output").unwrap();
        runtime.inject("processor", Message::new(7, b"gravity".to_vec())).unwrap();
        let report = runtime.run_until_idle(8).unwrap();
        assert_eq!(report.processed_messages, 2);
        assert_eq!(report.pending_messages, 0);
    }

    #[test]
    fn rejects_duplicate_cell_ids() {
        let mut runtime = Runtime::new();
        runtime.add_cell("cell-a", Uppercase).unwrap();
        assert_eq!(
            runtime.add_cell("cell-a", Uppercase).unwrap_err(),
            RuntimeError::DuplicateCell("cell-a".to_owned())
        );
    }

    #[test]
    fn enforces_execution_step_limit() {
        let mut runtime = Runtime::new();
        runtime.add_cell("duplicator", Duplicate).unwrap();
        runtime.connect("duplicator", "duplicator").unwrap();
        runtime.inject("duplicator", Message::new(1, b"x".to_vec())).unwrap();
        let error = runtime.run_until_idle(4).unwrap_err();
        assert!(matches!(
            error,
            RuntimeError::StepLimitExceeded { limit: 4, remaining: 5 }
        ));
    }

    #[test]
    fn rejects_connections_to_unknown_cells() {
        let mut runtime = Runtime::new();
        runtime.add_cell("known", Uppercase).unwrap();
        assert_eq!(
            runtime.connect("known", "missing").unwrap_err(),
            RuntimeError::UnknownCell("missing".to_owned())
        );
    }

    #[test]
    fn preserves_message_when_a_cell_fails() {
        let mut runtime = Runtime::new();
        runtime.add_cell("failure", Failing).unwrap();
        runtime.inject("failure", Message::new(9, b"keep-me".to_vec())).unwrap();
        assert!(matches!(runtime.run_until_idle(1), Err(RuntimeError::CellFailed { .. })));
        assert_eq!(runtime.pending_messages(), 1);
    }
}
