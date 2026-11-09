use gravity_core::{Cell, CellError, Message, Runtime};
use std::process::ExitCode;

struct Input;
impl Cell for Input {
    fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
        Ok(vec![message])
    }
}

struct Processor;
impl Cell for Processor {
    fn process(&mut self, mut message: Message) -> Result<Vec<Message>, CellError> {
        message.payload.make_ascii_uppercase();
        Ok(vec![message])
    }
}

struct Output;
impl Cell for Output {
    fn process(&mut self, message: Message) -> Result<Vec<Message>, CellError> {
        println!(
            "output received message {}: {}",
            message.id,
            String::from_utf8_lossy(&message.payload)
        );
        Ok(Vec::new())
    }
}

struct CountingProcessor(u64);
impl Cell for CountingProcessor {
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
            .map_err(|_| CellError::new("expected an eight-byte processor state"))?;
        self.0 = u64::from_be_bytes(bytes);
        Ok(())
    }
}

fn run_demo() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = Runtime::new();
    runtime.add_cell("input", Input)?;
    runtime.add_cell("processor", Processor)?;
    runtime.add_cell("output", Output)?;
    runtime.connect("input", "processor")?;
    runtime.connect("processor", "output")?;
    runtime.start()?;
    runtime.inject("input", Message::new(1, b"gravity genesis".to_vec()))?;
    let report = runtime.run_until_idle(1_000)?;
    println!(
        "runtime idle: {} messages processed, {} deliveries routed, {} pending",
        report.processed_messages, report.routed_messages, report.pending_messages
    );
    runtime.stop()?;
    Ok(())
}

fn run_evolution_demo() -> Result<(), Box<dyn std::error::Error>> {
    let mut runtime = Runtime::new();
    runtime.add_cell("input", Input)?;
    runtime.add_cell("processor-a", CountingProcessor(0))?;
    runtime.add_cell("output", Output)?;
    runtime.connect("input", "processor-a")?;
    runtime.connect("processor-a", "output")?;
    runtime.start()?;

    runtime.inject(
        "processor-a",
        Message::new(1, b"gravity evolution".to_vec()),
    )?;

    let mut transaction = runtime.topology_transaction();
    transaction.add_cell("processor-b", CountingProcessor(0))?;
    transaction.disconnect("input", "processor-a")?;
    transaction.connect("input", "processor-b")?;
    transaction.connect("processor-b", "output")?;
    transaction.partition_pending("processor-a", |message: &Message| {
        let midpoint = message.payload.len() / 2;
        let (left, right) = message.payload.split_at(midpoint);
        Ok(vec![
            (
                "processor-a".to_owned(),
                Message::new(message.id, left.to_vec()),
            ),
            (
                "processor-b".to_owned(),
                Message::new(message.id.saturating_add(1), right.to_vec()),
            ),
        ])
    });
    transaction.transfer_state("processor-a", "processor-b", |state| Ok(state.to_vec()));

    let report = runtime.evolve(transaction, |candidate| {
        if !candidate
            .edges
            .get("input")
            .is_some_and(|targets| targets.contains("processor-b"))
        {
            return Err("candidate topology does not route input to processor-b".into());
        }
        if !candidate.cell_states.contains_key("processor-b") {
            return Err("processor-b has no migrated state".into());
        }
        Ok(())
    })?;

    println!(
        "topology evolved live: {} -> {} cells, {} queued message(s) partitioned, {} state destination(s) migrated",
        report.cells_before,
        report.cells_after,
        report.partitioned_messages,
        report.migrated_destinations
    );
    let run = runtime.run_until_idle(1_000)?;
    println!(
        "evolved runtime idle: {} messages processed, {} deliveries routed, {} pending",
        run.processed_messages, run.routed_messages, run.pending_messages
    );
    runtime.stop()?;
    Ok(())
}

fn main() -> ExitCode {
    match std::env::args().nth(1).as_deref() {
        Some("demo") | None => match run_demo() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("gravity: {error}");
                ExitCode::FAILURE
            }
        },
        Some("evolve-demo") => match run_evolution_demo() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("gravity: {error}");
                ExitCode::FAILURE
            }
        },
        Some(_) => {
            eprintln!("usage: gravity [demo|evolve-demo]");
            ExitCode::from(2)
        }
    }
}
