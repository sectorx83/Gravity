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

fn main() -> ExitCode {
    match std::env::args().nth(1).as_deref() {
        Some("demo") | None => match run_demo() {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("gravity: {error}");
                ExitCode::FAILURE
            }
        },
        Some(_) => {
            eprintln!("usage: gravity [demo]");
            ExitCode::from(2)
        }
    }
}
