use laser_test_support::catalog::{self, CatalogState};
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

const USAGE: &str = "usage: laser-test-catalog <socket>...";
const READY: &str = "ready";
// How many binding reads the catalog leaves unanswered before it serves them.
const UNANSWERED_BINDING_READS: &str = "LASER_TEST_CATALOG_UNANSWERED_BINDING_READS";

fn main() -> ExitCode {
    let sockets: Vec<PathBuf> = std::env::args_os().skip(1).map(PathBuf::from).collect();
    if sockets.is_empty() {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }
    let mut served = CatalogState::default();
    if let Some(unanswered) = std::env::var_os(UNANSWERED_BINDING_READS) {
        match unanswered.to_str().and_then(|count| count.parse().ok()) {
            Some(count) => served.unanswered_binding_reads = count,
            None => {
                eprintln!("{UNANSWERED_BINDING_READS} must be a count");
                return ExitCode::from(2);
            }
        }
    }
    let state = Arc::new(Mutex::new(served));
    for (node, socket) in sockets.iter().enumerate() {
        if let Err(error) = catalog::spawn(socket, Arc::clone(&state), node) {
            eprintln!("cannot serve the catalog on {}: {error}", socket.display());
            return ExitCode::FAILURE;
        }
    }
    let mut stdout = std::io::stdout();
    if writeln!(stdout, "{READY}")
        .and_then(|()| stdout.flush())
        .is_err()
    {
        return ExitCode::FAILURE;
    }
    loop {
        std::thread::park();
    }
}
