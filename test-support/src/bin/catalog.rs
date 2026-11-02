use laser_test_support::catalog::{self, CatalogState};
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

const USAGE: &str = "usage: laser-test-catalog <socket>...";
const READY: &str = "ready";

fn main() -> ExitCode {
    let sockets: Vec<PathBuf> = std::env::args_os().skip(1).map(PathBuf::from).collect();
    if sockets.is_empty() {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }
    let state = Arc::new(Mutex::new(CatalogState::default()));
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
