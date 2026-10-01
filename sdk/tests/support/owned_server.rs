use std::ops::{Deref, DerefMut};
use std::process::Child;
#[cfg(target_os = "linux")]
use std::process::{Command, Stdio};

#[cfg(target_os = "linux")]
const LIFETIME_MONITOR: &str = r#"
process="$1"
start_time() {
    IFS= read -r task_stat < "/proc/$process/stat" || return 1
    set -- ${task_stat##*) }
    shift 19
    printf '%s' "$1"
}
before=$(start_time) || exit 0
IFS= read -r shutdown
current=$(start_time) || exit 0
[ "$before" = "$current" ] || exit 0
kill -TERM "$process" 2>/dev/null || true
"#;

/// Static fixtures do not run Drop at process exit. The monitor sees EOF when the test process exits.
pub(super) struct OwnedServer {
    child: Child,
    monitor: Option<Child>,
}

impl OwnedServer {
    pub(super) fn new(child: Child) -> Self {
        #[cfg(target_os = "linux")]
        let monitor = Some(
            Command::new("sh")
                .args(["-c", LIFETIME_MONITOR, "laser-test-server-lifetime"])
                .arg(child.id().to_string())
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("start the test server lifetime monitor"),
        );
        #[cfg(not(target_os = "linux"))]
        let monitor = None;
        Self { child, monitor }
    }
}

impl Deref for OwnedServer {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.child
    }
}

impl DerefMut for OwnedServer {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}

impl Drop for OwnedServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(mut monitor) = self.monitor.take() {
            drop(monitor.stdin.take());
            let _ = monitor.wait();
        }
    }
}

// No `use` lines here: a harness-free test target (the BDD runner) compiles
// this module with the test function stripped, and imports would be unused.
#[cfg(all(test, target_os = "linux"))]
mod tests {
    #[test]
    fn given_a_static_fixture_monitor_when_its_parent_pipe_closes_then_should_stop_only_its_child()
    {
        let child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("test child");
        let mut owned = super::OwnedServer::new(child);
        drop(owned.monitor.as_mut().expect("Linux monitor").stdin.take());
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Some(status) = owned.try_wait().expect("probe child") {
                assert!(!status.success());
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the lifetime monitor must stop the child"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
