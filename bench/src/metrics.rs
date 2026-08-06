use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::BenchError;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ProcessSnapshot {
    pub pid: u32,
    pub cpu_ticks: u64,
    pub rss_kib: u64,
    pub voluntary_context_switches: u64,
    pub involuntary_context_switches: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq)]
pub struct ProcessDelta {
    pub pid: u32,
    pub cpu_seconds: f64,
    pub final_rss_kib: u64,
    pub voluntary_context_switches: u64,
    pub involuntary_context_switches: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct CgroupSnapshot {
    pub path: String,
    pub cpu_usage_usec: u64,
    pub cpu_user_usec: u64,
    pub cpu_system_usec: u64,
    pub throttled_usec: u64,
    pub throttled_periods: u64,
    pub memory_current_bytes: u64,
    pub memory_peak_bytes: Option<u64>,
    pub io_accounting_available: bool,
    pub io_read_bytes: u64,
    pub io_write_bytes: u64,
    pub io_read_operations: u64,
    pub io_write_operations: u64,
}

impl ProcessSnapshot {
    /// Capture portable Linux process CPU, RSS, context-switch, and I/O counters.
    ///
    /// # Errors
    ///
    /// Returns an error when procfs is unavailable or a required field cannot be parsed.
    pub fn capture(pid: u32) -> Result<Self, BenchError> {
        let root = PathBuf::from(format!("/proc/{pid}"));
        let stat = read(&root.join("stat"))?;
        let status = read(&root.join("status"))?;
        let io = read(&root.join("io"))?;
        let close = stat.rfind(')').ok_or_else(|| {
            BenchError::Invalid(format!("process stat for {pid} has no command terminator"))
        })?;
        let fields = stat[close + 1..].split_whitespace().collect::<Vec<_>>();
        if fields.len() <= 12 {
            return Err(BenchError::Invalid(format!(
                "process stat for {pid} has too few fields"
            )));
        }
        let user_ticks = parse_u64(fields[11], "process user ticks")?;
        let system_ticks = parse_u64(fields[12], "process system ticks")?;
        Ok(Self {
            pid,
            cpu_ticks: user_ticks.saturating_add(system_ticks),
            rss_kib: named_value(&status, "VmRSS:")?,
            voluntary_context_switches: named_value(&status, "voluntary_ctxt_switches:")?,
            involuntary_context_switches: named_value(&status, "nonvoluntary_ctxt_switches:")?,
            read_bytes: named_value(&io, "read_bytes:")?,
            write_bytes: named_value(&io, "write_bytes:")?,
        })
    }

    /// Compute a nonnegative resource delta using the host's clock tick rate.
    ///
    /// # Errors
    ///
    /// Returns an error when snapshots refer to different processes or the tick rate is unavailable.
    pub fn delta(self, later: Self) -> Result<ProcessDelta, BenchError> {
        if self.pid != later.pid {
            return Err(BenchError::Invalid(
                "process snapshots refer to different PIDs".to_owned(),
            ));
        }
        let ticks_per_second = ticks_per_second()?;
        Ok(ProcessDelta {
            pid: self.pid,
            cpu_seconds: as_f64(later.cpu_ticks.saturating_sub(self.cpu_ticks))
                / as_f64(ticks_per_second),
            final_rss_kib: later.rss_kib,
            voluntary_context_switches: later
                .voluntary_context_switches
                .saturating_sub(self.voluntary_context_switches),
            involuntary_context_switches: later
                .involuntary_context_switches
                .saturating_sub(self.involuntary_context_switches),
            read_bytes: later.read_bytes.saturating_sub(self.read_bytes),
            write_bytes: later.write_bytes.saturating_sub(self.write_bytes),
        })
    }
}

impl CgroupSnapshot {
    /// Capture the unified cgroup v2 counters that contain one process.
    ///
    /// # Errors
    ///
    /// Returns an error when a declared unified cgroup exists but its required counters cannot be read or parsed.
    pub fn capture(pid: u32) -> Result<Option<Self>, BenchError> {
        let membership = read(&PathBuf::from(format!("/proc/{pid}/cgroup")))?;
        let Some(path) = membership.lines().find_map(|line| line.strip_prefix("0::")) else {
            return Ok(None);
        };
        let relative = path.trim_start_matches('/');
        let root = Path::new("/sys/fs/cgroup").join(relative);
        if !root.join("cgroup.controllers").is_file()
            && !Path::new("/sys/fs/cgroup/cgroup.controllers").is_file()
        {
            return Ok(None);
        }
        let cpu = read(&root.join("cpu.stat"))?;
        let io_path = root.join("io.stat");
        let io = fs::read_to_string(&io_path).unwrap_or_default();
        let (io_read_bytes, io_write_bytes, io_read_operations, io_write_operations) =
            io_totals(&io)?;
        Ok(Some(Self {
            path: format!("/{relative}"),
            cpu_usage_usec: named_value(&cpu, "usage_usec ")?,
            cpu_user_usec: named_value(&cpu, "user_usec ")?,
            cpu_system_usec: named_value(&cpu, "system_usec ")?,
            throttled_usec: named_value_or_zero(&cpu, "throttled_usec ")?,
            throttled_periods: named_value_or_zero(&cpu, "nr_throttled ")?,
            memory_current_bytes: parse_u64(
                read(&root.join("memory.current"))?.trim(),
                "memory.current",
            )?,
            memory_peak_bytes: fs::read_to_string(root.join("memory.peak"))
                .ok()
                .and_then(|value| value.trim().parse().ok()),
            io_accounting_available: io_path.is_file(),
            io_read_bytes,
            io_write_bytes,
            io_read_operations,
            io_write_operations,
        }))
    }
}

fn read(path: &Path) -> Result<String, BenchError> {
    fs::read_to_string(path).map_err(|source| BenchError::Read {
        path: path.to_path_buf(),
        source,
    })
}

fn named_value(source: &str, name: &str) -> Result<u64, BenchError> {
    source
        .lines()
        .find_map(|line| line.strip_prefix(name))
        .and_then(|value| value.split_whitespace().next())
        .ok_or_else(|| BenchError::Invalid(format!("process field `{name}` is missing")))
        .and_then(|value| parse_u64(value, name))
}

fn named_value_or_zero(source: &str, name: &str) -> Result<u64, BenchError> {
    source
        .lines()
        .find_map(|line| line.strip_prefix(name))
        .and_then(|value| value.split_whitespace().next())
        .map_or(Ok(0), |value| parse_u64(value, name))
}

fn io_totals(source: &str) -> Result<(u64, u64, u64, u64), BenchError> {
    let mut totals = (0_u64, 0_u64, 0_u64, 0_u64);
    for field in source
        .lines()
        .flat_map(|line| line.split_whitespace().skip(1))
    {
        let Some((name, value)) = field.split_once('=') else {
            continue;
        };
        let value = parse_u64(value, name)?;
        match name {
            "rbytes" => totals.0 = totals.0.saturating_add(value),
            "wbytes" => totals.1 = totals.1.saturating_add(value),
            "rios" => totals.2 = totals.2.saturating_add(value),
            "wios" => totals.3 = totals.3.saturating_add(value),
            _ => {}
        }
    }
    Ok(totals)
}

fn parse_u64(value: &str, name: &str) -> Result<u64, BenchError> {
    value
        .parse()
        .map_err(|error| BenchError::Invalid(format!("invalid {name}: {error}")))
}

fn ticks_per_second() -> Result<u64, BenchError> {
    static TICKS: std::sync::OnceLock<Result<u64, String>> = std::sync::OnceLock::new();
    TICKS
        .get_or_init(|| {
            let output = Command::new("getconf")
                .arg("CLK_TCK")
                .output()
                .map_err(|error| format!("failed to execute getconf: {error}"))?;
            if !output.status.success() {
                return Err("getconf CLK_TCK failed".to_owned());
            }
            String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse()
                .map_err(|error| format!("invalid CLK_TCK: {error}"))
        })
        .clone()
        .map_err(BenchError::Invalid)
}

#[allow(clippy::cast_precision_loss)]
fn as_f64(value: u64) -> f64 {
    value as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_current_process_when_captured_then_should_report_portable_counters() {
        let snapshot = ProcessSnapshot::capture(std::process::id())
            .expect("current process should exist in procfs");
        assert_eq!(snapshot.pid, std::process::id());
        assert!(snapshot.rss_kib > 0);
    }

    #[test]
    fn given_two_snapshots_when_differenced_then_should_not_underflow() {
        let earlier = ProcessSnapshot {
            pid: 7,
            cpu_ticks: 10,
            rss_kib: 20,
            voluntary_context_switches: 30,
            involuntary_context_switches: 40,
            read_bytes: 50,
            write_bytes: 60,
        };
        let later = ProcessSnapshot {
            pid: 7,
            cpu_ticks: 9,
            rss_kib: 18,
            voluntary_context_switches: 29,
            involuntary_context_switches: 39,
            read_bytes: 49,
            write_bytes: 59,
        };
        let delta = earlier.delta(later).expect("matching PIDs should differ");
        assert!(delta.cpu_seconds.abs() < f64::EPSILON);
        assert_eq!(delta.read_bytes, 0);
    }

    #[test]
    fn given_current_process_when_cgroup_v2_exists_then_should_capture_accounting() {
        let snapshot =
            CgroupSnapshot::capture(std::process::id()).expect("cgroup inspection should not fail");
        if let Some(snapshot) = snapshot {
            assert!(snapshot.path.starts_with('/'));
            assert!(snapshot.cpu_usage_usec > 0);
        }
    }
}
