use std::fs;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::net::TcpListener;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::BenchError;
use crate::host::HostSnapshot;
use crate::manifest::{ProvisionMode, SuiteManifest, Transport};
use crate::process::{ComposeServices, NativeIggy, NativePlane};
use crate::provision::ResolvedStack;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DoctorReport {
    pub tcp_vsr_only: bool,
    pub cpu_target_compatible: bool,
    pub required_tools: Vec<ToolCheck>,
    pub working_directory: ProbeStatus,
    pub tcp_bind: ProbeStatus,
    pub uds_bind: ProbeStatus,
    pub disk_available_bytes: u64,
    pub disk_required_bytes: u64,
    pub live_vsr: Option<bool>,
    pub host: HostSnapshot,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProbeStatus {
    Available,
    Unavailable,
}

impl ProbeStatus {
    #[must_use]
    pub fn is_available(self) -> bool {
        self == Self::Available
    }
}

impl From<bool> for ProbeStatus {
    fn from(available: bool) -> Self {
        if available {
            Self::Available
        } else {
            Self::Unavailable
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ToolCheck {
    pub name: String,
    pub available: bool,
}

/// Validate static environment requirements for a suite.
///
/// # Errors
///
/// Returns an error when VSR, CPU, tool, or host checks fail.
pub fn inspect(manifest: &SuiteManifest) -> Result<DoctorReport, BenchError> {
    let tcp_vsr_only = manifest
        .scenarios
        .iter()
        .all(|scenario| scenario.transport == Transport::TcpVsr);
    if !tcp_vsr_only {
        return Err(BenchError::Invalid(
            "every Iggy-backed benchmark must use tcp_vsr".to_owned(),
        ));
    }
    let cpu_target_compatible = cpu_compatible(&manifest.provisioning.cpu_target)?;
    if !cpu_target_compatible {
        return Err(BenchError::Invalid(format!(
            "host CPU does not support target `{}`",
            manifest.provisioning.cpu_target
        )));
    }
    let mut names = match manifest.provisioning.mode {
        ProvisionMode::Source => vec!["cargo", "rustc", "git"],
        ProvisionMode::Path => Vec::new(),
        ProvisionMode::Artifact => vec!["curl"],
        ProvisionMode::Compose => vec!["docker"],
    };
    names.push("getconf");
    if manifest
        .environment
        .host
        .as_ref()
        .is_some_and(|host| host.perf_counters)
    {
        names.push("perf");
    }
    let required_tools = names
        .iter()
        .map(|name| ToolCheck {
            name: (*name).to_owned(),
            available: tool_available(name),
        })
        .collect::<Vec<_>>();
    if let Some(missing) = required_tools.iter().find(|tool| !tool.available) {
        return Err(BenchError::Invalid(format!(
            "required tool `{}` is unavailable",
            missing.name
        )));
    }
    let working_directory_writable = writable_probe(Path::new("."))?;
    if !working_directory_writable {
        return Err(BenchError::Invalid(
            "benchmark working directory is not writable".to_owned(),
        ));
    }
    let tcp_bind_available = TcpListener::bind(("127.0.0.1", 0)).is_ok();
    if !tcp_bind_available {
        return Err(BenchError::Invalid(
            "benchmark host cannot bind a loopback TCP port".to_owned(),
        ));
    }
    let uds_bind_available = uds_probe()?;
    if manifest.requires_plane() && !uds_bind_available {
        return Err(BenchError::Invalid(
            "managed benchmark host cannot bind a Unix socket".to_owned(),
        ));
    }
    let disk_available_bytes = available_disk_bytes(Path::new("."))?;
    let disk_required_bytes = required_disk_bytes(manifest);
    if disk_available_bytes < disk_required_bytes {
        return Err(BenchError::Invalid(format!(
            "benchmark requires at least {disk_required_bytes} free bytes, found {disk_available_bytes}"
        )));
    }
    let host = HostSnapshot::capture(Path::new("."))?;
    if let Some(requirements) = manifest.environment.host.as_ref() {
        requirements.validate(&host, manifest.requires_plane())?;
    }
    Ok(DoctorReport {
        tcp_vsr_only,
        cpu_target_compatible,
        required_tools,
        working_directory: working_directory_writable.into(),
        tcp_bind: tcp_bind_available.into(),
        uds_bind: uds_bind_available.into(),
        disk_available_bytes,
        disk_required_bytes,
        live_vsr: None,
        host,
    })
}

fn writable_probe(directory: &Path) -> Result<bool, BenchError> {
    let path = directory.join(format!(".laser-bench-doctor-{}", std::process::id()));
    let result = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .and_then(|mut file| file.write_all(b"probe"));
    match result {
        Ok(()) => {
            fs::remove_file(&path).map_err(|source| BenchError::Write {
                path: path.clone(),
                source,
            })?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => Ok(false),
        Err(error) => Err(BenchError::Write {
            path,
            source: error,
        }),
    }
}

fn uds_probe() -> Result<bool, BenchError> {
    let path = std::env::temp_dir().join(format!(
        "laser-bench-doctor-{}-{}.sock",
        std::process::id(),
        std::thread::current().name().unwrap_or("thread")
    ));
    let listener = UnixListener::bind(&path);
    if listener.is_ok() {
        fs::remove_file(&path).map_err(|source| BenchError::Write {
            path: path.clone(),
            source,
        })?;
    }
    Ok(listener.is_ok())
}

fn available_disk_bytes(path: &Path) -> Result<u64, BenchError> {
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        BenchError::Invalid(format!("disk probe path `{}` contains NUL", path.display()))
    })?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: `path` is a valid NUL-terminated string and `stats` points to writable storage.
    if unsafe { libc::statvfs(path.as_ptr(), stats.as_mut_ptr()) } != 0 {
        return Err(BenchError::Invalid(format!(
            "failed to inspect free disk space: {}",
            std::io::Error::last_os_error()
        )));
    }
    // SAFETY: a successful `statvfs` call initialized the output structure.
    let stats = unsafe { stats.assume_init() };
    Ok(stats.f_bavail.saturating_mul(stats.f_frsize))
}

fn required_disk_bytes(manifest: &SuiteManifest) -> u64 {
    const BASELINE_BYTES: u64 = 1 << 30;
    let workload = manifest.scenarios.iter().fold(0_u64, |total, scenario| {
        let payload = u64::try_from(scenario.payload_bytes).unwrap_or(u64::MAX);
        let batch = u64::try_from(scenario.batch_size).unwrap_or(u64::MAX);
        let bytes = payload
            .saturating_mul(batch)
            .saturating_mul(scenario.operations);
        total.saturating_add(bytes)
    });
    BASELINE_BYTES.saturating_add(workload.saturating_mul(2))
}

/// Start the resolved raw server and complete an SDK VSR connection probe.
///
/// # Errors
///
/// Returns an error for absent binaries, process failure, or a failed VSR connection.
pub async fn inspect_live(
    mut report: DoctorReport,
    stack: &ResolvedStack,
    manifest: &SuiteManifest,
    directory: &Path,
) -> Result<DoctorReport, BenchError> {
    if stack.mode == ProvisionMode::Compose {
        let compose_file = manifest
            .provisioning
            .compose_file
            .as_deref()
            .ok_or_else(|| BenchError::Invalid("Compose mode requires compose_file".to_owned()))?;
        let (compose, server, plane) = ComposeServices::start(
            compose_file,
            directory,
            manifest.requires_plane(),
            manifest.environment.plane_profile,
        )?;
        let probe = server.probe_vsr().await;
        report.live_vsr = Some(probe.is_ok());
        drop(probe);
        if let Some(plane) = plane {
            plane.shutdown().await?;
        }
        server.shutdown().await?;
        compose.shutdown()?;
        if report.live_vsr != Some(true) {
            return Err(BenchError::Invalid("live VSR doctor failed".to_owned()));
        }
        return Ok(report);
    }
    let server_manifest = stack.iggy_server.as_ref().ok_or_else(|| {
        BenchError::Invalid("live VSR doctor requires a native Iggy server".to_owned())
    })?;
    let plane_socket = stack.plane.as_ref().map(|_| directory.join("plane.sock"));
    let server = NativeIggy::start(
        server_manifest,
        directory,
        plane_socket.as_deref(),
        &manifest.environment,
    )
    .await?;
    let mut plane = if let Some(plane_manifest) = &stack.plane {
        Some(
            NativePlane::start(
                plane_manifest,
                directory,
                &server,
                plane_socket.clone().ok_or_else(|| {
                    BenchError::Invalid("managed doctor requires a plane socket".to_owned())
                })?,
                &manifest.environment,
            )
            .await?,
        )
    } else {
        None
    };
    let probe = server.probe_vsr().await;
    report.live_vsr = Some(probe.is_ok());
    drop(probe);
    if let Some(plane) = plane.as_mut() {
        plane.wait_ready(Duration::from_secs(30)).await?;
    }
    if let Some(plane) = plane {
        plane.shutdown().await?;
    }
    server.shutdown().await?;
    if report.live_vsr != Some(true) {
        return Err(BenchError::Invalid("live VSR doctor failed".to_owned()));
    }
    Ok(report)
}

fn tool_available(program: &str) -> bool {
    Command::new(program)
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

fn cpu_compatible(cpu_target: &str) -> Result<bool, BenchError> {
    if cpu_target == "native" {
        return Ok(true);
    }
    let architecture = std::env::consts::ARCH;
    if cpu_target == "arm64" {
        return Ok(architecture == "aarch64");
    }
    if architecture != "x86_64" {
        return Ok(false);
    }
    let cpuinfo = fs::read_to_string("/proc/cpuinfo").map_err(|source| BenchError::Read {
        path: Path::new("/proc/cpuinfo").to_path_buf(),
        source,
    })?;
    let required: &[&str] = match cpu_target {
        "skylake" | "znver3" => &["avx2", "bmi2", "fma"],
        "icelake" => &["avx2", "avx512f", "avx512vl"],
        "sapphirerapids" => &["avx2", "avx512f", "avx512_bf16"],
        _ => {
            return Err(BenchError::Invalid(format!(
                "unsupported CPU target `{cpu_target}`"
            )));
        }
    };
    Ok(required.iter().all(|feature| {
        cpuinfo
            .lines()
            .filter_map(|line| line.strip_prefix("flags\t\t: "))
            .any(|flags| flags.split_whitespace().any(|flag| flag == *feature))
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_native_cpu_target_when_checked_then_should_be_compatible() {
        assert!(cpu_compatible("native").expect("native target should be valid"));
    }

    #[test]
    fn given_unknown_cpu_target_when_checked_then_should_reject_it() {
        assert!(cpu_compatible("future-cpu").is_err());
    }
}
