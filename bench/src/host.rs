use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::BenchError;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct HostRequirements {
    pub client_cpus: Vec<u32>,
    pub iggy_cpus: Vec<u32>,
    #[serde(default)]
    pub plane_cpus: Vec<u32>,
    pub numa_node: u32,
    pub clocksource: String,
    pub governor: String,
    pub smt_enabled: bool,
    pub turbo_enabled: bool,
    pub filesystem: String,
    pub disk_model: String,
    pub perf_counters: bool,
    pub max_steal_ticks: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_temperature_millidegrees_celsius: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct HostSnapshot {
    pub cpu_model: String,
    pub online_cpus: Vec<u32>,
    pub numa_nodes: String,
    pub clocksource: String,
    pub governors: BTreeMap<u32, String>,
    pub smt_enabled: Option<bool>,
    pub turbo_enabled: Option<bool>,
    pub filesystem: String,
    pub mount_options: String,
    pub disk_device: String,
    pub disk_model: String,
    pub cgroup_v2: bool,
    pub perf_event_paranoid: Option<i32>,
    pub steal_ticks: u64,
    pub thermal_millidegrees_celsius: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct HostAudit {
    pub before: HostSnapshot,
    pub after: HostSnapshot,
    pub valid: bool,
    pub invalidations: Vec<String>,
}

impl HostSnapshot {
    /// Capture the host controls that affect native benchmark interpretation.
    ///
    /// # Errors
    ///
    /// Returns an error when required Linux CPU, mount, or accounting files cannot be read.
    pub fn capture(path: &Path) -> Result<Self, BenchError> {
        let online_cpus = parse_cpu_list(&read_required("/sys/devices/system/cpu/online")?)?;
        let mount = mount_for(path)?;
        let disk = disk_identity(mount.major, mount.minor);
        Ok(Self {
            cpu_model: cpu_model()?,
            governors: governors(&online_cpus),
            online_cpus,
            numa_nodes: read_optional("/sys/devices/system/node/online")
                .unwrap_or_else(|| "unknown".to_owned()),
            clocksource: read_required(
                "/sys/devices/system/clocksource/clocksource0/current_clocksource",
            )?,
            smt_enabled: read_bool("/sys/devices/system/cpu/smt/active", false),
            turbo_enabled: turbo_enabled(),
            filesystem: mount.filesystem,
            mount_options: mount.options,
            disk_device: disk.0,
            disk_model: disk.1,
            cgroup_v2: Path::new("/sys/fs/cgroup/cgroup.controllers").is_file(),
            perf_event_paranoid: read_optional("/proc/sys/kernel/perf_event_paranoid")
                .and_then(|value| value.parse().ok()),
            steal_ticks: steal_ticks()?,
            thermal_millidegrees_celsius: thermal_readings(),
        })
    }
}

impl HostRequirements {
    /// Validate declared authoritative controls against a captured host.
    ///
    /// # Errors
    ///
    /// Returns an error when core sets overlap, selected CPUs are outside the declared NUMA node, or a required host value differs.
    pub fn validate(
        &self,
        snapshot: &HostSnapshot,
        plane_required: bool,
    ) -> Result<(), BenchError> {
        validate_core_sets(self, snapshot, plane_required)?;
        require_equal("clocksource", &self.clocksource, &snapshot.clocksource)?;
        require_equal("filesystem", &self.filesystem, &snapshot.filesystem)?;
        require_equal("disk model", &self.disk_model, &snapshot.disk_model)?;
        if self.perf_counters
            && !std::process::Command::new("perf")
                .arg("--version")
                .output()
                .is_ok_and(|output| output.status.success())
        {
            return Err(BenchError::Invalid(
                "authoritative perf counters are enabled but `perf` is unavailable".to_owned(),
            ));
        }
        require_equal("SMT state", &Some(self.smt_enabled), &snapshot.smt_enabled)?;
        require_equal(
            "turbo state",
            &Some(self.turbo_enabled),
            &snapshot.turbo_enabled,
        )?;
        for cpu in self.selected_cpus() {
            let actual = snapshot.governors.get(&cpu).ok_or_else(|| {
                BenchError::Invalid(format!("CPU {cpu} has no recorded governor"))
            })?;
            require_equal(&format!("CPU {cpu} governor"), &self.governor, actual)?;
        }
        let numa_cpus = parse_cpu_list(&read_required(&format!(
            "/sys/devices/system/node/node{}/cpulist",
            self.numa_node
        ))?)?;
        if !self
            .selected_cpus()
            .iter()
            .all(|cpu| numa_cpus.contains(cpu))
        {
            return Err(BenchError::Invalid(format!(
                "selected CPUs are not all on NUMA node {}",
                self.numa_node
            )));
        }
        Ok(())
    }

    fn selected_cpus(&self) -> Vec<u32> {
        self.client_cpus
            .iter()
            .chain(&self.iggy_cpus)
            .chain(&self.plane_cpus)
            .copied()
            .collect()
    }
}

impl HostAudit {
    #[must_use]
    pub fn finish(
        before: HostSnapshot,
        after: HostSnapshot,
        requirements: Option<&HostRequirements>,
    ) -> Self {
        let mut invalidations = Vec::new();
        if let Some(requirements) = requirements {
            let steal_delta = after.steal_ticks.saturating_sub(before.steal_ticks);
            if steal_delta > requirements.max_steal_ticks {
                invalidations.push(format!(
                    "CPU steal increased by {steal_delta} ticks, limit {}",
                    requirements.max_steal_ticks
                ));
            }
            if let Some(limit) = requirements.max_temperature_millidegrees_celsius {
                for (zone, temperature) in &after.thermal_millidegrees_celsius {
                    if *temperature > limit {
                        invalidations.push(format!(
                            "thermal zone {zone} reached {temperature} millidegrees Celsius, limit {limit}"
                        ));
                    }
                }
            }
            if before.clocksource != after.clocksource
                || before.governors != after.governors
                || before.smt_enabled != after.smt_enabled
                || before.turbo_enabled != after.turbo_enabled
            {
                invalidations.push("host controls changed during the campaign".to_owned());
            }
        }
        Self {
            before,
            after,
            valid: invalidations.is_empty(),
            invalidations,
        }
    }
}

/// Pin a native process to an exact CPU set.
///
/// # Errors
///
/// Returns an error when the set is empty, contains an unsupported CPU index, or Linux rejects the affinity update.
pub fn pin_process(pid: u32, cpus: &[u32]) -> Result<(), BenchError> {
    if cpus.is_empty() {
        return Err(BenchError::Invalid(
            "process CPU set cannot be empty".to_owned(),
        ));
    }
    let mut set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    unsafe {
        libc::CPU_ZERO(&mut set);
    }
    for cpu in cpus {
        let cpu = usize::try_from(*cpu)
            .map_err(|error| BenchError::Invalid(format!("invalid CPU index: {error}")))?;
        if cpu >= libc::CPU_SETSIZE as usize {
            return Err(BenchError::Invalid(format!(
                "CPU {cpu} exceeds CPU_SETSIZE"
            )));
        }
        unsafe {
            libc::CPU_SET(cpu, &mut set);
        }
    }
    let result = unsafe {
        libc::sched_setaffinity(
            i32::try_from(pid)
                .map_err(|error| BenchError::Invalid(format!("invalid process ID: {error}")))?,
            std::mem::size_of::<libc::cpu_set_t>(),
            &raw const set,
        )
    };
    if result != 0 {
        return Err(BenchError::Invalid(format!(
            "failed to pin process {pid}: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(())
}

fn validate_core_sets(
    requirements: &HostRequirements,
    snapshot: &HostSnapshot,
    plane_required: bool,
) -> Result<(), BenchError> {
    if requirements.client_cpus.is_empty()
        || requirements.iggy_cpus.is_empty()
        || (plane_required && requirements.plane_cpus.is_empty())
    {
        return Err(BenchError::Invalid(
            "authoritative client, Iggy, and required plane CPU sets cannot be empty".to_owned(),
        ));
    }
    let selected = requirements.selected_cpus();
    let unique = selected.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != selected.len() {
        return Err(BenchError::Invalid(
            "authoritative process CPU sets must be disjoint".to_owned(),
        ));
    }
    if !selected
        .iter()
        .all(|cpu| snapshot.online_cpus.contains(cpu))
    {
        return Err(BenchError::Invalid(
            "authoritative process CPU set contains an offline CPU".to_owned(),
        ));
    }
    Ok(())
}

fn require_equal<T>(name: &str, expected: &T, actual: &T) -> Result<(), BenchError>
where
    T: std::fmt::Debug + PartialEq,
{
    if expected == actual {
        return Ok(());
    }
    Err(BenchError::Invalid(format!(
        "host {name} mismatch: expected {expected:?}, found {actual:?}"
    )))
}

struct MountIdentity {
    major: u64,
    minor: u64,
    filesystem: String,
    options: String,
}

fn mount_for(path: &Path) -> Result<MountIdentity, BenchError> {
    let canonical = fs::canonicalize(path).map_err(|source| BenchError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let mountinfo = read_required("/proc/self/mountinfo")?;
    let mut best: Option<(usize, MountIdentity)> = None;
    for line in mountinfo.lines() {
        let Some((left, right)) = line.split_once(" - ") else {
            continue;
        };
        let fields = left.split_whitespace().collect::<Vec<_>>();
        let right = right.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 6 || right.is_empty() {
            continue;
        }
        let mount_path = PathBuf::from(unescape_mount(fields[4]));
        if !canonical.starts_with(&mount_path) {
            continue;
        }
        let Some((major, minor)) = fields[2].split_once(':') else {
            continue;
        };
        let identity = MountIdentity {
            major: major.parse().unwrap_or_default(),
            minor: minor.parse().unwrap_or_default(),
            filesystem: right[0].to_owned(),
            options: fields[5].to_owned(),
        };
        let depth = mount_path.as_os_str().len();
        if best.as_ref().is_none_or(|(current, _)| depth > *current) {
            best = Some((depth, identity));
        }
    }
    best.map(|(_, identity)| identity)
        .ok_or_else(|| BenchError::Invalid("benchmark filesystem mount was not found".to_owned()))
}

fn disk_identity(major: u64, minor: u64) -> (String, String) {
    let device = format!("{major}:{minor}");
    let path = PathBuf::from(format!("/sys/dev/block/{device}"));
    let canonical = fs::canonicalize(path).ok();
    let model = canonical
        .as_deref()
        .and_then(|path| {
            path.ancestors()
                .find_map(|parent| read_path(parent.join("device/model")))
        })
        .unwrap_or_else(|| "unknown".to_owned());
    (device, model)
}

fn governors(cpus: &[u32]) -> BTreeMap<u32, String> {
    cpus.iter()
        .filter_map(|cpu| {
            read_optional(&format!(
                "/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor"
            ))
            .map(|governor| (*cpu, governor))
        })
        .collect()
}

fn thermal_readings() -> BTreeMap<String, i64> {
    let Ok(entries) = fs::read_dir("/sys/class/thermal") else {
        return BTreeMap::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.starts_with("thermal_zone") {
                return None;
            }
            let value = read_path(entry.path().join("temp"))?.parse().ok()?;
            Some((name, value))
        })
        .collect()
}

fn cpu_model() -> Result<String, BenchError> {
    let cpuinfo = read_required("/proc/cpuinfo")?;
    cpuinfo
        .lines()
        .find_map(|line| {
            line.strip_prefix("model name\t:")
                .or_else(|| line.strip_prefix("Hardware\t:"))
                .map(str::trim)
                .map(str::to_owned)
        })
        .ok_or_else(|| BenchError::Invalid("CPU model was not found".to_owned()))
}

fn steal_ticks() -> Result<u64, BenchError> {
    let stat = read_required("/proc/stat")?;
    let cpu = stat
        .lines()
        .find(|line| line.starts_with("cpu "))
        .ok_or_else(|| BenchError::Invalid("aggregate CPU accounting was not found".to_owned()))?;
    Ok(cpu
        .split_whitespace()
        .nth(8)
        .and_then(|value| value.parse().ok())
        .unwrap_or_default())
}

fn turbo_enabled() -> Option<bool> {
    read_bool("/sys/devices/system/cpu/intel_pstate/no_turbo", true)
        .or_else(|| read_bool("/sys/devices/system/cpu/cpufreq/boost", false))
}

fn read_bool(path: &str, inverted: bool) -> Option<bool> {
    let enabled = read_optional(path)? == "1";
    Some(if inverted { !enabled } else { enabled })
}

fn read_required(path: &str) -> Result<String, BenchError> {
    fs::read_to_string(path)
        .map(|value| value.trim().to_owned())
        .map_err(|source| BenchError::Read {
            path: PathBuf::from(path),
            source,
        })
}

fn read_optional(path: &str) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_owned())
}

fn read_path(path: PathBuf) -> Option<String> {
    fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_owned())
}

fn unescape_mount(value: &str) -> String {
    value
        .replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
}

fn parse_cpu_list(value: &str) -> Result<Vec<u32>, BenchError> {
    let mut cpus = BTreeSet::new();
    for item in value.trim().split(',').filter(|item| !item.is_empty()) {
        if let Some((start, end)) = item.split_once('-') {
            let start = start.parse::<u32>().map_err(|error| {
                BenchError::Invalid(format!("invalid CPU range `{item}`: {error}"))
            })?;
            let end = end.parse::<u32>().map_err(|error| {
                BenchError::Invalid(format!("invalid CPU range `{item}`: {error}"))
            })?;
            if start > end {
                return Err(BenchError::Invalid(format!(
                    "invalid descending CPU range `{item}`"
                )));
            }
            cpus.extend(start..=end);
        } else {
            cpus.insert(
                item.parse::<u32>().map_err(|error| {
                    BenchError::Invalid(format!("invalid CPU `{item}`: {error}"))
                })?,
            );
        }
    }
    Ok(cpus.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{HostAudit, HostRequirements, HostSnapshot, parse_cpu_list};

    #[test]
    fn given_linux_cpu_list_when_parsed_then_should_expand_sorted_unique_cpus() {
        assert_eq!(
            parse_cpu_list("0-2,2,5").expect("CPU list should parse"),
            vec![0, 1, 2, 5]
        );
    }

    #[test]
    fn given_steal_above_limit_when_audited_then_should_invalidate_campaign() {
        let mut before = snapshot();
        before.steal_ticks = 10;
        let mut after = before.clone();
        after.steal_ticks = 12;
        let requirements = requirements();

        let audit = HostAudit::finish(before, after, Some(&requirements));

        assert!(!audit.valid);
        assert!(audit.invalidations[0].contains("steal"));
    }

    fn snapshot() -> HostSnapshot {
        HostSnapshot {
            cpu_model: "test".to_owned(),
            online_cpus: vec![0, 1, 2],
            numa_nodes: "0".to_owned(),
            clocksource: "tsc".to_owned(),
            governors: [(0, "performance".to_owned())].into_iter().collect(),
            smt_enabled: Some(false),
            turbo_enabled: Some(false),
            filesystem: "ext4".to_owned(),
            mount_options: "rw".to_owned(),
            disk_device: "8:0".to_owned(),
            disk_model: "test".to_owned(),
            cgroup_v2: true,
            perf_event_paranoid: Some(2),
            steal_ticks: 0,
            thermal_millidegrees_celsius: BTreeMap::new(),
        }
    }

    fn requirements() -> HostRequirements {
        HostRequirements {
            client_cpus: vec![0],
            iggy_cpus: vec![1],
            plane_cpus: vec![2],
            numa_node: 0,
            clocksource: "tsc".to_owned(),
            governor: "performance".to_owned(),
            smt_enabled: false,
            turbo_enabled: false,
            filesystem: "ext4".to_owned(),
            disk_model: "test".to_owned(),
            perf_counters: false,
            max_steal_ticks: 0,
            max_temperature_millidegrees_celsius: None,
        }
    }
}
