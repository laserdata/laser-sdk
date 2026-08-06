use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use strum::{Display, EnumString, IntoStaticStr};

use crate::BenchError;
use crate::agdx::AgdxDriver;
use crate::host::HostRequirements;
use crate::iggy::IggyBenchmarkKind;
use crate::local_memory::LocalMemoryDriver;
use crate::managed::ManagedDriver;
use crate::mcp::McpDriver;
use crate::process::PlaneProfile;
use crate::recovery::RecoveryDriver;
use crate::rust_client::RustClientDriver;
use crate::streaming::{
    StreamingConsumerPath, StreamingPipelinePath, StreamingProducerPath, is_c2_driver,
};

const AUTHORITATIVE_STREAMING_REPETITIONS: u32 = 10;
const AUTHORITATIVE_STREAMING_DURATION_SECONDS: u64 = 120;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct SuiteManifest {
    pub schema_version: u32,
    pub name: String,
    pub authoritative: bool,
    pub provisioning: Provisioning,
    pub environment: Environment,
    pub scenarios: Vec<Scenario>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Provisioning {
    pub mode: ProvisionMode,
    pub cpu_target: String,
    #[serde(default)]
    pub iggy_root: Option<PathBuf>,
    #[serde(default)]
    pub plane_root: Option<PathBuf>,
    #[serde(default)]
    pub iggy_server: Option<PathBuf>,
    #[serde(default)]
    pub iggy_bench: Option<PathBuf>,
    #[serde(default)]
    pub plane: Option<PathBuf>,
    #[serde(default)]
    pub cache_root: Option<PathBuf>,
    #[serde(default)]
    pub iggy_server_version: Option<String>,
    #[serde(default)]
    pub iggy_bench_version: Option<String>,
    #[serde(default)]
    pub plane_version: Option<String>,
    #[serde(default)]
    pub compose_file: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProvisionMode {
    Source,
    Path,
    Artifact,
    Compose,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[strum(
    serialize_all = "snake_case",
    parse_err_ty = BenchError,
    parse_err_fn = invalid_transport
)]
pub enum Transport {
    TcpVsr,
}

fn invalid_transport(value: &str) -> BenchError {
    BenchError::Invalid(format!("unsupported benchmark transport `{value}`"))
}

#[derive(
    Clone, Copy, Debug, Deserialize, Display, EnumString, IntoStaticStr, Serialize, PartialEq, Eq,
)]
pub enum BenchmarkLayer {
    #[serde(rename = "L1")]
    #[strum(serialize = "L1")]
    L1,
    #[serde(rename = "L2")]
    #[strum(serialize = "L2")]
    L2,
    #[serde(rename = "L3")]
    #[strum(serialize = "L3")]
    L3,
    #[serde(rename = "L4")]
    #[strum(serialize = "L4")]
    L4,
    #[serde(rename = "L5")]
    #[strum(serialize = "L5")]
    L5,
    #[serde(rename = "L6")]
    #[strum(serialize = "L6")]
    L6,
    #[serde(rename = "L7")]
    #[strum(serialize = "L7")]
    L7,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Environment {
    pub tier: String,
    pub durability_profile: String,
    pub cache_state: String,
    #[serde(default)]
    pub plane_profile: PlaneProfile,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres_dsn_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres_ssl_mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres_pool_connections: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres_schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres_timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub postgres_pid_env: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<HostRequirements>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct Scenario {
    pub name: String,
    pub layer: BenchmarkLayer,
    pub arm: String,
    pub driver: String,
    pub transport: Transport,
    pub repetitions: u32,
    pub warmup_seconds: u64,
    pub duration_seconds: u64,
    pub payload_bytes: usize,
    pub batch_size: usize,
    pub partitions: u32,
    pub producers: u32,
    pub consumers: u32,
    pub operations: u64,
    #[serde(default)]
    pub history_messages: Option<u64>,
    #[serde(default)]
    pub context_limit: Option<usize>,
    #[serde(default)]
    pub corpus_entries: Option<u64>,
    #[serde(default)]
    pub vector_dimensions: Option<usize>,
    #[serde(default)]
    pub offered_rate: Option<u64>,
    #[serde(default)]
    pub offered_rates: Vec<u64>,
    #[serde(default)]
    pub spin_dispatch: bool,
    #[serde(default)]
    pub timeout_millis: Option<u64>,
    #[serde(default)]
    pub max_in_flight: Option<usize>,
}

impl SuiteManifest {
    /// Load and validate a suite manifest.
    ///
    /// # Errors
    ///
    /// Returns an error when the file cannot be read, decoded, or validated.
    pub fn load(path: &Path) -> Result<Self, BenchError> {
        let source = fs::read_to_string(path).map_err(|source| BenchError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let manifest: Self = toml::from_str(&source)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Validate required provisioning and scenario fields.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported schema or an incomplete suite definition.
    pub fn validate(&self) -> Result<(), BenchError> {
        self.validate_suite_shape()?;
        self.validate_provisioning()?;
        self.validate_environment()?;
        let mut names = BTreeSet::new();
        for scenario in &self.scenarios {
            validate_scenario_shape(scenario, &mut names)?;
            validate_driver_for_layer(scenario)?;
            validate_offered_rate(scenario)?;
            validate_context_fetch_fields(scenario)?;
            validate_managed_fields(scenario)?;
            validate_local_memory_fields(scenario)?;
            self.validate_mcp_fields(scenario)?;
        }
        self.validate_authoritative_streaming()?;
        Ok(())
    }

    /// Expand offered-rate sweeps into independently reported scenario cells.
    #[must_use]
    pub fn expanded_scenarios(&self) -> Vec<Scenario> {
        self.scenarios
            .iter()
            .flat_map(|scenario| {
                if scenario.offered_rates.is_empty() {
                    return vec![scenario.clone()];
                }
                scenario
                    .offered_rates
                    .iter()
                    .map(|rate| {
                        let mut cell = scenario.clone();
                        cell.name = format!("{}-rate-{rate}", scenario.name);
                        cell.offered_rate = Some(*rate);
                        cell.offered_rates.clear();
                        cell
                    })
                    .collect()
            })
            .collect()
    }

    /// Whether any declared scenario requires plane.
    #[must_use]
    pub fn requires_plane(&self) -> bool {
        self.scenarios.iter().any(|scenario| {
            (scenario.layer == BenchmarkLayer::L4
                && scenario.driver.parse::<LocalMemoryDriver>().is_err())
                || scenario.name.starts_with("managed-")
                || scenario
                    .driver
                    .parse::<RecoveryDriver>()
                    .is_ok_and(RecoveryDriver::requires_plane)
        })
    }

    fn validate_suite_shape(&self) -> Result<(), BenchError> {
        if self.schema_version != 1 {
            return Err(BenchError::Invalid(format!(
                "unsupported suite schema version {}",
                self.schema_version
            )));
        }
        if self.name.trim().is_empty() || self.scenarios.is_empty() {
            return Err(BenchError::Invalid(
                "suite name and at least one scenario are required".to_owned(),
            ));
        }
        if self.provisioning.cpu_target.trim().is_empty() {
            return Err(BenchError::Invalid("CPU target is required".to_owned()));
        }
        Ok(())
    }

    fn validate_provisioning(&self) -> Result<(), BenchError> {
        match self.provisioning.mode {
            ProvisionMode::Source => {
                if self.provisioning.iggy_root.is_none() {
                    return Err(BenchError::Invalid(
                        "source mode requires iggy_root".to_owned(),
                    ));
                }
                if self.requires_plane() && self.provisioning.plane_root.is_none() {
                    return Err(BenchError::Invalid(
                        "managed source mode requires plane_root".to_owned(),
                    ));
                }
            }
            ProvisionMode::Path => {
                if self.provisioning.iggy_server.is_none() || self.provisioning.iggy_bench.is_none()
                {
                    return Err(BenchError::Invalid(
                        "path mode requires iggy_server and iggy_bench".to_owned(),
                    ));
                }
                if self.requires_plane() && self.provisioning.plane.is_none() {
                    return Err(BenchError::Invalid(
                        "managed path mode requires plane".to_owned(),
                    ));
                }
            }
            ProvisionMode::Artifact => {
                if self.provisioning.iggy_server_version.is_none()
                    || self.provisioning.iggy_bench_version.is_none()
                {
                    return Err(BenchError::Invalid(
                        "artifact mode requires iggy_server_version and iggy_bench_version"
                            .to_owned(),
                    ));
                }
                if self.requires_plane() && self.provisioning.plane_version.is_none() {
                    return Err(BenchError::Invalid(
                        "managed artifact mode requires plane_version".to_owned(),
                    ));
                }
            }
            ProvisionMode::Compose => {
                if self.provisioning.compose_file.is_none() {
                    return Err(BenchError::Invalid(
                        "compose mode requires compose_file".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn validate_environment(&self) -> Result<(), BenchError> {
        if self.authoritative && self.environment.host.is_none() {
            return Err(BenchError::Invalid(
                "authoritative suites require explicit host controls".to_owned(),
            ));
        }
        match self.environment.plane_profile {
            PlaneProfile::Embedded => Ok(()),
            PlaneProfile::Postgres => {
                let dsn_env = self
                    .environment
                    .postgres_dsn_env
                    .as_deref()
                    .filter(|value| !value.trim().is_empty());
                let ssl_mode = self
                    .environment
                    .postgres_ssl_mode
                    .as_deref()
                    .filter(|value| !value.trim().is_empty());
                if dsn_env.is_none()
                    || ssl_mode.is_none()
                    || self.environment.postgres_pool_connections == Some(0)
                {
                    return Err(BenchError::Invalid(
                        "postgres plane profile requires postgres_dsn_env, postgres_ssl_mode, and a nonzero pool when configured"
                            .to_owned(),
                    ));
                }
                Ok(())
            }
        }
    }

    fn validate_authoritative_streaming(&self) -> Result<(), BenchError> {
        if !self.authoritative {
            return Ok(());
        }
        if !self
            .scenarios
            .iter()
            .any(|scenario| is_c2_driver(&scenario.driver))
        {
            return Ok(());
        }
        let calibration = self.scenarios.iter().any(|scenario| {
            matches!(
                scenario.driver.parse::<StreamingProducerPath>(),
                Ok(StreamingProducerPath::StreamDirectAa)
            )
        });
        if !calibration {
            return Err(BenchError::Invalid(
                "authoritative direct-streaming suites require a stream_direct_aa calibration scenario"
                    .to_owned(),
            ));
        }
        for scenario in self.scenarios.iter().filter(|scenario| {
            is_c2_driver(&scenario.driver)
                || matches!(
                    scenario.driver.parse::<StreamingProducerPath>(),
                    Ok(StreamingProducerPath::StreamDirectAa)
                )
        }) {
            if scenario.repetitions < AUTHORITATIVE_STREAMING_REPETITIONS
                || scenario.duration_seconds < AUTHORITATIVE_STREAMING_DURATION_SECONDS
            {
                return Err(BenchError::Invalid(format!(
                    "authoritative streaming scenario `{}` requires at least {} repetitions and {} seconds per arm",
                    scenario.name,
                    AUTHORITATIVE_STREAMING_REPETITIONS,
                    AUTHORITATIVE_STREAMING_DURATION_SECONDS
                )));
            }
        }
        Ok(())
    }

    fn validate_mcp_fields(&self, scenario: &Scenario) -> Result<(), BenchError> {
        if !matches!(
            scenario.driver.parse::<McpDriver>(),
            Ok(McpDriver::McpGuaranteed | McpDriver::McpGuaranteedRecovery | McpDriver::McpTriage)
        ) {
            return Ok(());
        }
        if self
            .environment
            .postgres_dsn_env
            .as_deref()
            .is_none_or(|name| name.trim().is_empty())
        {
            return Err(BenchError::Invalid(format!(
                "guarantee-matched MCP scenario `{}` requires environment.postgres_dsn_env",
                scenario.name
            )));
        }
        if matches!(
            scenario.driver.parse::<McpDriver>(),
            Ok(McpDriver::McpTriage)
        ) && scenario.offered_rate.is_none()
        {
            return Err(BenchError::Invalid(format!(
                "MCP triage scenario `{}` requires offered_rate for shared trace replay",
                scenario.name
            )));
        }
        if self.authoritative
            && matches!(
                scenario.driver.parse::<McpDriver>(),
                Ok(McpDriver::McpGuaranteed | McpDriver::McpTriage)
            )
            && self
                .environment
                .postgres_pid_env
                .as_deref()
                .is_none_or(|name| name.trim().is_empty())
        {
            return Err(BenchError::Invalid(format!(
                "authoritative guarantee-matched MCP scenario `{}` requires environment.postgres_pid_env",
                scenario.name
            )));
        }
        Ok(())
    }
}

fn validate_local_memory_fields(scenario: &Scenario) -> Result<(), BenchError> {
    if scenario.driver.parse::<LocalMemoryDriver>().is_err() {
        return Ok(());
    }
    if scenario.layer != BenchmarkLayer::L4
        || scenario.corpus_entries.is_none_or(|entries| entries == 0)
        || scenario
            .vector_dimensions
            .is_none_or(|dimensions| dimensions == 0)
    {
        return Err(BenchError::Invalid(format!(
            "local memory scenario `{}` requires layer L4, nonzero corpus_entries, and nonzero vector_dimensions",
            scenario.name
        )));
    }
    Ok(())
}

fn validate_driver_for_layer(scenario: &Scenario) -> Result<(), BenchError> {
    let supported = match scenario.layer {
        BenchmarkLayer::L1 => scenario.driver.parse::<IggyBenchmarkKind>().is_ok(),
        BenchmarkLayer::L2 => {
            scenario.driver.parse::<StreamingProducerPath>().is_ok()
                || scenario.driver.parse::<StreamingConsumerPath>().is_ok()
                || scenario.driver.parse::<StreamingPipelinePath>().is_ok()
        }
        BenchmarkLayer::L3 => scenario.driver.parse::<AgdxDriver>().is_ok(),
        BenchmarkLayer::L4 => {
            scenario.driver.parse::<ManagedDriver>().is_ok()
                || scenario.driver.parse::<LocalMemoryDriver>().is_ok()
        }
        BenchmarkLayer::L5 => scenario.driver.parse::<McpDriver>().is_ok(),
        BenchmarkLayer::L6 => scenario.driver.parse::<RustClientDriver>().is_ok(),
        BenchmarkLayer::L7 => scenario.driver.parse::<RecoveryDriver>().is_ok(),
    };
    if supported {
        Ok(())
    } else {
        Err(BenchError::Invalid(format!(
            "scenario `{}` uses unsupported driver `{}` for layer {}",
            scenario.name, scenario.driver, scenario.layer
        )))
    }
}

fn validate_scenario_shape(
    scenario: &Scenario,
    names: &mut BTreeSet<String>,
) -> Result<(), BenchError> {
    if scenario.name.trim().is_empty()
        || !names.insert(scenario.name.clone())
        || scenario.transport != Transport::TcpVsr
        || scenario.driver.trim().is_empty()
        || scenario.repetitions == 0
        || scenario.duration_seconds == 0
        || scenario.payload_bytes == 0
        || scenario.batch_size == 0
        || scenario.partitions == 0
        || scenario.producers == 0
        || scenario.operations == 0
    {
        return Err(BenchError::Invalid(format!(
            "scenario `{}` must have a unique name, use tcp_vsr, and have no zero or empty required field",
            scenario.name
        )));
    }
    Ok(())
}

fn validate_offered_rate(scenario: &Scenario) -> Result<(), BenchError> {
    if scenario.offered_rate == Some(0)
        || scenario.offered_rates.contains(&0)
        || (scenario.offered_rate.is_some() && !scenario.offered_rates.is_empty())
        || scenario
            .offered_rates
            .windows(2)
            .any(|window| window[0] >= window[1])
    {
        return Err(BenchError::Invalid(format!(
            "scenario `{}` must declare either one offered_rate or a strictly increasing nonzero offered_rates sweep",
            scenario.name
        )));
    }
    Ok(())
}

fn validate_context_fetch_fields(scenario: &Scenario) -> Result<(), BenchError> {
    if scenario.driver == "context_fetch"
        && (scenario.history_messages == Some(0)
            || scenario.history_messages.is_none()
            || scenario.context_limit == Some(0)
            || scenario.context_limit.is_none())
    {
        return Err(BenchError::Invalid(format!(
            "context-fetch scenario `{}` requires nonzero history_messages and context_limit",
            scenario.name
        )));
    }
    Ok(())
}

fn validate_managed_fields(scenario: &Scenario) -> Result<(), BenchError> {
    if scenario.driver == "kv"
        && scenario.arm == "scan_page"
        && scenario.corpus_entries.is_none_or(|entries| entries == 0)
    {
        return Err(BenchError::Invalid(format!(
            "KV scan scenario `{}` requires nonzero corpus_entries",
            scenario.name
        )));
    }
    if scenario.driver == "query" && scenario.corpus_entries.is_none_or(|entries| entries == 0) {
        return Err(BenchError::Invalid(format!(
            "query scenario `{}` requires nonzero corpus_entries",
            scenario.name
        )));
    }
    if scenario.driver == "memory"
        && scenario.arm == "folded_recall"
        && scenario.corpus_entries.is_none_or(|entries| entries == 0)
    {
        return Err(BenchError::Invalid(format!(
            "folded memory recall scenario `{}` requires nonzero corpus_entries",
            scenario.name
        )));
    }
    if scenario.driver == "fork"
        && scenario.arm == "base_size"
        && scenario.corpus_entries.is_none_or(|entries| entries == 0)
    {
        return Err(BenchError::Invalid(format!(
            "fork scenario `{}` requires nonzero corpus_entries",
            scenario.name
        )));
    }
    if scenario.driver == "graph"
        && scenario.arm == "vector_start"
        && scenario.corpus_entries.is_none_or(|entries| entries == 0)
    {
        return Err(BenchError::Invalid(format!(
            "graph vector scenario `{}` requires nonzero corpus_entries",
            scenario.name
        )));
    }
    if scenario.driver.parse::<RecoveryDriver>().is_ok()
        && (scenario.layer != BenchmarkLayer::L7 || scenario.operations == 0)
    {
        return Err(BenchError::Invalid(format!(
            "recovery scenario `{}` requires layer L7 and nonzero operations",
            scenario.name
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::managed::KvArm;

    #[test]
    fn given_rate_sweep_when_expanded_then_should_create_named_independent_cells() {
        let mut manifest = SuiteManifest::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/suite-minimal.toml"),
        )
        .expect("fixture suite should load");
        manifest.scenarios[0].offered_rate = None;
        manifest.scenarios[0].offered_rates = vec![1_000, 2_000];
        manifest.validate().expect("rate sweep should validate");
        let expanded = manifest.expanded_scenarios();
        assert_eq!(expanded.len(), 2);
        assert_eq!(expanded[0].name, "stream_direct-rate-1000");
        assert_eq!(expanded[1].offered_rate, Some(2_000));
        assert!(expanded[1].offered_rates.is_empty());
    }

    #[test]
    fn given_ambiguous_rate_declaration_when_validated_then_should_reject_it() {
        let mut manifest = SuiteManifest::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/suite-minimal.toml"),
        )
        .expect("fixture suite should load");
        manifest.scenarios[0].offered_rates = vec![2_000];
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn given_unknown_driver_when_validated_then_should_reject_it() {
        let mut manifest = minimal_manifest();
        manifest.scenarios[0].driver = "unknown_driver".to_owned();

        let error = manifest
            .validate()
            .expect_err("an unknown driver should fail before provisioning");
        assert!(error.to_string().contains("unsupported driver"));
    }

    #[test]
    fn given_driver_on_wrong_layer_when_validated_then_should_reject_it() {
        let mut manifest = minimal_manifest();
        manifest.scenarios[0].layer = BenchmarkLayer::L3;

        let error = manifest
            .validate()
            .expect_err("a driver on the wrong layer should fail before provisioning");
        assert!(error.to_string().contains("unsupported driver"));
    }

    #[test]
    fn given_postgres_profile_without_connection_reference_when_validated_then_should_reject_it() {
        let mut manifest = SuiteManifest::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/suite-minimal.toml"),
        )
        .expect("fixture suite should load");
        manifest.environment.plane_profile = PlaneProfile::Postgres;
        assert!(manifest.validate().is_err());

        manifest.environment.postgres_dsn_env = Some("BENCH_POSTGRES_DSN".to_owned());
        manifest.environment.postgres_ssl_mode = Some("verify-full".to_owned());
        manifest
            .validate()
            .expect("complete Postgres profile should validate");
    }

    #[test]
    fn given_recovery_scenario_when_checked_then_should_require_plane() {
        let mut manifest = SuiteManifest::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/suite-minimal.toml"),
        )
        .expect("fixture suite should load");
        manifest.scenarios[0].layer = BenchmarkLayer::L7;
        manifest.scenarios[0].driver = "plane_restart_memory".to_owned();
        manifest.scenarios[0].arm = "plane_restart".to_owned();
        assert!(manifest.requires_plane());
        manifest
            .validate()
            .expect("recovery scenario should validate");
    }

    #[test]
    fn given_local_memory_scenario_when_validated_then_should_not_require_plane() {
        let mut manifest = minimal_manifest();
        manifest.scenarios[0].layer = BenchmarkLayer::L4;
        manifest.scenarios[0].driver = LocalMemoryDriver::VectorMemoryRecall.to_string();
        manifest.scenarios[0].arm = "in_process".to_owned();
        manifest.scenarios[0].corpus_entries = Some(1_000);
        manifest.scenarios[0].vector_dimensions = Some(384);

        manifest
            .validate()
            .expect("complete local memory scenario should validate");
        assert!(!manifest.requires_plane());
    }

    #[test]
    fn given_local_memory_without_dimensions_when_validated_then_should_reject_it() {
        let mut manifest = minimal_manifest();
        manifest.scenarios[0].layer = BenchmarkLayer::L4;
        manifest.scenarios[0].driver = LocalMemoryDriver::VectorMemoryRemember.to_string();
        manifest.scenarios[0].arm = "in_process".to_owned();
        manifest.scenarios[0].corpus_entries = Some(1_000);

        assert!(manifest.validate().is_err());
    }

    #[test]
    fn given_authoritative_direct_streaming_without_aa_when_validated_then_should_reject_it() {
        let mut manifest = minimal_manifest();
        manifest.authoritative = true;
        manifest.scenarios[0].repetitions = AUTHORITATIVE_STREAMING_REPETITIONS;
        manifest.scenarios[0].duration_seconds = AUTHORITATIVE_STREAMING_DURATION_SECONDS;

        assert!(manifest.validate().is_err());
    }

    #[test]
    fn given_short_authoritative_aa_when_validated_then_should_reject_it() {
        let mut manifest = authoritative_streaming_manifest();
        manifest.scenarios[1].duration_seconds = 30;

        assert!(manifest.validate().is_err());
    }

    #[test]
    fn given_complete_authoritative_aa_when_validated_then_should_accept_it() {
        authoritative_streaming_manifest()
            .validate()
            .expect("authoritative streaming calibration should validate");
    }

    #[test]
    fn given_short_authoritative_consumer_when_validated_then_should_reject_it() {
        let mut manifest = authoritative_streaming_manifest();
        let mut consumer = manifest.scenarios[0].clone();
        consumer.name = "stream-consumer-partition".to_owned();
        consumer.driver = StreamingConsumerPath::StreamConsumerPartition.to_string();
        consumer.duration_seconds = 30;
        manifest.scenarios.push(consumer);

        assert!(manifest.validate().is_err());
    }

    #[test]
    fn given_complete_authoritative_consumer_when_validated_then_should_accept_it() {
        let mut manifest = authoritative_streaming_manifest();
        let mut consumer = manifest.scenarios[0].clone();
        consumer.name = "stream-consumer-partition".to_owned();
        consumer.driver = StreamingConsumerPath::StreamConsumerPartition.to_string();
        manifest.scenarios.push(consumer);

        manifest
            .validate()
            .expect("authoritative consumer comparison should use the C2 campaign contract");
    }

    #[test]
    fn given_signed_plane_when_managed_artifact_suite_is_validated_then_should_accept_it() {
        let mut manifest = minimal_manifest();
        manifest.provisioning.mode = ProvisionMode::Artifact;
        manifest.provisioning.iggy_server_version = Some("server-version".to_owned());
        manifest.provisioning.iggy_bench_version = Some("bench-version".to_owned());
        manifest.provisioning.plane_version = Some("plane-version".to_owned());
        manifest.scenarios[0].layer = BenchmarkLayer::L4;
        manifest.scenarios[0].driver = ManagedDriver::Kv.to_string();
        manifest.scenarios[0].arm = KvArm::GetHit.to_string();

        manifest
            .validate()
            .expect("a signed plane release should satisfy artifact provisioning");
    }

    #[test]
    fn given_managed_artifact_suite_without_plane_version_when_validated_then_should_reject_it() {
        let mut manifest = minimal_manifest();
        manifest.provisioning.mode = ProvisionMode::Artifact;
        manifest.provisioning.iggy_server_version = Some("server-version".to_owned());
        manifest.provisioning.iggy_bench_version = Some("bench-version".to_owned());
        manifest.scenarios[0].layer = BenchmarkLayer::L4;
        manifest.scenarios[0].driver = ManagedDriver::Kv.to_string();
        manifest.scenarios[0].arm = KvArm::GetHit.to_string();

        let error = manifest
            .validate()
            .expect_err("managed artifact provisioning should require a plane version");
        assert!(error.to_string().contains("plane_version"));
    }

    #[test]
    fn given_authoritative_guaranteed_mcp_without_postgres_pid_when_validated_then_should_reject_it()
     {
        let mut manifest = minimal_manifest();
        manifest.authoritative = true;
        manifest.environment.host = Some(authoritative_host_requirements());
        manifest.environment.postgres_dsn_env = Some("BENCH_POSTGRES_DSN".to_owned());
        manifest.scenarios[0].layer = BenchmarkLayer::L5;
        manifest.scenarios[0].driver = McpDriver::McpGuaranteed.to_string();
        manifest.scenarios[0].arm = "guarantee_matched_mcp".to_owned();
        manifest.scenarios[0].batch_size = 1;
        manifest.scenarios[0].transport = Transport::TcpVsr;

        assert!(manifest.validate().is_err());

        manifest.environment.postgres_pid_env = Some("BENCH_POSTGRES_PID".to_owned());
        manifest
            .validate()
            .expect("authoritative guaranteed MCP should require and accept a postgres pid env");
    }

    #[test]
    fn given_authoritative_triage_without_postgres_pid_when_validated_then_should_reject_it() {
        let mut manifest = minimal_manifest();
        manifest.authoritative = true;
        manifest.environment.host = Some(authoritative_host_requirements());
        manifest.environment.postgres_dsn_env = Some("BENCH_POSTGRES_DSN".to_owned());
        manifest.scenarios[0].layer = BenchmarkLayer::L5;
        manifest.scenarios[0].driver = McpDriver::McpTriage.to_string();
        manifest.scenarios[0].arm = "triage".to_owned();
        manifest.scenarios[0].batch_size = 1;
        manifest.scenarios[0].offered_rate = Some(1_000);

        assert!(manifest.validate().is_err());

        manifest.environment.postgres_pid_env = Some("BENCH_POSTGRES_PID".to_owned());
        manifest
            .validate()
            .expect("authoritative MCP triage should require and accept a postgres pid env");
    }

    fn authoritative_streaming_manifest() -> SuiteManifest {
        let mut manifest = minimal_manifest();
        manifest.authoritative = true;
        manifest.environment.host = Some(authoritative_host_requirements());
        manifest.scenarios[0].repetitions = AUTHORITATIVE_STREAMING_REPETITIONS;
        manifest.scenarios[0].duration_seconds = AUTHORITATIVE_STREAMING_DURATION_SECONDS;
        let mut calibration = manifest.scenarios[0].clone();
        calibration.name = "stream-direct-aa".to_owned();
        calibration.arm = "raw_iggy_a_vs_raw_iggy_b".to_owned();
        calibration.driver = StreamingProducerPath::StreamDirectAa.to_string();
        manifest.scenarios.push(calibration);
        manifest
    }

    fn authoritative_host_requirements() -> HostRequirements {
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

    fn minimal_manifest() -> SuiteManifest {
        SuiteManifest::load(
            &Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/suite-minimal.toml"),
        )
        .expect("fixture suite should load")
    }
}
