use crate::destination::{BackendResourceId, FileFormat, TableFormat};
use crate::error::InvalidError;
use crate::query::{Consistency, SqlDialect};
use crate::schema::LogicalTypeKind;
use crate::topology::WireTopology;
use crate::validate::Validate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Capability feature bits advertised in [`OpVersions::features`]. Each constant
/// names one managed sub-feature a server serves beyond the base surface, so a
/// binary client feature-detects it (before attempting the op) the way the HTTP
/// surface reads the boolean flags on `Capabilities`. Additive and pinned
/// cross-repo: a new bit is set by a newer server and ignored by an older
/// client (which simply does not light up that capability).
pub mod feature {
    /// The key-value store serves compare-and-swap (`AGDX_KV_CAS`).
    pub const KV_CAS: u64 = 1 << 0;
    /// The query surface honors `Consistency::ReadYourWrites`.
    pub const READ_YOUR_WRITES: u64 = 1 << 1;
    /// The query surface honors `Consistency::Strong`.
    pub const STRONG_CONSISTENCY: u64 = 1 << 2;
    /// The key-value store serves fenced compare-and-swap (`AGDX_KV_CAS_FENCED`).
    pub const KV_CAS_FENCED: u64 = 1 << 3;
    /// The plane serves the agent and workflow control band (`AGDX_AGENT_*`).
    pub const AGENT_WORKFLOW: u64 = 1 << 4;
    /// The query surface serves lexical relevance search (`Query.text`).
    pub const KEYWORD_SEARCH: u64 = 1 << 5;
    /// The deployment publishes the change feed (`ChangeRecord`s on the
    /// changes topic) for bindings that opt into `notify`.
    pub const WATCH: u64 = 1 << 6;
    /// The streaming server serves the authorization control band (`AGDX_AUTHZ_*`).
    pub const AUTHZ: u64 = 1 << 7;
    /// The deployment serves destination and checkpoint lifecycle operations.
    pub const DESTINATIONS: u64 = 1 << 8;
    /// The key-value store serves the revocable fenced-lease contract at
    /// `KV_LEASE_OP_VERSION`: holder-scoped acquire, renewal
    /// (`AGDX_KV_LEASE_RENEW`), holder-and-fence-validated release, fenced
    /// compare-and-swap requiring a live lease, and the barriered read
    /// (`KvGet::min_position`). Subsumes [`KV_CAS_FENCED`]: a client must not
    /// send the reshaped lease ops (or a barriered read) to a server without
    /// this bit, which would silently decode them under the old contract.
    pub const KV_FENCED_LEASES: u64 = 1 << 9;
}

/// The wire op versions a server accepts, one per surface, plus the capability
/// feature bits it advertises. A pinned wire shape, mirrored by the HTTP
/// capabilities `versions` block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OpVersions {
    pub query: u32,
    pub control: u32,
    pub kv: u32,
    pub fork: u32,
    /// The agent envelope (AGDX) version LaserData Cloud consumes for its
    /// conversation projections. `0` means "not advertised" and is skipped on
    /// encode, so pre-AGDX hello frames stay byte-identical and decode unchanged.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub agent: u32,
    /// The knowledge-graph op version served. `0` means not served, skipped on
    /// encode so a pre-graph hello frame stays byte-identical. Mirrors the
    /// `managed_graph` HTTP capability flag. (Agentic memory rides this plus the
    /// query surface, so it has no op version of its own.)
    #[serde(default, skip_serializing_if = "is_zero")]
    pub graph: u32,
    /// Destination and checkpoint operation version. Zero means not served.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub checkpoint: u32,
    /// Capability feature bits (see [`feature`]): managed sub-features served
    /// beyond the base surface (compare-and-swap, read-your-writes, strong
    /// consistency). `0` (the default) is skipped on encode, so a pre-feature
    /// hello reply stays byte-identical and an old client just sees no extra
    /// capabilities.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub features: u64,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

impl OpVersions {
    /// Versions per surface. The struct is `#[non_exhaustive]` (new surfaces
    /// land without a breaking change), so this is the constructor.
    pub fn new(query: u32, control: u32, kv: u32, fork: u32) -> Self {
        Self {
            query,
            control,
            kv,
            fork,
            agent: 0,
            graph: 0,
            checkpoint: 0,
            features: 0,
        }
    }

    /// Returns a copy advertising this agent-envelope (AGDX) version.
    #[must_use]
    pub fn with_agent(mut self, agent: u32) -> Self {
        self.agent = agent;
        self
    }

    /// Returns a copy advertising the knowledge-graph op version served.
    #[must_use]
    pub fn with_graph(mut self, graph: u32) -> Self {
        self.graph = graph;
        self
    }

    /// Returns a copy advertising the destination and checkpoint version.
    #[must_use]
    pub fn with_checkpoint(mut self, checkpoint: u32) -> Self {
        self.checkpoint = checkpoint;
        self
    }

    /// Returns a copy advertising the capability feature bits in `features`
    /// (an OR of [`feature`] constants).
    #[must_use]
    pub fn with_features(mut self, features: u64) -> Self {
        self.features = features;
        self
    }

    /// Whether a [`feature`] bit (or set of bits) is advertised.
    pub const fn has_feature(&self, bit: u64) -> bool {
        self.features & bit == bit
    }
}

/// Body of the `AGDX_HELLO` probe reply: the wire op versions the server (and
/// its managed backend) accepts, mirroring the HTTP capabilities `versions`
/// block. A pinned wire shape. Pre-versioned
/// servers answer the probe with an empty body, which a client treats as "no
/// versions advertised", never an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct HelloReply {
    pub versions: OpVersions,
}

impl HelloReply {
    /// Constructor for the non-exhaustive wire struct.
    pub fn new(versions: OpVersions) -> Self {
        Self { versions }
    }
}

pub const BACKEND_DESCRIPTOR_VERSION: u32 = 1;

/// One observed backend resource and its structured, secret-free capabilities.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct BackendDescriptor {
    pub descriptor_version: u32,
    pub resource_id: BackendResourceId,
    pub mode: BackendMode,
    pub label: String,
    pub implementation: BackendImplementation,
    pub observed_backend_generation: u64,
    pub runtime_configuration_revision: u64,
    pub desired_state: BackendDesiredState,
    pub observed_state: BackendObservedState,
    pub readiness: BackendReadiness,
    pub materialization: Vec<MaterializationCapability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<QueryCapabilities>,
    pub schema: SchemaCapabilities,
    pub maintenance: MaintenanceCapabilities,
    pub limits: BackendLimits,
}

impl BackendDescriptor {
    pub fn new(
        resource_id: BackendResourceId,
        mode: BackendMode,
        label: impl Into<String>,
        implementation: BackendImplementation,
        observed_backend_generation: u64,
        runtime_configuration_revision: u64,
    ) -> Self {
        Self {
            descriptor_version: BACKEND_DESCRIPTOR_VERSION,
            resource_id,
            mode,
            label: label.into(),
            implementation,
            observed_backend_generation,
            runtime_configuration_revision,
            desired_state: BackendDesiredState::Disabled,
            observed_state: BackendObservedState::Disabled,
            readiness: BackendReadiness::not_ready(BackendReadinessCode::Disabled),
            materialization: Vec::new(),
            query: None,
            schema: SchemaCapabilities::default(),
            maintenance: MaintenanceCapabilities::default(),
            limits: BackendLimits::default(),
        }
    }

    #[must_use]
    pub fn with_state(
        mut self,
        desired_state: BackendDesiredState,
        observed_state: BackendObservedState,
        readiness: BackendReadiness,
    ) -> Self {
        self.desired_state = desired_state;
        self.observed_state = observed_state;
        self.readiness = readiness;
        self
    }

    #[must_use]
    pub fn with_materialization(mut self, capabilities: Vec<MaterializationCapability>) -> Self {
        self.materialization = capabilities;
        self
    }

    #[must_use]
    pub fn with_query(mut self, capabilities: QueryCapabilities) -> Self {
        self.query = Some(capabilities);
        self
    }

    #[must_use]
    pub fn with_schema(mut self, capabilities: SchemaCapabilities) -> Self {
        self.schema = capabilities;
        self
    }

    #[must_use]
    pub fn with_maintenance(mut self, capabilities: MaintenanceCapabilities) -> Self {
        self.maintenance = capabilities;
        self
    }

    #[must_use]
    pub fn with_limits(mut self, limits: BackendLimits) -> Self {
        self.limits = limits;
        self
    }
}

impl Validate for BackendDescriptor {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.descriptor_version != BACKEND_DESCRIPTOR_VERSION {
            return Err(InvalidError::new(format!(
                "backend descriptor version must be {BACKEND_DESCRIPTOR_VERSION}"
            )));
        }
        if self.resource_id.as_u128() == 0
            || self.observed_backend_generation == 0
            || self.runtime_configuration_revision == 0
        {
            return Err(InvalidError::new(
                "backend identity and observed revisions must be nonzero",
            ));
        }
        validate_descriptor_text("backend label", &self.label)?;
        validate_descriptor_text("backend implementation kind", &self.implementation.kind)?;
        validate_descriptor_text(
            "backend implementation version",
            &self.implementation.version,
        )?;
        self.readiness.validate()?;
        if self.readiness.ready && self.observed_state != BackendObservedState::Ready {
            return Err(InvalidError::new(
                "ready backend must report the ready observed state",
            ));
        }
        let mut materialization = BTreeSet::new();
        for capability in &self.materialization {
            let key = (capability.file_format as u8, capability.table_format as u8);
            if !materialization.insert(key) {
                return Err(InvalidError::new(
                    "backend repeats a materialization capability",
                ));
            }
        }
        if let Some(query) = &self.query {
            query.validate()?;
        }
        if self.readiness.ready {
            if self.schema.logical_schema && self.limits.max_schema_fields == 0 {
                return Err(InvalidError::new(
                    "ready logical-schema backend must advertise a schema-field limit",
                ));
            }
            if !self.materialization.is_empty() && self.limits.max_materialization_file_bytes == 0 {
                return Err(InvalidError::new(
                    "ready materialization backend must advertise a file-size limit",
                ));
            }
            if self.query.is_some()
                && (self.limits.max_query_rows == 0
                    || self.limits.max_query_bytes == 0
                    || self.limits.max_scan_bytes == 0
                    || self.limits.max_query_micros == 0
                    || self.limits.max_concurrent_queries == 0)
            {
                return Err(InvalidError::new(
                    "ready query backend must advertise nonzero query limits",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BackendMode {
    Operational,
    Lakehouse,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendImplementation {
    pub kind: String,
    pub version: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BackendDesiredState {
    Disabled,
    Enabled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BackendObservedState {
    Disabled,
    Starting,
    Ready,
    Degraded,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendReadiness {
    pub ready: bool,
    pub reasons: Vec<BackendReadinessReason>,
    pub observed_at_micros: u64,
}

impl BackendReadiness {
    pub fn not_ready(code: BackendReadinessCode) -> Self {
        Self {
            ready: false,
            reasons: vec![BackendReadinessReason { code, detail: None }],
            observed_at_micros: 0,
        }
    }

    pub fn ready(observed_at_micros: u64) -> Self {
        Self {
            ready: true,
            reasons: Vec::new(),
            observed_at_micros,
        }
    }
}

impl Validate for BackendReadiness {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.ready {
            if !self.reasons.is_empty() || self.observed_at_micros == 0 {
                return Err(InvalidError::new(
                    "ready backend requires an observation time and no failure reasons",
                ));
            }
            return Ok(());
        }
        if self.reasons.is_empty() {
            return Err(InvalidError::new(
                "not-ready backend requires at least one stable reason",
            ));
        }
        if self.reasons.len() > 32 {
            return Err(InvalidError::new(
                "backend readiness reason count exceeds cap 32",
            ));
        }
        for reason in &self.reasons {
            if let Some(detail) = &reason.detail {
                validate_descriptor_text("backend readiness detail", detail)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendReadinessReason {
    pub code: BackendReadinessCode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum BackendReadinessCode {
    Disabled,
    ConfigurationPending,
    ConfigurationRejected,
    CredentialUnavailable,
    ObjectStoreUnavailable,
    CatalogUnavailable,
    QueryRuntimeUnavailable,
    GenerationMismatch,
    ProbeFailed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterializationCapability {
    pub file_format: FileFormat,
    pub table_format: TableFormat,
    pub create_table: bool,
    pub append: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryCapabilities {
    pub dialects: Vec<SqlDialect>,
    pub time_travel: Vec<TimeTravelCapability>,
    pub consistency: Vec<Consistency>,
    pub logical_types: Vec<LogicalTypeKind>,
    pub paging: Vec<QueryPagingCapability>,
    pub cancellation: bool,
    pub execution_status: bool,
    pub raw_sql: bool,
}

impl Validate for QueryCapabilities {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.dialects.is_empty()
            || self.consistency.is_empty()
            || self.logical_types.is_empty()
            || self.paging.is_empty()
        {
            return Err(InvalidError::new(
                "query capabilities require dialect, consistency, and logical-type coverage",
            ));
        }
        ensure_unique(
            "query dialect",
            self.dialects.iter().map(|value| *value as u8),
        )?;
        ensure_unique(
            "query time-travel capability",
            self.time_travel.iter().map(|value| *value as u8),
        )?;
        ensure_unique(
            "query consistency",
            self.consistency.iter().map(|value| *value as u8),
        )?;
        ensure_unique(
            "query logical type",
            self.logical_types.iter().map(|value| *value as u8),
        )?;
        ensure_unique(
            "query paging capability",
            self.paging.iter().map(|value| *value as u8),
        )?;
        if self.raw_sql && self.dialects.is_empty() {
            return Err(InvalidError::new(
                "raw SQL capability requires an explicit dialect",
            ));
        }
        Ok(())
    }
}

fn ensure_unique(label: &str, values: impl IntoIterator<Item = u8>) -> Result<(), InvalidError> {
    let mut seen = BTreeSet::new();
    if values.into_iter().all(|value| seen.insert(value)) {
        Ok(())
    } else {
        Err(InvalidError::new(format!("backend repeats a {label}")))
    }
}

fn validate_descriptor_text(label: &str, value: &str) -> Result<(), InvalidError> {
    if value.is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
        return Err(InvalidError::new(format!(
            "{label} must contain 1..=4096 bytes without control characters"
        )));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TimeTravelCapability {
    SnapshotId,
    TimestampMicros,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum QueryPagingCapability {
    Offset,
    Cursor,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaCapabilities {
    pub logical_schema: bool,
    pub arrow_ipc_stream: bool,
    pub schema_evolution: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaintenanceCapabilities {
    pub expire_snapshots: bool,
    pub remove_orphan_files: bool,
    pub compact_data_files: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendLimits {
    pub max_query_rows: u64,
    pub max_query_bytes: u64,
    pub max_scan_bytes: u64,
    pub max_query_micros: u64,
    pub max_concurrent_queries: u32,
    pub max_schema_fields: u32,
    pub max_materialization_file_bytes: u64,
}

/// The managed backend's capability announcement to the streaming server, sent over their
/// private socket on connect (`AGDX_BACKEND_HELLO_CODE`). The streaming server caches the
/// `versions` and the advertised `backends`, and relays them verbatim when it answers a
/// client `AGDX_HELLO` / capabilities probe, so the streaming server never hardcodes feature
/// bits or backend identities the backend may or may not serve.
/// This makes the backend the single source of its own capability truth and
/// keeps the binary `features` bitset and the HTTP capability flags in agreement
/// with what is actually served. A separate type from [`HelloReply`] because the
/// direction and sender differ (backend to streaming server, not server to client).
const fn backend_ready_by_default() -> bool {
    true
}

const fn backend_is_ready(ready: &bool) -> bool {
    *ready
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct BackendAnnounce {
    pub versions: OpVersions,
    #[serde(
        default = "backend_ready_by_default",
        skip_serializing_if = "backend_is_ready"
    )]
    pub ready: bool,
    /// Materialization backends the server currently exposes (the ones it has
    /// open). A client routes only to an advertised id. Empty (the default) is
    /// skipped on encode, so a pre-backends announce stays byte-identical and an
    /// older reader simply sees no advertised backends.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backends: Vec<BackendDescriptor>,
    /// The stream/topic names this deployment uses. Absent (the default) is
    /// skipped on encode, so a pre-topology announce stays byte-identical and
    /// an older reader sees no advertised topology (falls back to its
    /// own [`WireTopology::default`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topology: Option<WireTopology>,
}

impl BackendAnnounce {
    /// Constructor for the non-exhaustive wire struct.
    pub fn new(versions: OpVersions) -> Self {
        Self {
            versions,
            ready: true,
            backends: Vec::new(),
            topology: None,
        }
    }

    #[must_use]
    pub const fn unavailable(mut self) -> Self {
        self.ready = false;
        self
    }

    /// Returns a copy advertising `backends`.
    #[must_use]
    pub fn with_backends(mut self, backends: Vec<BackendDescriptor>) -> Self {
        self.backends = backends;
        self
    }

    /// Returns a copy advertising the deployment's `topology`.
    #[must_use]
    pub fn with_topology(mut self, topology: WireTopology) -> Self {
        self.topology = Some(topology);
        self
    }
}

impl Validate for BackendAnnounce {
    fn validate(&self) -> Result<(), InvalidError> {
        let mut resources = BTreeSet::new();
        for backend in &self.backends {
            backend.validate()?;
            if !resources.insert(backend.resource_id) {
                return Err(InvalidError::new(
                    "backend announcement repeats a resource id",
                ));
            }
        }
        if self.ready
            && !self.backends.is_empty()
            && !self.backends.iter().any(|backend| backend.readiness.ready)
        {
            return Err(InvalidError::new(
                "ready backend announcement must contain a ready backend",
            ));
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::codes::{CONTROL_OP_VERSION, FORK_OP_VERSION, KV_OP_VERSION, QUERY_OP_VERSION};
    use crate::framing::{decode_named, encode_named};

    fn backend(resource_id: u128, mode: BackendMode, label: &str, kind: &str) -> BackendDescriptor {
        BackendDescriptor::new(
            BackendResourceId::from_u128(resource_id),
            mode,
            label,
            BackendImplementation {
                kind: kind.to_owned(),
                version: "1.0.0".to_owned(),
            },
            1,
            1,
        )
    }

    #[test]
    fn given_a_hello_reply_when_round_tripped_then_should_preserve_versions() {
        // The pinned `HelloReply` shape (CBOR named fields). The connect-time
        // probe decodes exactly this shape.
        let reply = HelloReply::new(OpVersions::new(
            QUERY_OP_VERSION,
            CONTROL_OP_VERSION,
            KV_OP_VERSION,
            FORK_OP_VERSION,
        ));
        let bytes = encode_named(&reply).expect("hello reply serializes");
        let back: HelloReply = decode_named(&bytes).expect("hello reply deserializes");
        assert_eq!(back, reply);
    }

    #[test]
    fn given_a_backend_announce_when_round_tripped_then_should_preserve_features() {
        let announce = BackendAnnounce::new(
            OpVersions::new(
                QUERY_OP_VERSION,
                CONTROL_OP_VERSION,
                KV_OP_VERSION,
                FORK_OP_VERSION,
            )
            .with_features(feature::KV_CAS | feature::READ_YOUR_WRITES),
        );
        let bytes = encode_named(&announce).expect("serializes");
        let back: BackendAnnounce = decode_named(&bytes).expect("deserializes");
        assert_eq!(back, announce);
        assert!(back.versions.has_feature(feature::KV_CAS));
    }

    #[test]
    fn given_an_unavailable_backend_announce_when_round_tripped_then_should_stay_unavailable() {
        let announce = BackendAnnounce::new(OpVersions::new(
            QUERY_OP_VERSION,
            CONTROL_OP_VERSION,
            KV_OP_VERSION,
            FORK_OP_VERSION,
        ))
        .unavailable();
        let bytes = encode_named(&announce).expect("serializes");
        let back: BackendAnnounce = decode_named(&bytes).expect("deserializes");
        assert!(!back.ready);
    }

    #[test]
    fn given_an_empty_hello_body_when_decoded_then_should_yield_no_versions() {
        // Pre-versioned servers answer the probe with an empty body. The probe
        // treats a failed decode as "no versions advertised", never an error.
        assert!(decode_named::<HelloReply>(&[]).is_err());
    }

    #[test]
    fn given_advertised_backends_when_round_tripped_then_should_preserve_them_and_skip_empty() {
        let announce = BackendAnnounce::new(OpVersions::new(
            QUERY_OP_VERSION,
            CONTROL_OP_VERSION,
            KV_OP_VERSION,
            FORK_OP_VERSION,
        ))
        .with_backends(vec![
            backend(1, BackendMode::Operational, "Embedded", "embedded"),
            backend(2, BackendMode::Lakehouse, "Analytics warehouse", "columnar"),
        ]);
        let bytes = encode_named(&announce).expect("encodes");
        let back: BackendAnnounce = decode_named(&bytes).expect("decodes");
        assert_eq!(back, announce);
        assert_eq!(back.backends.len(), 2);
        assert_eq!(
            back.backends[1].resource_id,
            BackendResourceId::from_u128(2)
        );
        assert_eq!(back.backends[1].implementation.kind, "columnar");
        assert_eq!(back.backends[1].label, "Analytics warehouse");

        // No advertised backends (the default) is omitted on the wire, so a
        // pre-backends announce stays byte-identical.
        let plain = BackendAnnounce::new(OpVersions::new(1, 1, 1, 1));
        let json = serde_json::to_string(&plain).expect("json");
        assert!(!json.contains("backends"), "empty backends omitted: {json}");
    }

    #[test]
    fn given_announce_without_topology_when_decoded_then_should_default_none() {
        let announce = BackendAnnounce::new(OpVersions::new(
            QUERY_OP_VERSION,
            CONTROL_OP_VERSION,
            KV_OP_VERSION,
            FORK_OP_VERSION,
        ));
        let bytes = encode_named(&announce).expect("encodes");
        let back: BackendAnnounce = decode_named(&bytes).expect("decodes");
        assert_eq!(back.topology, None);
        let json = serde_json::to_string(&announce).expect("json");
        assert!(
            !json.contains("topology"),
            "absent topology omitted: {json}"
        );
    }

    #[test]
    fn given_announce_with_topology_when_round_tripped_then_should_preserve_it() {
        let custom = WireTopology {
            ops_stream: "custom-ops".to_owned(),
            ..WireTopology::default()
        };
        let announce = BackendAnnounce::new(OpVersions::new(
            QUERY_OP_VERSION,
            CONTROL_OP_VERSION,
            KV_OP_VERSION,
            FORK_OP_VERSION,
        ))
        .with_topology(custom.clone());
        let bytes = encode_named(&announce).expect("encodes");
        let back: BackendAnnounce = decode_named(&bytes).expect("decodes");
        assert_eq!(back.topology, Some(custom));
    }

    #[test]
    fn given_advertised_features_when_round_tripped_then_should_preserve_bits_and_skip_zero() {
        let versions = OpVersions::new(
            QUERY_OP_VERSION,
            CONTROL_OP_VERSION,
            KV_OP_VERSION,
            FORK_OP_VERSION,
        )
        .with_features(feature::KV_CAS | feature::READ_YOUR_WRITES);
        assert!(versions.has_feature(feature::KV_CAS));
        assert!(versions.has_feature(feature::READ_YOUR_WRITES));
        assert!(!versions.has_feature(feature::STRONG_CONSISTENCY));
        // has_feature on a combined mask requires every bit present.
        assert!(versions.has_feature(feature::KV_CAS | feature::READ_YOUR_WRITES));
        assert!(!versions.has_feature(feature::KV_CAS | feature::STRONG_CONSISTENCY));
        let reply = HelloReply::new(versions);
        let bytes = encode_named(&reply).expect("encodes");
        let back: HelloReply = decode_named(&bytes).expect("decodes");
        assert_eq!(back, reply);
        assert!(back.versions.has_feature(feature::READ_YOUR_WRITES));
        // No advertised feature (0) is omitted on the wire, so a pre-feature
        // hello reply stays byte-identical.
        let plain = HelloReply::new(OpVersions::new(1, 1, 1, 1));
        let json = serde_json::to_string(&plain).expect("json");
        assert!(!json.contains("features"), "zero features omitted: {json}");
    }
}
