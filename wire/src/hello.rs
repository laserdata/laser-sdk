use serde::{Deserialize, Serialize};

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
            features: 0,
        }
    }

    /// Returns a copy advertising this agent-envelope (AGDX) version.
    #[must_use]
    pub fn with_agent(mut self, agent: u32) -> Self {
        self.agent = agent;
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

/// One materialization backend a server exposes, advertised so a client can see
/// what it may route to. `id` is the stable handle a binding references; `kind`
/// is the engine family as an opaque string, so a new engine is advertised by
/// name without any wire change. Carries identity only, never settings or
/// secrets. Integration-agnostic: the wire pins no specific engine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct BackendDescriptor {
    pub id: String,
    pub kind: String,
    /// Human-friendly display name for a UI, when the server has one. Advisory.
    /// Absent (the default, skipped on the wire) means a client derives a label
    /// from `id` or `kind`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// Engine or build version string, opaque to the wire. Advisory, for display
    /// and compatibility hints. Absent (the default, skipped on the wire) means
    /// the server did not report one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Opaque capability tags the backend declares about itself, so a consumer
    /// can reason about what this backend is good for (e.g. ingest, query, a
    /// particular query-surface feature, or a storage trait) and gate a decision
    /// before attempting an op. Each tag is an opaque string the wire pins no
    /// meaning to: a producer emits what it supports and a consumer matches the
    /// tags it understands, ignoring the rest, so a new capability is advertised
    /// by name with no wire change. Integration-agnostic. Empty (the default,
    /// skipped on the wire) means none declared.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capabilities: Vec<String>,
}

impl BackendDescriptor {
    /// A descriptor for the backend at `id` of engine family `kind`.
    pub fn new(id: impl Into<String>, kind: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            kind: kind.into(),
            label: None,
            version: None,
            capabilities: Vec::new(),
        }
    }

    /// Returns a copy with a human-friendly display label.
    #[must_use]
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Returns a copy advertising an engine or build version.
    #[must_use]
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Returns a copy advertising the opaque `capabilities` tags this backend
    /// declares about itself.
    #[must_use]
    pub fn with_capabilities<I, S>(mut self, capabilities: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.capabilities = capabilities.into_iter().map(Into::into).collect();
        self
    }

    /// Whether the backend declared the opaque capability `tag`.
    pub fn has_capability(&self, tag: &str) -> bool {
        self.capabilities.iter().any(|c| c == tag)
    }
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct BackendAnnounce {
    pub versions: OpVersions,
    /// Materialization backends the server currently exposes (the ones it has
    /// open). A client routes only to an advertised id. Empty (the default) is
    /// skipped on encode, so a pre-backends announce stays byte-identical and an
    /// older reader simply sees no advertised backends.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub backends: Vec<BackendDescriptor>,
}

impl BackendAnnounce {
    /// Constructor for the non-exhaustive wire struct.
    pub fn new(versions: OpVersions) -> Self {
        Self {
            versions,
            backends: Vec::new(),
        }
    }

    /// Returns a copy advertising `backends`.
    #[must_use]
    pub fn with_backends(mut self, backends: Vec<BackendDescriptor>) -> Self {
        self.backends = backends;
        self
    }
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::codes::{CONTROL_OP_VERSION, FORK_OP_VERSION, KV_OP_VERSION, QUERY_OP_VERSION};
    use crate::framing::{decode_named, encode_named};

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
            BackendDescriptor::new("embedded", "embedded"),
            BackendDescriptor::new("warehouse", "columnar")
                .with_label("Analytics warehouse")
                .with_version("2.1.0")
                .with_capabilities(["ingest", "query", "percentile"]),
        ]);
        let bytes = encode_named(&announce).expect("encodes");
        let back: BackendAnnounce = decode_named(&bytes).expect("decodes");
        assert_eq!(back, announce);
        assert_eq!(back.backends.len(), 2);
        assert_eq!(back.backends[1].id, "warehouse");
        assert_eq!(back.backends[1].kind, "columnar");
        assert_eq!(
            back.backends[1].label.as_deref(),
            Some("Analytics warehouse")
        );
        assert_eq!(back.backends[1].version.as_deref(), Some("2.1.0"));
        assert!(back.backends[1].has_capability("query"));
        assert!(!back.backends[1].has_capability("vector_search"));
        // The minimal descriptor omits the advisory fields on the wire.
        assert_eq!(back.backends[0].label, None);
        assert_eq!(back.backends[0].version, None);
        assert!(back.backends[0].capabilities.is_empty());
        let minimal_json = serde_json::to_string(&back.backends[0]).expect("json");
        assert!(
            !minimal_json.contains("label")
                && !minimal_json.contains("version")
                && !minimal_json.contains("capabilities"),
            "absent advisory fields omitted: {minimal_json}"
        );

        // No advertised backends (the default) is omitted on the wire, so a
        // pre-backends announce stays byte-identical.
        let plain = BackendAnnounce::new(OpVersions::new(1, 1, 1, 1));
        let json = serde_json::to_string(&plain).expect("json");
        assert!(!json.contains("backends"), "empty backends omitted: {json}");
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
