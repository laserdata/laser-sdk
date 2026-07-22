use laser_wire::query::Consistency;

pub use laser_wire::hello::{BackendDescriptor, OpVersions};

/// What the connected infrastructure serves beyond the open SDK surface. The open
/// SDK works on raw Apache Iggy with everything off (`OPEN`). LaserData Cloud
/// reports a richer set at connect, lighting up the matching paths with no change
/// to the caller's imports. Nothing here falls back to a working state on raw
/// Apache Iggy: when a capability is off, the matching call returns
/// `LaserError::Unsupported`.
///
/// Capabilities are grouped by where they live and what they depend on, rather
/// than a flat list of unrelated flags. [`managed`](Self::managed) is the root
/// (connected to a managed plane at all). The managed surfaces ([`query`](Self::query),
/// [`kv`](Self::kv), [`graph`](Self::graph), [`forks`](Self::forks),
/// [`a2a_gateway`](Self::a2a_gateway)) are served by that plane. A surface's
/// sub-features nest under it ([`QueryCaps::consistency`], [`KvCaps::cas`]) so a
/// dependent feature cannot be advertised apart from the surface it refines. The
/// platform-native features ([`sessions`](Self::sessions),
/// [`durable_dedup`](Self::durable_dedup)) are not plane surfaces.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Capabilities {
    /// Connected to a managed plane (LaserData Cloud). The root managed switch:
    /// with no plane, every managed surface below is unavailable.
    pub managed: bool,
    /// The managed query surface (`Laser::query`) and the read-consistency it
    /// serves.
    pub query: QueryCaps,
    /// The managed key-value surface (`Laser::kv`) and its conditional-write
    /// support.
    pub kv: KvCaps,
    /// The managed knowledge-graph surface (`Laser::graph`). Agentic memory
    /// composes the query and graph surfaces, so it has no capability of its own.
    pub graph: bool,
    /// Managed copy-on-write forks of the read model (`Laser::fork`).
    pub forks: bool,
    /// A managed A2A gateway (auth, streaming, persisted task store).
    pub a2a_gateway: bool,
    /// The managed run registry (`Laser::runs` submit / cancel / status /
    /// list). Off when the plane does not serve the band.
    pub agent_workflow: bool,
    /// The change feed (`Laser::watch`): `ChangeRecord`s published after each
    /// committed projector batch for a binding that opted into `notify`. Off
    /// when the deployment does not publish the feed.
    pub watch: bool,
    /// The authorization control surface (`Laser::whoami` and the role/binding
    /// verbs). Fork-native, advertised by the `AUTHZ` feature bit.
    pub authz: bool,
    /// Platform-native session lifecycle (the infrastructure tracks a session).
    pub sessions: bool,
    /// Platform-side durable deduplication (survives a cold start without replay).
    pub durable_dedup: bool,
    /// The wire op versions the server advertised in its `AGDX_HELLO` reply, or
    /// `None` against raw Apache Iggy and pre-versioned servers. When present, the
    /// SDK fails fast with the surface's typed `Version` error before a round-trip
    /// whenever its pinned op version is not the advertised one.
    pub versions: Option<OpVersions>,
    /// The materialization backends the server exposes, advertised at connect.
    /// Identity only (a stable `id` and an opaque engine `kind`), never settings
    /// or secrets. Empty against raw Apache Iggy and servers that advertise none.
    pub backends: Vec<BackendDescriptor>,
}

/// The managed query surface and the strongest read-consistency it serves.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryCaps {
    /// Whether `Laser::query` is served.
    pub available: bool,
    /// The strongest read-consistency level the surface honors. The ladder is
    /// `Eventual < ReadYourWrites < Strong`, so a level implies every weaker one
    /// (a `Strong` surface serves a read-your-writes query too). `Eventual` is
    /// the default when a query surface is available.
    pub consistency: Consistency,
    /// Whether the surface serves lexical relevance search (`.text()` /
    /// `.text_in()`). Off means a `text` query is refused locally before
    /// sending, since an unaware server would silently drop the additive field
    /// and answer wider than asked.
    pub keyword: bool,
}

/// The managed key-value surface and its conditional-write support.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KvCaps {
    /// Whether the managed key-value store is served (`Laser::kv` get/set/scan).
    pub available: bool,
    /// Whether the store serves compare-and-swap (`Kv::set(..).commit()`).
    /// Independent of plain get/set: a backend that cannot do a conditional write
    /// leaves it off, and `commit()` then returns `LaserError::Unsupported`.
    pub cas: bool,
    /// Whether the store serves fenced compare-and-swap (`Kv::cas_fenced(..)`).
    /// Independent of plain `cas`: a backend leaves it off when it cannot gate a
    /// write on a live fence sequence, and `cas_fenced` then returns
    /// `LaserError::Unsupported`.
    pub cas_fenced: bool,
}

impl Capabilities {
    /// The baseline every deployment has: nothing beyond the open SDK surface.
    pub const OPEN: Self = Self {
        managed: false,
        query: QueryCaps {
            available: false,
            consistency: Consistency::Eventual,
            keyword: false,
        },
        kv: KvCaps {
            available: false,
            cas: false,
            cas_fenced: false,
        },
        graph: false,
        forks: false,
        a2a_gateway: false,
        agent_workflow: false,
        watch: false,
        authz: false,
        sessions: false,
        durable_dedup: false,
        versions: None,
        backends: Vec::new(),
    };

    /// True when the connected infrastructure advertised nothing beyond the open
    /// SDK (`OPEN`).
    pub fn is_open_only(&self) -> bool {
        *self == Self::OPEN
    }

    // The struct is `#[non_exhaustive]`, so these chainable setters are the
    // supported way to build a custom set: `Capabilities::OPEN.with_query(true)`.

    /// Returns a copy connected to a managed plane (the root switch).
    #[must_use]
    pub fn with_managed(mut self, value: bool) -> Self {
        self.managed = value;
        self
    }

    /// Returns a copy with the managed query surface available.
    #[must_use]
    pub fn with_query(mut self, value: bool) -> Self {
        self.query.available = value;
        self
    }

    /// Returns a copy advertising the strongest read-consistency the query
    /// surface serves.
    #[must_use]
    pub fn with_query_consistency(mut self, level: Consistency) -> Self {
        self.query.consistency = level;
        self
    }

    /// Returns a copy advertising lexical relevance search on the query surface.
    #[must_use]
    pub fn with_query_keyword(mut self, value: bool) -> Self {
        self.query.keyword = value;
        self
    }

    /// Returns a copy with the managed key-value surface available.
    #[must_use]
    pub fn with_kv(mut self, value: bool) -> Self {
        self.kv.available = value;
        self
    }

    /// Returns a copy advertising key-value compare-and-swap.
    #[must_use]
    pub fn with_kv_cas(mut self, value: bool) -> Self {
        self.kv.cas = value;
        self
    }

    /// Returns a copy advertising key-value fenced compare-and-swap.
    #[must_use]
    pub fn with_kv_cas_fenced(mut self, value: bool) -> Self {
        self.kv.cas_fenced = value;
        self
    }

    /// Returns a copy with the managed graph surface available.
    #[must_use]
    pub fn with_graph(mut self, value: bool) -> Self {
        self.graph = value;
        self
    }

    /// Returns a copy with managed forks available.
    #[must_use]
    pub fn with_forks(mut self, value: bool) -> Self {
        self.forks = value;
        self
    }

    /// Returns a copy advertising the managed A2A gateway.
    #[must_use]
    pub fn with_a2a_gateway(mut self, value: bool) -> Self {
        self.a2a_gateway = value;
        self
    }

    /// Returns a copy advertising the managed agent and workflow control band.
    #[must_use]
    pub fn with_agent_workflow(mut self, value: bool) -> Self {
        self.agent_workflow = value;
        self
    }

    /// Returns a copy with platform-native sessions.
    #[must_use]
    pub fn with_sessions(mut self, value: bool) -> Self {
        self.sessions = value;
        self
    }

    /// Returns a copy with platform-side durable dedup.
    #[must_use]
    pub fn with_durable_dedup(mut self, value: bool) -> Self {
        self.durable_dedup = value;
        self
    }

    /// Returns a copy with the advertised wire op `versions`.
    #[must_use]
    pub fn with_versions(mut self, value: Option<OpVersions>) -> Self {
        self.versions = value;
        self
    }

    /// Returns a copy advertising the materialization `backends` the server
    /// exposes (identity only: a stable `id` and an opaque engine `kind`).
    #[must_use]
    pub fn with_backends(mut self, value: Vec<BackendDescriptor>) -> Self {
        self.backends = value;
        self
    }

    /// Fold the per-surface sub-features a hello reply's op versions advertise
    /// into this set: the compare-and-swap bit, and the consistency bits (raising
    /// the served level to the strongest advertised). Additive, so a sub-feature
    /// already set by a BYO builder survives, and one the server does not
    /// advertise stays off.
    #[cfg(any(
        feature = "fork",
        feature = "graph",
        feature = "kv",
        feature = "projections",
        feature = "query",
        feature = "rbac",
        feature = "runs",
        test
    ))]
    pub(crate) fn merge_features(&mut self, versions: &OpVersions) {
        use laser_wire::hello::feature;
        self.kv.cas |= versions.has_feature(feature::KV_CAS);
        self.kv.cas_fenced |= versions.has_feature(feature::KV_CAS_FENCED);
        self.agent_workflow |= versions.has_feature(feature::AGENT_WORKFLOW);
        self.query.keyword |= versions.has_feature(feature::KEYWORD_SEARCH);
        self.watch |= versions.has_feature(feature::WATCH);
        self.authz |= versions.has_feature(feature::AUTHZ);
        if versions.has_feature(feature::STRONG_CONSISTENCY) {
            self.query.consistency = self.query.consistency.max(Consistency::Strong);
        } else if versions.has_feature(feature::READ_YOUR_WRITES) {
            self.query.consistency = self.query.consistency.max(Consistency::ReadYourWrites);
        }
    }

    /// Whether the query surface serves read-consistency `level`. Because the
    /// levels form a ladder, this is `level <= self.query.consistency`: `Eventual`
    /// is always served, and a `Strong` surface subsumes a read-your-writes query.
    /// An unknown future level is treated as not served (fail-safe).
    pub fn serves_consistency(&self, level: Consistency) -> bool {
        level <= self.query.consistency
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::hello::feature;

    #[test]
    fn given_advertised_feature_bits_when_merged_then_should_light_up_the_capabilities() {
        let mut caps = Capabilities::OPEN;
        let versions = OpVersions::new(1, 1, 1, 1)
            .with_features(feature::KV_CAS | feature::READ_YOUR_WRITES | feature::KV_CAS_FENCED);
        caps.merge_features(&versions);
        assert!(caps.kv.cas, "KV_CAS bit should set kv.cas");
        assert!(
            caps.kv.cas_fenced,
            "KV_CAS_FENCED bit should set kv.cas_fenced"
        );
        assert_eq!(
            caps.query.consistency,
            Consistency::ReadYourWrites,
            "the read-your-writes bit should raise the level"
        );
    }

    #[test]
    fn given_the_strong_bit_when_merged_then_should_raise_the_level_to_strong() {
        let mut caps = Capabilities::OPEN;
        caps.merge_features(
            &OpVersions::new(1, 1, 1, 1).with_features(feature::STRONG_CONSISTENCY),
        );
        assert_eq!(caps.query.consistency, Consistency::Strong);
        // Strong subsumes the weaker level, structurally.
        assert!(caps.serves_consistency(Consistency::ReadYourWrites));
    }

    #[test]
    fn given_an_explicit_sub_feature_when_merging_unadvertised_then_should_preserve_it() {
        let mut caps = Capabilities::OPEN.with_kv_cas(true);
        caps.merge_features(&OpVersions::new(1, 1, 1, 1));
        assert!(
            caps.kv.cas,
            "an explicit sub-feature must survive the merge"
        );
    }

    #[test]
    fn given_open_capabilities_when_checking_consistency_then_should_serve_only_eventual() {
        assert!(Capabilities::OPEN.serves_consistency(Consistency::Eventual));
        assert!(!Capabilities::OPEN.serves_consistency(Consistency::ReadYourWrites));
        assert!(!Capabilities::OPEN.serves_consistency(Consistency::Strong));
    }

    #[test]
    fn given_read_your_writes_only_when_checked_then_should_not_serve_strong() {
        let ryw = Capabilities::OPEN.with_query_consistency(Consistency::ReadYourWrites);
        assert!(ryw.serves_consistency(Consistency::ReadYourWrites));
        assert!(
            !ryw.serves_consistency(Consistency::Strong),
            "read-your-writes must not imply strong"
        );
    }

    #[test]
    fn given_strong_when_checked_then_should_subsume_read_your_writes() {
        let strong = Capabilities::OPEN.with_query_consistency(Consistency::Strong);
        assert!(strong.serves_consistency(Consistency::Strong));
        assert!(
            strong.serves_consistency(Consistency::ReadYourWrites),
            "strong must subsume read-your-writes"
        );
    }

    #[test]
    fn given_advertised_backends_when_set_then_should_expose_them_and_open_has_none() {
        assert!(Capabilities::OPEN.backends.is_empty());
        let caps = Capabilities::OPEN.with_backends(vec![
            BackendDescriptor::new("embedded", "embedded"),
            BackendDescriptor::new("warehouse", "columnar").with_version("2.1.0"),
        ]);
        assert_eq!(caps.backends.len(), 2);
        assert_eq!(caps.backends[1].id, "warehouse");
        assert!(!caps.is_open_only() && !caps.managed);
    }
}
