use laser_wire::checkpoint::CheckpointReadConsistency;
use laser_wire::filter::FilterCodec;
use laser_wire::query::Consistency;

pub use laser_wire::destination::BackendResourceId;
pub use laser_wire::hello::{
    BackendDescriptor, BackendDesiredState, BackendImplementation, BackendLimits, BackendMode,
    BackendObservedState, BackendReadiness, BackendReadinessCode, BackendReadinessReason,
    FilterAnnounce, MaintenanceCapabilities, MaterializationCapability, OpVersions,
    QueryCapabilities, QueryPagingCapability, SchemaCapabilities, TimeTravelCapability,
};

/// What the connected infrastructure serves beyond the open SDK surface. The open
/// SDK works on Apache Iggy with everything off (`OPEN`). LaserData Cloud
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
/// dependent feature cannot be advertised apart from the surface it refines.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Capabilities {
    /// Connected to a managed plane (LaserData Cloud). The root managed switch:
    /// with no plane, every managed surface below is unavailable.
    pub managed: bool,
    /// The managed query surface (`Laser::query`) and the read-consistency it
    /// serves.
    pub query: QueryCaps,
    /// Materialization destination declarations and managed checkpoint lifecycle.
    pub destinations: DestinationCaps,
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
    /// Server-side consumer filters, administered through a consumer group's
    /// `filter()` handle: native filtered reads and, with a managed plane,
    /// the group policy catalog.
    pub filters: FilterCaps,
    /// The wire op versions the server advertised in its `AGDX_HELLO` reply, or
    /// `None` against Apache Iggy and pre-versioned servers. When present, the
    /// SDK fails fast with the surface's typed `Version` error before a round-trip
    /// whenever its pinned op version is not the advertised one.
    pub versions: Option<OpVersions>,
    /// Structured, versioned, secret-free backend observations advertised at connect.
    pub backends: Vec<BackendDescriptor>,
    /// What the `AGDX_HELLO` probe established at connect. A group consumer
    /// reads natively only when the managed surfaces are positively absent.
    pub hello: HelloOutcome,
}

/// The outcome of the connect-time `AGDX_HELLO` probe.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HelloOutcome {
    /// No probe ran: a bring-your-own client or an explicit capability set.
    #[default]
    Unknown,
    /// The server answered with a managed announcement.
    Answered,
    /// The server answered without one: Apache Iggy refusing the command or
    /// an older server's empty body. The managed surfaces are absent.
    Rejected,
    /// The probe failed or timed out, so nothing is established.
    Failed,
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
    /// Whether an executing query can continue through opaque cursor pages.
    pub cursor_paging: bool,
    /// Whether query cancellation is served.
    pub cancellation: bool,
    /// Whether query execution status is served.
    pub execution_status: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DestinationCaps {
    pub available: bool,
    pub consistency: CheckpointReadConsistency,
}

/// Server-side consumer filters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilterCaps {
    /// Filtered reads, fenced acknowledgments, previews, sample tests, and
    /// validation. Served by the streaming server itself, advertised by the
    /// `CONSUMER_FILTERS` feature bit, so it needs no managed plane.
    pub native: bool,
    /// Saved filters, revisions, group bindings, and mutation outcomes. Needs a
    /// ready managed plane that advertises the `filter` op version.
    pub catalog: bool,
    /// Group-aware reads: the server resolves a consumer group's own policy,
    /// delivers an unbound group unfiltered, and fences acknowledgments by
    /// policy generation. Advertised by the `GROUP_POLICY_READS` feature bit.
    /// A normal group consumer needs it on a server that serves filters.
    pub group_policy_reads: bool,
    /// The evaluator version and codecs the server announced. `None` from a
    /// server that predates the announcement, which then evaluates as this
    /// build does.
    pub evaluation: Option<FilterAnnounce>,
}

impl FilterCaps {
    /// Whether the server evaluates a filter built for `evaluator_version`
    /// with `codec` exactly as this build does.
    #[must_use]
    pub fn evaluates(&self, evaluator_version: u32, codec: FilterCodec) -> bool {
        self.evaluation
            .as_ref()
            .is_none_or(|evaluation| evaluation.evaluates(evaluator_version, codec))
    }
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
    /// Whether the store serves the revocable fenced-lease contract
    /// (`KV_LEASE_OP_VERSION`): holder-scoped acquire, renewal, fence-validated
    /// release, fenced compare-and-swap requiring a live lease, and the
    /// barriered read. The SDK's lease, renew, release, and `cas_fenced` calls
    /// all gate on this bit, because a server without it would decode their
    /// reshaped payloads under the old contract instead of rejecting them.
    pub fenced_leases: bool,
}

impl Capabilities {
    /// The baseline every deployment has: nothing beyond the open SDK surface.
    pub const OPEN: Self = Self {
        managed: false,
        query: QueryCaps {
            available: false,
            consistency: Consistency::Eventual,
            keyword: false,
            cursor_paging: false,
            cancellation: false,
            execution_status: false,
        },
        destinations: DestinationCaps {
            available: false,
            consistency: CheckpointReadConsistency::PotentiallyStale,
        },
        kv: KvCaps {
            available: false,
            cas: false,
            cas_fenced: false,
            fenced_leases: false,
        },
        graph: false,
        forks: false,
        a2a_gateway: false,
        agent_workflow: false,
        watch: false,
        authz: false,
        filters: FilterCaps {
            native: false,
            catalog: false,
            group_policy_reads: false,
            evaluation: None,
        },
        versions: None,
        backends: Vec::new(),
        hello: HelloOutcome::Unknown,
    };

    /// True when the connected infrastructure advertised nothing beyond the open
    /// SDK (`OPEN`).
    pub fn is_open_only(&self) -> bool {
        let mut open = Self::OPEN;
        open.hello = self.hello;
        *self == open
    }

    pub fn backend(&self, resource_id: BackendResourceId) -> Option<&BackendDescriptor> {
        self.backends
            .iter()
            .find(|backend| backend.resource_id == resource_id)
    }

    pub fn enabled_backends(&self) -> impl Iterator<Item = &BackendDescriptor> {
        self.backends
            .iter()
            .filter(|backend| backend.desired_state == BackendDesiredState::Enabled)
    }

    pub fn unready_backends(&self) -> impl Iterator<Item = &BackendDescriptor> {
        self.enabled_backends().filter(|backend| {
            backend.observed_state != BackendObservedState::Ready || !backend.readiness.ready
        })
    }

    pub fn readiness_reasons(
        &self,
        resource_id: BackendResourceId,
    ) -> Option<&[BackendReadinessReason]> {
        self.backend(resource_id)
            .map(|backend| backend.readiness.reasons.as_slice())
    }

    pub fn is_ready(&self) -> bool {
        if !self.managed {
            return false;
        }
        let mut enabled = self.enabled_backends();
        let Some(first) = enabled.next() else {
            return false;
        };
        let ready = |backend: &BackendDescriptor| {
            backend.observed_state == BackendObservedState::Ready && backend.readiness.ready
        };
        ready(first) && enabled.all(ready)
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

    #[must_use]
    pub fn with_query_execution(mut self, paging: bool, cancellation: bool, status: bool) -> Self {
        self.query.cursor_paging = paging;
        self.query.cancellation = cancellation;
        self.query.execution_status = status;
        self
    }

    #[must_use]
    pub fn with_destinations(mut self, value: bool) -> Self {
        self.destinations.available = value;
        self
    }

    #[must_use]
    pub fn with_destination_consistency(mut self, value: CheckpointReadConsistency) -> Self {
        self.destinations.consistency = value;
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

    /// Returns a copy advertising the key-value fenced-lease contract.
    #[must_use]
    pub fn with_kv_fenced_leases(mut self, value: bool) -> Self {
        self.kv.fenced_leases = value;
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

    /// Returns a copy advertising native consumer filters and, separately, the
    /// saved-filter catalog.
    #[must_use]
    pub fn with_filters(mut self, native: bool, catalog: bool) -> Self {
        self.filters = FilterCaps {
            native,
            catalog,
            group_policy_reads: native,
            evaluation: None,
        };
        self
    }

    /// Returns a copy with the advertised wire op `versions`.
    #[must_use]
    pub fn with_versions(mut self, value: Option<OpVersions>) -> Self {
        self.versions = value;
        self
    }

    /// Returns a copy advertising the structured backend observations.
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
    #[cfg(any(feature = "streaming", test))]
    pub(crate) fn merge_features(&mut self, versions: &OpVersions) {
        use laser_wire::hello::feature;
        self.kv.cas |= versions.has_feature(feature::KV_CAS);
        self.kv.cas_fenced |= versions.has_feature(feature::KV_CAS_FENCED);
        self.kv.fenced_leases |= versions.has_feature(feature::KV_FENCED_LEASES);
        self.kv.cas_fenced |= self.kv.fenced_leases;
        self.agent_workflow |= versions.has_feature(feature::AGENT_WORKFLOW);
        self.query.keyword |= versions.has_feature(feature::KEYWORD_SEARCH);
        self.watch |= versions.has_feature(feature::WATCH);
        self.authz |= versions.has_feature(feature::AUTHZ);
        self.filters.native |= versions.has_feature(feature::CONSUMER_FILTERS);
        self.filters.group_policy_reads |= versions.has_feature(feature::GROUP_POLICY_READS);
        self.destinations.available |=
            versions.checkpoint > 0 && versions.has_feature(feature::DESTINATIONS);
        if self.destinations.available {
            self.destinations.consistency = CheckpointReadConsistency::Linearizable;
        }
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

    fn backend(
        id: u128,
        desired: BackendDesiredState,
        observed: BackendObservedState,
        readiness: BackendReadiness,
    ) -> BackendDescriptor {
        BackendDescriptor::new(
            BackendResourceId::from_u128(id),
            BackendMode::Operational,
            format!("backend-{id}"),
            BackendImplementation {
                kind: "embedded".to_owned(),
                version: "1.0.0".to_owned(),
            },
            1,
            1,
        )
        .with_state(desired, observed, readiness)
    }

    #[test]
    fn given_advertised_feature_bits_when_merged_then_should_light_up_the_capabilities() {
        let mut caps = Capabilities::OPEN;
        let versions = OpVersions::new(1, 1, 1, 1).with_features(
            feature::KV_CAS
                | feature::READ_YOUR_WRITES
                | feature::KV_CAS_FENCED
                | feature::KV_FENCED_LEASES,
        );
        caps.merge_features(&versions);
        assert!(caps.kv.cas, "KV_CAS bit should set kv.cas");
        assert!(
            caps.kv.cas_fenced,
            "KV_CAS_FENCED bit should set kv.cas_fenced"
        );
        assert!(
            caps.kv.fenced_leases,
            "KV_FENCED_LEASES bit should set kv.fenced_leases"
        );
        assert_eq!(
            caps.query.consistency,
            Consistency::ReadYourWrites,
            "the read-your-writes bit should raise the level"
        );
    }

    #[test]
    fn given_the_consumer_filters_bit_when_merged_then_should_light_up_native_filters_only() {
        let mut caps = Capabilities::OPEN;
        caps.merge_features(&OpVersions::new(1, 1, 1, 1).with_features(feature::CONSUMER_FILTERS));
        assert!(caps.filters.native);
        assert!(
            !caps.filters.catalog,
            "the catalog needs a ready plane with the filter op version"
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
    fn given_only_fenced_leases_when_merged_then_should_also_advertise_fenced_cas() {
        let mut caps = Capabilities::OPEN;
        caps.merge_features(&OpVersions::new(1, 1, 1, 1).with_features(feature::KV_FENCED_LEASES));
        assert!(caps.kv.fenced_leases);
        assert!(
            caps.kv.cas_fenced,
            "the full fenced-lease contract subsumes fenced CAS"
        );
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
            BackendDescriptor::new(
                laser_wire::destination::BackendResourceId::from_u128(1),
                BackendMode::Operational,
                "Embedded",
                BackendImplementation {
                    kind: "embedded".to_owned(),
                    version: "1.0.0".to_owned(),
                },
                1,
                1,
            ),
            BackendDescriptor::new(
                laser_wire::destination::BackendResourceId::from_u128(2),
                BackendMode::Lakehouse,
                "Warehouse",
                BackendImplementation {
                    kind: "columnar".to_owned(),
                    version: "2.1.0".to_owned(),
                },
                1,
                1,
            ),
        ]);
        assert_eq!(caps.backends.len(), 2);
        assert_eq!(caps.backends[1].label, "Warehouse");
        assert!(!caps.is_open_only() && !caps.managed);
    }

    #[test]
    fn given_enabled_ready_backends_when_checking_readiness_then_should_be_ready() {
        let first_id = BackendResourceId::from_u128(1);
        let caps = Capabilities::OPEN.with_managed(true).with_backends(vec![
            backend(
                1,
                BackendDesiredState::Enabled,
                BackendObservedState::Ready,
                BackendReadiness::ready(1),
            ),
            backend(
                2,
                BackendDesiredState::Disabled,
                BackendObservedState::Disabled,
                BackendReadiness::not_ready(BackendReadinessCode::Disabled),
            ),
        ]);
        assert!(caps.is_ready());
        assert_eq!(
            caps.backend(first_id).map(|backend| backend.label.as_str()),
            Some("backend-1")
        );
        assert_eq!(caps.enabled_backends().count(), 1);
        assert_eq!(caps.unready_backends().count(), 0);
        assert_eq!(caps.readiness_reasons(first_id), Some([].as_slice()));
    }

    #[test]
    fn given_starting_backend_when_checking_readiness_then_reason_should_be_exposed() {
        let id = BackendResourceId::from_u128(3);
        let caps = Capabilities::OPEN
            .with_managed(true)
            .with_backends(vec![backend(
                3,
                BackendDesiredState::Enabled,
                BackendObservedState::Starting,
                BackendReadiness::not_ready(BackendReadinessCode::ConfigurationPending),
            )]);
        assert!(!caps.is_ready());
        assert_eq!(caps.unready_backends().count(), 1);
        assert_eq!(
            caps.readiness_reasons(id)
                .and_then(|reasons| reasons.first())
                .map(|reason| reason.code),
            Some(BackendReadinessCode::ConfigurationPending)
        );
    }

    #[test]
    fn given_open_capabilities_when_the_probe_outcome_changes_then_should_still_be_open_only() {
        for hello in [
            HelloOutcome::Unknown,
            HelloOutcome::Answered,
            HelloOutcome::Rejected,
            HelloOutcome::Failed,
        ] {
            let mut caps = Capabilities::OPEN;
            caps.hello = hello;
            assert!(caps.is_open_only());
            assert!(!caps.with_managed(true).is_open_only());
        }
    }

    #[test]
    fn given_managed_without_enabled_backends_when_checking_readiness_then_should_not_be_ready() {
        assert!(!Capabilities::OPEN.with_managed(true).is_ready());
    }
}
