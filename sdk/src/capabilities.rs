/// The premium capability set the connected streaming infrastructure advertises.
/// The open SDK works on raw Apache Iggy with all flags false (`OPEN`).
/// LaserData Cloud reports a richer set during login negotiation, activating
/// the matching paths without any change to the caller's imports. None of
/// these features fall back to a working state on raw Apache Iggy. When a flag
/// is false the matching call returns `LaserError::Unsupported`.
///
/// Naming rule: a `managed_*` flag is a service LaserData Cloud serves off the
/// log (query, kv, memory). A bare flag is an infrastructure-native capability
/// (`managed_host`, `sessions`, `forks`, `durable_dedup`, `a2a_gateway`).
/// The distinction is intentional, and it says where the capability lives.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Capabilities {
    /// Infrastructure-native session start (the platform tracks a session lifecycle).
    pub sessions: bool,
    /// Infrastructure-native copy-on-write forks of the read model (`Laser::fork`).
    pub forks: bool,
    /// Infrastructure-side durable deduplication (survives a cold start without replay).
    pub durable_dedup: bool,
    /// A managed memory service backs the `Memory` trait (durable vector recall).
    pub managed_memory: bool,
    /// LaserData Cloud serves `Laser::query`. False against raw Apache Iggy, where
    /// `Laser::query` returns `LaserError::Unsupported`.
    /// (The SDK's own in-process test/example query worker also sets this, so
    /// the examples can answer queries in local, offline demos.)
    pub managed_query: bool,
    /// LaserData Cloud serves the managed key-value store (`Laser::kv`). Implied by
    /// `managed_host`. False against raw Apache Iggy, where `Laser::kv` returns
    /// `LaserError::Unsupported`.
    pub managed_kv: bool,
    /// Connected to LaserData Cloud, which serves query, KV, registry browse, and
    /// forks. Probed once at connect via `AGDX_HELLO_CODE`. False against raw Apache
    /// Iggy, where those features return `LaserError::Unsupported`. Implies
    /// `managed_query` / `managed_kv` / `forks`.
    pub managed_host: bool,
    /// A managed A2A gateway is available (auth, streaming, persisted task store).
    pub a2a_gateway: bool,
    /// The managed key-value store serves compare-and-swap (`Kv::set(..).commit()`).
    /// Set from the `AGDX_HELLO` reply's `feature::KV_CAS` bit. False when a
    /// backend cannot do a conditional write, where `commit()` returns
    /// `LaserError::Unsupported`. Independent of `managed_kv`: a deployment can
    /// serve plain get/set without CAS.
    pub kv_cas: bool,
    /// The query surface honors `Consistency::ReadYourWrites` (`.read_your_writes()`).
    /// Set from the `feature::READ_YOUR_WRITES` bit. False where the deployment
    /// cannot wait for the projector, and such a query returns `Unsupported`.
    pub read_your_writes: bool,
    /// The query surface honors `Consistency::Strong`, set from the
    /// `feature::STRONG_CONSISTENCY` bit. The strongest level, off by default.
    pub strong_consistency: bool,
    /// The wire op versions the connected server advertised in its `AGDX_HELLO`
    /// reply, mirroring the HTTP capabilities `versions` block. `None` against
    /// raw Apache Iggy and against pre-versioned servers whose hello reply has
    /// an empty body. When present, the SDK fails fast with the surface's
    /// typed `Version` error before a round-trip whenever its own pinned op
    /// version is not the advertised one.
    pub versions: Option<OpVersions>,
}

pub use laser_wire::hello::OpVersions;

impl Capabilities {
    /// The baseline every deployment has: nothing beyond the open SDK surface.
    pub const OPEN: Self = Self {
        sessions: false,
        forks: false,
        durable_dedup: false,
        managed_memory: false,
        managed_query: false,
        managed_kv: false,
        managed_host: false,
        a2a_gateway: false,
        kv_cas: false,
        read_your_writes: false,
        strong_consistency: false,
        versions: None,
    };

    /// True when the connected infrastructure advertised nothing beyond the open SDK (`OPEN`).
    pub fn is_open_only(&self) -> bool {
        *self == Self::OPEN
    }

    // The struct is `#[non_exhaustive]` (new capabilities land without
    // a breaking change), so callers outside the crate cannot use struct-update
    // syntax. These chainable setters are the supported way to build a custom
    // set: `Capabilities::OPEN.with_managed_query(true)`.

    /// Returns a copy with `sessions` set to `value`.
    #[must_use]
    pub fn with_sessions(mut self, value: bool) -> Self {
        self.sessions = value;
        self
    }

    /// Returns a copy with `forks` set to `value`.
    #[must_use]
    pub fn with_forks(mut self, value: bool) -> Self {
        self.forks = value;
        self
    }

    /// Returns a copy with `durable_dedup` set to `value`.
    #[must_use]
    pub fn with_durable_dedup(mut self, value: bool) -> Self {
        self.durable_dedup = value;
        self
    }

    /// Returns a copy with `managed_memory` set to `value`.
    #[must_use]
    pub fn with_managed_memory(mut self, value: bool) -> Self {
        self.managed_memory = value;
        self
    }

    /// Returns a copy with `managed_query` set to `value`.
    #[must_use]
    pub fn with_managed_query(mut self, value: bool) -> Self {
        self.managed_query = value;
        self
    }

    /// Returns a copy with `managed_kv` set to `value`.
    #[must_use]
    pub fn with_managed_kv(mut self, value: bool) -> Self {
        self.managed_kv = value;
        self
    }

    /// Returns a copy with `managed_host` set to `value`.
    #[must_use]
    pub fn with_managed_host(mut self, value: bool) -> Self {
        self.managed_host = value;
        self
    }

    /// Returns a copy with `a2a_gateway` set to `value`.
    #[must_use]
    pub fn with_a2a_gateway(mut self, value: bool) -> Self {
        self.a2a_gateway = value;
        self
    }

    /// Returns a copy with `kv_cas` set to `value`.
    #[must_use]
    pub fn with_kv_cas(mut self, value: bool) -> Self {
        self.kv_cas = value;
        self
    }

    /// Returns a copy with `read_your_writes` set to `value`.
    #[must_use]
    pub fn with_read_your_writes(mut self, value: bool) -> Self {
        self.read_your_writes = value;
        self
    }

    /// Returns a copy with `strong_consistency` set to `value`.
    #[must_use]
    pub fn with_strong_consistency(mut self, value: bool) -> Self {
        self.strong_consistency = value;
        self
    }

    /// Returns a copy with the advertised wire op `versions` set to `value`.
    #[must_use]
    pub fn with_versions(mut self, value: Option<OpVersions>) -> Self {
        self.versions = value;
        self
    }

    /// OR in the per-feature managed sub-capabilities a hello reply's op
    /// versions advertise (compare-and-swap, read-your-writes, strong
    /// consistency). The connect probe calls this. A flag already set by a BYO
    /// `with_kv_cas(..)` is preserved, and a feature the server does not
    /// advertise stays off so the matching call returns `Unsupported`.
    #[cfg(feature = "query")]
    pub(crate) fn merge_features(&mut self, versions: &OpVersions) {
        use laser_wire::hello::feature;
        self.kv_cas |= versions.has_feature(feature::KV_CAS);
        self.read_your_writes |= versions.has_feature(feature::READ_YOUR_WRITES);
        self.strong_consistency |= versions.has_feature(feature::STRONG_CONSISTENCY);
    }

    /// Whether this capability set serves the given read-consistency `level`.
    /// `Eventual` is always served. The stronger levels require the matching
    /// advertised feature: a server that does not implement a level silently
    /// ignores the additive `consistency` field and serves an eventual read,
    /// so the client refuses a level it cannot guarantee, never downgrading.
    /// `Strong` subsumes `ReadYourWrites` (it is read-your-writes plus
    /// cross-replica agreement), so a deployment advertising `strong_consistency`
    /// serves a `ReadYourWrites` query too. An unknown future level is treated
    /// as not served (fail-safe).
    pub fn serves_consistency(&self, level: laser_wire::query::Consistency) -> bool {
        use laser_wire::query::Consistency;
        match level {
            Consistency::Eventual => true,
            Consistency::ReadYourWrites => self.read_your_writes || self.strong_consistency,
            Consistency::Strong => self.strong_consistency,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "query")]
    use laser_wire::hello::feature;

    #[cfg(feature = "query")]
    #[test]
    fn given_advertised_feature_bits_when_merged_then_should_light_up_the_capabilities() {
        let mut caps = Capabilities::OPEN;
        let versions =
            OpVersions::new(1, 1, 1, 1).with_features(feature::KV_CAS | feature::READ_YOUR_WRITES);
        caps.merge_features(&versions);
        assert!(caps.kv_cas, "KV_CAS bit should set kv_cas");
        assert!(caps.read_your_writes, "READ_YOUR_WRITES bit should set it");
        assert!(
            !caps.strong_consistency,
            "an unadvertised bit must stay off"
        );
    }

    #[cfg(feature = "query")]
    #[test]
    fn given_no_advertised_features_when_merged_then_should_leave_capabilities_off() {
        let mut caps = Capabilities::OPEN;
        caps.merge_features(&OpVersions::new(1, 1, 1, 1));
        assert!(!caps.kv_cas && !caps.read_your_writes && !caps.strong_consistency);
    }

    #[cfg(feature = "query")]
    #[test]
    fn given_an_explicit_flag_when_merging_unadvertised_features_then_should_preserve_it() {
        // A BYO `with_kv_cas(true)` must survive a probe whose server does not
        // advertise the bit (the merge only ever adds).
        let mut caps = Capabilities::OPEN.with_kv_cas(true);
        caps.merge_features(&OpVersions::new(1, 1, 1, 1));
        assert!(
            caps.kv_cas,
            "an explicit flag must not be cleared by the merge"
        );
    }

    #[test]
    fn given_open_capabilities_when_checking_consistency_then_should_serve_only_eventual() {
        use laser_wire::query::Consistency;
        // Eventual needs no capability, the default served everywhere including
        // raw open Iggy.
        assert!(Capabilities::OPEN.serves_consistency(Consistency::Eventual));
        // The stronger levels are refused unless explicitly advertised, so
        // the query layer fails fast instead of silently downgrading.
        assert!(!Capabilities::OPEN.serves_consistency(Consistency::ReadYourWrites));
        assert!(!Capabilities::OPEN.serves_consistency(Consistency::Strong));
    }

    #[test]
    fn given_read_your_writes_only_when_checked_then_should_not_serve_strong() {
        use laser_wire::query::Consistency;
        // The weaker level does not imply the stronger one.
        let ryw = Capabilities::OPEN.with_read_your_writes(true);
        assert!(ryw.serves_consistency(Consistency::ReadYourWrites));
        assert!(
            !ryw.serves_consistency(Consistency::Strong),
            "read-your-writes must not imply strong"
        );
    }

    #[test]
    fn given_strong_only_when_checked_then_should_subsume_read_your_writes() {
        use laser_wire::query::Consistency;
        // Strong is read-your-writes plus cross-replica agreement, so advertising
        // it serves a read-your-writes query too. A strong-only deployment must
        // not refuse the weaker level it can satisfy.
        let strong = Capabilities::OPEN.with_strong_consistency(true);
        assert!(strong.serves_consistency(Consistency::Strong));
        assert!(
            strong.serves_consistency(Consistency::ReadYourWrites),
            "strong must subsume read-your-writes"
        );
    }
}
