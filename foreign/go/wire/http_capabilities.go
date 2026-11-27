package wire

// QueryCapsView is the managed query surface on the HTTP capabilities reply.
//
// Every sub-feature defaults off: over-advertising would turn a clean
// unsupported error into a silent wrong answer.
type QueryCapsView struct {
	// Available reports whether the query route is served at all.
	Available   bool `cbor:"available"`
	Projections bool `cbor:"projections"`
	Schemas     bool `cbor:"schemas"`
	// Consistency is the strongest read level served. The levels form a
	// ladder, so naming one implies the weaker ones.
	Consistency Consistency `cbor:"consistency"`
	Keyword     bool        `cbor:"keyword"`
}

// FilterCapsView is the consumer-filter surface on the HTTP capabilities
// reply.
//
// Native evaluation is served by the streaming server itself and holds with
// the managed backend off. The catalog of saved filters and group bindings
// needs the backend, so the two are reported separately.
type FilterCapsView struct {
	// Native covers filtered reads, fenced acknowledgments, sample tests and
	// validation.
	Native bool `cbor:"native"`
	// GroupPolicyReads covers group-aware reads, including pass-through when
	// no policy is bound.
	GroupPolicyReads bool `cbor:"group_policy_reads"`
	Preview          bool `cbor:"preview"`
	// PrimaryRouting covers filtered reads from the partition primary through
	// an attached session.
	PrimaryRouting bool `cbor:"primary_routing"`
	Catalog        bool `cbor:"catalog"`
	ManagedGroups  bool `cbor:"managed_groups"`
	// EvaluatorVersion is the evaluation rules served, and zero when native
	// evaluation is off.
	EvaluatorVersion uint32        `cbor:"evaluator_version"`
	Codecs           []FilterCodec `cbor:"codecs,omitempty"`
}

// ServedFilterCaps is the view a server serving native evaluation and a ready
// catalog advertises.
func ServedFilterCaps(native, catalog bool) FilterCapsView {
	view := FilterCapsView{
		Native:         native,
		Preview:        native,
		PrimaryRouting: native,
		Catalog:        catalog,
		ManagedGroups:  catalog,
	}
	if native {
		served := ServedFilterAnnounce()
		view.EvaluatorVersion, view.Codecs = served.EvaluatorVersion, served.Codecs
	}
	return view
}

// WithGroupPolicyReads advertises group-aware reads.
func (view FilterCapsView) WithGroupPolicyReads(served bool) FilterCapsView {
	view.GroupPolicyReads = served
	return view
}

// WithEvaluation reports the evaluation a server announced instead of this
// build's own, so a client advertises what will actually run.
func (view FilterCapsView) WithEvaluation(announce FilterAnnounce) FilterCapsView {
	if view.Native {
		view.EvaluatorVersion, view.Codecs = announce.EvaluatorVersion, announce.Codecs
	}
	return view
}

// KvCapsView is the managed key-value surface on the HTTP capabilities reply.
type KvCapsView struct {
	Available bool `cbor:"available"`
	// Cas is independent of plain reads and writes: a backend that cannot do a
	// conditional write leaves it off and one returns a clean unsupported
	// error instead of a wrong result.
	Cas bool `cbor:"cas"`
	// CasFenced is independent of plain conditional writes: a backend leaves
	// it off when it cannot gate a write on a live fence sequence.
	CasFenced bool `cbor:"cas_fenced"`
	// FencedLeases covers the full revocable fenced-lease contract, which
	// subsumes the fence-only conditional write.
	FencedLeases bool `cbor:"fenced_leases"`
}

// Capabilities is what the managed surface offers on this server, so a browser
// client can feature-detect before showing a view it cannot drive.
//
// It is richer than the binary probe, carrying per-surface flags beside the
// wire op versions the JSON bodies must match, and it answers truthfully even
// when the managed backend is off: the reply still succeeds, reporting that
// nothing managed is served.
type Capabilities struct {
	// Managed is the root: with no backend every managed surface below is off.
	Managed bool          `cbor:"managed"`
	Query   QueryCapsView `cbor:"query"`
	Kv      KvCapsView    `cbor:"kv"`
	// Graph covers the traversal and neighbor reads. The agentic-memory API
	// composes the query and graph surfaces, so it has no flag of its own.
	Graph bool `cbor:"graph"`
	Fork  bool `cbor:"fork"`
	// Sessions covers the managed session reads.
	Sessions bool `cbor:"sessions"`
	// Watch covers the change feed. It is off by default, because a client
	// that waited on an unpublished feed would wait forever.
	Watch bool `cbor:"watch"`
	// Authz covers the authorization control band, so a console can show its
	// roles and bindings.
	Authz    bool           `cbor:"authz"`
	Filters  FilterCapsView `cbor:"filters"`
	Versions OpVersions     `cbor:"versions"`
	// Backends are the materialization backends the server exposes, as
	// identity only: no settings and no secrets.
	Backends []BackendDescriptor `cbor:"backends,omitempty"`
	// Topology is the stream and topic naming this deployment uses. Absent
	// leaves an older reader on its own defaults.
	Topology *WireTopology `cbor:"topology,omitempty"`
}

// NewCapabilities is the base reply for a server with or without a managed
// backend. Every sub-feature starts off and is advertised explicitly.
func NewCapabilities(enabled bool, versions OpVersions) Capabilities {
	return Capabilities{
		Managed: enabled,
		Query: QueryCapsView{
			Available: enabled, Projections: enabled, Schemas: enabled,
			Consistency: ConsistencyEventual,
		},
		Kv:       KvCapsView{Available: enabled},
		Fork:     enabled,
		Versions: versions,
	}
}

// CapabilitiesFromVersions builds the HTTP capabilities from the same op
// versions the binary probe answers with.
//
// A server should use this so its two capability carriages cannot disagree:
// the per-surface flags are read straight off the feature bits rather than set
// twice.
func CapabilitiesFromVersions(enabled bool, versions OpVersions) Capabilities {
	consistency := ConsistencyEventual
	if versions.HasFeature(CapabilityReadYourWrites) {
		consistency = ConsistencyReadYourWrites
	}

	capabilities := NewCapabilities(enabled, versions)
	capabilities.Kv.Cas = versions.HasFeature(CapabilityKvCas)
	capabilities.Kv.CasFenced = versions.HasFeature(CapabilityKvCasFenced)
	if versions.HasFeature(CapabilityKvFencedLeases) {
		// The full lease contract subsumes the fence-only conditional write.
		capabilities.Kv.FencedLeases = true
		capabilities.Kv.CasFenced = true
	}
	capabilities.Query.Keyword = versions.HasFeature(CapabilityKeywordSearch)
	capabilities.Query.Consistency = consistency
	capabilities.Watch = versions.HasFeature(CapabilityWatch)
	capabilities.Sessions = enabled && versions.HasFeature(CapabilitySessions)
	capabilities.Authz = versions.HasFeature(CapabilityAuthz)
	// The graph surface needs a backend that serves it, advertised as a
	// non-zero graph op version, so it is gated on that rather than implied.
	capabilities.Graph = enabled && versions.Graph > 0
	capabilities.Filters = ServedFilterCaps(
		versions.HasFeature(CapabilityConsumerFilters),
		enabled && versions.Filter > 0,
	).WithGroupPolicyReads(versions.HasFeature(CapabilityGroupPolicyReads))
	return capabilities
}
