package wire

// The capability bits advertised in OpVersions.Features.
//
// These are loose bit constants rather than a type, so they stay distinct from
// the authorization Feature a grant names.
//
// Each constant names one managed sub-feature a server serves beyond the base
// surface, so a binary client feature-detects it before attempting the op. The
// set is additive and pinned cross-repo: a newer server sets a new bit and an
// older client ignores it, simply not lighting up that capability.
const (
	// CapabilityKvCas means the key-value store serves compare-and-swap.
	CapabilityKvCas uint64 = 1 << 0
	// CapabilityReadYourWrites means the query surface honors that consistency.
	CapabilityReadYourWrites uint64 = 1 << 1
	// CapabilityStrongConsistency was retired with linearizable query reads.
	// No server sets it and the bit is never reused.
	CapabilityStrongConsistency uint64 = 1 << 2
	// CapabilityKvCasFenced means the store serves fenced compare-and-swap.
	CapabilityKvCasFenced uint64 = 1 << 3
	// CapabilityAgentWorkflow was retired with the run registry. No server sets
	// it and the bit is never reused.
	CapabilityAgentWorkflow uint64 = 1 << 4
	// CapabilityKeywordSearch means the query surface serves lexical relevance
	// search.
	CapabilityKeywordSearch uint64 = 1 << 5
	// CapabilityWatch means the deployment publishes the change feed for
	// bindings that opt into notify.
	CapabilityWatch uint64 = 1 << 6
	// CapabilityAuthz means the streaming server serves the authorization
	// control band.
	CapabilityAuthz uint64 = 1 << 7
	// CapabilityDestinations was retired with lakehouse destinations. No
	// server sets it and the bit is never reused.
	CapabilityDestinations uint64 = 1 << 8
	// CapabilityKvFencedLeases means the store serves the revocable fenced-lease
	// contract: holder-scoped acquire, renewal, holder-and-fence-validated
	// release, fenced compare-and-swap requiring a live lease, and the
	// barriered read. It subsumes CapabilityKvCasFenced, and a client must not
	// send the reshaped lease ops to a server without this bit, which would
	// silently decode them under the old contract.
	CapabilityKvFencedLeases uint64 = 1 << 9
	// CapabilityConsumerFilters means the streaming server serves consumer
	// filters: filtered reads, fenced acknowledgments, previews, sample tests
	// and validation. Server-native, so it holds with the managed backend
	// disabled. The saved-filter catalog additionally needs the backend's
	// filter op version.
	CapabilityConsumerFilters uint64 = 1 << 10
	// CapabilityGroupPolicyReads means the streaming server serves group-aware
	// reads: a group reference resolves the group's own policy, an unbound
	// group is delivered unfiltered without payload decoding, and pages and
	// acknowledgments carry the execution mode and policy generation. A
	// client must not send the automatic selector without this bit.
	CapabilityGroupPolicyReads uint64 = 1 << 11
	// CapabilitySessions means the managed backend serves session reads for one
	// named stream.
	CapabilitySessions uint64 = 1 << 12
	// CapabilityStreamTenancy means every managed name is scoped to one stream,
	// and each stream's change feed and dead letters ride their own ops
	// topics.
	CapabilityStreamTenancy uint64 = 1 << 13
	// CapabilityMaterializations means the managed backend serves whole-record
	// projections, control replies that wait for the applied outcome, and
	// binding status on the projection browse.
	CapabilityMaterializations uint64 = 1 << 14
)

// OpVersions is the wire op versions a server accepts, one per surface, plus
// the capability feature bits it advertises.
//
// A zero version means a surface is not served and is skipped on encode, so a
// hello frame from before that surface existed stays byte-identical.
type OpVersions struct {
	Query   uint32 `cbor:"query"`
	Control uint32 `cbor:"control"`
	Kv      uint32 `cbor:"kv"`
	Fork    uint32 `cbor:"fork"`
	// Agent is the AGDX envelope version the Cloud consumes for its
	// conversation projections.
	Agent uint32 `cbor:"agent,omitempty"`
	// Graph is the knowledge-graph op version served. Agentic memory rides
	// this plus the query surface, so it has no op version of its own.
	Graph uint32 `cbor:"graph,omitempty"`
	// Filter is the consumer-filter catalog op version served by the managed
	// backend. Zero means saved filters and group bindings are not served.
	Filter uint32 `cbor:"filter,omitempty"`
	// Features is the OR of the advertised capability bits.
	Features uint64 `cbor:"features,omitempty"`
}

// NewOpVersions names the base surface versions. The remaining surfaces are
// added with the With* helpers, so a new surface lands without breaking
// callers.
func NewOpVersions(query, control, kv, fork uint32) OpVersions {
	return OpVersions{Query: query, Control: control, Kv: kv, Fork: fork}
}

// WithAgent advertises an agent-envelope version.
func (versions OpVersions) WithAgent(agent uint32) OpVersions {
	versions.Agent = agent
	return versions
}

// WithGraph advertises a knowledge-graph op version.
func (versions OpVersions) WithGraph(graph uint32) OpVersions {
	versions.Graph = graph
	return versions
}

// WithFilter advertises a consumer-filter catalog version.
func (versions OpVersions) WithFilter(filter uint32) OpVersions {
	versions.Filter = filter
	return versions
}

// WithFeatures advertises capability bits.
func (versions OpVersions) WithFeatures(features uint64) OpVersions {
	versions.Features = features
	return versions
}

// HasFeature reports whether a bit, or every bit of a set, is advertised.
func (versions OpVersions) HasFeature(bit uint64) bool {
	return versions.Features&bit == bit
}

// HelloReply is the body of the AGDX_HELLO probe reply: the wire op versions
// the server and its managed backend accept.
//
// A pre-versioned server answers the probe with an empty body, which a client
// treats as "no versions advertised" rather than an error.
type HelloReply struct {
	Versions OpVersions `cbor:"versions"`
}
