package laser

import (
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// HelloOutcome is what the connect-time AGDX_HELLO probe established.
//
// It is kept apart from the capability bits because "absent" and "unknown" are
// different answers. A group consumer reads natively only when the managed
// surfaces are positively absent, never when the probe simply failed.
type HelloOutcome int

const (
	// HelloUnknown means no probe ran: a caller-supplied client or an explicit
	// capability set.
	HelloUnknown HelloOutcome = iota
	// HelloAnswered means the server returned a managed announcement.
	HelloAnswered
	// HelloRejected means the server answered without one: Apache Iggy
	// refusing the command, or an older server's empty body. The managed
	// surfaces are absent.
	HelloRejected
	// HelloFailed means the probe established nothing, because the transport
	// failed, the deadline passed, or the reply did not decode.
	HelloFailed
)

func (outcome HelloOutcome) String() string {
	switch outcome {
	case HelloAnswered:
		return "answered"
	case HelloRejected:
		return "rejected"
	case HelloFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// QueryCaps is the managed query surface and the strongest read-consistency it
// serves.
type QueryCaps struct {
	// Available reports whether the query surface is served.
	Available bool
	// Consistency is the strongest level the surface honors. The ladder runs
	// eventual then read-your-writes, so a level implies every weaker one.
	Consistency wire.Consistency
	// Keyword reports lexical relevance search. Off means a text query is
	// refused locally before sending, because an unaware server would drop the
	// additive field and answer wider than asked.
	Keyword bool
}

// KvCaps is the managed key-value surface and its conditional-write support.
type KvCaps struct {
	// Available reports plain get, set, delete and scan.
	Available bool
	// CAS reports compare-and-swap, independent of plain get and set.
	CAS bool
	// CASFenced reports compare-and-swap gated on a live fence sequence.
	CASFenced bool
	// FencedLeases reports the revocable fenced-lease contract: holder-scoped
	// acquire, renewal, fence-validated release, fenced compare-and-swap, and
	// the barriered read. A server without it would decode the reshaped
	// payloads under the old contract instead of refusing them, so every one of
	// those calls gates on this bit.
	FencedLeases bool
}

// FilterCaps is server-side consumer filtering.
type FilterCaps struct {
	// Native reports filtered reads, fenced acknowledgments, previews, sample
	// tests and validation. Served by the streaming server itself, so it needs
	// no managed backend.
	Native bool
	// Catalog reports saved filters, revisions, group bindings and mutation
	// outcomes. Needs a ready managed backend advertising the filter op
	// version.
	Catalog bool
	// GroupPolicyReads reports group-aware reads: the server resolves a
	// group's own policy, delivers an unbound group unfiltered, and fences
	// acknowledgments by policy generation.
	GroupPolicyReads bool
	// Evaluation is the evaluator version and codecs the server announced, nil
	// from a server that predates the announcement, which then evaluates as
	// this build does.
	Evaluation *wire.FilterAnnounce
	// MaxWait is the longest filtered wait the server holds, zero when it
	// serves none, so a filtered reader paces idle rounds itself.
	MaxWait time.Duration
}

// Evaluates reports whether the server runs a filter built for this evaluator
// version and codec exactly as this build does. A server that announced
// nothing is assumed to agree, which is what keeps an older deployment usable.
func (caps FilterCaps) Evaluates(evaluatorVersion uint32, codec wire.FilterCodec) bool {
	if caps.Evaluation == nil {
		return true
	}
	return caps.Evaluation.Evaluates(evaluatorVersion, codec)
}

// Capabilities is what the connected deployment serves. Managed is the root
// switch: with no backend, every managed surface below it is unavailable.
type Capabilities struct {
	Managed bool
	Query   QueryCaps
	Kv      KvCaps
	// Graph is the managed knowledge-graph surface. Agentic memory composes the
	// query and graph surfaces, so it has no capability of its own.
	Graph bool
	// Forks is managed copy-on-write forks of the read model.
	Forks bool
	// A2AGateway is a managed A2A gateway. It is never announced, only
	// configured by the caller.
	A2AGateway bool
	// Sessions is managed reads of agent sessions in one stream.
	Sessions         bool
	Materializations bool
	// StreamTenancy reports a deployment that scopes every managed name to one
	// stream, with each stream's change feed on its own ops topic.
	StreamTenancy bool
	// Watch is the change feed, off when the deployment does not publish it.
	Watch bool
	// Authz is the authorization control surface.
	Authz   bool
	Filters FilterCaps
	// Versions are the wire op versions the server advertised, nil against
	// Apache Iggy and pre-versioned servers. When present, a surface whose
	// pinned op version is not the advertised one fails before a round trip.
	Versions *wire.OpVersions
	// Backends are the structured, versioned, secret-free observations the
	// deployment advertised at connect.
	Backends []wire.BackendDescriptor
	Hello    HelloOutcome
}

// OpenCapabilities is the baseline every deployment has: nothing beyond the
// open SDK surface.
func OpenCapabilities() Capabilities {
	return Capabilities{Query: QueryCaps{Consistency: wire.ConsistencyEventual}}
}

// IsOpenOnly reports a deployment that advertised nothing beyond the open SDK
// surface, whatever its capability probe answered.
func (caps Capabilities) IsOpenOnly() bool {
	open := OpenCapabilities()
	return !caps.Managed && caps.Query == open.Query && caps.Kv == open.Kv &&
		!caps.Graph && !caps.Forks && !caps.A2AGateway && !caps.Sessions &&
		!caps.Materializations && !caps.StreamTenancy && !caps.Watch && !caps.Authz &&
		caps.Filters == open.Filters && caps.Versions == nil && len(caps.Backends) == 0
}

// ServesConsistency reports whether the query surface serves this level.
// Because the levels form a ladder this is "level is at most what is served",
// so eventual is always served. An unknown future level is treated as not
// served, which keeps the check fail-safe.
// EnabledBackends are the backends an operator asked to be running.
//
// A disabled backend is not a problem to wait out: nobody asked it to serve,
// so its state says nothing about whether the deployment is ready.
func (capabilities Capabilities) EnabledBackends() []wire.BackendDescriptor {
	enabled := make([]wire.BackendDescriptor, 0, len(capabilities.Backends))
	for _, backend := range capabilities.Backends {
		if backend.DesiredState == wire.BackendEnabledState {
			enabled = append(enabled, backend)
		}
	}
	return enabled
}

// UnreadyBackends are the enabled backends that are not serving, which is
// what a caller waiting on readiness is waiting for.
func (capabilities Capabilities) UnreadyBackends() []wire.BackendDescriptor {
	unready := make([]wire.BackendDescriptor, 0, len(capabilities.Backends))
	for _, backend := range capabilities.EnabledBackends() {
		if backend.ObservedState != wire.BackendObservedReady ||
			!backend.Readiness.Ready {
			unready = append(unready, backend)
		}
	}
	return unready
}

// Ready reports a deployment whose every enabled backend is serving.
//
// A deployment with no enabled backend at all is not ready: there is nothing
// there to serve, which is a different thing from something that is serving.
func (capabilities Capabilities) Ready() bool {
	if !capabilities.Managed {
		return false
	}
	if len(capabilities.EnabledBackends()) == 0 {
		return false
	}
	return len(capabilities.UnreadyBackends()) == 0
}

// ServesConsistency reports whether the deployment serves reads at level.
func (capabilities Capabilities) ServesConsistency(level wire.Consistency) bool {
	switch level {
	case wire.ConsistencyEventual, wire.ConsistencyReadYourWrites:
		return level.Rank() <= capabilities.Consistency().Rank()
	default:
		return false
	}
}

// Consistency is the strongest read-consistency the query surface serves.
func (capabilities Capabilities) Consistency() wire.Consistency {
	if capabilities.Query.Consistency == "" {
		return wire.ConsistencyEventual
	}
	return capabilities.Query.Consistency
}

// mergeFeatures lights up the surfaces the advertised feature bits name.
//
// Every merge is additive: the caller's set is the starting point and never a
// ceiling, so a client built with an explicit capability still gains what the
// connected deployment serves.
func (capabilities *Capabilities) mergeFeatures(versions wire.OpVersions) {
	capabilities.Kv.CAS = capabilities.Kv.CAS || versions.HasFeature(wire.CapabilityKvCas)
	capabilities.Kv.CASFenced = capabilities.Kv.CASFenced ||
		versions.HasFeature(wire.CapabilityKvCasFenced)
	capabilities.Kv.FencedLeases = capabilities.Kv.FencedLeases ||
		versions.HasFeature(wire.CapabilityKvFencedLeases)
	// The lease contract subsumes fenced compare-and-swap, so a server naming
	// the leases serves it whether or not it names the narrower bit too.
	capabilities.Kv.CASFenced = capabilities.Kv.CASFenced || capabilities.Kv.FencedLeases
	capabilities.Sessions = capabilities.Sessions ||
		versions.HasFeature(wire.CapabilitySessions)
	capabilities.Materializations = capabilities.Materializations ||
		versions.HasFeature(wire.CapabilityMaterializations)
	capabilities.StreamTenancy = capabilities.StreamTenancy ||
		versions.HasFeature(wire.CapabilityStreamTenancy)
	capabilities.Query.Keyword = capabilities.Query.Keyword ||
		versions.HasFeature(wire.CapabilityKeywordSearch)
	capabilities.Watch = capabilities.Watch || versions.HasFeature(wire.CapabilityWatch)
	capabilities.Authz = capabilities.Authz || versions.HasFeature(wire.CapabilityAuthz)
	capabilities.Filters.Native = capabilities.Filters.Native ||
		versions.HasFeature(wire.CapabilityConsumerFilters)
	capabilities.Filters.GroupPolicyReads = capabilities.Filters.GroupPolicyReads ||
		versions.HasFeature(wire.CapabilityGroupPolicyReads)
	if versions.HasFeature(wire.CapabilityReadYourWrites) {
		capabilities.raiseConsistency(wire.ConsistencyReadYourWrites)
	}
}

func (capabilities *Capabilities) raiseConsistency(level wire.Consistency) {
	if level.Rank() > capabilities.Consistency().Rank() {
		capabilities.Query.Consistency = level
	}
}

// mergeAnnouncement folds a server's AGDX_HELLO reply into the capability set.
//
// A surface the caller set explicitly is kept, so a ready announcement only
// adds what the deployment serves. The managed surfaces are gated on the
// backend announcing itself ready, which is what keeps a configured but
// unanswered backend fail-closed.
func (capabilities *Capabilities) mergeAnnouncement(announce *wire.BackendAnnounce) {
	versions := announce.Versions
	copied := versions
	capabilities.Versions = &copied
	capabilities.Backends = announce.Backends
	capabilities.Authz = capabilities.Authz || versions.HasFeature(wire.CapabilityAuthz)
	capabilities.Filters.Native = capabilities.Filters.Native ||
		versions.HasFeature(wire.CapabilityConsumerFilters)
	// Served by the streaming server itself, like the native reads: a server
	// without a managed backend resolves every group as unbound.
	capabilities.Filters.GroupPolicyReads = capabilities.Filters.GroupPolicyReads ||
		versions.HasFeature(wire.CapabilityGroupPolicyReads)
	if capabilities.Filters.Native {
		capabilities.Filters.Evaluation = announce.Filters
		capabilities.Filters.MaxWait = 0
		if announce.Filters != nil {
			capabilities.Filters.MaxWait =
				time.Duration(announce.Filters.MaxWaitMs) * time.Millisecond
		}
	}
	if !announce.IsReady() {
		return
	}
	capabilities.Managed = true
	capabilities.Filters.Catalog = capabilities.Filters.Catalog ||
		((capabilities.Filters.Native || capabilities.Filters.GroupPolicyReads) &&
			versions.Filter > 0)
	capabilities.Kv.Available = capabilities.Kv.Available || versions.Kv > 0
	capabilities.Forks = capabilities.Forks || versions.Fork > 0
	capabilities.Graph = capabilities.Graph || versions.Graph > 0
	capabilities.mergeFeatures(versions)
	for _, backend := range announce.Backends {
		if !backend.Readiness.Ready || backend.Query == nil {
			continue
		}
		capabilities.Query.Available = capabilities.Query.Available || versions.Query > 0
		for _, level := range backend.Query.Consistency {
			if level == wire.ConsistencyReadYourWrites {
				capabilities.raiseConsistency(wire.ConsistencyReadYourWrites)
			}
		}
	}
}
