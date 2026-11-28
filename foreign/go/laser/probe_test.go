package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// fakeServer answers the one call the probe makes, so the negotiation is tested
// without a deployment.
type fakeServer struct {
	reply []byte
	err   error
	codes []uint32
}

func (server *fakeServer) SendBinaryRequest(
	_ context.Context, code uint32, _ []byte,
) ([]byte, error) {
	server.codes = append(server.codes, code)
	return server.reply, server.err
}

// announcing encodes a reply the way a server would, which also keeps this
// test honest about the contract: a shape that did not round trip would not
// reach the merge at all.
func announcing(t *testing.T, announce wire.BackendAnnounce) *fakeServer {
	t.Helper()
	encoded, err := wire.Marshal(announce)
	if err != nil {
		t.Fatalf("encode the announcement: %v", err)
	}
	return &fakeServer{reply: encoded}
}

func negotiate(t *testing.T, server laser.BinaryRequester) laser.Capabilities {
	t.Helper()
	return laser.NegotiateCapabilities(context.Background(), server, laser.OpenCapabilities())
}

func readyBackend(t *testing.T, id byte, consistency ...wire.Consistency) wire.BackendDescriptor {
	t.Helper()
	backend := wire.NewBackendDescriptor(
		wire.BackendResourceID{ID: wire.IDFromBytes([16]byte{15: id})},
		wire.BackendOperational,
		"Embedded",
		wire.BackendImplementation{Kind: "embedded", Version: "1.0.0"},
		1, 1,
	)
	backend.Readiness = wire.BackendReadiness{Ready: true, ObservedAtMicros: 1}
	if len(consistency) > 0 {
		backend.Query = &wire.QueryCapabilities{
			Dialects:     []wire.SqlDialect{wire.DialectPostgres},
			Consistency:  consistency,
			LogicalTypes: []wire.LogicalTypeKind{wire.KindLong},
		}
	}
	return backend
}

// Raw Apache Iggy does not know the command. That is an answer: the managed
// surfaces are positively absent, which is what lets a consumer read natively
// instead of assuming it could not classify the deployment.
func TestGivenApacheIggyWhenProbedThenShouldReportTheSurfacesAbsent(t *testing.T) {
	server := &fakeServer{err: ierror.InvalidCommand{}}

	negotiated := negotiate(t, server)

	if negotiated.Hello != laser.HelloRejected {
		t.Errorf("want a rejected probe, got %s", negotiated.Hello)
	}
	if negotiated.Managed || negotiated.Versions != nil {
		t.Errorf("want no managed surface, got %+v", negotiated)
	}
	if len(server.codes) != 1 || server.codes[0] != wire.AgdxHelloCode {
		t.Errorf("want one AGDX_HELLO probe, got %v", server.codes)
	}
}

// A server from before the announcement existed answers with an empty body.
// That is the same answer as a refusal, not a failure.
func TestGivenAnEmptyReplyWhenProbedThenShouldReportTheSurfacesAbsent(t *testing.T) {
	negotiated := negotiate(t, &fakeServer{reply: []byte{}})

	if negotiated.Hello != laser.HelloRejected {
		t.Errorf("want a rejected probe, got %s", negotiated.Hello)
	}
	if negotiated.Managed {
		t.Error("an empty body announces nothing")
	}
}

// A probe that establishes nothing must not read as absence. The distinction is
// what keeps an unclassified deployment from being treated as plain Iggy.
func TestGivenAFailedProbeWhenNegotiatedThenShouldEstablishNothing(t *testing.T) {
	for name, server := range map[string]*fakeServer{
		"transport failed":  {err: errors.New("connection reset")},
		"reply undecodable": {reply: []byte{0xff, 0xff, 0xff}},
	} {
		t.Run(name, func(t *testing.T) {
			negotiated := negotiate(t, server)
			if negotiated.Hello != laser.HelloFailed {
				t.Errorf("want a failed probe, got %s", negotiated.Hello)
			}
			if negotiated.Managed || negotiated.Versions != nil {
				t.Errorf("a failed probe must add nothing, got %+v", negotiated)
			}
		})
	}
}

// A ready announcement lights up exactly the surfaces whose op versions it
// advertises, and nothing else.
func TestGivenAReadyAnnouncementWhenNegotiatedThenShouldLightUpTheAdvertisedSurfaces(t *testing.T) {
	announce := wire.NewBackendAnnounce(
		wire.NewOpVersions(1, 1, 2, 0).WithGraph(3).WithFilter(1))
	announce.Backends = []wire.BackendDescriptor{readyBackend(t, 1, wire.ConsistencyEventual)}

	negotiated := negotiate(t, announcing(t, announce))

	if negotiated.Hello != laser.HelloAnswered || !negotiated.Managed {
		t.Fatalf("want an answered managed probe, got %+v", negotiated)
	}
	if !negotiated.Kv.Available || !negotiated.Graph || !negotiated.Query.Available {
		t.Errorf("want kv, graph and query served, got %+v", negotiated)
	}
	// The fork op version is zero, so the surface stays off even though the
	// backend is ready.
	if negotiated.Forks {
		t.Error("a zero fork version must leave forks unavailable")
	}
	if negotiated.Versions == nil || negotiated.Versions.Kv != 2 {
		t.Errorf("want the advertised versions carried, got %+v", negotiated.Versions)
	}
}

// A configured but unanswered backend stays fail-closed: the managed root
// switch is off, so none of the surfaces under it can be reached.
func TestGivenAnUnreadyAnnouncementWhenNegotiatedThenShouldLeaveManagedOff(t *testing.T) {
	announce := wire.BackendAnnounce{
		Versions: wire.NewOpVersions(1, 1, 1, 1).
			WithFeatures(wire.CapabilityConsumerFilters),
		Ready: false,
	}

	negotiated := negotiate(t, announcing(t, announce))

	if negotiated.Hello != laser.HelloAnswered {
		t.Errorf("want an answered probe, got %s", negotiated.Hello)
	}
	if negotiated.Managed || negotiated.Kv.Available || negotiated.Forks {
		t.Errorf("an unready backend must serve no managed surface, got %+v", negotiated)
	}
	// Native filtering is served by the streaming server itself, so it does not
	// wait on the managed backend.
	if !negotiated.Filters.Native {
		t.Error("native filters come from the server, not the managed backend")
	}
}

// The lease contract subsumes fenced compare-and-swap, so a server naming only
// the leases still serves the narrower call.
func TestGivenFencedLeasesWhenNegotiatedThenShouldImplyFencedCompareAndSwap(t *testing.T) {
	announce := wire.NewBackendAnnounce(
		wire.NewOpVersions(1, 1, 1, 1).WithFeatures(wire.CapabilityKvFencedLeases))

	negotiated := negotiate(t, announcing(t, announce))

	if !negotiated.Kv.FencedLeases || !negotiated.Kv.CASFenced {
		t.Errorf("fenced leases must imply fenced CAS, got %+v", negotiated.Kv)
	}
	if negotiated.Kv.CAS {
		t.Error("plain compare-and-swap has its own bit and was not announced")
	}
}

// The caller's set is a starting point, never a ceiling.
func TestGivenAConfiguredCapabilityWhenNegotiatedThenShouldKeepIt(t *testing.T) {
	configured := laser.OpenCapabilities()
	configured.A2AGateway = true
	configured.Kv.CAS = true
	announce := wire.NewBackendAnnounce(wire.NewOpVersions(1, 1, 1, 1))

	negotiated := laser.NegotiateCapabilities(
		context.Background(), announcing(t, announce), configured)

	if !negotiated.A2AGateway {
		t.Error("a gateway the caller configured is never announced, so it must survive")
	}
	if !negotiated.Kv.CAS {
		t.Error("a configured capability must not be cleared by an announcement")
	}
	if !negotiated.Kv.Available {
		t.Error("the announcement must still add what it serves")
	}
}

// Read-your-writes is reachable two ways, and either must raise the ladder.
func TestGivenReadYourWritesWhenAnnouncedThenShouldRaiseTheConsistencyLadder(t *testing.T) {
	byFeature := wire.NewBackendAnnounce(
		wire.NewOpVersions(1, 1, 1, 1).WithFeatures(wire.CapabilityReadYourWrites))
	byBackend := wire.NewBackendAnnounce(wire.NewOpVersions(1, 1, 1, 1))
	byBackend.Backends = []wire.BackendDescriptor{
		readyBackend(t, 2, wire.ConsistencyEventual, wire.ConsistencyReadYourWrites),
	}

	for name, announce := range map[string]wire.BackendAnnounce{
		"feature bit": byFeature, "backend capability": byBackend,
	} {
		t.Run(name, func(t *testing.T) {
			negotiated := negotiate(t, announcing(t, announce))
			if !negotiated.ServesConsistency(wire.ConsistencyReadYourWrites) {
				t.Errorf("want read-your-writes served, got %q", negotiated.Query.Consistency)
			}
		})
	}
}

// Eventual is the floor: it is served whenever the surface is, and an
// unrecognized future level is refused rather than assumed.
func TestGivenTheConsistencyLadderWhenCheckedThenShouldServeEventualAndRefuseTheUnknown(
	t *testing.T,
) {
	open := laser.OpenCapabilities()
	if !open.ServesConsistency(wire.ConsistencyEventual) {
		t.Error("eventual is always served")
	}
	if open.ServesConsistency(wire.ConsistencyReadYourWrites) {
		t.Error("read-your-writes must be announced before it is served")
	}
	if open.ServesConsistency(wire.Consistency("linearizable")) {
		t.Error("an unknown level must be treated as not served")
	}
}

// The announced wait is what a filtered reader paces against, so it has to
// survive the merge with its units intact.
func TestGivenAFilterAnnouncementWhenNegotiatedThenShouldCarryTheServerWait(t *testing.T) {
	announce := wire.NewBackendAnnounce(
		wire.NewOpVersions(1, 1, 1, 1).WithFilter(1).
			WithFeatures(wire.CapabilityConsumerFilters))
	announce.Filters = &wire.FilterAnnounce{
		EvaluatorVersion: wire.FilterEvaluatorVersion,
		Codecs:           []wire.FilterCodec{wire.CodecJSON},
		MaxWaitMs:        2500,
	}

	negotiated := negotiate(t, announcing(t, announce))

	if negotiated.Filters.MaxWait != 2500*time.Millisecond {
		t.Errorf("want a 2.5s wait, got %v", negotiated.Filters.MaxWait)
	}
	if !negotiated.Filters.Catalog {
		t.Error("a ready backend advertising the filter op version serves the catalog")
	}
	if !negotiated.Filters.Evaluates(wire.FilterEvaluatorVersion, wire.CodecJSON) {
		t.Error("the announced codec must evaluate")
	}
	if negotiated.Filters.Evaluates(wire.FilterEvaluatorVersion, wire.CodecAvro) {
		t.Error("a codec the server did not announce must not evaluate")
	}
}

// A server that announced no evaluation predates the announcement, so it
// evaluates as this build does. Assuming otherwise would refuse every filter
// against an older deployment.
func TestGivenNoFilterAnnouncementWhenCheckedThenShouldAssumeAgreement(t *testing.T) {
	var caps laser.FilterCaps
	if !caps.Evaluates(wire.FilterEvaluatorVersion, wire.CodecAvro) {
		t.Error("an unannounced evaluator is assumed to agree")
	}
}

func TestGivenARefreshedCapabilityWhenUsingAnExistingScopedHandleThenShouldUseTheNewGate(t *testing.T) {
	announce := wire.NewBackendAnnounce(wire.NewOpVersions(1, 1, 1, 0))
	announce.Backends = []wire.BackendDescriptor{readyBackend(t, 1, wire.ConsistencyEventual)}
	server := &managedClient{reply: announcing(t, announce).reply}
	client := laser.FromClient(server, laser.Options{})
	scoped := client.WithDefaultStream("events")
	open := scoped.WithCapabilities(laser.OpenCapabilities())
	if caps := client.RefreshCapabilities(context.Background()); !caps.Kv.Available {
		t.Fatal("expected the fresh probe to advertise KV")
	}
	server.reply = batchBytes(t, wire.OkReply(wire.KvOutcome{Kind: wire.KvValue}))
	if _, _, err := scoped.Kv("counts").Get(context.Background(), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := open.Kv("counts").Get(context.Background(), []byte("one")); err == nil {
		t.Fatal("refresh changed a pinned capability override")
	}
}
