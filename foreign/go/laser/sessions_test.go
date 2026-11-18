package laser_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

type sessionServer struct {
	iggcon.Client
	outcome  wire.SessionOutcome
	failure  *wire.SessionError
	codes    []uint32
	requests [][]byte
}

func (server *sessionServer) Close() error { return nil }

func (server *sessionServer) SendBinaryRequest(
	_ context.Context, code uint32, payload []byte,
) ([]byte, error) {
	server.codes = append(server.codes, code)
	server.requests = append(server.requests, payload)
	if server.failure != nil {
		return wire.Marshal(wire.SessionReply{Err: server.failure})
	}
	return wire.Marshal(wire.SessionReply{Outcome: &server.outcome})
}

func sessionCapabilities() *laser.Capabilities {
	versions := wire.NewOpVersions(
		wire.QueryOpVersion, wire.ControlOpVersion, wire.KvOpVersion, wire.ForkOpVersion)
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	capabilities.Sessions = true
	capabilities.Hello = laser.HelloAnswered
	capabilities.Versions = &versions
	return &capabilities
}

func sessionHandle(server iggcon.Client, options laser.Options) *laser.Laser {
	if options.Capabilities == nil {
		options.Capabilities = sessionCapabilities()
	}
	return laser.FromClient(server, options)
}

func decodeSessionRequest[T any](t *testing.T, server *sessionServer, index int, target *T) {
	t.Helper()
	if err := wire.Unmarshal(server.requests[index], target); err != nil {
		t.Fatalf("decode request %d: %v", index, err)
	}
}

func conversation(marker byte) wire.ConversationID {
	return wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: marker})}
}

// The index is per stream, so a session id alone does not say where to look.
// Every request names the stream before anything else.
func TestGivenASessionReadWhenSentThenShouldNameItsStream(t *testing.T) {
	server := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionInfoOutcome,
		Info: &wire.SessionInfo{Stream: "agents", Status: wire.SessionActive},
	}}

	info, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).
		Sessions().Get(context.Background(), conversation(1))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if info.Stream != "agents" {
		t.Errorf("want the stream carried back, got %q", info.Stream)
	}

	var request wire.SessionGetRequest
	decodeSessionRequest(t, server, 0, &request)
	if request.Stream != "agents" {
		t.Errorf("want the stream named, got %q", request.Stream)
	}
	if request.ID != conversation(1) {
		t.Error("want the session named")
	}
	if server.codes[0] != wire.AgdxSessionGetCode {
		t.Errorf("want the session get code, got %d", server.codes[0])
	}
}

// A session stream is not a managed resource name, so it is sent as given
// rather than scoped: scoping it would name an index that does not exist.
func TestGivenAScopingHandleWhenReadingSessionsThenShouldSendTheStreamUnscoped(t *testing.T) {
	server := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionInfoOutcome,
		Info: &wire.SessionInfo{Stream: "agents", Status: wire.SessionActive},
	}}

	if _, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).
		Sessions().Get(context.Background(), conversation(1)); err != nil {
		t.Fatalf("get: %v", err)
	}

	var request wire.SessionGetRequest
	decodeSessionRequest(t, server, 0, &request)
	if request.Stream != "agents" {
		t.Errorf("a session stream is not scoped, got %q", request.Stream)
	}
}

// Without a stream there is nowhere to look, so the refusal says how to give
// one rather than failing at the server.
func TestGivenNoStreamWhenReadingSessionsThenShouldRefuseWithTheRemedy(t *testing.T) {
	server := &sessionServer{}

	_, err := sessionHandle(server, laser.Options{}).Sessions().
		Get(context.Background(), conversation(1))

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "DefaultStream") {
		t.Errorf("want the remedy named, got %v", err)
	}
	if len(server.codes) != 0 {
		t.Error("a read with no stream must not reach the socket")
	}
}

// A named stream reads another index than the handle's default.
func TestGivenANamedStreamWhenReadingSessionsThenShouldUseIt(t *testing.T) {
	server := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionInfoOutcome,
		Info: &wire.SessionInfo{Stream: "billing", Status: wire.SessionActive},
	}}

	if _, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).
		SessionsIn("billing").Get(context.Background(), conversation(1)); err != nil {
		t.Fatalf("get: %v", err)
	}

	var request wire.SessionGetRequest
	decodeSessionRequest(t, server, 0, &request)
	if request.Stream != "billing" {
		t.Errorf("want the named stream, got %q", request.Stream)
	}
}

func TestGivenAFilteredListingWhenFetchedThenShouldCarryEveryFilter(t *testing.T) {
	server := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionPageOutcome, Page: &wire.SessionPage{},
	}}
	root := conversation(2)
	agent, err := wire.NewAgentID("planner")
	if err != nil {
		t.Fatalf("build an agent id: %v", err)
	}

	if _, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).Sessions().
		List().Status(wire.SessionActive).Root(root).LabelPrefix("nightly-").
		Agent(agent).Text("invoice").Page(3).Results(25).
		Fetch(context.Background()); err != nil {
		t.Fatalf("list: %v", err)
	}

	var request wire.SessionListRequest
	decodeSessionRequest(t, server, 0, &request)
	if request.Status == nil || *request.Status != wire.SessionActive {
		t.Errorf("want the status filter, got %v", request.Status)
	}
	if request.Root == nil || *request.Root != root {
		t.Errorf("want the root filter, got %v", request.Root)
	}
	if request.LabelPrefix == nil || *request.LabelPrefix != "nightly-" {
		t.Errorf("want the label prefix, got %v", request.LabelPrefix)
	}
	if request.Agent == nil || *request.Agent != agent {
		t.Errorf("want the agent filter, got %v", request.Agent)
	}
	if request.Text == nil || *request.Text != "invoice" {
		t.Errorf("want the text filter, got %v", request.Text)
	}
	if request.Page != 3 || request.Results != 25 {
		t.Errorf("want page 3 of 25, got %d of %d", request.Page, request.Results)
	}
}

// An unset filter must be absent on the wire, or it would narrow the listing
// to the empty string rather than not narrowing it at all.
func TestGivenAnUnfilteredListingWhenFetchedThenShouldSendNoFilters(t *testing.T) {
	server := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionPageOutcome, Page: &wire.SessionPage{},
	}}

	if _, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).Sessions().
		List().Fetch(context.Background()); err != nil {
		t.Fatalf("list: %v", err)
	}

	var request wire.SessionListRequest
	decodeSessionRequest(t, server, 0, &request)
	if request.Status != nil || request.Root != nil || request.LabelPrefix != nil ||
		request.Agent != nil || request.Text != nil {
		t.Errorf("want no filters, got %+v", request)
	}
}

// A walk would shift under appends, so the first page returns a boundary and
// later pages pass it back to keep anything folded after it out.
func TestGivenAWalkBoundWhenPagedThenShouldPinTheBoundary(t *testing.T) {
	server := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionPageOutcome, Page: &wire.SessionPage{},
	}}
	bound := laser.SessionBound{AsOf: 1_700_000, Series: 4}

	if _, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).Sessions().
		List().Page(2).Bound(bound).Fetch(context.Background()); err != nil {
		t.Fatalf("list: %v", err)
	}

	var request wire.SessionListRequest
	decodeSessionRequest(t, server, 0, &request)
	if request.AsOf == nil || *request.AsOf != 1_700_000 {
		t.Errorf("want the boundary pinned, got %v", request.AsOf)
	}
	// The boundary means nothing without the series it belongs to, since a
	// rebuilt index restarts the sequence.
	if request.Series == nil || *request.Series != 4 {
		t.Errorf("want the series carried with it, got %v", request.Series)
	}
}

func TestGivenATimelineReadWhenFetchedThenShouldCarryItsPageAndBound(t *testing.T) {
	server := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionEventsOutcome, Events: &wire.SessionEventsPage{},
	}}

	if _, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).Sessions().
		Events(conversation(3)).Page(2).Results(50).
		Bound(laser.SessionBound{AsOf: 99, Series: 1}).
		Fetch(context.Background()); err != nil {
		t.Fatalf("events: %v", err)
	}

	var request wire.SessionEventsRequest
	decodeSessionRequest(t, server, 0, &request)
	if request.ID != conversation(3) || request.Page != 2 || request.Results != 50 {
		t.Errorf("want the timeline page, got %+v", request)
	}
	if request.AsOf == nil || request.Series == nil {
		t.Errorf("want the boundary carried, got %v / %v", request.AsOf, request.Series)
	}
	if server.codes[0] != wire.AgdxSessionEventsCode {
		t.Errorf("want the events code, got %d", server.codes[0])
	}
}

func TestGivenAStateReadWhenFetchedThenShouldCarryTheHistoryBound(t *testing.T) {
	server := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionStateOutcome, State: &wire.SessionStateView{},
	}}

	if _, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).Sessions().
		State(context.Background(), conversation(4), 10); err != nil {
		t.Fatalf("state: %v", err)
	}

	var request wire.SessionStateRequest
	decodeSessionRequest(t, server, 0, &request)
	if request.HistoryLimit != 10 {
		t.Errorf("want ten history rows, got %d", request.HistoryLimit)
	}
}

// The lane-only read exists so a native lane writer does not pay for the rest,
// so the flag has to reach the wire.
func TestGivenALaneOnlyReadWhenFetchedThenShouldAskForTheLaneAlone(t *testing.T) {
	plain := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionSourcesOutcome, Sources: &wire.SessionSources{},
	}}
	if _, err := sessionHandle(plain, laser.Options{DefaultStream: "agents"}).Sessions().
		Sources(context.Background(), conversation(5)); err != nil {
		t.Fatalf("sources: %v", err)
	}
	var full wire.SessionSourcesRequest
	decodeSessionRequest(t, plain, 0, &full)
	if full.LaneOnly {
		t.Error("a full read must not ask for the lane alone")
	}

	lane := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionSourcesOutcome, Sources: &wire.SessionSources{},
	}}
	if _, err := sessionHandle(lane, laser.Options{DefaultStream: "agents"}).Sessions().
		LaneSources(context.Background(), conversation(5)); err != nil {
		t.Fatalf("lane sources: %v", err)
	}
	var only wire.SessionSourcesRequest
	decodeSessionRequest(t, lane, 0, &only)
	if !only.LaneOnly {
		t.Error("want the lane-only flag set")
	}
}

func TestGivenAChangeFeedReadWhenFetchedThenShouldCarryItsPage(t *testing.T) {
	server := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionChangesOutcome, Changes: &wire.SessionChanges{},
	}}

	if _, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).Sessions().
		Changes(context.Background(), 4, 20); err != nil {
		t.Fatalf("changes: %v", err)
	}

	var request wire.SessionChangesRequest
	decodeSessionRequest(t, server, 0, &request)
	if request.Page != 4 || request.Results != 20 {
		t.Errorf("want page 4 of 20, got %d of %d", request.Page, request.Results)
	}
}

func TestGivenNoSessionSurfaceWhenCalledThenShouldRefuseIt(t *testing.T) {
	capabilities := sessionCapabilities()
	capabilities.Sessions = false
	server := &sessionServer{}

	_, err := sessionHandle(server, laser.Options{
		DefaultStream: "agents", Capabilities: capabilities}).Sessions().
		Get(context.Background(), conversation(1))

	var unsupported *laser.UnsupportedError
	if !errors.As(err, &unsupported) {
		t.Fatalf("want an UnsupportedError, got %T: %v", err, err)
	}
	if len(server.codes) != 0 {
		t.Error("an unsupported call must not reach the socket")
	}
}

func TestGivenASessionFailureWhenReportedThenShouldProjectOntoAResultCode(t *testing.T) {
	server := &sessionServer{failure: &wire.SessionError{
		Kind: wire.SessionNotFound, Message: "no such session"}}

	_, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).Sessions().
		Get(context.Background(), conversation(1))

	var failure *laser.SessionError
	if !errors.As(err, &failure) {
		t.Fatalf("want a SessionError, got %T: %v", err, err)
	}
	if failure.ResultCode().String() == "" {
		t.Error("a session failure must project onto a result code")
	}
}

// An outcome shaped for another read means the client and the server disagree
// about the contract.
func TestGivenASessionOutcomeForAnotherReadWhenHandledThenShouldRefuseIt(t *testing.T) {
	server := &sessionServer{outcome: wire.SessionOutcome{
		Kind: wire.SessionPageOutcome, Page: &wire.SessionPage{},
	}}

	_, err := sessionHandle(server, laser.Options{DefaultStream: "agents"}).Sessions().
		Get(context.Background(), conversation(1))

	var codec *laser.CodecError
	if !errors.As(err, &codec) {
		t.Fatalf("want a CodecError, got %T: %v", err, err)
	}
}
