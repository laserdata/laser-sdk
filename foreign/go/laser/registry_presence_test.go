package laser_test

import (
	"context"
	"errors"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// presenceServer answers the client-metadata discovery read with scripted
// pages, counting the reads so a test can prove the ttl suppressed one.
//
// Pages are served in order and the last one repeats, so a test that only
// cares about a single page does not have to script a refresh's worth.
type presenceServer struct {
	clusterOfOne
	pages  []wire.ClientMetadataList
	reads  int
	refuse error
}

func (server *presenceServer) Close() error { return nil }

func (server *presenceServer) SendBinaryRequest(
	_ context.Context, code uint32, _ []byte,
) ([]byte, error) {
	if code != wire.AgdxGetClientsMetadataCode {
		return nil, errors.New("unexpected code")
	}
	server.reads++
	if server.refuse != nil {
		return nil, server.refuse
	}
	page := wire.ClientMetadataList{}
	if len(server.pages) > 0 {
		page = server.pages[0]
		if len(server.pages) > 1 {
			server.pages = server.pages[1:]
		}
	}
	encoded, err := wire.Marshal(page)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

// presenceCapabilities is a deployment that answered the hello, so a refused
// read is a real failure rather than "this server serves no presence".
func presenceCapabilities() *laser.Capabilities {
	capabilities := laser.OpenCapabilities()
	capabilities.Managed = true
	capabilities.Hello = laser.HelloAnswered
	return &capabilities
}

// advertised encodes one connection advertising presence for an agent,
// authenticated as principal.
func advertised(t *testing.T, agent string, inbox string, principal *uint32) wire.ClientMetadata {
	t.Helper()
	presence := wire.NewAgentPresence(agentID(t, agent))
	if inbox != "" {
		presence.Inbox = &inbox
	}
	encoded, err := wire.Marshal(presence)
	if err != nil {
		t.Fatalf("encode presence: %v", err)
	}
	return wire.ClientMetadata{UserID: principal, Metadata: encoded}
}

func principalOf(value uint32) *uint32 { return &value }

// seedPresence folds one page of connections into a fresh registry.
func seedPresence(
	t *testing.T, clients []wire.ClientMetadata,
) (*laser.AgentRegistry, *presenceServer) {
	t.Helper()
	server := &presenceServer{pages: []wire.ClientMetadataList{{Clients: clients}}}
	handle := laser.FromClient(server, laser.Options{Capabilities: presenceCapabilities()})
	registry := laser.NewAgentRegistry()
	if _, err := registry.RefreshPresence(context.Background(), handle); err != nil {
		t.Fatalf("refresh presence: %v", err)
	}
	return registry, server
}

// Presence is what binds a claimed agent id to the identity the deployment
// actually authenticated, so both halves have to survive the fold.
func TestGivenAdvertisedPresenceWhenRefreshedThenShouldBindTheAgentToItsPrincipal(t *testing.T) {
	registry, _ := seedPresence(t, []wire.ClientMetadata{
		advertised(t, "planner", "planner-inbox", principalOf(7)),
	})

	if held := registry.Presences(); held != 1 {
		t.Fatalf("expected one presence, got %d", held)
	}
	principal := registry.PrincipalFor(agentID(t, "planner"))
	if principal == nil || *principal != laser.PrincipalID(7) {
		t.Fatalf("expected principal 7, got %v", principal)
	}
	if inbox := registry.InboxFor(agentID(t, "planner")); inbox != "planner-inbox" {
		t.Fatalf("expected the advertised inbox, got %q", inbox)
	}
	// The principal-scoped read answers only for the right principal, which
	// is what stops one connection reading another's inbox as its own.
	if inbox := registry.InboxForPrincipal(
		agentID(t, "planner"), laser.PrincipalID(7)); inbox != "planner-inbox" {
		t.Fatalf("expected the inbox for its own principal, got %q", inbox)
	}
	if inbox := registry.InboxForPrincipal(
		agentID(t, "planner"), laser.PrincipalID(8)); inbox != "" {
		t.Fatalf("expected no inbox for another principal, got %q", inbox)
	}
}

// The metadata channel is opaque, so a blob that is not a presence body
// belongs to some other application and must not break the fold around it.
func TestGivenOpaqueMetadataWhenRefreshedThenShouldSkipItAndKeepFolding(t *testing.T) {
	registry, _ := seedPresence(t, []wire.ClientMetadata{
		{UserID: principalOf(1), Metadata: []byte("not a presence body")},
		advertised(t, "auditor", "auditor-inbox", principalOf(2)),
	})

	if held := registry.Presences(); held != 1 {
		t.Fatalf("expected the one real presence, got %d", held)
	}
	if registry.PrincipalFor(agentID(t, "auditor")) == nil {
		t.Error("the presence after the opaque blob should still have folded")
	}
}

// Paging the whole connection table is the dominant per-send cost, so a
// refresh inside the ttl must reuse what it holds rather than read again.
func TestGivenARecentRefreshWhenRefreshedAgainThenShouldReuseWithoutReading(t *testing.T) {
	registry, server := seedPresence(t, []wire.ClientMetadata{
		advertised(t, "planner", "planner-inbox", principalOf(7)),
	})
	reads := server.reads

	handle := laser.FromClient(server, laser.Options{Capabilities: presenceCapabilities()})
	held, err := registry.RefreshPresence(context.Background(), handle)
	if err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if held != 1 {
		t.Fatalf("expected the held presence to be reported, got %d", held)
	}
	if server.reads != reads {
		t.Fatalf("expected no read inside the ttl, got %d more", server.reads-reads)
	}
}

// Apache Iggy keeps no connection metadata. A deployment that never answered
// the hello is read as serving no presence rather than failing the send.
func TestGivenADeploymentThatServesNoPresenceWhenRefreshedThenShouldFoldNothing(t *testing.T) {
	server := &presenceServer{refuse: errors.New("no such command")}
	capabilities := laser.OpenCapabilities()
	capabilities.Hello = laser.HelloRejected
	handle := laser.FromClient(server, laser.Options{Capabilities: &capabilities})
	registry := laser.NewAgentRegistry()

	held, err := registry.RefreshPresence(context.Background(), handle)
	if err != nil {
		t.Fatalf("expected a rejected hello to read as no presence, got %v", err)
	}
	if held != 0 {
		t.Fatalf("expected no presence, got %d", held)
	}
	if server.reads != 0 {
		t.Fatal("a server that rejected the hello should not be read at all")
	}
}

// A deployment whose hello outcome was never established, refusing the read,
// is also read as serving no presence: nothing promised the surface.
func TestGivenAnUnpromisedReadThatRefusesWhenRefreshedThenShouldFoldNothing(t *testing.T) {
	server := &presenceServer{refuse: errors.New("no such command")}
	capabilities := laser.OpenCapabilities()
	capabilities.Hello = laser.HelloUnknown
	handle := laser.FromClient(server, laser.Options{Capabilities: &capabilities})
	registry := laser.NewAgentRegistry()

	held, err := registry.RefreshPresence(context.Background(), handle)
	if err != nil {
		t.Fatalf("expected an unpromised refusal to read as no presence, got %v", err)
	}
	if held != 0 {
		t.Fatalf("expected no presence, got %d", held)
	}
}

// A deployment that did answer the hello and then refuses is a real failure:
// the surface was promised, so swallowing the error would hide a broken one.
func TestGivenAPromisedReadThatRefusesWhenRefreshedThenShouldReportIt(t *testing.T) {
	server := &presenceServer{refuse: errors.New("backend down")}
	handle := laser.FromClient(server, laser.Options{Capabilities: presenceCapabilities()})
	registry := laser.NewAgentRegistry()

	if _, err := registry.RefreshPresence(context.Background(), handle); err == nil {
		t.Fatal("expected a promised surface's refusal to be reported")
	}
}

// Presence is live transport truth keyed by the claimed agent, so the latest
// connection claiming an id wins rather than the first one read.
func TestGivenTwoConnectionsClaimingOneAgentWhenRefreshedThenShouldKeepTheLast(t *testing.T) {
	registry, _ := seedPresence(t, []wire.ClientMetadata{
		advertised(t, "planner", "old-inbox", principalOf(1)),
		advertised(t, "planner", "new-inbox", principalOf(2)),
	})

	if inbox := registry.InboxFor(agentID(t, "planner")); inbox != "new-inbox" {
		t.Fatalf("expected the last claim to win, got %q", inbox)
	}
	principal := registry.PrincipalFor(agentID(t, "planner"))
	if principal == nil || *principal != laser.PrincipalID(2) {
		t.Fatalf("expected the last claim's principal, got %v", principal)
	}
}

// A page cursor that does not advance would page the same connections
// forever, so it is reported rather than spun on.
func TestGivenANonAdvancingCursorWhenReadingAllThenShouldRefuseRatherThanSpin(t *testing.T) {
	stuck := uint32(5)
	server := &presenceServer{pages: []wire.ClientMetadataList{{
		Clients:    []wire.ClientMetadata{advertised(t, "planner", "inbox", principalOf(1))},
		NextCursor: &stuck,
	}}}
	handle := laser.FromClient(server, laser.Options{Capabilities: presenceCapabilities()})

	_, err := handle.ClientMetadata().All(context.Background())
	var protocol *laser.ProtocolError
	if err == nil {
		t.Fatal("expected a stuck cursor to be refused")
	}
	if !errors.As(err, &protocol) {
		t.Fatalf("expected *ProtocolError, got %T: %v", err, err)
	}
}

// A cursor that does advance pages to the end and collects every page.
func TestGivenSeveralPagesWhenReadingAllThenShouldFollowTheCursorToTheEnd(t *testing.T) {
	first := uint32(1)
	server := &presenceServer{pages: []wire.ClientMetadataList{
		{
			Clients:    []wire.ClientMetadata{advertised(t, "planner", "a", principalOf(1))},
			NextCursor: &first,
		},
		{Clients: []wire.ClientMetadata{advertised(t, "auditor", "b", principalOf(2))}},
	}}
	handle := laser.FromClient(server, laser.Options{Capabilities: presenceCapabilities()})

	clients, err := handle.ClientMetadata().All(context.Background())
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(clients) != 2 {
		t.Fatalf("expected both pages' connections, got %d", len(clients))
	}
	if server.reads != 2 {
		t.Fatalf("expected two reads, got %d", server.reads)
	}
}

// A connection that advertised nothing is not a presence, and must not cost
// a decode attempt or land as an empty one.
func TestGivenAConnectionWithNoMetadataWhenRefreshedThenShouldNotFoldAPresence(t *testing.T) {
	registry, _ := seedPresence(t, []wire.ClientMetadata{
		{ClientID: 1, UserID: principalOf(3)},
	})

	if held := registry.Presences(); held != 0 {
		t.Fatalf("expected no presence from a silent connection, got %d", held)
	}
}

// An agent with no live presence reads as absent rather than as an agent
// under no principal, since routing treats those differently.
func TestGivenNoPresenceWhenReadThenShouldReportAbsentRatherThanUnbound(t *testing.T) {
	registry := laser.NewAgentRegistry()

	if principal := registry.PrincipalFor(agentID(t, "ghost")); principal != nil {
		t.Fatalf("expected no principal for an agent with no presence, got %v", *principal)
	}
	if inbox := registry.InboxFor(agentID(t, "ghost")); inbox != "" {
		t.Fatalf("expected no inbox, got %q", inbox)
	}
}

// Presence is separate from the card fold, so folding one must not disturb
// the other: a card without presence still resolves by capability.
func TestGivenACardAndNoPresenceWhenResolvedThenShouldStillBeCapable(t *testing.T) {
	now := time.Now()
	registry, _ := seedPresence(t, nil)
	registry.Observe(agentID(t, "planner"), cardFor("summarise", nil, nil, nil, nil), now)

	if len(registry.Capable("summarise", now)) != 1 {
		t.Error("a card with no live presence should still advertise its skill")
	}
}

// Guard the interface the fake stands in for, so a change to it fails here
// rather than in a cast at run time.
var _ iggcon.Client = (*presenceServer)(nil)

// A server without live presence still routes, because the card names where
// the agent reads.
func TestGivenACardInboxAndNoPresenceWhenInboxAskedThenShouldFallBackToTheCard(t *testing.T) {
	registry := laser.NewAgentRegistry()
	inbox := "worker-inbox"
	card := cardFor("run", nil, nil, nil, nil)
	card.Inbox = &inbox
	registry.Observe(agentID(t, "worker"), card, time.Now())

	if got := registry.InboxFor(agentID(t, "worker")); got != "worker-inbox" {
		t.Fatalf("expected the card inbox, got %q", got)
	}
	if got := registry.InboxFor(agentID(t, "stranger")); got != "" {
		t.Fatalf("expected no inbox for an agent with neither, got %q", got)
	}
}

func TestGivenPresenceAndACardInboxWhenInboxAskedThenShouldPreferPresence(t *testing.T) {
	registry, _ := seedPresence(t, []wire.ClientMetadata{
		advertised(t, "worker", "live-inbox", principalOf(1)),
	})
	inbox := "card-inbox"
	card := cardFor("run", nil, nil, nil, nil)
	card.Inbox = &inbox
	registry.Observe(agentID(t, "worker"), card, time.Now())

	if got := registry.InboxFor(agentID(t, "worker")); got != "live-inbox" {
		t.Fatalf("expected the live presence inbox, got %q", got)
	}
}
