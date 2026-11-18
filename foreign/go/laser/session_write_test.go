package laser_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// Two processes that know a seed must reach one conversation without talking
// to each other, which is the whole point of deriving rather than minting.
func TestGivenOneSeedWhenDerivedThenShouldAlwaysReachTheSameConversation(t *testing.T) {
	first := laser.DeriveConversationID("user-42")
	second := laser.DeriveConversationID("user-42")

	if first != second {
		t.Error("one seed must reach one conversation")
	}
	if laser.DeriveConversationID("user-43") == first {
		t.Error("two seeds must not collide")
	}
	if first.IsZero() {
		t.Error("a derived conversation must be a usable id")
	}
}

// A derived id is wire-visible identity: the same seed must reach the same
// conversation in every client, or two peers address different sessions while
// believing they share one.
//
// This is the golden vector the Rust client pins, so matching it proves the two
// derivations agree rather than only that this one is self-consistent.
func TestGivenTheGoldenSeedWhenDerivedThenShouldMatchEveryOtherClient(t *testing.T) {
	const golden = "6X4VM88293CP9BFK3H58TFMMS7"

	if got := laser.DeriveConversationID("user-1").String(); got != golden {
		t.Errorf("the derivation has drifted from the other clients: want %s, got %s",
			golden, got)
	}
}

// The version byte is what makes a derived id stable across builds. Changing
// it remaps every derived conversation in every deployment, so it is pinned by
// the golden vector above and named here as the thing to bump deliberately.
func TestGivenADerivedIdWhenSaltedThenShouldDifferInEachHalf(t *testing.T) {
	derived := laser.DeriveConversationID("user-42")

	// The halves are salted differently, so a single-salt mistake would make
	// every id a repeated 64-bit pattern rather than 128 bits of identity.
	high, low := derivedHalves(t, derived)
	if high == low {
		t.Error("the two halves are salted differently and must differ")
	}
	if high == 0 || low == 0 {
		t.Error("a derived half must not be empty")
	}
}

func derivedHalves(t *testing.T, id wire.ConversationID) (uint64, uint64) {
	t.Helper()
	var high, low uint64
	for index := range 8 {
		high = high<<8 | uint64(id.ID[index])
		low = low<<8 | uint64(id.ID[index+8])
	}
	return high, low
}

// All three parts seed the id, so two applications sharing a stream share a
// session only when they deliberately pick the same namespace and label.
func TestGivenASessionLabelWhenDerivedThenShouldSeparateByStreamAndNamespace(t *testing.T) {
	base := laser.DeriveSessionID("agents", "support", "nightly")

	for name, derived := range map[string]wire.ConversationID{
		"another stream":    laser.DeriveSessionID("billing", "support", "nightly"),
		"another namespace": laser.DeriveSessionID("agents", "triage", "nightly"),
		"another label":     laser.DeriveSessionID("agents", "support", "weekly"),
	} {
		if derived == base {
			t.Errorf("%s must reach a different session", name)
		}
	}

	// The three are joined by a separator rather than concatenated, so a
	// stream ending where a namespace begins cannot collide with the other
	// split of the same characters.
	if laser.DeriveSessionID("a", "b", "c") == laser.DeriveSessionID("ab", "", "c") {
		t.Error("the seed must not depend on where the parts happen to split")
	}
}

// A fresh conversation is for a one-shot exchange, so two must never collide.
func TestGivenFreshConversationsWhenMintedThenShouldBeDistinct(t *testing.T) {
	seen := map[wire.ConversationID]bool{}
	for range 1000 {
		id := laser.NewConversationID()
		if id.IsZero() {
			t.Fatal("a minted conversation must be usable")
		}
		if seen[id] {
			t.Fatal("two minted conversations collided")
		}
		seen[id] = true
	}
}

// One user's work stays on one partition and therefore in order, which is what
// the per-user policy buys.
func TestGivenAPolicyWhenMappingAKeyThenShouldFollowIt(t *testing.T) {
	stable := laser.PerUser.ConversationFor("ada")
	if stable != laser.PerUser.ConversationFor("ada") {
		t.Error("per-user must reach one conversation for one key")
	}

	first := laser.PerCall.ConversationFor("ada")
	if first == laser.PerCall.ConversationFor("ada") {
		t.Error("per-call must open a fresh conversation each time")
	}
}

// A job that runs nightly resumes its session rather than starting a new one
// each time.
func TestGivenALabelledSessionWhenCreatedTwiceThenShouldBeTheSameSession(t *testing.T) {
	server := &streamServer{}
	sessions := streamHandle(server, laser.Options{DefaultStream: "agents"}).Sessions()

	first := sessions.Create("nightly-report")
	second := sessions.Create("nightly-report")

	if first.Conversation() != second.Conversation() {
		t.Error("one label must reach one session")
	}
	if first.Label() != "nightly-report" {
		t.Errorf("want the label kept, got %q", first.Label())
	}
	if sessions.Start().Conversation() == first.Conversation() {
		t.Error("a fresh session is not the labelled one")
	}
}

// A session writes as an agent, so one that names none is refused where the
// mistake was made rather than at the server.
func TestGivenNoAgentWhenBeginningASessionThenShouldRefuseWithTheRemedy(t *testing.T) {
	server := &streamServer{}
	session := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Sessions().Create("nightly")

	_, err := session.Begin(context.Background())

	var invalid *laser.InvalidError
	if !errors.As(err, &invalid) {
		t.Fatalf("want an InvalidError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "Session.As") {
		t.Errorf("want the remedy named, got %v", err)
	}
	if len(server.calls) != 0 {
		t.Error("a refused session must not reach the socket")
	}
}

func TestGivenASessionWhenBegunThenShouldWriteItsStartRecord(t *testing.T) {
	server := &streamServer{}
	sessions := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		SessionsWith(laser.SessionConfig{
			Namespace:   "support",
			IdleTimeout: 90 * time.Second,
		})
	session := sessions.Create("nightly").As(agentID(t, "planner"))

	if _, err := session.Begin(context.Background()); err != nil {
		t.Fatalf("begin: %v", err)
	}

	envelope := sentEnvelope(t, server)
	if envelope.Kind != wire.KindStatus {
		t.Errorf("want a status, got %q", envelope.Kind)
	}
	if envelope.TaskState == nil || *envelope.TaskState != wire.TaskSubmitted {
		t.Errorf("want a submitted session, got %v", envelope.TaskState)
	}
	var start wire.SessionStart
	if err := wire.Unmarshal(envelope.Body, &start); err != nil {
		t.Fatalf("decode the start: %v", err)
	}
	if start.Label == nil || *start.Label != "nightly" {
		t.Errorf("want the label written, got %v", start.Label)
	}
	if start.Namespace == nil || *start.Namespace != "support" {
		t.Errorf("want the namespace written, got %v", start.Namespace)
	}
	if start.IdleTimeoutMicros == nil || *start.IdleTimeoutMicros != 90_000_000 {
		t.Errorf("want the idle timeout written, got %v", start.IdleTimeoutMicros)
	}
	if start.Agent != agentID(t, "planner") {
		t.Errorf("want the author written, got %v", start.Agent)
	}
	// An operator needs to know which client opened a session.
	if start.SDK.Language != "go" || start.SDK.Version == "" {
		t.Errorf("want this client named, got %+v", start.SDK)
	}
	// The session is the partition key, so its records stay ordered.
	if string(server.sentPartitions.Value) != session.Conversation().String() {
		t.Errorf("want the session as the key, got %q", server.sentPartitions.Value)
	}
}

func TestGivenASessionWhenCompletedThenShouldCloseItTerminally(t *testing.T) {
	server := &streamServer{}
	session := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Sessions().Create("nightly").As(agentID(t, "planner"))

	if _, err := session.Complete(context.Background(), "all done"); err != nil {
		t.Fatalf("complete: %v", err)
	}

	envelope := sentEnvelope(t, server)
	if envelope.TaskState == nil || *envelope.TaskState != wire.TaskCompleted {
		t.Errorf("want a completed session, got %v", envelope.TaskState)
	}
	if !envelope.Last {
		t.Error("a completed session is finished")
	}
	var end wire.SessionEnd
	if err := wire.Unmarshal(envelope.Body, &end); err != nil {
		t.Fatalf("decode the end: %v", err)
	}
	if end.Reason == nil || *end.Reason != "all done" {
		t.Errorf("want the reason written, got %v", end.Reason)
	}
}

// A reader needs to see why a session ended, not only that it stopped.
func TestGivenAFailedSessionWhenClosedThenShouldCarryTheError(t *testing.T) {
	server := &streamServer{}
	session := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Sessions().Create("nightly").As(agentID(t, "planner"))

	refusal := "the model refused"
	if _, err := session.Fail(context.Background(), wire.AgentErrorBody{
		Code: wire.ErrorToolFailure, Message: &refusal,
	}); err != nil {
		t.Fatalf("fail: %v", err)
	}

	envelope := sentEnvelope(t, server)
	if envelope.TaskState == nil || *envelope.TaskState != wire.TaskFailed {
		t.Errorf("want a failed session, got %v", envelope.TaskState)
	}
	var end wire.SessionEnd
	if err := wire.Unmarshal(envelope.Body, &end); err != nil {
		t.Fatalf("decode the end: %v", err)
	}
	if end.Error == nil || end.Error.Message == nil ||
		*end.Error.Message != "the model refused" {
		t.Errorf("want the error written, got %+v", end.Error)
	}
}

// A session's memory is its own, so one session's notes do not leak into
// another's recall.
func TestGivenASessionWhenRememberingThenShouldScopeToTheConversation(t *testing.T) {
	server := &streamServer{}
	sessions := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		SessionsWith(laser.SessionConfig{Namespace: "support"})
	session := sessions.Create("nightly")

	memory := session.Memory()
	scope := memory.Scope()
	if scope.Conversation == nil || *scope.Conversation != session.Conversation() {
		t.Errorf("want the memory scoped to its session, got %+v", scope.Conversation)
	}
	if memory.Namespace() != "support" {
		t.Errorf("want the session namespace, got %q", memory.Namespace())
	}

	// Another session's memory must mint different ids for one body.
	other := sessions.Create("weekly")
	mine := laser.ContentMemoryID(session.Memory().Scope(), laser.MemoryFact, []byte("x"))
	theirs := laser.ContentMemoryID(other.Memory().Scope(), laser.MemoryFact, []byte("x"))
	if mine == theirs {
		t.Error("two sessions must not share a memory item")
	}
}

// A memory namespace can be set apart from the session namespace, for an
// application that shares memory across its sessions.
func TestGivenAMemoryNamespaceWhenSetThenShouldUseIt(t *testing.T) {
	server := &streamServer{}
	sessions := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		SessionsWith(laser.SessionConfig{
			Namespace: "support", MemoryNamespace: "shared",
		})

	if got := sessions.Create("nightly").Memory().Namespace(); got != "shared" {
		t.Errorf("want the memory namespace, got %q", got)
	}
}

// Linking a memory or a graph fact back needs the session named the way other
// surfaces name it.
func TestGivenASessionWhenReferencedThenShouldNameItsStreamAndConversation(t *testing.T) {
	server := &streamServer{}
	session := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		Sessions().Create("nightly")

	reference := session.Reference()
	if reference.Stream != "agents" {
		t.Errorf("want the stream named, got %q", reference.Stream)
	}
	if reference.Session != session.Conversation() {
		t.Error("want the conversation named")
	}
}

// A session's context is bounded by its configuration, or a long session
// builds a prompt that no longer fits.
func TestGivenASessionWhenAssemblingContextThenShouldApplyItsBound(t *testing.T) {
	server := &streamServer{}
	sessions := streamHandle(server, laser.Options{DefaultStream: "agents"}).
		SessionsWith(laser.SessionConfig{ContextTokens: 10})
	session := sessions.Create("nightly")

	assembler := session.Context().Add("agent.sessions",
		contextRecord(t, session.Conversation(), 10, 0, strings.Repeat("a", 400)),
		contextRecord(t, session.Conversation(), 20, 1, strings.Repeat("b", 20)),
	)

	if got := len(assembler.Messages()); got != 1 {
		t.Errorf("want the bound applied, got %d messages", got)
	}
	if assembler.Conversation() != session.Conversation() {
		t.Error("want the session's own conversation assembled")
	}
}

func TestGivenNoConfigurationWhenOpeningSessionsThenShouldTakeTheDefaults(t *testing.T) {
	server := &streamServer{}
	sessions := streamHandle(server, laser.Options{DefaultStream: "agents"}).Sessions()

	config := sessions.Config()
	if config.Stream != "agents" {
		t.Errorf("want the handle's stream, got %q", config.Stream)
	}
	if config.Namespace == "" || config.MemoryNamespace != config.Namespace {
		t.Errorf("want memory to follow the session namespace, got %+v", config)
	}
	if config.ContextTokens != laser.DefaultSessionContextTokens {
		t.Errorf("want the default bound, got %d", config.ContextTokens)
	}
	if config.SDK.Language != "go" {
		t.Errorf("want this client named, got %+v", config.SDK)
	}
}
