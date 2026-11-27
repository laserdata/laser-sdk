package wire_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// fixtureHeader mirrors one entry of header_block_envelope.json.
type fixtureHeader struct {
	Key   string          `json:"key"`
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value"`
}

// The header block is what a plain Iggy consumer and a projection route on
// without decoding the body, so it must match the Rust block key for key,
// kind for kind, in the same sorted order.
func TestGivenAnEnvelopeWhenHeadersEncodedThenShouldMatchTheFixtureBlock(t *testing.T) {
	var expected []fixtureHeader
	if err := json.Unmarshal(readFixture(t, "header_block_envelope.json"), &expected); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 2})}
	parent := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 4})}
	root := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 5})}
	source, err := wire.NewAgentID("planner")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}

	envelope := &wire.AgentEnvelope{
		Kind:         wire.KindEvent,
		Conversation: conversation,
		Parent:       &parent,
		Root:         &root,
		Source:       source,
	}
	// No target, so a shared session topic addresses every agent.
	block := wire.HeadersForEnvelope(envelope, wire.ContentJSON, true).Encode()

	if len(block) != len(expected) {
		t.Fatalf("want %d headers, got %d: %v", len(expected), len(block), keysOf(block))
	}
	for i, want := range expected {
		got := block[i]
		if got.Key != want.Key {
			t.Errorf("header %d: want key %q, got %q", i, want.Key, got.Key)
			continue
		}
		if kind := kindName(got.Value.Kind); kind != want.Kind {
			t.Errorf("header %q: want kind %q, got %q", want.Key, want.Kind, kind)
			continue
		}
		wantValue := string(want.Value)
		if gotValue := jsonValueOf(t, got.Value); gotValue != wantValue {
			t.Errorf("header %q: want value %s, got %s", want.Key, wantValue, gotValue)
		}
	}
}

func keysOf(block []wire.HeaderEntry) []string {
	keys := make([]string, len(block))
	for i, entry := range block {
		keys[i] = entry.Key
	}
	return keys
}

func kindName(kind wire.HeaderFieldKind) string {
	switch kind {
	case wire.HeaderText:
		return "text"
	case wire.HeaderUint8:
		return "uint8"
	case wire.HeaderUint32:
		return "uint32"
	case wire.HeaderUint64:
		return "uint64"
	default:
		return "float64"
	}
}

func jsonValueOf(t *testing.T, field wire.HeaderField) string {
	t.Helper()
	var value any
	switch field.Kind {
	case wire.HeaderText:
		value = field.Text
	case wire.HeaderUint8:
		value = field.Uint8
	case wire.HeaderUint32:
		value = field.Uint32
	case wire.HeaderUint64:
		// The shared blocks render a 64-bit value as a string, because one can
		// exceed the exact integer range a JSON number carries.
		value = strconv.FormatUint(field.Uint64, 10)
	default:
		value = field.Float64
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("encode header value: %v", err)
	}
	return string(encoded)
}

// A generic record carries no envelope stamp, so neither the version nor the
// codec header appears.
func TestGivenAGenericRecordWhenHeadersEncodedThenShouldOmitTheEnvelopeStamp(t *testing.T) {
	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 7})}
	block := wire.NewRecordHeaders(conversation).Encode()

	if len(block) != 1 {
		t.Fatalf("want only the conversation header, got %v", keysOf(block))
	}
	if block[0].Key != wire.HeaderConversationID {
		t.Fatalf("want %q, got %q", wire.HeaderConversationID, block[0].Key)
	}
}

// Broadcast rides agdx.to as the literal star and must never read back as an
// agent id.
func TestGivenAnAddresseeWhenParsedThenShouldDistinguishBroadcastFromAnAgent(t *testing.T) {
	broadcast, err := wire.ParseAddressee("*")
	if err != nil {
		t.Fatalf("parse broadcast: %v", err)
	}
	if !broadcast.IsBroadcast {
		t.Fatal("a star must parse as broadcast")
	}
	if broadcast.String() != "*" {
		t.Fatalf("broadcast must render as a star, got %q", broadcast.String())
	}

	named, err := wire.ParseAddressee("planner")
	if err != nil {
		t.Fatalf("parse agent: %v", err)
	}
	if named.IsBroadcast {
		t.Fatal("a named agent must not parse as broadcast")
	}
	if named.Agent.String() != "planner" {
		t.Fatalf("want planner, got %q", named.Agent.String())
	}

	if _, err := wire.ParseAddressee(""); err == nil {
		t.Error("an empty addressee must be rejected")
	}
}

// The command codes are a pinned cross-repo contract, so the arithmetic that
// derives them is asserted rather than trusted.
func TestGivenTheCommandCodesWhenComputedThenShouldMatchThePinnedValues(t *testing.T) {
	for name, pair := range map[string][2]uint32{
		"hello":           {wire.AgdxHelloCode, 1_000_000},
		"query":           {wire.AgdxQueryCode, 1_000_200},
		"kv get":          {wire.AgdxKvGetCode, 1_000_300},
		"fork create":     {wire.AgdxForkCreateCode, 1_000_400},
		"graph query":     {wire.AgdxGraphQueryCode, 1_000_600},
		"session get":     {wire.AgdxSessionGetCode, 1_000_710},
		"filtered poll":   {wire.AgdxFilteredPollCode, 1_000_800},
		"authz whoami":    {wire.AgdxAuthzWhoamiCode, 1_000_100},
		"control publish": {wire.AgdxControlPublishCode, 1_000_108},
	} {
		if pair[0] != pair[1] {
			t.Errorf("%s: want code %d, got %d", name, pair[1], pair[0])
		}
	}

	if !wire.IsBarrierRead(wire.AgdxKvGetCode) {
		t.Error("a key-value get is a barrier read")
	}
	if wire.IsBarrierRead(wire.AgdxKvSetCode) {
		t.Error("a key-value set is not a barrier read")
	}
	if !wire.IsIdempotentManagedRequest(wire.AgdxKvSetCode) {
		t.Error("a key-value set needs a stable operation identity")
	}
	if wire.IsIdempotentManagedRequest(wire.AgdxKvGetCode) {
		t.Error("a key-value get does not mutate")
	}
}

// Stream scoping must be idempotent: scoping an already-scoped name twice
// would otherwise produce stream:a/stream:b/name.
func TestGivenAScopedResourceWhenScopedAgainThenShouldStayUnchanged(t *testing.T) {
	once := wire.ScopedResource("orders", "sessions")
	if once != "stream:orders/sessions" {
		t.Fatalf("want stream:orders/sessions, got %q", once)
	}
	if twice := wire.ScopedResource("other", once); twice != once {
		t.Fatalf("re-scoping changed the name: %q", twice)
	}

	stream, local, ok := wire.SplitScopedResource(once)
	if !ok || stream != "orders" || local != "sessions" {
		t.Fatalf("split gave (%q, %q, %v)", stream, local, ok)
	}
	if _, _, ok := wire.SplitScopedResource("sessions"); ok {
		t.Error("a bare name has no stream part")
	}
	if _, _, ok := wire.SplitScopedResource("stream:orders"); ok {
		t.Error("the stream resource itself has no local part")
	}
}

// The ops topics are pinned constants, and drift breaks the managed control
// surface silently.
func TestGivenTheOpsTopicsWhenScopedThenShouldMatchTheTenancyForm(t *testing.T) {
	if got := wire.StreamOpsTopic("orders", wire.ChangesTopic); got != "stream:orders/_agdx/changes" {
		t.Fatalf("want stream:orders/_agdx/changes, got %q", got)
	}
	if wire.OpsStream != "_agdx" {
		t.Fatalf("the ops stream name is pinned, got %q", wire.OpsStream)
	}
}

// The generic record block, which a record carrying no agent envelope stamps.
// A plain Iggy consumer routes on these without decoding any body, so the keys
// and their kinds must match the contract entry for entry.
func TestGivenGenericRecordHeadersWhenEncodedThenShouldMatchTheFixtureBlock(t *testing.T) {
	var expected []fixtureHeader
	if err := json.Unmarshal(
		readFixture(t, "header_block_generic.json"), &expected); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}

	conversation := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 2})}
	parent := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 4})}
	root := wire.ConversationID{ID: wire.IDFromBytes([16]byte{15: 5})}
	agent, err := wire.NewAgentID("planner")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}
	critic, err := wire.NewAgentID("critic")
	if err != nil {
		t.Fatalf("agent id: %v", err)
	}

	headers := wire.NewRecordHeaders(conversation)
	headers.Parent = &parent
	headers.Root = &root
	headers.Agent = &agent
	addressee := wire.AgentAddressee(critic)
	headers.Addressee = &addressee
	causalParent := "1:42"
	headers.CausalParent = &causalParent
	idempotency := "job-1"
	headers.IdempotencyKey = &idempotency
	correlation := "corr-1"
	headers.Correlation = &correlation
	fence := uint64(7)
	headers.Fence = &fence
	deadline := uint64(1_717_171_717_000_000)
	headers.DeadlineMicros = &deadline
	inputTokens := uint64(100)
	headers.InputTokens = &inputTokens
	outputTokens := uint64(50)
	headers.OutputTokens = &outputTokens
	cost := 0.25
	headers.CostUSD = &cost

	block := headers.Encode()
	if len(block) != len(expected) {
		t.Fatalf("want %d headers, got %d: %v", len(expected), len(block), keysOf(block))
	}
	for index, want := range expected {
		got := block[index]
		if got.Key != want.Key {
			t.Errorf("header %d: want key %q, got %q", index, want.Key, got.Key)
			continue
		}
		if kind := kindName(got.Value.Kind); kind != want.Kind {
			t.Errorf("header %q: want kind %q, got %q", want.Key, want.Kind, kind)
			continue
		}
		if value := jsonValueOf(t, got.Value); value != string(want.Value) {
			t.Errorf("header %q: want value %s, got %s", want.Key, want.Value, value)
		}
	}
}
