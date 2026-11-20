package laser_test

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/laserdata/laser-sdk/foreign/go/laser"
	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// contractHeader mirrors one entry of the shared header block fixtures.
type contractHeader struct {
	Key   string `json:"key"`
	Kind  string `json:"kind"`
	Value any    `json:"value"`
}

// readContractFixture reads one file of the Rust-owned fixture corpus.
func readContractFixture(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "..", "wire", "fixtures", name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return raw
}

// The spine a plain record carries, rendered through the client's own
// Provenance rather than the contract type beneath it.
//
// The wire test supplies each header's text itself, so it cannot catch a
// client that renders a field into the wrong shape. This one starts from the
// typed Provenance a caller builds, which is where that goes wrong.
func TestGivenAProvenanceWhenRenderedThenShouldMatchTheContractBlock(t *testing.T) {
	var expected []contractHeader
	if err := json.Unmarshal(
		readContractFixture(t, "header_block_generic.json"), &expected); err != nil {
		t.Fatalf("decode the fixture: %v", err)
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
	cause := laser.NewMessageID(1, 42)
	deadline := time.UnixMicro(1_717_171_717_000_000)
	cost := 0.25
	provenance := &laser.Provenance{
		Conversation:       conversation,
		CausalParent:       &cause,
		ParentConversation: &parent,
		RootConversation:   &root,
		Agent:              &agent,
		TargetAgent:        &critic,
		Usage: &laser.LlmUsage{
			InputTokens:  uint64Pointer(100),
			OutputTokens: uint64Pointer(50),
			CostUSD:      &cost,
		},
		DeadlineAt:     &deadline,
		IdempotencyKey: textPointer("job-1"),
		CorrelationID:  textPointer("corr-1"),
		FenceToken:     uint64Pointer(7),
	}

	entries, err := provenance.Headers()
	if err != nil {
		t.Fatalf("render the headers: %v", err)
	}
	rendered := map[string]string{}
	for _, entry := range entries {
		rendered[string(entry.Key.Value)] = headerText(t, entry)
	}
	if len(rendered) != len(expected) {
		t.Fatalf("rendered %d headers, the contract block has %d",
			len(rendered), len(expected))
	}
	for _, want := range expected {
		got, carried := rendered[want.Key]
		if !carried {
			t.Fatalf("the block is missing %s", want.Key)
		}
		if got != contractText(t, want) {
			t.Fatalf("%s rendered as %q, the contract says %q",
				want.Key, got, contractText(t, want))
		}
	}
}

// headerText renders one encoded header the way the fixture spells it.
func headerText(t *testing.T, entry iggcon.HeaderEntry) string {
	t.Helper()
	switch entry.Value.Kind {
	case iggcon.String:
		return string(entry.Value.Value)
	case iggcon.Uint64:
		if len(entry.Value.Value) != 8 {
			t.Fatalf("%s is not eight bytes", entry.Key.Value)
		}
		return strconv.FormatUint(
			binary.LittleEndian.Uint64(entry.Value.Value), 10)
	case iggcon.Double:
		if len(entry.Value.Value) != 8 {
			t.Fatalf("%s is not eight bytes", entry.Key.Value)
		}
		return formatFloat(math.Float64frombits(
			binary.LittleEndian.Uint64(entry.Value.Value)))
	default:
		t.Fatalf("%s has kind %v", entry.Key.Value, entry.Value.Kind)
		return ""
	}
}

// formatFloat renders a float the one way both sides spell it.
func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

// contractText is one fixture entry's value as text.
func contractText(t *testing.T, header contractHeader) string {
	t.Helper()
	switch value := header.Value.(type) {
	case string:
		return value
	case float64:
		// The fixture writes a float as a JSON number, which is how the
		// cost header rides.
		return formatFloat(value)
	default:
		t.Fatalf("%s has an unexpected fixture value %T", header.Key, header.Value)
		return ""
	}
}
