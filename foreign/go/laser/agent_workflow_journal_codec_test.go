package laser

import (
	"encoding/hex"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The journal is cross-client, so its encoding is pinned against bytes the
// Rust client itself produced rather than against this port's own output.
//
// Emitted by a temporary test inside sdk/src/agent/workflow.rs, encoding
// each WorkflowJournalEntry variant through laser_wire::framing::encode_named
// (ciborium), on rustc 1.100.0. A run this client starts resumes on any
// client only while these hold.
var journalVectors = []struct {
	name  string
	entry workflowJournalEntry
	bytes string
}{
	{
		name:  "run_started",
		entry: workflowJournalEntry{Kind: journalRunStarted, Workflow: "pipeline"},
		bytes: "a16a52756e53746172746564a168776f726b666c6f7768706970656c696e65",
	},
	{
		name: "step_dispatched",
		entry: workflowJournalEntry{
			Kind: journalStepDispatched, Label: "settle", Attempt: 2,
			ReplyFrom: []uint64{7, 9},
		},
		bytes: "a16e5374657044697370617463686564a3656c6162656c66736574746c6567617474656d7074026a7265706c795f66726f6d820709",
	},
	{
		name: "step_completed",
		entry: workflowJournalEntry{
			Kind: journalStepCompleted, Label: "settle", Output: []byte{1, 2, 3},
			Agents: []string{"worker"}, Input: "d1",
		},
		bytes: "a16d53746570436f6d706c65746564a4656c6162656c66736574746c65666f757470757483010203666167656e74738166776f726b657265696e707574626431",
	},
	{
		name: "step_completed_no_input",
		entry: workflowJournalEntry{
			Kind: journalStepCompleted, Label: "settle",
			Output: []byte{}, Agents: []string{},
		},
		bytes: "a16d53746570436f6d706c65746564a3656c6162656c66736574746c65666f757470757480666167656e747380",
	},
	{
		name:  "step_compensated",
		entry: workflowJournalEntry{Kind: journalStepCompensated, Label: "settle"},
		bytes: "a16f53746570436f6d70656e7361746564a1656c6162656c66736574746c65",
	},
}

// A Go-written journal has to be byte-identical to a Rust-written one, or a
// run started on one client cannot be resumed on the other.
func TestGivenAJournalEntryWhenEncodedThenShouldMatchTheRustClientsBytes(t *testing.T) {
	for _, vector := range journalVectors {
		encoded, err := wire.Marshal(vector.entry)
		if err != nil {
			t.Fatalf("%s: encode: %v", vector.name, err)
		}
		if got := hex.EncodeToString(encoded); got != vector.bytes {
			t.Errorf("%s:\n want %s\n  got %s", vector.name, vector.bytes, got)
		}
	}
}

// And the other direction: a journal another client wrote has to fold here.
func TestGivenTheRustClientsBytesWhenDecodedThenShouldReadBackTheEntry(t *testing.T) {
	for _, vector := range journalVectors {
		raw, err := hex.DecodeString(vector.bytes)
		if err != nil {
			t.Fatalf("%s: bad vector: %v", vector.name, err)
		}
		var decoded workflowJournalEntry
		if err := wire.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("%s: decode: %v", vector.name, err)
		}
		if decoded.Kind != vector.entry.Kind ||
			decoded.Workflow != vector.entry.Workflow ||
			decoded.Label != vector.entry.Label ||
			decoded.Attempt != vector.entry.Attempt ||
			decoded.Input != vector.entry.Input {
			t.Errorf("%s: want %+v, got %+v", vector.name, vector.entry, decoded)
		}
		if string(decoded.Output) != string(vector.entry.Output) {
			t.Errorf("%s: want output %v, got %v",
				vector.name, vector.entry.Output, decoded.Output)
		}
		if len(decoded.ReplyFrom) != len(vector.entry.ReplyFrom) {
			t.Errorf("%s: want reply positions %v, got %v",
				vector.name, vector.entry.ReplyFrom, decoded.ReplyFrom)
		}
	}
}

// A variant this client does not know is another client's newer entry.
// Refusing it is deliberate: folding a history only partly understood would
// resume a run from a state nobody wrote.
func TestGivenAnUnknownJournalVariantWhenDecodedThenShouldRefuseIt(t *testing.T) {
	// {"StepVanished": {"label": "settle"}}
	raw, err := hex.DecodeString("a16c5374657056616e6973686564a1656c6162656c66736574746c65")
	if err != nil {
		t.Fatalf("bad vector: %v", err)
	}
	var decoded workflowJournalEntry
	if err := wire.Unmarshal(raw, &decoded); err == nil {
		t.Fatal("want an unknown journal variant refused")
	}
}
