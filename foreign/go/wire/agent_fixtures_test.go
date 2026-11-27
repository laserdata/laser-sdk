package wire_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func fixturePath(t *testing.T, name string) string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "..", "wire", "fixtures", name))
	if err != nil {
		t.Fatalf("resolve %s: %v", name, err)
	}
	return path
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(fixturePath(t, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return raw
}

// envelopeFixtures are the corpus files that hold a bare AgentEnvelope,
// including the negatives, which must still decode: they are structurally
// valid CBOR that Validate rejects on the per-kind matrix.
var envelopeFixtures = []string{
	"agent_command.bin",
	"agent_command_signed.bin",
	"agent_response.bin",
	"agent_event.bin",
	"agent_chunk.bin",
	"agent_chunk_open.bin",
	"agent_chunk_terminal.bin",
	"agent_error.bin",
	"agent_status_task.bin",
	"agent_status_card.bin",
	"agent_status_session.bin",
	"agent_must_understand.bin",
	"agent_invalid_chunk_late_deadline.bin",
	"agent_invalid_chunk_no_sequence.bin",
	"agent_invalid_chunk_open_no_operation.bin",
	"agent_invalid_command_no_correlation.bin",
	"agent_invalid_error_last.bin",
	"agent_invalid_event_task_state.bin",
	"agent_invalid_response_channel.bin",
	"agent_invalid_status_bad_operation.bin",
	"agent_invalid_status_no_operation.bin",
	"agent_invalid_status_fact_no_deadline.bin",
}

// A Go producer must write the same bytes the Rust producer writes, or the
// other clients read a different record. Decoding each envelope fixture into
// the Go struct and re-encoding proves the field set, the field order and the
// skip rules all match.
func TestGivenEnvelopeFixturesWhenDecodedThenShouldReEncodeByteIdentically(t *testing.T) {
	for _, name := range envelopeFixtures {
		t.Run(name, func(t *testing.T) {
			raw := readFixture(t, name)

			var envelope wire.AgentEnvelope
			if err := wire.Unmarshal(raw, &envelope); err != nil {
				t.Fatalf("decode: %v", err)
			}
			again, err := wire.Marshal(envelope)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if string(again) != string(raw) {
				t.Fatalf("re-encode differs from the Rust bytes (%d in, %d out)%s",
					len(raw), len(again), describeDiff(raw, again))
			}
		})
	}
}

func describeDiff(want, got []byte) string {
	limit := min(len(want), len(got))
	for i := 0; i < limit; i++ {
		if want[i] != got[i] {
			lo := max(0, i-12)
			hiWant := min(len(want), i+12)
			hiGot := min(len(got), i+12)
			return "\n  first difference at byte " + itoa(i) +
				"\n  want ..." + hexOf(want[lo:hiWant]) +
				"\n  got  ..." + hexOf(got[lo:hiGot])
		}
	}
	return "\n  one is a prefix of the other"
}

func hexOf(payload []byte) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 0, len(payload)*3)
	for _, b := range payload {
		out = append(out, digits[b>>4], digits[b&0x0f], ' ')
	}
	return string(out)
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits []byte
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}
