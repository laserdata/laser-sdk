package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// crockfordAlphabet is Crockford base32, the display form every wire id shares.
const crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

// idBytes is the payload width of a wire id: 128 bits, big-endian.
const idBytes = 16

// ID is the shared body of the wire id newtypes. It holds the 128-bit value as
// its big-endian bytes, which is both the wire form and, unlike a pair of
// words, directly comparable.
//
// Go has no 128-bit integer, so the Rust u128 is carried as its bytes and the
// text form is produced by the same five-bits-at-a-time walk.
type ID [idBytes]byte

// IDFromBytes wraps the 16 big-endian payload bytes.
func IDFromBytes(payload [idBytes]byte) ID { return ID(payload) }

// Bytes is the big-endian payload form.
func (id ID) Bytes() [idBytes]byte { return [idBytes]byte(id) }

// IsZero reports the all-zero id, which the contract treats as absent.
func (id ID) IsZero() bool {
	for _, b := range id {
		if b != 0 {
			return false
		}
	}
	return true
}

// String renders the 26-character Crockford base32 form.
func (id ID) String() string {
	var out [26]byte
	// 26 characters carry 130 bits, five per character, filled from the least
	// significant end so the top character holds the remaining three bits.
	value := id
	for slot := 25; slot >= 0; slot-- {
		out[slot] = crockfordAlphabet[value[idBytes-1]&0x1f]
		value = shiftRight5(value)
	}
	return string(out[:])
}

// shiftRight5 divides the big-endian value by 32.
func shiftRight5(value ID) ID {
	var out ID
	var carry byte
	for i := 0; i < idBytes; i++ {
		out[i] = value[i]>>5 | carry
		carry = value[i] << 3
	}
	return out
}

// IDParseError reports why an id text could not be read.
type IDParseError struct{ Reason string }

func (e *IDParseError) Error() string { return "wire: invalid id: " + e.Reason }

// ParseID reads the 26-character Crockford base32 form, accepting lower case.
func ParseID(text string) (ID, error) {
	if len(text) != 26 {
		return ID{}, &IDParseError{Reason: fmt.Sprintf("want 26 characters, got %d", len(text))}
	}
	var value ID
	for i := 0; i < len(text); i++ {
		digit := strings.IndexByte(crockfordAlphabet, byte(unicode.ToUpper(rune(text[i]))))
		if digit < 0 {
			return ID{}, &IDParseError{Reason: fmt.Sprintf("character %q is not Crockford base32", text[i])}
		}
		// The top character may use only three of its five bits, the ULID
		// overflow rule, so a leading digit above 7 cannot fit 128 bits.
		if i == 0 && digit > 7 {
			return ID{}, &IDParseError{Reason: "value overflows 128 bits"}
		}
		value = shiftLeft5(value)
		value[idBytes-1] |= byte(digit)
	}
	return value, nil
}

func shiftLeft5(value ID) ID {
	var out ID
	var carry byte
	for i := idBytes - 1; i >= 0; i-- {
		out[i] = value[i]<<5 | carry
		carry = value[i] >> 3
	}
	return out
}

// MarshalCBOR writes the id as a fixed-width 16-byte CBOR byte string, not a
// bignum, matching the wire_id! serde in wire/src/agent.rs.
func (id ID) MarshalCBOR() (cborform.Value, error) {
	payload := id.Bytes()
	// The binary encoding carries the bytes and a human-readable one carries
	// the Crockford text, which is what the contract's own serializer does.
	return cborform.Textual{Binary: cborform.Bytes(payload[:]), Text: id.String()}, nil
}

// UnmarshalCBOR accepts the binary byte-string form, the JSON array-of-bytes
// form, and the Crockford text form, the three shapes the Rust visitor takes.
func (id *ID) UnmarshalCBOR(item cborform.Value) error {
	switch typed := item.(type) {
	case cborform.Bytes:
		if len(typed) != idBytes {
			return fmt.Errorf("wire: an id wants %d bytes, got %d", idBytes, len(typed))
		}
		copy(id[:], typed)
		return nil
	case cborform.Text:
		parsed, err := ParseID(string(typed))
		if err != nil {
			return err
		}
		*id = parsed
		return nil
	case cborform.Array:
		if len(typed) != idBytes {
			return fmt.Errorf("wire: an id wants %d bytes, got %d", idBytes, len(typed))
		}
		for i, element := range typed {
			number, ok := element.(cborform.Uint)
			if !ok || number > 0xff {
				return errors.New("wire: an id byte array must hold bytes")
			}
			id[i] = byte(number)
		}
		return nil
	case cborform.Null:
		return errors.New("wire: an id must not be null")
	default:
		return fmt.Errorf("wire: cannot decode %T as an id", item)
	}
}

// The distinct id types. They share ID's representation but stay separate
// types so a conversation cannot be passed where a correlation is wanted.
type (
	// RecordID is a record's producer-assigned identity, a ULID minted before
	// publish. Portable in a way a log position is not.
	RecordID struct{ ID }
	// ConversationID is the unit of ordering and the partition key.
	ConversationID struct{ ID }
	// CorrelationID pairs a request with its reply.
	CorrelationID struct{ ID }
	// ChannelID groups a chunk stream when several run under one correlation.
	ChannelID struct{ ID }
	// MemoryID identifies a memory row.
	MemoryID struct{ ID }
	// NodeID identifies a graph node.
	NodeID struct{ ID }
	// EdgeID identifies a graph edge.
	EdgeID struct{ ID }
)

// logPositionBytes is the width of the packed Iggy locator.
const logPositionBytes = 20

// LogPosition is the Iggy binding's packing of the causal locator.
//
// It rides the wire as one opaque byte string so the envelope names no server:
// stream, topic and partition as big-endian u32 then the offset as u64. A
// consumer that cannot read the bytes falls back to the portable record id.
type LogPosition struct {
	StreamID    uint32
	TopicID     uint32
	PartitionID uint32
	Offset      uint64
}

// PackedBytes is the 20-byte big-endian wire form.
func (position LogPosition) PackedBytes() [logPositionBytes]byte {
	var out [logPositionBytes]byte
	binary.BigEndian.PutUint32(out[0:4], position.StreamID)
	binary.BigEndian.PutUint32(out[4:8], position.TopicID)
	binary.BigEndian.PutUint32(out[8:12], position.PartitionID)
	binary.BigEndian.PutUint64(out[12:20], position.Offset)
	return out
}

// LogPositionFromBytes unpacks the 20-byte locator.
func LogPositionFromBytes(payload [logPositionBytes]byte) LogPosition {
	return LogPosition{
		StreamID:    binary.BigEndian.Uint32(payload[0:4]),
		TopicID:     binary.BigEndian.Uint32(payload[4:8]),
		PartitionID: binary.BigEndian.Uint32(payload[8:12]),
		Offset:      binary.BigEndian.Uint64(payload[12:20]),
	}
}

// MarshalCBOR writes the locator as one opaque byte string.
func (position LogPosition) MarshalCBOR() (cborform.Value, error) {
	payload := position.PackedBytes()
	return cborform.Bytes(payload[:]), nil
}

// UnmarshalCBOR reads the packed locator, accepting the JSON array form too.
func (position *LogPosition) UnmarshalCBOR(item cborform.Value) error {
	var payload [logPositionBytes]byte
	switch typed := item.(type) {
	case cborform.Bytes:
		if len(typed) != logPositionBytes {
			return fmt.Errorf("wire: a log position wants %d bytes, got %d", logPositionBytes, len(typed))
		}
		copy(payload[:], typed)
	case cborform.Array:
		if len(typed) != logPositionBytes {
			return fmt.Errorf("wire: a log position wants %d bytes, got %d", logPositionBytes, len(typed))
		}
		for i, element := range typed {
			number, ok := element.(cborform.Uint)
			if !ok || number > 0xff {
				return errors.New("wire: a log position byte array must hold bytes")
			}
			payload[i] = byte(number)
		}
	default:
		return fmt.Errorf("wire: cannot decode %T as a log position", item)
	}
	*position = LogPositionFromBytes(payload)
	return nil
}

// AgentID is an agent's identity: a bounded, human-readable name.
//
// A string rather than an opaque number, because an agent is a named principal
// every edge protocol spells out as text. On a shared topic it is an authorship
// claim, not an authenticated identity.
type AgentID struct{ value string }

// NewAgentID validates and wraps an agent name.
func NewAgentID(value string) (AgentID, error) {
	if value == "" {
		return AgentID{}, &InvalidError{Reason: "agent id must not be empty"}
	}
	if len(value) > MaxAgentStringBytes {
		return AgentID{}, &InvalidError{Reason: fmt.Sprintf(
			"agent id is %dB, exceeds cap %dB", len(value), MaxAgentStringBytes)}
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return AgentID{}, &InvalidError{Reason: fmt.Sprintf(
				"agent id must not contain control characters (found %q)", character)}
		}
	}
	return AgentID{value: value}, nil
}

// String is the agent name.
func (id AgentID) String() string { return id.value }

// IsZero reports an unset agent id.
func (id AgentID) IsZero() bool { return id.value == "" }

// MarshalCBOR writes the agent name as a text string.
func (id AgentID) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(id.value), nil
}

// UnmarshalCBOR reads and validates an agent name.
func (id *AgentID) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as an agent id", item)
	}
	parsed, err := NewAgentID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// IdempotencyKey is a producer-supplied business key, often a natural key like
// "job-123-attempt-2" so consoles and dead-letter capsules stay readable.
type IdempotencyKey struct{ value string }

// NewIdempotencyKey validates and wraps an idempotency key.
func NewIdempotencyKey(value string) (IdempotencyKey, error) {
	if value == "" {
		return IdempotencyKey{}, &InvalidError{Reason: "idempotency key must not be empty"}
	}
	if len(value) > MaxIdempotencyKeyBytes {
		return IdempotencyKey{}, &InvalidError{Reason: fmt.Sprintf(
			"idempotency key is %dB, exceeds cap %dB", len(value), MaxIdempotencyKeyBytes)}
	}
	return IdempotencyKey{value: value}, nil
}

// String is the key text.
func (key IdempotencyKey) String() string { return key.value }

// IsZero reports an unset key.
func (key IdempotencyKey) IsZero() bool { return key.value == "" }

// MarshalCBOR writes the key as a text string.
func (key IdempotencyKey) MarshalCBOR() (cborform.Value, error) {
	return cborform.Text(key.value), nil
}

// UnmarshalCBOR reads and validates a key.
func (key *IdempotencyKey) UnmarshalCBOR(item cborform.Value) error {
	text, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as an idempotency key", item)
	}
	parsed, err := NewIdempotencyKey(string(text))
	if err != nil {
		return err
	}
	*key = parsed
	return nil
}

// BackendResourceID is the stable identity of one operator-owned backend
// resource. Like the agent ids it shares ID's representation but stays a
// distinct type, so a resource id cannot be passed where a record id belongs.
type BackendResourceID struct{ ID }
