package wire

import "github.com/laserdata/laser-sdk/foreign/go/internal/cborform"

// The key-record contract: an agent or operator's Ed25519 verifying key as it
// rides the registry topic.
const (
	KeyRecordVersion  uint32 = 1
	KeyIDBytes               = 8
	VerifyingKeyBytes        = 32
)

// KeyKind says whose key a record carries.
type KeyKind string

const (
	KeyAgent    KeyKind = "agent"
	KeyOperator KeyKind = "operator"
)

// KeyRecord is one principal's verifying key with its validity window.
type KeyRecord struct {
	V         uint32 `cbor:"v"`
	Principal string `cbor:"principal"`
	KeyID     []byte `cbor:"key_id"`
	// VerifyingKey is the Ed25519 public key. Verification itself is
	// SDK-side, so the wire layer stays crypto-free.
	VerifyingKey    []byte  `cbor:"verifying_key"`
	Kind            KeyKind `cbor:"kind"`
	ValidFromMicros uint64  `cbor:"valid_from_micros"`
	ValidToMicros   *uint64 `cbor:"valid_to_micros"`
	Revoked         bool    `cbor:"revoked"`
}

// NewKeyRecord builds a key record at the current version.
func NewKeyRecord(principal string, keyID, verifyingKey []byte, kind KeyKind) KeyRecord {
	return KeyRecord{
		V:            KeyRecordVersion,
		Principal:    principal,
		KeyID:        keyID,
		VerifyingKey: verifyingKey,
		Kind:         kind,
	}
}

// Validate checks the record's version, principal and key widths.
func (record KeyRecord) Validate() error {
	if record.V != KeyRecordVersion {
		return &InvalidError{Reason: "unsupported key record version"}
	}
	if record.Principal == "" {
		return &InvalidError{Reason: "key principal must not be empty"}
	}
	if len(record.KeyID) != KeyIDBytes {
		return &InvalidError{Reason: "Ed25519 key id must be 8 bytes"}
	}
	if len(record.VerifyingKey) != VerifyingKeyBytes {
		return &InvalidError{Reason: "Ed25519 public key must be 32 bytes"}
	}
	return nil
}

// ValidAt reports whether the key is usable at an instant: inside its validity
// window and not revoked.
func (record KeyRecord) ValidAt(micros uint64) bool {
	if record.Revoked || micros < record.ValidFromMicros {
		return false
	}
	return record.ValidToMicros == nil || micros < *record.ValidToMicros
}

// UnmarshalCBOR reads the value, refusing one outside the closed vocabulary.
func (value *KeyKind) UnmarshalCBOR(item cborform.Value) error {
	return decodeClosedEnum(item, value, "key kind", KeyAgent, KeyOperator)
}
