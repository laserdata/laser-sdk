package wire

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// U128 is a 128-bit unsigned integer, held as its big-endian bytes because Go
// has no native 128-bit integer.
//
// It carries the managed operation identity. On the binary band it rides as the
// narrowest integer that fits: a plain CBOR unsigned integer up to the 64-bit
// range, and a positive bignum above it. On a human-readable surface it rides
// as decimal text, since JSON numbers lose precision past 2^53.
type U128 [16]byte

// U128FromUint64 widens a 64-bit value.
func U128FromUint64(value uint64) U128 {
	var out U128
	for i := 15; i >= 8; i-- {
		out[i] = byte(value)
		value >>= 8
	}
	return out
}

// IsZero reports the zero operation identity, which the contract refuses.
func (value U128) IsZero() bool {
	for _, b := range value {
		if b != 0 {
			return false
		}
	}
	return true
}

// Uint64 narrows the value, reporting false when it does not fit.
func (value U128) Uint64() (uint64, bool) {
	for _, b := range value[:8] {
		if b != 0 {
			return 0, false
		}
	}
	var out uint64
	for _, b := range value[8:] {
		out = out<<8 | uint64(b)
	}
	return out, true
}

// String renders the value in decimal.
func (value U128) String() string {
	if narrow, ok := value.Uint64(); ok {
		return strconv.FormatUint(narrow, 10)
	}
	return new(big.Int).SetBytes(value[:]).String()
}

// ParseU128 reads a decimal value.
func ParseU128(text string) (U128, error) {
	parsed, ok := new(big.Int).SetString(text, 10)
	if !ok || parsed.Sign() < 0 || parsed.BitLen() > 128 {
		return U128{}, &InvalidError{Reason: fmt.Sprintf("not a 128-bit unsigned integer: %q", text)}
	}
	var out U128
	parsed.FillBytes(out[:])
	return out, nil
}

// trimmed is the value's big-endian bytes with leading zeros removed, the
// minimal form a bignum carries.
func (value U128) trimmed() []byte {
	start := 0
	for start < len(value) && value[start] == 0 {
		start++
	}
	return value[start:]
}

// cborPositiveBignumTag is RFC 8949's tag for an unsigned bignum.
const cborPositiveBignumTag = 2

// MarshalCBOR writes the narrowest form: a plain unsigned integer where the
// value fits 64 bits, a positive bignum above that.
func (value U128) MarshalCBOR() (cborform.Value, error) {
	// A 128-bit integer rides as a number in the binary encoding and as
	// decimal text in a human-readable one, because JSON has no number that
	// carries 128 bits exactly.
	var binary cborform.Value = cborform.Tag{
		Number:  cborPositiveBignumTag,
		Content: cborform.Bytes(value.trimmed()),
	}
	if narrow, fits := value.Uint64(); fits {
		binary = cborform.Uint(narrow)
	}
	return cborform.Textual{Binary: binary, Text: value.String()}, nil
}

// UnmarshalCBOR reads an unsigned integer, a positive bignum, or decimal text.
func (value *U128) UnmarshalCBOR(item cborform.Value) error {
	switch typed := item.(type) {
	case cborform.Uint:
		*value = U128FromUint64(uint64(typed))
		return nil
	case cborform.Tag:
		if typed.Number != cborPositiveBignumTag {
			return fmt.Errorf("wire: tag %d is not an unsigned bignum", typed.Number)
		}
		payload, ok := typed.Content.(cborform.Bytes)
		if !ok {
			return fmt.Errorf("wire: a bignum must carry bytes, got %T", typed.Content)
		}
		if len(payload) > 16 {
			return fmt.Errorf("wire: a bignum of %d bytes overflows 128 bits", len(payload))
		}
		var out U128
		copy(out[16-len(payload):], payload)
		*value = out
		return nil
	case cborform.Text:
		parsed, err := ParseU128(string(typed))
		if err != nil {
			return err
		}
		*value = parsed
		return nil
	default:
		return fmt.Errorf("wire: cannot decode %T as a 128-bit integer", item)
	}
}
