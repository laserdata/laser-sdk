package wire

import (
	"fmt"
	"math"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// ByteList is a byte sequence that rides the wire as a CBOR array of integers
// rather than a byte string.
//
// Most byte fields in the contract opt into the byte-string encoding, which is
// smaller and unambiguous. A field that does not opt in takes serde's default
// for a bare byte vector, which is an array of integers. The two are different
// bytes on the wire, so a field must use the type matching its own Rust
// declaration: using the wrong one produces a record the other clients decode
// differently.
type ByteList []byte

// MarshalCBOR writes the array-of-integers form.
func (list ByteList) MarshalCBOR() (cborform.Value, error) {
	items := make(cborform.Array, 0, len(list))
	for _, b := range list {
		items = append(items, cborform.Uint(uint64(b)))
	}
	return items, nil
}

// UnmarshalCBOR reads the array-of-integers form, and accepts a byte string
// too so a value crossing from the binary band still decodes.
func (list *ByteList) UnmarshalCBOR(item cborform.Value) error {
	switch typed := item.(type) {
	case cborform.Array:
		out := make(ByteList, 0, len(typed))
		for i, element := range typed {
			number, ok := element.(cborform.Uint)
			if !ok || number > math.MaxUint8 {
				return fmt.Errorf("wire: byte list element %d is not a byte", i)
			}
			out = append(out, byte(number))
		}
		*list = out
		return nil
	case cborform.Bytes:
		out := make(ByteList, len(typed))
		copy(out, typed)
		*list = out
		return nil
	default:
		return fmt.Errorf("wire: cannot decode %T as a byte list", item)
	}
}
