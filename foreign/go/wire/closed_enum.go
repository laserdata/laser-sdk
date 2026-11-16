package wire

import (
	"fmt"
	"strings"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// The contract draws its string vocabularies two ways, and the difference is
// load-bearing.
//
// One kind carries a passthrough variant, so a value a newer peer named
// decodes to the unrecognized sink and the record still reads. The other has
// none, and serde refuses a value outside the set: the vocabulary is closed,
// and a record naming something else is malformed rather than newer.
//
// A decoder that accepted anything would turn the second kind into the first,
// so a Go reader would act on a record every other client refuses. These
// decoders keep the closed ones closed.

// decodeClosedEnum reads a value from a closed vocabulary, refusing anything
// outside it.
func decodeClosedEnum[T ~string](
	item cborform.Value, target *T, label string, allowed ...T,
) error {
	name, ok := item.(cborform.Text)
	if !ok {
		return fmt.Errorf("wire: cannot decode %T as a %s", item, label)
	}
	for _, candidate := range allowed {
		if T(name) == candidate {
			*target = candidate
			return nil
		}
	}
	return fmt.Errorf("wire: unknown %s %q, expected one of %s",
		label, string(name), listNames(allowed))
}

// listNames renders a vocabulary for an error, so the message says what was
// expected rather than only what was refused.
func listNames[T ~string](allowed []T) string {
	names := make([]string, len(allowed))
	for index, value := range allowed {
		names[index] = string(value)
	}
	return strings.Join(names, ", ")
}
