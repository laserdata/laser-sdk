package wire_test

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// The fixture corpus proves this port's bytes are the contract's bytes, for
// the shapes it carries. A round trip proves this port agrees with itself.
// Neither proves that a shape the corpus never carried encodes the way the
// contract would encode it.
//
// Exporting the round-trip samples lets the contract crate answer that. A
// Rust test decodes each one into its own typed peer and re-encodes it: a
// value that decodes and returns the same bytes is a value both
// implementations agree on, which is exactly what a fixture proves, for a
// shape no fixture exists for.
//
// Run with LASER_GO_VECTORS set to a directory, then run the Rust side:
//
//	LASER_GO_VECTORS=/tmp/go-vectors go test ./wire -run Export
//	cargo test -p laser-wire --test go_differential
func TestGivenTheRoundTripSamplesWhenExportedThenShouldWriteOneVectorPerShape(t *testing.T) {
	directory := os.Getenv("LASER_GO_VECTORS")
	if directory == "" {
		t.Skip("set LASER_GO_VECTORS to export the differential vectors")
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatalf("create the vector directory: %v", err)
	}

	written := 0
	for _, sample := range roundTripSamples {
		typeOf := reflect.TypeOf(sample)
		populated := reflect.New(typeOf).Elem()
		populateForRoundTrip(populated, 0)

		encoded, err := wire.Marshal(populated.Interface())
		if err != nil {
			t.Errorf("%s: encode: %v", typeOf.Name(), err)
			continue
		}
		path := filepath.Join(directory, typeOf.Name()+".bin")
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		written++
	}
	t.Logf("wrote %d vectors to %s", written, directory)
}
