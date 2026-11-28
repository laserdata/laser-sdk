package wire_test

import (
	"bytes"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenATextValueWhenAUnionListsBytesBeforeStringThenShouldEncodeTheStringBranch(t *testing.T) {
	schema, err := wire.CompileAvroWriterSchema(`["bytes","string"]`)
	if err != nil {
		t.Fatalf("compile the schema: %v", err)
	}

	encoded, err := schema.EncodeJSON([]byte(`"abcd"`))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Branch 1, then the four characters as text, not as decoded base64.
	if want := []byte{2, 8, 'a', 'b', 'c', 'd'}; !bytes.Equal(encoded, want) {
		t.Fatalf("want %v, got %v", want, encoded)
	}
}

func TestGivenAByteArrayWhenAUnionListsBytesBeforeStringThenShouldEncodeTheBytesBranch(t *testing.T) {
	schema, err := wire.CompileAvroWriterSchema(`["bytes","string"]`)
	if err != nil {
		t.Fatalf("compile the schema: %v", err)
	}

	encoded, err := schema.EncodeJSON([]byte(`[1,2]`))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if want := []byte{0, 4, 1, 2}; !bytes.Equal(encoded, want) {
		t.Fatalf("want %v, got %v", want, encoded)
	}
}
