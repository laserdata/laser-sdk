package wire_test

import (
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenAGoValueWhenTurnedIntoADocumentThenShouldHoldWhatTheCodecEncodes(t *testing.T) {
	document, err := wire.DynValueOf(map[string]any{"service": "api", "replicas": 3, "tags": []string{"eu"}})
	if err != nil {
		t.Fatalf("build the document: %v", err)
	}

	service, found := document.Field("service")
	if text, ok := service.Text(); !found || !ok || text != "api" {
		t.Errorf("want the service field read back, got %q found=%v", text, found)
	}
	encoded, err := wire.Marshal(map[string]any{"service": "api", "replicas": 3, "tags": []string{"eu"}})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var decoded wire.DynValue
	if err := wire.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !document.Equal(decoded) {
		t.Error("want the document equal to the value decoded from its encoding")
	}
}

func TestGivenAValueTheCodecCannotEncodeWhenTurnedIntoADocumentThenShouldRefuseIt(t *testing.T) {
	if _, err := wire.DynValueOf(make(chan int)); err == nil {
		t.Error("want a value with no encoding refused")
	}
}
