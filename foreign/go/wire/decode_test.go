package wire_test

import (
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

func TestGivenADuplicateFieldWhenDecodedThenShouldBeRefused(t *testing.T) {
	duplicated := []byte{0xa2, 0x64, 'c', 'o', 'd', 'e', 0x01, 0x64, 'c', 'o', 'd', 'e', 0x02}

	var item wire.BatchItem
	err := wire.Unmarshal(duplicated, &item)
	if err == nil || !strings.Contains(err.Error(), `the duplicate field "code"`) {
		t.Fatalf("a field named twice must be refused, got %v and code %d", err, item.Code)
	}
}

func TestGivenADuplicateUnknownFieldWhenDecodedThenShouldBeIgnored(t *testing.T) {
	repeated := []byte{0xa3, 0x61, 'x', 0x01, 0x61, 'x', 0x02, 0x64, 'c', 'o', 'd', 'e', 0x07}

	var item wire.BatchItem
	if err := wire.Unmarshal(repeated, &item); err != nil {
		t.Fatalf("an unknown field is skipped however often it repeats: %v", err)
	}
	if item.Code != 7 {
		t.Fatalf("code = %d, want 7", item.Code)
	}
}
