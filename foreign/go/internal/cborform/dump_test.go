package cborform_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// TestDumpFixture prints the structure of named fixtures, for reading the exact
// shape the Rust encoder produces while porting a module. It is a tool rather
// than an assertion and stays skipped unless LASER_DUMP names fixtures:
//
//	LASER_DUMP=managed_read.bin,hello_reply.bin go test ./internal/cborform -run LaserDump -v
func TestGivenLaserDumpWhenSetThenShouldPrintTheFixtureStructure(t *testing.T) {
	names := os.Getenv("LASER_DUMP")
	if names == "" {
		t.Skip("set LASER_DUMP to a comma-separated list of fixture names")
	}
	dir := fixturesDir(t)
	for _, name := range strings.Split(names, ",") {
		name = strings.TrimSpace(name)
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Errorf("read %s: %v", name, err)
			continue
		}
		if strings.HasSuffix(name, ".json") {
			t.Logf("== %s (%d bytes, JSON)\n%s", name, len(raw), raw)
			continue
		}
		value, err := cborform.Decode(raw)
		if err != nil {
			t.Errorf("%s: decode: %v", name, err)
			continue
		}
		t.Logf("== %s (%d bytes)\n%s", name, len(raw), render(value, 1))
	}
}

func render(value cborform.Value, depth int) string {
	pad := strings.Repeat("  ", depth)
	var out strings.Builder
	switch typed := value.(type) {
	case cborform.Map:
		for _, entry := range typed {
			switch entry.Value.(type) {
			case cborform.Map, cborform.Array:
				fmt.Fprintf(&out, "%s%s:\n%s", pad, describe(entry.Key), render(entry.Value, depth+1))
			default:
				fmt.Fprintf(&out, "%s%s: %s\n", pad, describe(entry.Key), describe(entry.Value))
			}
		}
	case cborform.Array:
		for i, item := range typed {
			switch item.(type) {
			case cborform.Map, cborform.Array:
				fmt.Fprintf(&out, "%s[%d]:\n%s", pad, i, render(item, depth+1))
			default:
				fmt.Fprintf(&out, "%s[%d] %s\n", pad, i, describe(item))
			}
		}
	default:
		fmt.Fprintf(&out, "%s%s\n", pad, describe(value))
	}
	return out.String()
}

func describe(value cborform.Value) string {
	switch typed := value.(type) {
	case cborform.Text:
		return fmt.Sprintf("%q", string(typed))
	case cborform.Bytes:
		if len(typed) > 24 {
			return fmt.Sprintf("bytes(%d) %x...", len(typed), typed[:24])
		}
		return fmt.Sprintf("bytes(%d) %x", len(typed), typed)
	case cborform.Uint:
		return fmt.Sprintf("uint %d", uint64(typed))
	case cborform.NegInt:
		return fmt.Sprintf("nint -%d", uint64(typed)+1)
	case cborform.Bool:
		return fmt.Sprintf("bool %v", bool(typed))
	case cborform.Float:
		return fmt.Sprintf("float%d %v", typed.Bits, typed.Value)
	case cborform.Null:
		return "null"
	case cborform.Undefined:
		return "undefined"
	case cborform.Tag:
		return fmt.Sprintf("tag(%d) %s", typed.Number, describe(typed.Content))
	case cborform.Simple:
		return fmt.Sprintf("simple(%d)", uint8(typed))
	default:
		return fmt.Sprintf("%T", value)
	}
}
