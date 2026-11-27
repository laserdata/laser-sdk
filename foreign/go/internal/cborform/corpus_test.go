package cborform_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/laserdata/laser-sdk/foreign/go/internal/cborform"
)

// fixturesDir is the Rust-owned golden corpus. wire/src/fixtures.rs embeds it
// so a port's conformance suite can assert the exact canonical bytes.
func fixturesDir(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "wire", "fixtures"))
	if err != nil {
		t.Fatalf("resolve fixtures dir: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("fixtures dir %s: %v", dir, err)
	}
	return dir
}

func corpus(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(fixturesDir(t))
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names
}

// The band is named-field CBOR. Decoding a fixture and re-emitting it with the
// shortest legal head for every length and integer must reproduce the Rust
// bytes exactly, or a Go producer would write records the other clients read
// differently.
func TestGivenTheGoldenCorpusWhenReEncodedThenShouldMatchRustBytes(t *testing.T) {
	dir := fixturesDir(t)
	var binary, jsonCount int

	for _, name := range corpus(t) {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("%s: read: %v", name, err)
		}

		if strings.HasSuffix(name, ".json") {
			var any interface{}
			if err := json.Unmarshal(raw, &any); err != nil {
				t.Errorf("%s: not valid JSON: %v", name, err)
				continue
			}
			jsonCount++
			continue
		}

		value, err := cborform.Decode(raw)
		if err != nil {
			t.Errorf("%s: decode: %v", name, err)
			continue
		}
		again, err := cborform.Encode(value)
		if err != nil {
			t.Errorf("%s: encode: %v", name, err)
			continue
		}
		if string(again) != string(raw) {
			t.Errorf("%s: re-encode differs from the Rust bytes (%d in, %d out)%s",
				name, len(raw), len(again), firstDiff(raw, again))
			continue
		}
		binary++
	}

	t.Logf("round-tripped %d CBOR fixtures byte-identically, parsed %d JSON fixtures", binary, jsonCount)
	if binary+jsonCount != len(corpus(t)) {
		t.Errorf("covered %d of %d fixtures", binary+jsonCount, len(corpus(t)))
	}
}

// firstDiff locates the first differing byte, so a failure names the offset
// instead of dumping two blobs.
func firstDiff(want, got []byte) string {
	limit := len(want)
	if len(got) < limit {
		limit = len(got)
	}
	for i := 0; i < limit; i++ {
		if want[i] != got[i] {
			return "\n  first difference at byte " + itoa(i) +
				": want 0x" + hex(want[i]) + ", got 0x" + hex(got[i])
		}
	}
	return "\n  one is a prefix of the other"
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var digits []byte
	for v > 0 {
		digits = append([]byte{byte('0' + v%10)}, digits...)
		v /= 10
	}
	return string(digits)
}

func hex(b byte) string {
	const digits = "0123456789abcdef"
	return string([]byte{digits[b>>4], digits[b&0x0f]})
}

// Every fixture must be classified, so a new one added on the Rust side fails
// here instead of silently going untested.
func TestGivenTheCorpusWhenListedThenShouldBeNonEmptyAndClassified(t *testing.T) {
	names := corpus(t)
	if len(names) == 0 {
		t.Fatal("expected fixtures on disk")
	}
	for _, name := range names {
		if !strings.HasSuffix(name, ".bin") && !strings.HasSuffix(name, ".json") {
			t.Errorf("unclassified fixture extension: %s", name)
		}
	}
	t.Logf("corpus holds %d fixtures", len(names))
}
