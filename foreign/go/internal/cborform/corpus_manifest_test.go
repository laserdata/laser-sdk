package cborform_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"testing"
)

// rustCorpus reads the fixture names out of the corpus!() macro in
// wire/src/fixtures.rs. Parsing the Rust source rather than keeping a Go-side
// list means a fixture added or removed upstream fails here immediately,
// with no manifest to maintain by hand.
func rustCorpus(t *testing.T) []string {
	t.Helper()
	path, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "wire", "src", "fixtures.rs"))
	if err != nil {
		t.Fatalf("resolve fixtures.rs: %v", err)
	}
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixtures.rs: %v", err)
	}
	block := regexp.MustCompile(`(?s)corpus!\((.*?)\n\);`).FindSubmatch(source)
	if block == nil {
		t.Fatal("could not find the corpus!() block in wire/src/fixtures.rs")
	}
	var names []string
	for _, match := range regexp.MustCompile(`"([^"]+)"`).FindAllSubmatch(block[1], -1) {
		names = append(names, string(match[1]))
	}
	if len(names) == 0 {
		t.Fatal("corpus!() listed no fixtures")
	}
	sort.Strings(names)
	return names
}

// The Go port must assert against exactly the corpus the Rust crate publishes:
// no fixture silently untested, none named that no longer exists.
func TestGivenTheRustCorpusWhenComparedToDiskThenShouldMatchExactly(t *testing.T) {
	declared := rustCorpus(t)
	onDisk := corpus(t)

	declaredSet := make(map[string]bool, len(declared))
	for _, name := range declared {
		declaredSet[name] = true
	}
	diskSet := make(map[string]bool, len(onDisk))
	for _, name := range onDisk {
		diskSet[name] = true
	}

	var missing, extra []string
	for _, name := range declared {
		if !diskSet[name] {
			missing = append(missing, name)
		}
	}
	for _, name := range onDisk {
		if !declaredSet[name] {
			extra = append(extra, name)
		}
	}

	if len(missing) != 0 {
		t.Errorf("corpus!() names fixtures absent from wire/fixtures: %v", missing)
	}
	if len(extra) != 0 {
		t.Errorf("wire/fixtures holds files the Go suite does not cover: %v", extra)
	}
	t.Logf("Rust corpus and Go coverage agree on %d fixtures", len(declared))
}
