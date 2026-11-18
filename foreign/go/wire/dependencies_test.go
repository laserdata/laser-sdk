package wire_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The packages this contract port is allowed to reach, beyond the standard
// library and its own module.
//
// google.golang.org/protobuf resolves descriptors for the Protobuf filter
// codec, which is the one job no standard library package does.
var allowedDependencies = []string{"google.golang.org/protobuf/"}

// portablePackages are the directories that must stay runtime-free, the Go
// peer of the Rust crate's deny-wire policy.
var portablePackages = []string{"wire", filepath.Join("internal", "cborform")}

// The contract is the portable half of this client: it has no I/O, no clock,
// no randomness, and no transport. The client package dials through the Apache
// Iggy Go SDK, and an import of it from here would pull a transport into the
// half that is meant to run anywhere, wasm included.
//
// Rust states the same rule with `just deny-wire`, which bans iggy, tokio,
// bytes and the rest from laser-wire's portable surface. This is its Go peer,
// read statically so it needs no toolchain beyond the test binary.
func TestGivenTheContractPackagesWhenScannedThenShouldImportNoTransport(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("locate the module root: %v", err)
	}
	offenders := map[string][]string{}
	for _, pkg := range portablePackages {
		for _, file := range goFilesIn(t, filepath.Join(root, pkg)) {
			for _, path := range importsOf(t, file) {
				if permittedDependency(path) {
					continue
				}
				relative, err := filepath.Rel(root, file)
				if err != nil {
					relative = file
				}
				offenders[path] = append(offenders[path], relative)
			}
		}
	}
	if len(offenders) == 0 {
		return
	}
	for _, path := range sortedDependencyKeys(offenders) {
		t.Errorf("the portable contract must not import %s, reached from %s",
			path, strings.Join(offenders[path], ", "))
	}
}

// permittedDependency reports an import the contract may carry: the standard
// library, this module, or one of the allowed third-party trees.
func permittedDependency(path string) bool {
	// A standard library path has no dot in its first segment, which is what
	// separates it from a module path.
	first, _, _ := strings.Cut(path, "/")
	if !strings.Contains(first, ".") {
		return true
	}
	if strings.HasPrefix(path, "github.com/laserdata/laser-sdk/foreign/go/") {
		return true
	}
	for _, allowed := range allowedDependencies {
		if strings.HasPrefix(path, allowed) {
			return true
		}
	}
	return false
}

// goFilesIn lists the Go sources of one package directory, tests included: a
// test that reached for a transport would still link it into this package.
func goFilesIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var files []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		files = append(files, filepath.Join(dir, entry.Name()))
	}
	if len(files) == 0 {
		t.Fatalf("no Go sources under %s, so this check would pass vacuously", dir)
	}
	return files
}

func importsOf(t *testing.T, file string) []string {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	paths := make([]string, 0, len(parsed.Imports))
	for _, item := range parsed.Imports {
		path, err := strconv.Unquote(item.Path.Value)
		if err != nil {
			t.Fatalf("read an import of %s: %v", file, err)
		}
		paths = append(paths, path)
	}
	return paths
}

func sortedDependencyKeys(values map[string][]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
