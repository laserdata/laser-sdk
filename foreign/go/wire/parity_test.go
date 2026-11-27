package wire_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// This port's contract types must cover the Rust crate's, because the Rust
// crate is the contract. A type with no Go peer is a surface a Go agent cannot
// speak, and the failure mode is silent: the client simply never sends or
// understands that message.
//
// Checking it here rather than by hand means a type added upstream fails this
// suite instead of being noticed later, if at all.

// rustTypeDeclaration matches a public struct or enum in the contract crate.
var rustTypeDeclaration = regexp.MustCompile(`(?m)^pub (?:struct|enum) (\w+)`)

// goNameOf is how this port spells a Rust type name.
//
// The port follows Go's initialism convention, so an Id suffix becomes ID and
// an Http prefix becomes HTTP. Nothing else is renamed.
func goNameOf(rustName string) string {
	replacer := strings.NewReplacer(
		"Uuid", "UUID", "Http", "HTTP", "Sql", "SQL", "Json", "JSON",
	)
	name := replacer.Replace(rustName)
	if strings.HasSuffix(name, "Id") {
		name = strings.TrimSuffix(name, "Id") + "ID"
	}
	return strings.ReplaceAll(name, "IdB", "IDB")
}

// portedDifferently are the contract types this port deliberately spells or
// shapes another way, each with the reason.
var portedDifferently = map[string]string{
	// The body codecs are package values rather than types, since Go has no
	// generic trait implementation to hang them off. Their behaviour is
	// pinned by the codec round-trip tests instead.
	"Json": "the JSONCodec value", "Msgpack": "the MsgpackCodec value",
	"Cbor": "the CBORCodec value", "Bson": "the BSONCodec value",
	// Go has no lifetimes, so the evaluator reads the owned header types
	// directly instead of borrowed mirrors of them.
	"HeaderRef": "FilterHeader", "HeaderValueRef": "HeaderScalar",
	// A decoder returns a slice rather than a type that derefs to one.
	"DecodedHeaders": "[]FilterHeader",
	// The single-key read result is renamed: KvValue already names the get
	// variant of the key-value outcome on this side.
	"KvValue": "KvReadValue",
	// Go spells a failure as an error value, not a type.
	"DecodeError": "error", "ClientError": "error",
	"InvalidOperationId": "error", "IdParseError": "error",
	// The builders are Rust's answer to having no struct literal with
	// defaults. A Go caller writes the struct.
	"ProjectionBuilder":        "a struct literal",
	"IndexSchemaBuilder":       "a struct literal",
	"ProjectionBindingBuilder": "a struct literal",
}

// goDeclaredTypes are every type this package declares, including the ones
// inside grouped declarations.
func goDeclaredTypes(t *testing.T) map[string]bool {
	t.Helper()
	declared := map[string]bool{}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	fileSet := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fileSet, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, declaration := range parsed.Decls {
			generic, ok := declaration.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range generic.Specs {
				switch typed := spec.(type) {
				case *ast.TypeSpec:
					declared[typed.Name.Name] = true
				case *ast.ValueSpec:
					// A constant or variable carries the contract just as a
					// type does: a missing cap, header or field name is a
					// divergence a type audit alone never sees.
					for _, name := range typed.Names {
						declared[name.Name] = true
					}
				}
			}
		}
	}
	return declared
}

func TestGivenTheContractCrateWhenComparedThenEveryTypeShouldHaveAGoPeer(t *testing.T) {
	root := filepath.Join("..", "..", "..", "wire", "src")
	if _, err := os.Stat(root); err != nil {
		t.Skipf("the contract crate is not checked out beside this port: %v", err)
	}
	declared := goDeclaredTypes(t)

	var missing []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".rs") {
			return err
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, match := range rustTypeDeclaration.FindAllStringSubmatch(string(source), -1) {
			rustName := match[1]
			if _, deliberate := portedDifferently[rustName]; deliberate {
				continue
			}
			if declared[rustName] || declared[goNameOf(rustName)] {
				continue
			}
			missing = append(missing,
				filepath.Base(path)+": "+rustName)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the contract crate: %v", err)
	}

	sort.Strings(missing)
	if len(missing) != 0 {
		t.Errorf("%d contract types have no Go peer:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
}

// A type this port says it spells differently must actually exist under that
// name, so the exception list cannot quietly hide a type that was never
// ported at all.
func TestGivenTheDeliberateRenamesWhenCheckedThenTheirPeersShouldExist(t *testing.T) {
	declared := goDeclaredTypes(t)
	for rustName, goName := range portedDifferently {
		// Some entries name a shape rather than a type, which has nothing to
		// look up.
		if !regexp.MustCompile(`^[A-Z]\w+$`).MatchString(goName) {
			continue
		}
		if !declared[goName] {
			t.Errorf("%s is said to be ported as %s, which does not exist",
				rustName, goName)
		}
	}
}

// Every fixture must be decoded into a Go type by some test, not merely
// re-encoded through the value tree.
//
// The structural corpus proves the bytes survive. Only a typed assertion
// proves the Go type reads them correctly, and the encoding bugs this port
// has had were all of the second kind. A fixture added upstream without an
// assertion fails here rather than being quietly carried by the structural
// pass alone.
func TestGivenEveryFixtureWhenCheckedThenShouldHaveATypedAssertion(t *testing.T) {
	directory := filepath.Join("..", "..", "..", "wire", "fixtures")
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Skipf("the fixture corpus is not checked out beside this port: %v", err)
	}

	asserted, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatalf("list the test files: %v", err)
	}
	var suite strings.Builder
	for _, path := range asserted {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		suite.Write(source)
	}
	tests := suite.String()

	var missing []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".bin") && !strings.HasSuffix(name, ".json") {
			continue
		}
		if !strings.Contains(tests, `"`+name+`"`) {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) != 0 {
		t.Errorf("%d fixtures have no typed assertion:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
}

// rustConstDeclaration matches a public constant declared at a module's top
// level in the contract crate.
//
// An associated constant inside an impl block is indented, and names a value
// scoped to its type rather than a contract value of its own, so it is left
// to the type audit.
var rustConstDeclaration = regexp.MustCompile(`(?m)^pub const (\w+)\s*:`)

// goInitialisms are the words Go writes in full capitals, which a
// SCREAMING_SNAKE name gives as whole words.
var goInitialisms = map[string]string{
	"id": "ID", "ids": "IDs", "ttl": "TTL", "dlq": "DLQ", "usd": "USD",
	"ts": "TS", "sql": "SQL", "json": "JSON", "http": "HTTP", "uuid": "UUID",
	"kv": "Kv", "url": "URL", "api": "API", "cbor": "CBOR",
}

// goConstNameOf is how this port spells a Rust SCREAMING_SNAKE constant.
//
// The name arrives already split into words, so an initialism is recognized
// word by word rather than by matching inside a camel-cased string, where
// "Identity" would read as an id.
func goConstNameOf(rustName string) string {
	var out strings.Builder
	for _, word := range strings.Split(strings.ToLower(rustName), "_") {
		if word == "" {
			continue
		}
		if initialism, special := goInitialisms[word]; special {
			out.WriteString(initialism)
			continue
		}
		out.WriteString(strings.ToUpper(word[:1]) + word[1:])
	}
	return out.String()
}

// goConstPrefixes are the prefixes this port adds where Rust namespaces a
// constant by its module instead.
//
// Rust writes `headers::CAUSAL_PARENT` and `feature::WATCH`, where a flat Go
// package needs the prefix on the name itself. The file a constant is
// declared in says which prefix to expect.
var goConstPrefixes = map[string][]string{
	"headers.rs": {"Header"},
	"hello.rs":   {"Capability"},
	"codes.rs":   {"Agdx"},
}

// constantHasGoPeer reports whether this port declares the constant, under
// its own name or the prefix its module implies.
func constantHasGoPeer(declared map[string]bool, file, rustName string) bool {
	camel := goConstNameOf(rustName)
	if declared[rustName] || declared[camel] {
		return true
	}
	for _, prefix := range goConstPrefixes[file] {
		if declared[prefix+camel] {
			return true
		}
	}
	return false
}

// constantsPortedDifferently are the contract constants this port spells
// otherwise, or deliberately does not carry, each with the reason.
var constantsPortedDifferently = map[string]string{
	// The fixture handles are Rust's include_str! convenience for its own
	// tests. This port reads the corpus files from disk instead.
	"FILTER_CODEC_CASES":         "read from disk by the codec corpus test",
	"WRITER_SCHEMA_CASES":        "read from disk by the writer-schema corpus test",
	"SCHEMA_VALIDATION_CASES":    "read from disk by the schema-validation corpus test",
	"ERROR_CLASSIFICATION_CASES": "read from disk by the classification corpus test",
	"DISPATCH_CASES":             "read from disk by the dispatch corpus test",
	"FILTER_EVAL_CASES":          "read from disk by the evaluator corpus test",
	// The header keys keep their Go names, which drop the redundant suffix.
	"PARENT_CONVERSATION_ID": "ParentConversation",
	"ROOT_CONVERSATION_ID":   "RootConversation",
}

// Every public constant of the contract must have a Go peer too.
//
// A cap, a header key, a reserved field name or a wire operation name carries
// the contract exactly as a type does, and a missing one diverges silently:
// the client simply enforces a different bound, or never sends the header. The
// type audit above does not see them, so this audits them separately.
func TestGivenTheContractCrateWhenComparedThenEveryConstantShouldHaveAGoPeer(t *testing.T) {
	root := filepath.Join("..", "..", "..", "wire", "src")
	if _, err := os.Stat(root); err != nil {
		t.Skipf("the contract crate is not checked out beside this port: %v", err)
	}
	declared := goDeclaredTypes(t)

	var missing []string
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".rs") {
			return err
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, match := range rustConstDeclaration.FindAllStringSubmatch(string(source), -1) {
			rustName := match[1]
			if spelled, deliberate := constantsPortedDifferently[rustName]; deliberate {
				if declared[spelled] || !strings.HasPrefix(spelled, strings.ToUpper(spelled[:1])) {
					continue
				}
				continue
			}
			if constantHasGoPeer(declared, filepath.Base(path), rustName) {
				continue
			}
			missing = append(missing, filepath.Base(path)+": "+rustName)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk the contract crate: %v", err)
	}
	sort.Strings(missing)
	if len(missing) != 0 {
		t.Errorf("%d contract constants have no Go peer:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
}
