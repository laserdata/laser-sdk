package wire_test

import (
	"fmt"
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

// The audits here cover what the type and constant audits cannot see.
//
// A type audit proves a name exists. It does not prove the type carries the
// same fields, that an enum names every variant, or that a record this port
// accepts is one the contract accepts. Each of those diverges silently: the
// client drops a field it never declared, or sends a request every server
// refuses. A gap found by hand once is a gap that returns, so each is checked
// here instead.

func contractRoot(t *testing.T) string {
	t.Helper()
	root := filepath.Join("..", "..", "..", "wire", "src")
	if _, err := os.Stat(root); err != nil {
		t.Skipf("the contract crate is not checked out beside this port: %v", err)
	}
	return root
}

// eachContractFile runs visit over every source file of the contract crate.
func eachContractFile(t *testing.T, visit func(name, source string)) {
	t.Helper()
	err := filepath.WalkDir(contractRoot(t), func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".rs") {
			return err
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		visit(filepath.Base(path), string(source))
		return nil
	})
	if err != nil {
		t.Fatalf("walk the contract crate: %v", err)
	}
}

var (
	rustStructBody = regexp.MustCompile(`(?ms)^pub struct (\w+) \{(.*?)^\}`)
	rustFieldName  = regexp.MustCompile(`(?m)^\s+pub (\w+)\s*:`)
)

// goStructFields maps each Go struct to the wire names of its fields.
//
// A field's wire name is its cbor tag, since that is what crosses the wire. A
// struct whose fields carry no tags encodes as something other than a map,
// such as the positional array a Rust tuple becomes, and is compared by its
// own codec rather than field by field.
func goStructFields(t *testing.T) map[string][]string {
	t.Helper()
	fields := map[string][]string{}
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
		ast.Inspect(parsed, func(node ast.Node) bool {
			spec, ok := node.(*ast.TypeSpec)
			if !ok {
				return true
			}
			structType, ok := spec.Type.(*ast.StructType)
			if !ok {
				return true
			}
			var tagged []string
			for _, field := range structType.Fields.List {
				if field.Tag == nil {
					continue
				}
				if tag := cborTagName(field.Tag.Value); tag != "" {
					tagged = append(tagged, tag)
				}
			}
			fields[spec.Name.Name] = tagged
			return true
		})
	}
	return fields
}

var cborTag = regexp.MustCompile(`cbor:"([^",]+)`)

func cborTagName(literal string) string {
	if match := cborTag.FindStringSubmatch(literal); match != nil {
		return match[1]
	}
	return ""
}

// Every field of a contract struct must reach this port.
//
// A field with no Go peer is dropped on encode and ignored on decode, so the
// record still round-trips and the loss is invisible until a server acts on
// the value that never arrived.
func TestGivenTheContractCrateWhenComparedThenEveryStructFieldShouldHaveAGoPeer(t *testing.T) {
	declared := goStructFields(t)

	var missing []string
	eachContractFile(t, func(file, source string) {
		for _, match := range rustStructBody.FindAllStringSubmatch(source, -1) {
			name, body := match[1], match[2]
			fields, ported := declared[name]
			if !ported {
				fields, ported = declared[goNameOf(name)]
			}
			// A struct this port spells otherwise, or encodes positionally,
			// is covered by the type audit and its own codec test.
			if !ported || len(fields) == 0 {
				continue
			}
			carried := map[string]bool{}
			for _, field := range fields {
				carried[field] = true
			}
			for _, field := range rustFieldName.FindAllStringSubmatch(body, -1) {
				if !carried[field[1]] {
					missing = append(missing, file+": "+name+"."+field[1])
				}
			}
		}
	})
	sort.Strings(missing)
	if len(missing) != 0 {
		t.Errorf("%d contract struct fields have no Go peer:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
}

var rustEnumBody = regexp.MustCompile(`(?ms)^pub enum (\w+) \{(.*?)^\}`)

// rustVariantName matches a variant at an enum's own indent level.
var rustVariantName = regexp.MustCompile(`(?m)^\s{4}([A-Z]\w*)`)

var (
	rustDocComment = regexp.MustCompile(`(?m)///[^\n]*`)
	rustAttribute  = regexp.MustCompile(`#\[[^\]]*\]`)
	camelBoundary  = regexp.MustCompile(`(.)([A-Z])`)
)

// Every variant of a contract enum must reach this port.
//
// A variant with no Go peer decodes as an unknown one, and a reply enum
// decodes fail-closed, so a client meets an error where the contract named an
// outcome. A variant reaches Go as a constant, a wire name, or the name
// itself, and any of the three counts.
func TestGivenTheContractCrateWhenComparedThenEveryEnumVariantShouldHaveAGoPeer(t *testing.T) {
	source := goSourceText(t)

	var missing []string
	eachContractFile(t, func(file, text string) {
		for _, match := range rustEnumBody.FindAllStringSubmatch(text, -1) {
			name, body := match[1], match[2]
			body = rustAttribute.ReplaceAllString(rustDocComment.ReplaceAllString(body, ""), "")
			for _, variant := range rustVariantName.FindAllStringSubmatch(body, -1) {
				if variantHasGoPeer(source, variant[1]) {
					continue
				}
				missing = append(missing, file+": "+name+"::"+variant[1])
			}
		}
	})
	sort.Strings(missing)
	if len(missing) != 0 {
		t.Errorf("%d contract enum variants have no Go peer:\n  %s",
			len(missing), strings.Join(missing, "\n  "))
	}
}

// variantsPortedDifferently are the contract variants this port carries as
// behaviour rather than as a named value.
var variantsPortedDifferently = map[string]string{
	// Go returns a plain error from Unmarshal where Rust names a framing
	// error variant. requirePayload enforces the same rule.
	"MissingPayload": "requirePayload",
}

func variantHasGoPeer(source, variant string) bool {
	if _, deliberate := variantsPortedDifferently[variant]; deliberate {
		return true
	}
	snake := strings.ToLower(camelBoundary.ReplaceAllString(variant, "${1}_${2}"))
	return strings.Contains(source, variant) ||
		strings.Contains(source, `"`+snake+`"`)
}

func goSourceText(t *testing.T) string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	var all strings.Builder
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		all.Write(source)
	}
	return all.String()
}

var rustValidateImpl = regexp.MustCompile(`(?m)^impl Validate for (\w+)`)

var goValidateMethod = regexp.MustCompile(`func \(\w+ \*?(\w+)\) Validate\(\) error`)

// Not every rule is a trait impl. Some are free functions over a value the
// contract does not own a Validate impl for, and those were invisible to the
// audit above until a port went looking for one and found it missing.
var rustValidateFunction = regexp.MustCompile(`(?m)^pub fn (validate_\w+)`)

var goValidateFunction = regexp.MustCompile(`(?m)^func (Validate\w+)\(`)

// validatorsAwaitingAPort are the contract's validation rules this port does
// not enforce yet, each of which refuses a request the server would refuse.
//
// A missing rule does not change a byte: an unvalidated request encodes and
// decodes identically, and the server still refuses it. It costs the caller a
// round trip and a worse message instead of failing where the mistake was
// made. They are listed rather than quietly skipped so a rule added upstream
// still fails this audit, and so the remaining work is visible.
var validatorsAwaitingAPort = map[string]bool{
	// Rust validates a private request enum, whose arms this port validates
	// on each request type instead.
	"request": true,
}

// Every free-function validation rule the contract declares must have a Go
// peer.
//
// These are the rules that are not Validate impls, so the audit over those
// never saw them. validate_role was missing for exactly that reason.
func TestGivenTheContractCrateWhenComparedThenEveryValidatorFunctionShouldHaveAGoPeer(
	t *testing.T,
) {
	ported := map[string]bool{}
	for _, match := range goValidateFunction.FindAllStringSubmatch(goSourceText(t), -1) {
		ported[match[1]] = true
	}

	var missing []string
	eachContractFile(t, func(file, text string) {
		for _, match := range rustValidateFunction.FindAllStringSubmatch(text, -1) {
			if !ported[goName(match[1])] {
				missing = append(missing, file+": "+match[1]+" -> "+goName(match[1]))
			}
		}
	})
	sort.Strings(missing)
	for _, name := range missing {
		t.Errorf("the contract validates this and the Go port does not: %s", name)
	}
}

// goName renders a Rust snake_case name the way this port spells it, so a
// comparison is between the two spellings of one thing rather than between a
// name and a guess.
func goName(rustName string) string {
	var built strings.Builder
	for _, part := range strings.Split(rustName, "_") {
		if initialism, known := goInitialisms[part]; known {
			built.WriteString(initialism)
			continue
		}
		if part == "" {
			continue
		}
		built.WriteString(strings.ToUpper(part[:1]) + part[1:])
	}
	return built.String()
}

// Every validation rule the contract declares must be enforced here, or
// declared as outstanding.
//
// A rule added upstream that this port neither enforces nor declares fails
// here, so the backlog can only shrink.
func TestGivenTheContractCrateWhenComparedThenEveryValidationRuleShouldBeEnforcedOrDeclared(
	t *testing.T,
) {
	source := goSourceText(t)
	enforced := map[string]bool{}
	for _, match := range goValidateMethod.FindAllStringSubmatch(source, -1) {
		enforced[match[1]] = true
	}

	var undeclared, stale []string
	declared := map[string]bool{}
	eachContractFile(t, func(file, text string) {
		for _, match := range rustValidateImpl.FindAllStringSubmatch(text, -1) {
			name := match[1]
			declared[name] = true
			if enforced[name] || enforced[goNameOf(name)] {
				if validatorsAwaitingAPort[name] {
					stale = append(stale, name)
				}
				continue
			}
			if validatorsAwaitingAPort[name] {
				continue
			}
			undeclared = append(undeclared, file+": "+name)
		}
	})

	// A rule that left the contract must leave the backlog with it.
	for name := range validatorsAwaitingAPort {
		if !declared[name] {
			stale = append(stale, name)
		}
	}

	sort.Strings(undeclared)
	sort.Strings(stale)
	if len(undeclared) != 0 {
		t.Errorf("%d contract validation rules are neither enforced nor declared:\n  %s",
			len(undeclared), strings.Join(undeclared, "\n  "))
	}
	if len(stale) != 0 {
		t.Errorf("%d rules are declared outstanding but need not be: %s",
			len(stale), strings.Join(stale, " "))
	}
}

// wireEncodedGoTypes are the package's types that actually cross the wire: a
// struct whose fields carry cbor tags, or a type with a codec of its own.
//
// A type without either is a runtime shape, such as a transport handle or a
// compiled filter, and has no encoding to prove.
func wireEncodedGoTypes(t *testing.T) map[string]bool {
	t.Helper()
	source := goSourceText(t)
	encoded := map[string]bool{}
	for name, fields := range goStructFields(t) {
		if len(fields) != 0 {
			encoded[name] = true
		}
	}
	for _, match := range regexp.MustCompile(
		`func \(\w+ \*?(\w+)\) MarshalCBOR\(\)`).FindAllStringSubmatch(source, -1) {
		encoded[match[1]] = true
	}
	return encoded
}

// goTypesReachedByFixtures are the types a fixture decodes into, plus every
// type those reach, since decoding a record decodes everything inside it.
func goTypesReachedByFixtures(t *testing.T) map[string]bool {
	t.Helper()
	references := goTypeReferences(t)
	reached := map[string]bool{}
	var walk func(name string)
	walk = func(name string) {
		if reached[name] {
			return
		}
		reached[name] = true
		for _, referenced := range references[name] {
			walk(referenced)
		}
	}
	for _, seed := range fixtureDecodeTargets(t) {
		if _, known := references[seed]; known {
			walk(seed)
		}
	}
	return reached
}

var fixtureTarget = regexp.MustCompile(
	`(?:roundTripFixture|roundTripJSONFixture)\[\[?\]?wire\.(\w+)\]|var \w+ wire\.(\w+)`)

func fixtureDecodeTargets(t *testing.T) []string {
	t.Helper()
	var targets []string
	for _, match := range fixtureTarget.FindAllStringSubmatch(goTestText(t), -1) {
		for _, group := range match[1:] {
			if group != "" {
				targets = append(targets, group)
			}
		}
	}
	return targets
}

var goTypeDeclaration = regexp.MustCompile(
	`(?ms)^type (\w+) (struct \{.*?^\}|[^\n]+)|^\t(\w+) (struct \{.*?^\t\})`)

var goIdentifier = regexp.MustCompile(`\b([A-Z]\w+)\b`)

// goTypeReferences maps each type to the named types its declaration holds.
func goTypeReferences(t *testing.T) map[string][]string {
	t.Helper()
	references := map[string][]string{}
	for _, match := range goTypeDeclaration.FindAllStringSubmatch(goSourceText(t), -1) {
		name, body := match[1], match[2]
		if name == "" {
			name, body = match[3], match[4]
		}
		var held []string
		for _, identifier := range goIdentifier.FindAllStringSubmatch(body, -1) {
			held = append(held, identifier[1])
		}
		references[name] = held
	}
	return references
}

func goTestText(t *testing.T) string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	var all strings.Builder
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), "_test.go") {
			source, err := os.ReadFile(entry.Name())
			if err != nil {
				t.Fatalf("read %s: %v", entry.Name(), err)
			}
			all.Write(source)
		}
	}
	return all.String()
}

func sortedKeys(set map[string]bool) []string {
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

var rustSeededDefault = regexp.MustCompile(`(?m)#\[serde\([^)]*default = "(\w+)"[^)]*\)\]\s*(?:#\[[^\]]*\]\s*)*pub (\w+)`)

// goSeededDecode matches a type that seeds non-zero defaults before decoding
// into itself, the Go form of serde's `default = "..."`.
// The seed must actually carry a value: a bare `plain{}` fills nothing, so
// matching the shape alone would pass a type that seeds no default.
var goSeededDecode = regexp.MustCompile(`func \(\w+ \*(\w+)\) UnmarshalCBOR\([^)]*\) error \{\s*(?:// [^\n]*\n\s*)*type plain \w+\s*decoded := plain[({]\s*\w`)

// A contract field whose default is not the zero value must be seeded here.
//
// Go fills an absent field with the zero value, where serde calls the named
// function. A boolean defaulting to true is the dangerous shape: a record
// written before the field existed reads back as false, which inverts the
// meaning rather than losing it. Nothing else in this suite would notice,
// because such a record round-trips byte-identically either way.
func TestGivenAContractDefaultWhenNotTheZeroValueThenShouldBeSeededOnDecode(t *testing.T) {
	seeded := map[string]bool{}
	for _, match := range goSeededDecode.FindAllStringSubmatch(goSourceText(t), -1) {
		seeded[match[1]] = true
	}

	var unseeded []string
	eachContractFile(t, func(file, source string) {
		for _, match := range rustStructBody.FindAllStringSubmatch(source, -1) {
			name, body := match[1], match[2]
			defaults := rustSeededDefault.FindAllStringSubmatch(body, -1)
			if len(defaults) == 0 {
				continue
			}
			if seeded[name] || seeded[goNameOf(name)] {
				continue
			}
			for _, field := range defaults {
				unseeded = append(unseeded,
					fmt.Sprintf("%s: %s.%s (serde default %s)",
						file, name, field[2], field[1]))
			}
		}
	})
	sort.Strings(unseeded)
	if len(unseeded) != 0 {
		t.Errorf("%d contract fields default to a non-zero value that this port "+
			"does not seed:\n  %s", len(unseeded), strings.Join(unseeded, "\n  "))
	}
}
