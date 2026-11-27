package wire_test

import (
	"strings"
	"testing"
	"time"

	"github.com/laserdata/laser-sdk/foreign/go/wire"
)

// These cases are the shared regex contract: the same patterns the Rust and
// TypeScript suites pin, with the same verdicts. Go's own regexp reads the
// Perl classes and word boundaries as ASCII and refuses the class set
// operations, so a divergence here means a filter the server compiled selects
// different records through the Go client.

func matcher(t *testing.T, kind wire.TextMatch, pattern string, fold bool) *wire.TextMatcher {
	t.Helper()
	compiled, err := wire.CompileTextMatcher(wire.TextPredicate{
		Field: "type", Kind: kind, Pattern: pattern, CaseInsensitive: fold,
	})
	if err != nil {
		t.Fatalf("compile %s %q: %v", kind, pattern, err)
	}
	return compiled
}

func matchesRegex(t *testing.T, pattern, value string, fold bool) bool {
	t.Helper()
	return matcher(t, wire.TextRegex, pattern, fold).Matches(value)
}

func TestGivenEachTextKindWhenMatchingEventTypesThenShouldFollowItsRule(t *testing.T) {
	const value = "metrics.cpu.v1.reported"
	cases := []struct {
		kind    wire.TextMatch
		pattern string
		want    bool
	}{
		{wire.TextEquals, value, true},
		{wire.TextPrefix, "metrics.", true},
		{wire.TextSuffix, ".reported", true},
		{wire.TextContains, ".v1.", true},
		{wire.TextGlob, "metrics.*.v?.reported", true},
		{wire.TextRegex, `^metrics\.[a-z]+\.v[0-9]+\.`, true},
		{wire.TextSuffix, ".deleted", false},
	}
	for _, testCase := range cases {
		if got := matcher(t, testCase.kind, testCase.pattern, false).Matches(value); got !=
			testCase.want {
			t.Errorf("%s %q against %q is %v", testCase.kind, testCase.pattern, value, got)
		}
	}
	// A glob is anchored at both ends, so a prefix match is not enough.
	if matcher(t, wire.TextGlob, "metrics.*", false).Matches("storage.metrics.x") {
		t.Error("a glob matches the whole value")
	}
}

func TestGivenCaseInsensitiveMatchingWhenTheCaseDiffersThenShouldStillMatch(t *testing.T) {
	if !matcher(t, wire.TextEquals, "Safe", true).Matches("SAFE") {
		t.Error("equals ignoring case")
	}
	if !matcher(t, wire.TextContains, "CPU", true).Matches("metrics.cpu.v1") {
		t.Error("contains ignoring case")
	}
	if !matcher(t, wire.TextGlob, "METRICS.*", true).Matches("metrics.x") {
		t.Error("glob ignoring case")
	}
	if !matchesRegex(t, "^metrics", "METRICS.x", true) {
		t.Error("regex ignoring case")
	}
	if matcher(t, wire.TextEquals, "Safe", false).Matches("SAFE") {
		t.Error("a case-sensitive match must not fold")
	}
	// A regex folds with simple case folding, so the Kelvin sign matches k.
	if !matchesRegex(t, "^k$", "K", true) {
		t.Error("simple case folding reaches the Kelvin sign")
	}
}

// The plain kinds lowercase both sides with the full Unicode mapping, which
// expands some characters and has a final form for a Greek sigma.
func TestGivenExpandingOrContextualLowercaseWhenFoldedThenShouldMatchTheReference(t *testing.T) {
	if !matcher(t, wire.TextGlob, "İ?", true).Matches("İx") {
		t.Error("an expanding lowercase keeps the wildcards aligned")
	}
	// Each glob literal folds on its own, so a sigma in a pattern never takes
	// the word-final form a whole-string fold would give it.
	if !matcher(t, wire.TextGlob, "ΟΔΟΣ*", true).Matches("ΟΔΟΣΑ") {
		t.Error("a Greek literal folds per character")
	}
	if !matcher(t, wire.TextEquals, "ΟΔΟΣ", true).Matches("οδος") {
		t.Error("a word-final sigma lowercases to its final form")
	}
}

func TestGivenUnportableOrBrokenPatternsWhenValidatedThenShouldRefuseThem(t *testing.T) {
	refused := []struct {
		kind    wire.TextMatch
		pattern string
	}{
		{wire.TextRegex, `(a)\1`},
		{wire.TextRegex, "(?=a)"},
		{wire.TextRegex, "(?i)a"},
		{wire.TextRegex, "(?P<name>a)"},
		{wire.TextRegex, "("},
		{wire.TextRegex, "a{2,1}"},
		{wire.TextRegex, "[z-a]"},
		{wire.TextRegex, `\e`},
		{wire.TextRegex, `\0`},
		{wire.TextRegex, `\x{110000}`},
		{wire.TextGlob, `a\`},
	}
	for _, testCase := range refused {
		_, err := wire.CompileTextMatcher(wire.TextPredicate{
			Field: "type", Kind: testCase.kind, Pattern: testCase.pattern,
		})
		if err == nil {
			t.Errorf("%s %q must be refused", testCase.kind, testCase.pattern)
		}
	}
	// A question mark inside a class is not a group, so this one compiles.
	if !matchesRegex(t, "(?:a|b)[(?]", "b?", false) {
		t.Error("a bracketed question mark is a literal")
	}
}

func TestGivenUnicodePerlClassesWhenMatchingThenShouldUseTheUnicodeDefinitions(t *testing.T) {
	cases := []struct {
		pattern, value string
		want           bool
	}{
		{`^\d+$`, "١٢٣", true},
		{`^\w+$`, "żółw_1", true},
		{`^\s$`, " ", true},
		{`\bżółw\b`, "a żółw b", true},
		{`\bżół\b`, "żółw", false},
		{`\b{start}żółw\b{end}`, "żółw", true},
		{`^\W$`, "-", true},
	}
	for _, testCase := range cases {
		if got := matchesRegex(t, testCase.pattern, testCase.value, false); got != testCase.want {
			t.Errorf("%q against %q is %v", testCase.pattern, testCase.value, got)
		}
	}
}

func TestGivenADotOrADollarWhenANewlineIsInvolvedThenShouldFollowTheReference(t *testing.T) {
	cases := []struct {
		pattern, value string
		want           bool
	}{
		{"^.$", "\n", false},
		{"^.$", "\r", true},
		{"^.$", "🙂", true},
		{"a$", "a\n", false},
		{`a\z`, "ba", true},
	}
	for _, testCase := range cases {
		if got := matchesRegex(t, testCase.pattern, testCase.value, false); got != testCase.want {
			t.Errorf("%q against %q is %v", testCase.pattern, testCase.value, got)
		}
	}
}

func TestGivenClassSetOperationsWhenMatchingThenShouldFollowTheReference(t *testing.T) {
	cases := []struct {
		pattern, value string
		want           bool
	}{
		{"^[a-z&&[^aeiou]]+$", "rhythm", true},
		{"^[a-z&&[^aeiou]]+$", "audio", false},
		{`^[\w--\d]+$`, "abc", true},
		{`^[\w--\d]+$`, "a1", false},
		{"^[a-c~~b-d]+$", "ad", true},
		{"[a-c~~b-d]", "b", false},
		{"^[[:alpha:][:digit:]]+$", "a1Z", true},
		{"^[]a]+$", "]a", true},
		{`^\p{Greek}+$`, "Ωω", true},
	}
	for _, testCase := range cases {
		if got := matchesRegex(t, testCase.pattern, testCase.value, false); got != testCase.want {
			t.Errorf("%q against %q is %v", testCase.pattern, testCase.value, got)
		}
	}
}

// Folding happens before negation, so a negated class under a case-insensitive
// pattern excludes both cases rather than admitting the other one.
func TestGivenACaseInsensitiveNegatedClassWhenFoldedThenShouldExcludeBothCases(t *testing.T) {
	cases := []struct {
		pattern, value string
		want           bool
	}{
		{"[^a]", "A", false},
		{"[^k]", "K", false},
		{`\p{ascii}`, "ſ", true},
		{`\P{Ll}`, "A", false},
	}
	for _, testCase := range cases {
		if got := matchesRegex(t, testCase.pattern, testCase.value, true); got != testCase.want {
			t.Errorf("%q against %q ignoring case is %v", testCase.pattern, testCase.value, got)
		}
	}
}

func TestGivenUnicodePropertySpellingsWhenCompiledThenShouldResolveThemLikeTheReference(
	t *testing.T,
) {
	for _, pattern := range []string{`\p{greek}`, `\p{sc=Grek}`, `\pL`} {
		if !matchesRegex(t, pattern, "Ω", false) {
			t.Errorf("%q names the Greek capital omega", pattern)
		}
	}
	if !matchesRegex(t, `\p{gc:lu}`, "A", false) {
		t.Error("a general category query resolves by its alias")
	}
	if !matchesRegex(t, `\p{isAlpha}`, "ż", false) {
		t.Error("an is-prefixed property name resolves")
	}
	if matchesRegex(t, `\p{Script!=Greek}`, "Ω", false) {
		t.Error("an inequality query negates the property")
	}
	// A bare sc is the Currency_Symbol category, not the Script property.
	if !matchesRegex(t, `\p{sc}`, "€", false) {
		t.Error("a bare sc is the currency symbol category")
	}
	for _, pattern := range []string{`\p{NotAThing}`, `\p{Script}`, `\p{age=1.1}`} {
		if _, err := wire.CompileTextMatcher(wire.TextPredicate{
			Field: "type", Kind: wire.TextRegex, Pattern: pattern,
		}); err == nil {
			t.Errorf("%q must be refused", pattern)
		}
	}
}

// Go ships script memberships but not script extensions. Refusing the pattern
// tells the caller plainly, where approximating it with the script property
// would quietly select different records than the server.
func TestGivenScriptExtensionsWhenCompiledThenShouldRefuseItPlainly(t *testing.T) {
	_, err := wire.CompileTextMatcher(wire.TextPredicate{
		Field: "type", Kind: wire.TextRegex, Pattern: `\p{Script_Extensions=Greek}`,
	})
	if err == nil {
		t.Fatal("script extensions must be refused, not approximated")
	}
	if !strings.Contains(err.Error(), "Script_Extensions") {
		t.Errorf("the refusal names the property, got %q", err)
	}
}

func TestGivenEscapesAndRepetitionsWhenCompiledThenShouldFollowTheReferenceSyntax(t *testing.T) {
	accepted := []struct{ pattern, value string }{
		{`^\x41\u{1F642}\U00000042$`, "A🙂B"},
		{`^\%\-\ $`, "%- "},
		{"^a{ 2 }$", "aa"},
		{"^a{2,}$", "aaaa"},
		{`^\b{3}a$`, "a"},
		{"a**", ""},
	}
	for _, testCase := range accepted {
		if !matchesRegex(t, testCase.pattern, testCase.value, false) {
			t.Errorf("%q must match %q", testCase.pattern, testCase.value)
		}
	}
	for _, pattern := range []string{
		"*a", "a{", "a{,2}", "a{1, }", `\b{foo}`, `[\b]`, `[a-\d]`,
	} {
		if _, err := wire.CompileTextMatcher(wire.TextPredicate{
			Field: "type", Kind: wire.TextRegex, Pattern: pattern,
		}); err == nil {
			t.Errorf("%q must be refused", pattern)
		}
	}
}

// The simulation never backtracks, so a pattern built to blow up a
// backtracking engine finishes in time linear in the value.
func TestGivenAPathologicalPatternWhenMatchingALongInputThenShouldFinishInLinearTime(
	t *testing.T,
) {
	started := time.Now()
	long := strings.Repeat("a", 50_000)
	if matchesRegex(t, "(a*)*b", long, false) {
		t.Error("there is no b to find")
	}
	if matchesRegex(t, "^(a|a)*$", long+"!", false) {
		t.Error("the trailing character is not in the pattern")
	}
	if elapsed := time.Since(started); elapsed > 30*time.Second {
		t.Errorf("the pathological patterns took %s", elapsed)
	}
}

func TestGivenPatternsBeyondTheSizeOrNestLimitWhenCompiledThenShouldRefuseThem(t *testing.T) {
	compiles := func(pattern string) error {
		_, err := wire.CompileTextMatcher(wire.TextPredicate{
			Field: "type", Kind: wire.TextRegex, Pattern: pattern,
		})
		return err
	}
	for _, pattern := range []string{
		"a{8185}", `\w{5}`, `\b{1000000}`,
		strings.Repeat("(?:", 64) + "a" + strings.Repeat(")", 64),
		strings.Repeat("[", 64) + "a" + strings.Repeat("]", 64),
	} {
		if err := compiles(pattern); err != nil {
			t.Errorf("a pattern the server accepts must compile: %v", err)
		}
	}
	for _, testCase := range []struct{ pattern, reason string }{
		{"a{8186}", "size limit"},
		{`\w{6}`, "size limit"},
		{strings.Repeat("(?:", 65) + "a" + strings.Repeat(")", 65), "nest limit"},
		{strings.Repeat("[", 65) + "a" + strings.Repeat("]", 65), "nest limit"},
	} {
		err := compiles(testCase.pattern)
		if err == nil {
			t.Errorf("a pattern beyond the %s must be refused", testCase.reason)
			continue
		}
		if !strings.Contains(err.Error(), testCase.reason) {
			t.Errorf("the refusal names the %s, got %q", testCase.reason, err)
		}
	}
}

// The reference implementation compiles a glob to an anchored regex and pins
// that against an independent two-pointer oracle. Go matches globs with the
// oracle directly, so the cross-check runs the other way: every pattern must
// agree with the equivalent regex through this port's own engine.
func TestGivenGlobPatternsWhenMatchedThenShouldAgreeWithTheEquivalentRegex(t *testing.T) {
	patterns := []string{
		"", "*", "**", "?", "*?", "a*b*c", "*aaab", "a**b", `a\*b`, `a\?b`, `a\\b`,
		"*ł?", "*\n*", "[a]", "(?x)", "a.b", "a+b", "^a$",
	}
	values := []string{
		"", "a", "ab", "ac", "aaab", "aaaaac", "a*b", "a?b", `a\b`, "aXXbYYbZc",
		"ł🙂", "aaał🙂", "a\nb", "[a]", "(?x)", "a.b", "a+b", "^a$",
	}
	for _, pattern := range patterns {
		glob := matcher(t, wire.TextGlob, pattern, false)
		// The same glob as a regex: anchored at both ends, with a dot that
		// matches a newline, which is how the reference compiles one.
		equivalent := matcher(t, wire.TextRegex, `\A(?:`+globRegexSource(pattern)+`)\z`, false)
		for _, value := range values {
			if got, want := glob.Matches(value), equivalent.Matches(value); got != want {
				t.Errorf("glob %q against %q is %v, the equivalent regex says %v",
					pattern, value, got, want)
			}
		}
	}
}

// globRegexSource is the glob translated to the regular expression it stands
// for, with every literal escaped.
func globRegexSource(pattern string) string {
	var source strings.Builder
	characters := []rune(pattern)
	for index := 0; index < len(characters); index++ {
		character := characters[index]
		switch character {
		case '*':
			source.WriteString("[\\x{0}-\\x{10FFFF}]*")
			continue
		case '?':
			source.WriteString("[\\x{0}-\\x{10FFFF}]")
			continue
		case '\\':
			index++
			if index >= len(characters) {
				return source.String()
			}
			character = characters[index]
		}
		source.WriteString(quoteRegexLiteral(character))
	}
	return source.String()
}

func quoteRegexLiteral(character rune) string {
	if character < 0x80 && !((character >= '0' && character <= '9') ||
		(character >= 'A' && character <= 'Z') ||
		(character >= 'a' && character <= 'z') ||
		character == '<' || character == '>') {
		return `\` + string(character)
	}
	return string(character)
}
