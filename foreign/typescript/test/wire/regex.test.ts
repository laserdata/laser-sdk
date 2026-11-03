import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import { TextMatcher } from "../../src/wire/filter-eval.js"
import { textPredicateValidate, type TextMatch } from "../../src/wire/filter.js"
import { RustRegex } from "../../src/wire/regex.js"

function matcher(kind: TextMatch, pattern: string, caseInsensitive = false): TextMatcher {
  return new TextMatcher(kind, pattern, caseInsensitive)
}

function matches(pattern: string, value: string, caseInsensitive = false): boolean {
  return RustRegex.compile(pattern, caseInsensitive).matches(value)
}

void test("given_each_kind_when_matching_event_types_then_should_follow_its_rule", () => {
  const value = "metrics.cpu.v1.reported"
  assert.ok(matcher("equals", value).matches(value))
  assert.ok(matcher("prefix", "metrics.").matches(value))
  assert.ok(matcher("suffix", ".reported").matches(value))
  assert.ok(matcher("contains", ".v1.").matches(value))
  assert.ok(matcher("glob", "metrics.*.v?.reported").matches(value))
  assert.ok(matcher("regex", "^metrics\\.[a-z]+\\.v[0-9]+\\.").matches(value))
  assert.ok(!matcher("suffix", ".deleted").matches(value))
  assert.ok(!matcher("glob", "metrics.*").matches("storage.metrics.x"))
})

void test("given_case_insensitive_matching_when_the_case_differs_then_should_still_match", () => {
  assert.ok(matcher("equals", "Safe", true).matches("SAFE"))
  assert.ok(matcher("contains", "CPU", true).matches("metrics.cpu.v1"))
  assert.ok(matcher("glob", "METRICS.*", true).matches("metrics.x"))
  assert.ok(matcher("regex", "^metrics", true).matches("METRICS.x"))
  assert.ok(!matcher("equals", "Safe").matches("SAFE"))
  // Regex folds with simple case folding, so the Kelvin sign matches k.
  assert.ok(matcher("regex", "^k$", true).matches("K"))
})

void test("given_unportable_or_broken_patterns_when_validated_then_should_refuse_them", () => {
  for (const [kind, pattern] of [
    ["regex", "(a)\\1"],
    ["regex", "(?=a)"],
    ["regex", "(?i)a"],
    ["regex", "(?P<name>a)"],
    ["regex", "("],
    ["regex", "a{2,1}"],
    ["regex", "[z-a]"],
    ["regex", "\\e"],
    ["regex", "\\0"],
    ["regex", "\\x{110000}"],
    ["glob", "a\\"]
  ] as const) {
    assert.throws(
      () => {
        textPredicateValidate({ field: "type", kind, pattern })
      },
      InvalidError,
      pattern
    )
  }
  assert.ok(matcher("regex", "(?:a|b)[(?]").matches("b?"))
})

void test("given_unicode_perl_classes_when_matching_then_should_follow_the_rust_unicode_definitions", () => {
  assert.ok(matches("^\\d+$", "١٢٣"))
  assert.ok(matches("^\\w+$", "żółw_1"))
  assert.ok(matches("^\\s$", " "))
  assert.ok(matches("\\bżółw\\b", "a żółw b"))
  assert.ok(!matches("\\bżół\\b", "żółw"))
  assert.ok(matches("\\b{start}żółw\\b{end}", "żółw"))
  assert.ok(matches("^\\W$", "-"))
})

void test("given_a_dot_or_a_dollar_when_a_newline_is_involved_then_should_follow_rust", () => {
  assert.ok(!matches("^.$", "\n"))
  assert.ok(matches("^.$", "\r"))
  assert.ok(matches("^.$", "🙂"))
  assert.ok(!matches("a$", "a\n"))
  assert.ok(matches("a\\z", "ba"))
})

void test("given_class_set_operations_when_matching_then_should_follow_rust", () => {
  assert.ok(matches("^[a-z&&[^aeiou]]+$", "rhythm"))
  assert.ok(!matches("^[a-z&&[^aeiou]]+$", "audio"))
  assert.ok(matches("^[\\w--\\d]+$", "abc"))
  assert.ok(!matches("^[\\w--\\d]+$", "a1"))
  assert.ok(matches("^[a-c~~b-d]+$", "ad"))
  assert.ok(!matches("[a-c~~b-d]", "b"))
  assert.ok(matches("^[[:alpha:][:digit:]]+$", "a1Z"))
  assert.ok(matches("^[]a]+$", "]a"))
  assert.ok(matches("^\\p{Greek}+$", "Ωω"))
})

void test("given_a_case_insensitive_negated_class_when_the_case_differs_then_should_exclude_both_cases", () => {
  assert.ok(!matches("[^a]", "A", true))
  assert.ok(!matches("[^k]", "K", true))
  assert.ok(!matches("[^k]", "K", true))
  assert.ok(matches("\\p{ascii}", "ſ", true))
  assert.ok(!matches("\\P{Ll}", "A", true))
})

void test("given_unicode_property_spellings_when_compiled_then_should_resolve_them_like_rust", () => {
  for (const pattern of ["\\p{greek}", "\\p{sc=Grek}", "\\p{Script_Extensions=Greek}", "\\pL"]) {
    assert.ok(matches(pattern, "Ω"), pattern)
  }
  assert.ok(matches("\\p{gc:lu}", "A"))
  assert.ok(matches("\\p{isAlpha}", "ż"))
  assert.ok(!matches("\\p{Script!=Greek}", "Ω"))
  // A bare `sc` is the Currency_Symbol category, not the Script property.
  assert.ok(matches("\\p{sc}", "€"))
  for (const pattern of ["\\p{NotAThing}", "\\p{Script}", "\\p{age=1.1}"]) {
    assert.throws(() => RustRegex.compile(pattern, false), InvalidError, pattern)
  }
})

void test("given_escapes_and_repetitions_when_compiled_then_should_follow_the_rust_syntax", () => {
  assert.ok(matches("^\\x41\\u{1F642}\\U00000042$", "A🙂B"))
  assert.ok(matches("^\\%\\-\\ $", "%- "))
  assert.ok(matches("^a{ 2 }$", "aa"))
  assert.ok(matches("^a{2,}$", "aaaa"))
  assert.ok(matches("^\\b{3}a$", "a"))
  assert.ok(matches("a**", ""))
  for (const pattern of ["*a", "a{", "a{,2}", "a{1, }", "\\b{foo}", "[\\b]", "[a-\\d]"]) {
    assert.throws(() => RustRegex.compile(pattern, false), InvalidError, pattern)
  }
})

void test("given_a_pathological_pattern_when_matching_a_long_input_then_should_finish_in_linear_time", () => {
  const started = performance.now()
  assert.ok(!matches("(a*)*b", "a".repeat(50_000)))
  assert.ok(!matches("^(a|a)*$", `${"a".repeat(50_000)}!`))
  assert.ok(performance.now() - started < 5_000)
})

void test("given_patterns_beyond_the_size_or_nest_limit_when_compiled_then_should_refuse_them", () => {
  RustRegex.compile("a{8185}", false)
  assert.throws(() => RustRegex.compile("a{8186}", false), /size limit/u)
  RustRegex.compile("\\w{5}", false)
  assert.throws(() => RustRegex.compile("\\w{6}", false), /size limit/u)
  RustRegex.compile("\\b{1000000}", false)
  RustRegex.compile(`${"(?:".repeat(64)}a${")".repeat(64)}`, false)
  assert.throws(
    () => RustRegex.compile(`${"(?:".repeat(65)}a${")".repeat(65)}`, false),
    /nest limit/u
  )
  RustRegex.compile(`${"[".repeat(64)}a${"]".repeat(64)}`, false)
  assert.throws(() => RustRegex.compile(`${"[".repeat(65)}a${"]".repeat(65)}`, false), /nest/u)
})
