package wire

import (
	"fmt"
	"strings"
)

const (
	globAnyRun = '*'
	globAnyOne = '?'
	globEscape = '\\'
)

// MaxRegexPredicates is how many regex and glob predicates one filter may
// hold. The bound keeps one compiled filter's programs small, and a record
// costs at most this many linear passes over its text fields.
const MaxRegexPredicates = 4

// TextMatcher is a validated text predicate ready to run per record.
//
// Compiling is the validation, so a pattern is checked exactly once and every
// later evaluation is a match.
type TextMatcher struct {
	kind            TextMatch
	source          string
	caseInsensitive bool
	// pattern is the comparison text for the plain kinds, lowercased when the
	// match ignores case.
	pattern string
	glob    []globToken
	regex   regexProgram
}

// regexProgram is a compiled regular expression that searches a value in time
// linear in its length.
type regexProgram interface {
	MatchString(value string) bool
}

type globToken struct {
	anyRun  bool
	anyOne  bool
	literal rune
}

// CompileTextMatcher validates a text predicate and prepares it for matching.
func CompileTextMatcher(predicate TextPredicate) (*TextMatcher, error) {
	if len(predicate.Pattern) > MaxFilterStringBytes {
		return nil, &InvalidError{Reason: fmt.Sprintf(
			"a text pattern exceeds %dB", MaxFilterStringBytes)}
	}
	matcher := &TextMatcher{
		kind:            predicate.Kind,
		source:          predicate.Pattern,
		caseInsensitive: predicate.CaseInsensitive,
	}
	switch predicate.Kind {
	case TextRegex:
		program, err := compileFilterRegex(predicate.Pattern, predicate.CaseInsensitive)
		if err != nil {
			return nil, err
		}
		matcher.regex = program
	case TextGlob:
		tokens, err := compileGlob(predicate.Pattern, predicate.CaseInsensitive)
		if err != nil {
			return nil, err
		}
		matcher.glob = tokens
	case TextEquals, TextPrefix, TextSuffix, TextContains:
		matcher.pattern = predicate.Pattern
		if predicate.CaseInsensitive {
			matcher.pattern = toLowercase(predicate.Pattern)
		}
	default:
		return nil, &InvalidError{Reason: fmt.Sprintf(
			"unknown text match kind %q", string(predicate.Kind))}
	}
	return matcher, nil
}

// Kind is how the matcher compares.
func (matcher *TextMatcher) Kind() TextMatch { return matcher.kind }

// Label is the pattern as written, with its case mode, for explanations.
func (matcher *TextMatcher) Label() string {
	if matcher.caseInsensitive {
		return fmt.Sprintf("%q ignoring case", matcher.source)
	}
	return fmt.Sprintf("%q", matcher.source)
}

// Matches reports whether the value satisfies the predicate.
//
// A regex folds case inside the engine, which uses simple case folding. Every
// other kind lowercases both sides, which is the full Unicode mapping.
func (matcher *TextMatcher) Matches(value string) bool {
	switch matcher.kind {
	case TextRegex:
		return matcher.regex.MatchString(value)
	case TextGlob:
		if matcher.caseInsensitive {
			value = toLowercase(value)
		}
		return globMatches(matcher.glob, []rune(value))
	}
	if matcher.caseInsensitive {
		value = toLowercase(value)
	}
	switch matcher.kind {
	case TextEquals:
		return value == matcher.pattern
	case TextPrefix:
		return strings.HasPrefix(value, matcher.pattern)
	case TextSuffix:
		return strings.HasSuffix(value, matcher.pattern)
	case TextContains:
		return strings.Contains(value, matcher.pattern)
	default:
		return false
	}
}

// compileGlob resolves a glob's escapes into tokens. A case-insensitive glob
// lowercases each literal on its own, so a character whose lowercase form is
// longer becomes that many literal tokens and the stars and single-character
// wildcards around it keep their positions.
func compileGlob(pattern string, caseInsensitive bool) ([]globToken, error) {
	tokens := make([]globToken, 0, len(pattern))
	characters := []rune(pattern)
	for index := 0; index < len(characters); index++ {
		character := characters[index]
		switch character {
		case globAnyRun:
			tokens = append(tokens, globToken{anyRun: true})
			continue
		case globAnyOne:
			tokens = append(tokens, globToken{anyOne: true})
			continue
		case globEscape:
			index++
			if index >= len(characters) {
				return nil, &InvalidError{
					Reason: "a glob pattern cannot end with an unescaped backslash"}
			}
			character = characters[index]
		}
		literal := string(character)
		if caseInsensitive {
			literal = toLowercase(literal)
		}
		for _, folded := range literal {
			tokens = append(tokens, globToken{literal: folded})
		}
	}
	return tokens, nil
}

// globMatches runs the anchored two-pointer match: on a mismatch after a star,
// the star absorbs one more character and matching resumes.
func globMatches(tokens []globToken, text []rune) bool {
	position, cursor := 0, 0
	resumeToken, resumeText, resumable := 0, 0, false
	for position < len(text) {
		token, inRange := globToken{}, cursor < len(tokens)
		if inRange {
			token = tokens[cursor]
		}
		switch {
		case inRange && token.anyRun:
			resumeToken, resumeText, resumable = cursor, position, true
			cursor++
		case inRange && token.anyOne:
			cursor++
			position++
		case inRange && !token.anyRun && !token.anyOne && token.literal == text[position]:
			cursor++
			position++
		case resumable:
			cursor = resumeToken + 1
			position = resumeText + 1
			resumeText = position
		default:
			return false
		}
	}
	for _, token := range tokens[cursor:] {
		if !token.anyRun {
			return false
		}
	}
	return true
}
