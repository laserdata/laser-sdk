package wire

import (
	"strings"
	"unicode"
)

// lowercaseLatinDotI is the one code point whose lowercase form is longer than
// itself: a capital I with a dot above lowercases to an i followed by a
// combining dot. Go's per-rune mapping drops the dot, so it is spelled here.
const lowercaseLatinDotI = "i̇"

const (
	capitalLetterIWithDotAbove = 'İ'
	capitalLetterSigma         = 'Σ'
	smallLetterSigma           = 'σ'
	smallLetterFinalSigma      = 'ς'
)

// toLowercase folds text the way the contract's reference evaluator does: the
// full Unicode lowercase mapping, including the expansions and the final-sigma
// rule that a per-rune mapping misses.
//
// Case-insensitive text predicates compare lowercased values, so a fold that
// differed here would select different records in Go than in the other SDKs.
func toLowercase(text string) string {
	// The common case is text with nothing special in it.
	if !strings.ContainsRune(text, capitalLetterIWithDotAbove) &&
		!strings.ContainsRune(text, capitalLetterSigma) {
		return strings.ToLower(text)
	}
	var folded strings.Builder
	folded.Grow(len(text))
	for index, character := range text {
		switch character {
		case capitalLetterIWithDotAbove:
			folded.WriteString(lowercaseLatinDotI)
		case capitalLetterSigma:
			folded.WriteRune(sigmaFor(text, index))
		default:
			folded.WriteRune(unicode.ToLower(character))
		}
	}
	return folded.String()
}

// sigmaFor picks the final form of a lowercase sigma. A sigma is final when a
// cased letter precedes it and none follows, ignoring the marks and modifiers
// in between.
func sigmaFor(text string, index int) rune {
	const sigmaWidth = 2 // the capital sigma is two bytes in UTF-8
	precededByCased := casedAfterIgnorable(reversedRunes(text[:index]))
	followedByCased := casedAfterIgnorable(forwardRunes(text[index+sigmaWidth:]))
	if precededByCased && !followedByCased {
		return smallLetterFinalSigma
	}
	return smallLetterSigma
}

func forwardRunes(text string) []rune { return []rune(text) }

func reversedRunes(text string) []rune {
	runes := []rune(text)
	for head, tail := 0, len(runes)-1; head < tail; head, tail = head+1, tail-1 {
		runes[head], runes[tail] = runes[tail], runes[head]
	}
	return runes
}

// casedAfterIgnorable skips case-ignorable characters and reports whether the
// first character past them is cased.
func casedAfterIgnorable(runes []rune) bool {
	for _, character := range runes {
		if isCaseIgnorable(character) {
			continue
		}
		return isCased(character)
	}
	return false
}

// isCased reports the Unicode Cased property: an upper, lower, or title case
// letter.
func isCased(character rune) bool {
	return unicode.Is(unicode.Lu, character) ||
		unicode.Is(unicode.Ll, character) ||
		unicode.Is(unicode.Lt, character) ||
		unicode.Is(unicode.Other_Uppercase, character) ||
		unicode.Is(unicode.Other_Lowercase, character)
}

// isCaseIgnorable reports the Unicode Case_Ignorable property: the marks,
// formatting characters, modifier letters and symbols, and the handful of word
// separators that sit inside a word without ending it.
func isCaseIgnorable(character rune) bool {
	switch character {
	case '\'', '.', ':', '·', '·', '״', '‘', '’',
		'․', '︓', '﹒', '﹕', '＇', '．', '：':
		return true
	}
	return unicode.Is(unicode.Mn, character) ||
		unicode.Is(unicode.Me, character) ||
		unicode.Is(unicode.Cf, character) ||
		unicode.Is(unicode.Lm, character) ||
		unicode.Is(unicode.Sk, character)
}
