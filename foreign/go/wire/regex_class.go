package wire

import (
	"sort"
	"strings"
	"unicode"
)

// runeRange is one inclusive span of code points.
type runeRange struct{ lo, hi rune }

// runeSet is a sorted, non-overlapping set of code points. Character classes
// are resolved to explicit sets at compile time, so matching one is a binary
// search and never a second regex engine.
type runeSet []runeRange

// newRuneSet sorts and merges spans into a set.
func newRuneSet(ranges []runeRange) runeSet {
	if len(ranges) == 0 {
		return nil
	}
	sorted := make(runeSet, len(ranges))
	copy(sorted, ranges)
	sort.Slice(sorted, func(left, right int) bool {
		if sorted[left].lo != sorted[right].lo {
			return sorted[left].lo < sorted[right].lo
		}
		return sorted[left].hi < sorted[right].hi
	})
	merged := sorted[:1]
	for _, span := range sorted[1:] {
		last := &merged[len(merged)-1]
		switch {
		case span.lo > last.hi+1:
			merged = append(merged, span)
		case span.hi > last.hi:
			last.hi = span.hi
		}
	}
	return merged
}

func (set runeSet) contains(character rune) bool {
	low, high := 0, len(set)-1
	for low <= high {
		middle := (low + high) / 2
		switch {
		case character < set[middle].lo:
			high = middle - 1
		case character > set[middle].hi:
			low = middle + 1
		default:
			return true
		}
	}
	return false
}

func (set runeSet) union(other runeSet) runeSet {
	return newRuneSet(append(append([]runeRange{}, set...), other...))
}

// negate is every code point the set leaves out.
func (set runeSet) negate() runeSet {
	var inverted []runeRange
	next := rune(0)
	for _, span := range set {
		if span.lo > next {
			inverted = append(inverted, runeRange{next, span.lo - 1})
		}
		if span.hi+1 > next {
			next = span.hi + 1
		}
	}
	if next <= unicode.MaxRune {
		inverted = append(inverted, runeRange{next, unicode.MaxRune})
	}
	return newRuneSet(inverted)
}

func (set runeSet) intersect(other runeSet) runeSet {
	var shared []runeRange
	left, right := 0, 0
	for left < len(set) && right < len(other) {
		lo, hi := set[left].lo, set[left].hi
		if other[right].lo > lo {
			lo = other[right].lo
		}
		if other[right].hi < hi {
			hi = other[right].hi
		}
		if lo <= hi {
			shared = append(shared, runeRange{lo, hi})
		}
		if set[left].hi < other[right].hi {
			left++
		} else {
			right++
		}
	}
	return newRuneSet(shared)
}

func (set runeSet) difference(other runeSet) runeSet {
	return set.intersect(other.negate())
}

func (set runeSet) symmetricDifference(other runeSet) runeSet {
	return set.difference(other).union(other.difference(set))
}

// maxCasedRune is the highest code point with a case mapping, read from the
// Unicode tables this build ships, so folding never walks the planes above it.
var maxCasedRune = func() rune {
	highest := rune(0)
	for _, span := range unicode.CaseRanges {
		if rune(span.Hi) > highest {
			highest = rune(span.Hi)
		}
	}
	return highest
}()

// caseFold adds every simple case variant of the set's members.
//
// This is the simple folding the reference engine applies to a
// case-insensitive pattern, which is not the full lowercasing the other text
// match kinds use.
func (set runeSet) caseFold() runeSet {
	folded := append([]runeRange{}, set...)
	for _, span := range set {
		high := span.hi
		if high > maxCasedRune {
			high = maxCasedRune
		}
		for character := span.lo; character <= high; character++ {
			for variant := unicode.SimpleFold(character); variant != character; variant = unicode.SimpleFold(variant) {
				folded = append(folded, runeRange{variant, variant})
			}
		}
	}
	return newRuneSet(folded)
}

// rangesOf converts a Unicode table into a set. A strided span lists every
// member it names, since a set holds contiguous spans only.
func rangesOf(table *unicode.RangeTable) runeSet {
	ranges := make([]runeRange, 0, len(table.R16)+len(table.R32))
	appendSpan := func(lo, hi rune, stride rune) {
		if stride == 1 {
			ranges = append(ranges, runeRange{lo, hi})
			return
		}
		for character := lo; character <= hi; character += stride {
			ranges = append(ranges, runeRange{character, character})
		}
	}
	for _, span := range table.R16 {
		appendSpan(rune(span.Lo), rune(span.Hi), rune(span.Stride))
	}
	for _, span := range table.R32 {
		appendSpan(rune(span.Lo), rune(span.Hi), rune(span.Stride))
	}
	return newRuneSet(ranges)
}

// tablesOf is the union of several Unicode tables.
func tablesOf(tables ...*unicode.RangeTable) runeSet {
	var combined runeSet
	for _, table := range tables {
		combined = combined.union(rangesOf(table))
	}
	return combined
}

// The Perl classes, Unicode-aware as the reference engine defines them.
var (
	classDigit = rangesOf(unicode.Nd)
	classSpace = rangesOf(unicode.White_Space)
	// A word character is alphabetic, a mark, a decimal digit, a connector
	// punctuation, or a joiner.
	classWord = tablesOf(unicode.L, unicode.Nl, unicode.Other_Alphabetic,
		unicode.M, unicode.Nd, unicode.Pc, unicode.Join_Control)
)

// The POSIX bracket classes, which are ASCII-only in every engine.
var posixClasses = map[string]runeSet{
	"alnum":  {{'0', '9'}, {'A', 'Z'}, {'a', 'z'}},
	"alpha":  {{'A', 'Z'}, {'a', 'z'}},
	"ascii":  {{0x00, 0x7f}},
	"blank":  {{'\t', '\t'}, {' ', ' '}},
	"cntrl":  {{0x00, 0x1f}, {0x7f, 0x7f}},
	"digit":  {{'0', '9'}},
	"graph":  {{0x21, 0x7e}},
	"lower":  {{'a', 'z'}},
	"print":  {{0x20, 0x7e}},
	"punct":  {{0x21, 0x2f}, {0x3a, 0x40}, {0x5b, 0x60}, {0x7b, 0x7e}},
	"space":  {{0x09, 0x0d}, {' ', ' '}},
	"upper":  {{'A', 'Z'}},
	"word":   {{'0', '9'}, {'A', 'Z'}, {'_', '_'}, {'a', 'z'}},
	"xdigit": {{'0', '9'}, {'A', 'F'}, {'a', 'f'}},
}

// derivedProperties are the Unicode binary properties the reference engine
// names that Go does not ship directly, each composed from the tables it does.
var derivedProperties = map[string]func() runeSet{
	"Any":      func() runeSet { return runeSet{{0, unicode.MaxRune}} },
	"Assigned": assignedRunes,
	"ASCII":    func() runeSet { return runeSet{{0, 0x7f}} },
	"Alphabetic": func() runeSet {
		return tablesOf(unicode.L, unicode.Nl, unicode.Other_Alphabetic)
	},
	"Lowercase": func() runeSet { return tablesOf(unicode.Ll, unicode.Other_Lowercase) },
	"Uppercase": func() runeSet { return tablesOf(unicode.Lu, unicode.Other_Uppercase) },
	"Cased": func() runeSet {
		return tablesOf(unicode.Ll, unicode.Lu, unicode.Lt,
			unicode.Other_Lowercase, unicode.Other_Uppercase)
	},
	"Case_Ignorable": func() runeSet {
		return tablesOf(unicode.Mn, unicode.Me, unicode.Cf, unicode.Lm, unicode.Sk).
			union(wordBreakMidLetters)
	},
	"Math":         func() runeSet { return tablesOf(unicode.Sm, unicode.Other_Math) },
	"ID_Start":     identifierStart,
	"ID_Continue":  identifierContinue,
	"XID_Start":    identifierStart,
	"XID_Continue": identifierContinue,
	"Word": func() runeSet {
		return tablesOf(unicode.L, unicode.Nl, unicode.Other_Alphabetic,
			unicode.M, unicode.Nd, unicode.Pc, unicode.Join_Control)
	},
}

// assignedRunes is every code point Unicode has given a general category, the
// complement of the unassigned category Go does not ship as a table.
func assignedRunes() runeSet {
	return tablesOf(unicode.L, unicode.M, unicode.N,
		unicode.P, unicode.S, unicode.Z, unicode.C)
}

func identifierStart() runeSet {
	return tablesOf(unicode.L, unicode.Nl, unicode.Other_ID_Start).
		difference(tablesOf(unicode.Pattern_Syntax, unicode.Pattern_White_Space))
}

func identifierContinue() runeSet {
	return identifierStart().
		union(tablesOf(unicode.Mn, unicode.Mc, unicode.Nd, unicode.Pc,
			unicode.Other_ID_Continue)).
		difference(tablesOf(unicode.Pattern_Syntax, unicode.Pattern_White_Space))
}

// wordBreakMidLetters are the word separators the Case_Ignorable property
// includes alongside the marks and modifiers.
var wordBreakMidLetters = runeSet{
	{'\'', '\''}, {'.', '.'}, {':', ':'}, {'·', '·'}, {'·', '·'},
	{'״', '״'}, {'‘', '‘'}, {'’', '’'},
	{'․', '․'}, {'︓', '︓'}, {'﹒', '﹒'},
	{'﹕', '﹕'}, {'＇', '＇'}, {'．', '．'},
	{'：', '：'},
}

// normalizePropertyName folds a Unicode property or value name the way the
// reference engine's symbolic lookup does: an "is" prefix, spaces, underscores
// and hyphens are all insignificant, and so is case.
func normalizePropertyName(name string) string {
	prefixed := len(name) >= 2 && strings.EqualFold(name[:2], "is")
	if prefixed {
		name = name[2:]
	}
	var normalized strings.Builder
	for _, character := range name {
		switch {
		case character == ' ' || character == '_' || character == '-':
		case character >= 'A' && character <= 'Z':
			normalized.WriteRune(character + ('a' - 'A'))
		case character < 0x80:
			normalized.WriteRune(character)
		}
	}
	// "isc" is a property of its own, so stripping the prefix off it would
	// resolve the wrong one.
	if prefixed && normalized.String() == "c" {
		return "isc"
	}
	return normalized.String()
}

// normalizedTable indexes a Unicode table map by normalized name.
func normalizedTable(tables map[string]*unicode.RangeTable) map[string]*unicode.RangeTable {
	indexed := make(map[string]*unicode.RangeTable, len(tables)*2)
	for name, table := range tables {
		indexed[normalizePropertyName(name)] = table
	}
	return indexed
}

// aliasTable unpacks one of the packed name tables into normalized alias to
// canonical name.
func aliasTable(packed string) map[string]string {
	table := make(map[string]string)
	for _, entry := range strings.Split(packed, ",") {
		canonical, aliases, _ := strings.Cut(entry, "=")
		table[normalizePropertyName(canonical)] = canonical
		for _, alias := range strings.Split(aliases, "|") {
			if alias != "" {
				table[alias] = canonical
			}
		}
	}
	return table
}

var (
	normalizedCategories = normalizedTable(unicode.Categories)
	normalizedScripts    = normalizedTable(unicode.Scripts)
	normalizedProperties = normalizedTable(unicode.Properties)
	normalizedDerived    = func() map[string]func() runeSet {
		indexed := make(map[string]func() runeSet, len(derivedProperties))
		for name, build := range derivedProperties {
			indexed[normalizePropertyName(name)] = build
		}
		return indexed
	}()

	// The reference engine's own name tables, so one spelling resolves to the
	// same property, category or script in every SDK.
	propertyNameAliases   = aliasTable(regexPropertyNames)
	categoryValueAliases  = aliasTable(regexCategoryValues)
	scriptValueAliases    = aliasTable(regexScriptValues)
	namedBinaryProperties = func() map[string]bool {
		named := make(map[string]bool)
		for _, name := range strings.Split(regexBinaryProperties, ",") {
			named[name] = true
		}
		return named
	}()
)

// resolveUnicodeClass is the set a property query names.
//
// A query with no property name resolves as a binary property, then a general
// category, then a script, which is the order the reference engine tries.
func resolveUnicodeClass(name, value string, hasValue bool) (runeSet, error) {
	if !hasValue {
		return resolveBareProperty(name)
	}
	property, named := propertyNameAliases[normalizePropertyName(name)]
	if !named {
		return nil, notFoundProperty(name)
	}
	normalizedValue := normalizePropertyName(value)
	switch property {
	case "General_Category":
		if set, found := lookupCategory(normalizedValue); found {
			return set, nil
		}
	case "Script", "Script_Extensions":
		if property == "Script_Extensions" {
			// Go ships script memberships but not script extensions, and
			// approximating one with the other would quietly match a
			// different set of records than the server.
			return nil, unsupportedProperty("Script_Extensions")
		}
		if set, found := lookupScript(normalizedValue); found {
			return set, nil
		}
	case "Age", "Grapheme_Cluster_Break", "Sentence_Break", "Word_Break", "Block":
		return nil, unsupportedProperty(property)
	default:
		return nil, notFoundProperty(name)
	}
	return nil, &InvalidError{
		Reason: "invalid regex: Unicode property value not found: " + value}
}

// resolveBareProperty resolves a query that names no property, such as
// \p{Greek} or \p{Lu}.
func resolveBareProperty(name string) (runeSet, error) {
	normalized := normalizePropertyName(name)
	// A one-letter query keeps its general category meaning, so a bare sc is
	// the currency symbol category rather than the script property.
	if normalized != "cf" && normalized != "sc" && normalized != "lc" {
		if canonical, named := propertyNameAliases[normalized]; named {
			if !namedBinaryProperties[canonical] {
				// A property that needs a value cannot stand on its own.
				return nil, notFoundProperty(name)
			}
			if set, found := lookupBinaryProperty(normalizePropertyName(canonical)); found {
				return set, nil
			}
			return nil, unsupportedProperty(canonical)
		}
	}
	if set, found := lookupCategory(normalized); found {
		return set, nil
	}
	if set, found := lookupScript(normalized); found {
		return set, nil
	}
	return nil, notFoundProperty(name)
}

func notFoundProperty(name string) error {
	return &InvalidError{Reason: "invalid regex: Unicode property not found: " + name}
}

func unsupportedProperty(name string) error {
	return &InvalidError{Reason: "invalid regex: the Unicode property " + name +
		" is not supported in Go"}
}

func lookupBinaryProperty(normalized string) (runeSet, bool) {
	if build, found := normalizedDerived[normalized]; found {
		return build(), true
	}
	if table, found := normalizedProperties[normalized]; found {
		return rangesOf(table), true
	}
	return nil, false
}

func lookupScript(normalized string) (runeSet, bool) {
	if canonical, named := scriptValueAliases[normalized]; named {
		normalized = normalizePropertyName(canonical)
	}
	if table, found := normalizedScripts[normalized]; found {
		return rangesOf(table), true
	}
	return nil, false
}

// categoryKeys maps every spelling of a general category to the Go table that
// holds it. The reference engine names categories in full ("Letter") where Go
// names them by their abbreviation ("L"), so each entry's aliases bridge the
// two.
var categoryKeys = func() map[string]string {
	keys := make(map[string]string)
	for _, entry := range strings.Split(regexCategoryValues, ",") {
		canonical, aliases, _ := strings.Cut(entry, "=")
		spellings := append([]string{normalizePropertyName(canonical)},
			strings.Split(aliases, "|")...)
		goKey := ""
		for _, spelling := range spellings {
			if table, found := normalizedCategories[spelling]; found {
				for name, candidate := range unicode.Categories {
					if candidate == table {
						goKey = name
						break
					}
				}
				break
			}
		}
		if goKey == "" {
			continue
		}
		for _, spelling := range spellings {
			if spelling != "" {
				keys[spelling] = goKey
			}
		}
	}
	return keys
}()

func lookupCategory(normalized string) (runeSet, bool) {
	switch normalized {
	case "any":
		return runeSet{{0, unicode.MaxRune}}, true
	case "assigned":
		return assignedRunes(), true
	case "ascii":
		return runeSet{{0, 0x7f}}, true
	case "casedletter", "lc":
		return tablesOf(unicode.Lu, unicode.Ll, unicode.Lt), true
	case "unassigned", "cn":
		// Go ships no unassigned table, so it is what no category covers.
		return assignedRunes().negate(), true
	}
	if key, named := categoryKeys[normalized]; named {
		return rangesOf(unicode.Categories[key]), true
	}
	if table, found := normalizedCategories[normalized]; found {
		return rangesOf(table), true
	}
	return nil, false
}
