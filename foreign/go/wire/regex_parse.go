package wire

import (
	"strconv"
	"strings"
	"unicode"
)

// regexEscapeKind discriminates what an escape sequence resolved to.
type regexEscapeKind uint8

const (
	escapeLiteral regexEscapeKind = iota
	escapeClass
	escapeLook
)

type regexEscape struct {
	kind    regexEscapeKind
	literal rune
	class   runeSet
	cost    int
	look    regexLook
}

// atomNode builds a single-character node from a set of accepted code points.
func (parser *regexParser) atomNode(set runeSet, cost, depth int) regexNode {
	return regexNode{kind: nodeAtom, atom: set, atomCost: cost, atomDepth: depth}
}

// folded applies simple case folding when the pattern ignores case.
//
// Folding happens before any negation, so a negated class under a
// case-insensitive pattern excludes both cases rather than admitting the
// other one.
func (parser *regexParser) folded(set runeSet) runeSet {
	if parser.caseInsensitive {
		return set.caseFold()
	}
	return set
}

func (parser *regexParser) literalNode(character rune) regexNode {
	one := runeSet{{character, character}}
	cost := regexLiteralByteCost * utf8Width(character)
	if parser.caseInsensitive {
		cost = setCost(asciiCaseVariants(one))
	}
	return parser.atomNode(parser.folded(one), cost, 0)
}

func (parser *regexParser) parsePrimitive() (regexNode, error) {
	character := parser.current()
	if character == '\\' {
		escape, err := parser.parseEscape()
		if err != nil {
			return regexNode{}, err
		}
		switch escape.kind {
		case escapeLiteral:
			return parser.literalNode(escape.literal), nil
		case escapeLook:
			return regexNode{kind: nodeLook, look: escape.look}, nil
		default:
			return parser.atomNode(escape.class, escape.cost, 0), nil
		}
	}
	switch character {
	case '.':
		parser.index++
		// A dot is every character but a newline: the filter regexes are not
		// compiled with the dot-matches-newline flag.
		newline := runeSet{{'\n', '\n'}}
		return parser.atomNode(parser.folded(newline).negate(), regexDotCost, 0), nil
	case '^':
		parser.index++
		return regexNode{kind: nodeLook, look: lookStart}, nil
	case '$':
		parser.index++
		return regexNode{kind: nodeLook, look: lookEnd}, nil
	}
	parser.index++
	return parser.literalNode(character), nil
}

func (parser *regexParser) parseEscape() (regexEscape, error) {
	parser.index++
	if parser.eof() {
		return regexEscape{}, regexError("incomplete escape sequence")
	}
	character := parser.current()
	switch {
	case character >= '0' && character <= '9':
		return regexEscape{}, regexError("backreferences are not supported")
	case character == 'x' || character == 'u' || character == 'U':
		return parser.parseHexEscape()
	case character == 'p' || character == 'P':
		return parser.parseUnicodeClass()
	case strings.ContainsRune("dswDSW", character):
		parser.index++
		lower := unicode.ToLower(character)
		var base runeSet
		switch lower {
		case 'd':
			base = classDigit
		case 's':
			base = classSpace
		default:
			base = classWord
		}
		base = parser.folded(base)
		if character != lower {
			base = base.negate()
		}
		cost, named := perlClassCost[character]
		if !named {
			cost = regexClassCost
		}
		return regexEscape{kind: escapeClass, class: base, cost: cost}, nil
	}

	parser.index++
	if isRegexEscapeable(character) {
		return regexEscape{kind: escapeLiteral, literal: character}, nil
	}
	switch character {
	case 'a':
		return regexEscape{kind: escapeLiteral, literal: 0x07}, nil
	case 'f':
		return regexEscape{kind: escapeLiteral, literal: 0x0c}, nil
	case 't':
		return regexEscape{kind: escapeLiteral, literal: '\t'}, nil
	case 'n':
		return regexEscape{kind: escapeLiteral, literal: '\n'}, nil
	case 'r':
		return regexEscape{kind: escapeLiteral, literal: '\r'}, nil
	case 'v':
		return regexEscape{kind: escapeLiteral, literal: 0x0b}, nil
	case 'A':
		return regexEscape{kind: escapeLook, look: lookStart}, nil
	case 'z':
		return regexEscape{kind: escapeLook, look: lookEnd}, nil
	case 'b':
		if parser.current() == '{' {
			look, err := parser.specialWordBoundary()
			return regexEscape{kind: escapeLook, look: look}, err
		}
		return regexEscape{kind: escapeLook, look: lookWord}, nil
	case 'B':
		return regexEscape{kind: escapeLook, look: lookNotWord}, nil
	case '<':
		return regexEscape{kind: escapeLook, look: lookWordStart}, nil
	case '>':
		return regexEscape{kind: escapeLook, look: lookWordEnd}, nil
	}
	return regexEscape{}, regexError("unrecognized escape sequence")
}

// specialWordBoundary reads the named boundaries. Anything that cannot be one,
// such as a counted repetition of a word boundary, is left for the repetition
// parser.
func (parser *regexParser) specialWordBoundary() (regexLook, error) {
	start := parser.index
	parser.index++
	if parser.eof() {
		return lookWord, regexError(
			"unexpected end of a special word boundary or repetition")
	}
	nameable := func(character rune) bool {
		return character == '-' ||
			(character >= 'A' && character <= 'Z') ||
			(character >= 'a' && character <= 'z')
	}
	if !nameable(parser.current()) {
		parser.index = start
		return lookWord, nil
	}
	var name strings.Builder
	for !parser.eof() && nameable(parser.current()) {
		name.WriteRune(parser.current())
		parser.index++
	}
	if parser.current() != '}' {
		return lookWord, regexError("unclosed special word boundary")
	}
	parser.index++
	switch name.String() {
	case "start":
		return lookWordStart, nil
	case "end":
		return lookWordEnd, nil
	case "start-half":
		return lookWordStartHalf, nil
	case "end-half":
		return lookWordEndHalf, nil
	}
	return lookWord, regexError("unrecognized special word boundary")
}

func (parser *regexParser) parseHexEscape() (regexEscape, error) {
	digits := 8
	switch parser.current() {
	case 'x':
		digits = 2
	case 'u':
		digits = 4
	}
	parser.index++
	if parser.eof() {
		return regexEscape{}, regexError("incomplete escape sequence")
	}
	var text strings.Builder
	if parser.current() == '{' {
		parser.index++
		for !parser.eof() && parser.current() != '}' {
			if !isHexDigit(parser.current()) {
				return regexEscape{}, regexError(
					"hexadecimal literal is not a hexadecimal digit")
			}
			text.WriteRune(parser.current())
			parser.index++
		}
		if parser.eof() {
			return regexEscape{}, regexError("incomplete escape sequence")
		}
		parser.index++
		if text.Len() == 0 {
			return regexEscape{}, regexError("hexadecimal literal empty")
		}
	} else {
		for count := 0; count < digits; count++ {
			if parser.eof() {
				return regexEscape{}, regexError("incomplete escape sequence")
			}
			if !isHexDigit(parser.current()) {
				return regexEscape{}, regexError(
					"hexadecimal literal is not a hexadecimal digit")
			}
			text.WriteRune(parser.current())
			parser.index++
		}
	}
	notScalar := regexError(
		"hexadecimal literal does not correspond to a Unicode scalar value")
	significant := strings.TrimLeft(text.String(), "0")
	if significant == "" {
		significant = "0"
	}
	// More digits than a scalar value can hold overflows before it is parsed.
	if len(significant) > 8 {
		return regexEscape{}, notScalar
	}
	value, err := strconv.ParseUint(significant, 16, 64)
	if err != nil || value > unicode.MaxRune {
		return regexEscape{}, notScalar
	}
	character := rune(value)
	// A surrogate half is not a scalar value, so it never names a character.
	if character >= 0xd800 && character <= 0xdfff {
		return regexEscape{}, notScalar
	}
	return regexEscape{kind: escapeLiteral, literal: character}, nil
}

func (parser *regexParser) parseUnicodeClass() (regexEscape, error) {
	negated := parser.current() == 'P'
	parser.index++
	if parser.eof() {
		return regexEscape{}, regexError("incomplete escape sequence")
	}
	var set runeSet
	var err error
	if parser.current() == '{' {
		var name strings.Builder
		parser.index++
		for !parser.eof() && parser.current() != '}' {
			name.WriteRune(parser.current())
			parser.index++
		}
		if parser.eof() {
			return regexEscape{}, regexError("incomplete escape sequence")
		}
		parser.index++
		query := name.String()
		switch {
		case strings.Contains(query, "!="):
			// An inequality query negates whatever the escape already said.
			negated = !negated
			parts := strings.SplitN(query, "!=", 2)
			set, err = resolveUnicodeClass(parts[0], parts[1], true)
		case strings.Contains(query, ":"):
			parts := strings.SplitN(query, ":", 2)
			set, err = resolveUnicodeClass(parts[0], parts[1], true)
		case strings.Contains(query, "="):
			parts := strings.SplitN(query, "=", 2)
			set, err = resolveUnicodeClass(parts[0], parts[1], true)
		default:
			set, err = resolveUnicodeClass(query, "", false)
		}
	} else {
		letter := parser.current()
		if letter == '\\' {
			return regexEscape{}, regexError("invalid Unicode character class")
		}
		parser.index++
		set, err = resolveUnicodeClass(string(letter), "", false)
	}
	if err != nil {
		return regexEscape{}, err
	}
	set = parser.folded(set)
	if negated {
		set = set.negate()
	}
	return regexEscape{kind: escapeClass, class: set, cost: regexClassCost}, nil
}

// classNesting is one parsed bracketed class.
//
// The cost model needs the members as written, before folding and negation,
// because that is the shape the reference engine sizes a class from.
type classNesting struct {
	set   runeSet
	depth int
	// plain is the members as written, and isPlain says whether the class is
	// made only of literals, ranges and POSIX classes, whose exact cost is
	// known. A Unicode class, a Perl class, or a negation makes it opaque.
	plain   runeSet
	isPlain bool
	// perlCost is the cost of the one Perl class this bracket holds, and zero
	// when it holds anything else.
	perlCost int
}

func (parser *regexParser) parseClass() (regexNode, error) {
	bracketed, err := parser.parseBracketed()
	if err != nil {
		return regexNode{}, err
	}
	// An opaque class can still hold a single character, the cheapest program,
	// so it is sized at that lower bound rather than refused.
	cost := regexLiteralByteCost
	switch {
	case bracketed.isPlain:
		plain := bracketed.plain
		if parser.caseInsensitive {
			plain = asciiCaseVariants(plain)
		}
		cost = setCost(plain)
	case bracketed.perlCost > 0:
		cost = bracketed.perlCost
	}
	return parser.atomNode(bracketed.set, cost, bracketed.depth+1), nil
}

// parseBracketed reads from an opening bracket through its matching close.
func (parser *regexParser) parseBracketed() (classNesting, error) {
	unclosed := regexError("unclosed character class")
	parser.index++
	if parser.eof() {
		return classNesting{}, unclosed
	}
	negated := false
	if parser.current() == '^' {
		negated = true
		parser.index++
		if parser.eof() {
			return classNesting{}, unclosed
		}
	}

	builder := classBuilder{isPlain: true}
	// A leading hyphen or close bracket is a literal, not an operator.
	for parser.current() == '-' {
		builder.addPlain(runeSet{{'-', '-'}})
		parser.index++
		if parser.eof() {
			return classNesting{}, unclosed
		}
	}
	// A close bracket in the first position is a literal too.
	if builder.members == 0 && parser.current() == ']' {
		builder.addPlain(runeSet{{']', ']'}})
		parser.index++
		if parser.eof() {
			return classNesting{}, unclosed
		}
	}

	for {
		if parser.eof() {
			return classNesting{}, unclosed
		}
		character := parser.current()
		switch {
		case character == '[':
			if posix, found := parser.asciiClass(); found {
				builder.addPosix(posix)
				continue
			}
			nested, err := parser.parseBracketed()
			if err != nil {
				return classNesting{}, err
			}
			builder.addNested(nested)
			continue
		case character == ']':
			parser.index++
			return builder.finish(parser, negated), nil
		}
		if pair := string(character) + string(parser.peek()); pair == "&&" ||
			pair == "--" || pair == "~~" {
			parser.index += 2
			builder.operate(pair)
			continue
		}
		member, err := parser.classRange()
		if err != nil {
			return classNesting{}, err
		}
		builder.addMember(member.set, member.perlCost, member.plain)
	}
}

// classBuilder accumulates one bracketed class, including its set operations.
//
// Each operation nests the set one level deeper, which the pattern's nest
// limit counts.
type classBuilder struct {
	union      runeSet
	unionPlain runeSet
	unionDepth int

	left      runeSet
	leftPlain runeSet
	leftDepth int
	operator  string

	isPlain  bool
	perlCost int
	members  int
	depth    int
}

func (builder *classBuilder) addPlain(set runeSet) {
	builder.addMember(set, 0, true)
}

func (builder *classBuilder) addPosix(set runeSet) {
	// A POSIX class lists plain ASCII ranges, so it costs what they cost.
	builder.addMember(set, 0, true)
}

func (builder *classBuilder) addMember(set runeSet, perlCost int, plain bool) {
	builder.union = builder.union.union(set)
	if plain {
		builder.unionPlain = builder.unionPlain.union(set)
	} else {
		builder.isPlain = false
	}
	if builder.members == 0 {
		builder.perlCost = perlCost
	} else {
		builder.perlCost = 0
	}
	builder.members++
}

func (builder *classBuilder) addNested(nested classNesting) {
	builder.union = builder.union.union(nested.set)
	if nested.isPlain {
		builder.unionPlain = builder.unionPlain.union(nested.plain)
	} else {
		builder.isPlain = false
	}
	if nested.depth+1 > builder.unionDepth {
		builder.unionDepth = nested.depth + 1
	}
	builder.perlCost = 0
	builder.members++
}

// operate closes the current union as the left side of a set operation.
func (builder *classBuilder) operate(operator string) {
	left, plain := builder.combined()
	depth := builder.unionDepth
	if builder.operator != "" {
		if builder.leftDepth > depth {
			depth = builder.leftDepth
		}
		depth++
	}
	builder.left, builder.leftPlain, builder.leftDepth = left, plain, depth
	builder.operator = operator
	builder.union, builder.unionPlain, builder.unionDepth = nil, nil, 0
	builder.perlCost = 0
	builder.members = 0
}

// combined applies the pending operation to the current union.
func (builder *classBuilder) combined() (runeSet, runeSet) {
	if builder.operator == "" {
		return builder.union, builder.unionPlain
	}
	switch builder.operator {
	case "&&":
		return builder.left.intersect(builder.union),
			builder.leftPlain.intersect(builder.unionPlain)
	case "--":
		return builder.left.difference(builder.union),
			builder.leftPlain.difference(builder.unionPlain)
	default:
		return builder.left.symmetricDifference(builder.union),
			builder.leftPlain.symmetricDifference(builder.unionPlain)
	}
}

func (builder *classBuilder) finish(parser *regexParser, negated bool) classNesting {
	set, plain := builder.combined()
	depth := builder.unionDepth
	if builder.operator != "" {
		if builder.leftDepth > depth {
			depth = builder.leftDepth
		}
		depth++
	}
	folded := parser.folded(set)
	if negated {
		folded = folded.negate()
	}
	return classNesting{
		set:   folded,
		depth: depth,
		plain: plain,
		// A negated class has no exact cost, since the reference engine sizes
		// the complement rather than what was written.
		isPlain:  builder.isPlain && !negated,
		perlCost: perlCostOrZero(builder, negated),
	}
}

func perlCostOrZero(builder *classBuilder, negated bool) int {
	if negated || builder.operator != "" || builder.members != 1 {
		return 0
	}
	return builder.perlCost
}

// classMemberSet is one member of a bracketed class, with what the cost model
// needs to know about it.
type classMemberSet struct {
	set runeSet
	// plain is true for a literal, a range, or a POSIX class, whose exact cost
	// is known. A Unicode or Perl class makes a class opaque.
	plain    bool
	perlCost int
}

// classRange reads one member of a class: a character, an escape, or a span.
func (parser *regexParser) classRange() (classMemberSet, error) {
	first, err := parser.classPrimitive()
	if err != nil {
		return classMemberSet{}, err
	}
	if parser.eof() {
		return classMemberSet{}, regexError("unclosed character class")
	}
	if parser.current() != '-' || parser.peek() == ']' || parser.peek() == '-' {
		set, err := classItemSet(first)
		if err != nil {
			return classMemberSet{}, err
		}
		perlCost := 0
		if first.kind == escapeClass {
			perlCost = first.cost
		}
		return classMemberSet{
			set:      set,
			plain:    first.kind == escapeLiteral,
			perlCost: perlCost,
		}, nil
	}
	parser.index++
	if parser.eof() {
		return classMemberSet{}, regexError("unclosed character class")
	}
	second, err := parser.classPrimitive()
	if err != nil {
		return classMemberSet{}, err
	}
	if first.kind != escapeLiteral || second.kind != escapeLiteral {
		return classMemberSet{}, regexError("invalid range boundary, must be a literal")
	}
	if first.literal > second.literal {
		return classMemberSet{}, regexError("invalid character class range")
	}
	return classMemberSet{
		set:   runeSet{{first.literal, second.literal}},
		plain: true,
	}, nil
}

func (parser *regexParser) classPrimitive() (regexEscape, error) {
	if parser.current() == '\\' {
		return parser.parseEscape()
	}
	character := parser.current()
	parser.index++
	return regexEscape{kind: escapeLiteral, literal: character}, nil
}

func classItemSet(escape regexEscape) (runeSet, error) {
	switch escape.kind {
	case escapeLiteral:
		return runeSet{{escape.literal, escape.literal}}, nil
	case escapeClass:
		return escape.class, nil
	default:
		return nil, regexError("escape sequence is not valid in a character class")
	}
}

// asciiClass reads a POSIX bracket class. Anything else leaves the parser
// where it was, so the bracket opens a nested class instead.
func (parser *regexParser) asciiClass() (runeSet, bool) {
	start := parser.index
	set, found := parser.asciiClassItem()
	if !found {
		parser.index = start
	}
	return set, found
}

func (parser *regexParser) asciiClassItem() (runeSet, bool) {
	parser.index++
	if parser.eof() || parser.current() != ':' {
		return nil, false
	}
	parser.index++
	if parser.eof() {
		return nil, false
	}
	negated := false
	if parser.current() == '^' {
		negated = true
		parser.index++
		if parser.eof() {
			return nil, false
		}
	}
	nameStart := parser.index
	for !parser.eof() && parser.current() != ':' {
		parser.index++
	}
	if parser.eof() || parser.peek() != ']' {
		return nil, false
	}
	name := string(parser.characters[nameStart:parser.index])
	parser.index += 2
	set, named := posixClasses[name]
	if !named {
		parser.index = nameStart
		return nil, false
	}
	if negated {
		return set.negate(), true
	}
	return set, true
}

func isHexDigit(character rune) bool {
	return (character >= '0' && character <= '9') ||
		(character >= 'a' && character <= 'f') ||
		(character >= 'A' && character <= 'F')
}

// isRegexEscapeable reports whether a character escapes to itself. Any ASCII
// character other than a letter, a digit, and the angle brackets may.
func isRegexEscapeable(character rune) bool {
	switch {
	case character >= 0x80:
		return false
	case character >= '0' && character <= '9':
		return false
	case character >= 'A' && character <= 'Z':
		return false
	case character >= 'a' && character <= 'z':
		return false
	case character == '<' || character == '>':
		return false
	default:
		return true
	}
}
