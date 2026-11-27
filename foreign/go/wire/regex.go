package wire

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// A port of the regex syntax the server compiles filter regexes with, run as a
// Thompson NFA simulation so matching stays linear in the input like the
// server's engine. Go's own regexp refuses some of this syntax and reads the
// Perl classes and word boundaries as ASCII where the contract reads them as
// Unicode, so a filter that compiled on the server would select different
// records through it.

const (
	regexNestLimit = 64
	regexSizeLimit = 256 << 10
	// The size limit bounds the compiled program. These lower bounds on its
	// byte cost are measured against the reference engine, so a pattern the
	// server accepts is never refused here. A large Unicode class costs far
	// more there than this generic class bound, so some patterns pass here and
	// fail on the server.
	regexBaseCost        = 200
	regexLiteralByteCost = 32
	regexClassCost       = 64
	regexClassRangeCost  = 8
	regexRepeatCost      = 40
	regexDotCost         = 999
	// A guard on this engine's own program, above anything the size limit
	// admits.
	maxRegexInstructions = 1 << 16
)

// perlClassCost is what the reference engine's Unicode Perl classes cost.
var perlClassCost = map[rune]int{
	'd': 5136, 'D': 13097, 's': 680, 'S': 1926, 'w': 52388, 'W': 52388,
}

func regexError(reason string) error {
	return &InvalidError{Reason: "invalid regex: " + reason}
}

// UnportableRegexConstruct names the construct that keeps a pattern from being
// portable, or the empty string when there is none.
//
// Inline flags, lookaround, and backreferences have no shared meaning across
// the SDK regex engines, so every SDK refuses them before compiling.
func UnportableRegexConstruct(pattern string) string {
	characters := []rune(pattern)
	inClass := false
	for index := 0; index < len(characters); index++ {
		switch character := characters[index]; {
		case character == '\\':
			if index+1 < len(characters) {
				escaped := characters[index+1]
				if escaped >= '1' && escaped <= '9' {
					return "backreferences"
				}
				if escaped == 'k' {
					return "named backreferences"
				}
			}
			index++
		case character == '[':
			inClass = true
		case character == ']':
			inClass = false
		case character == '(' && !inClass && index+1 < len(characters) &&
			characters[index+1] == '?':
			if index+2 >= len(characters) || characters[index+2] != ':' {
				return "groups starting with (? other than (?:"
			}
			index++
		}
	}
	return ""
}

// compileFilterRegex compiles a text predicate's regular expression.
func compileFilterRegex(pattern string, caseInsensitive bool) (regexProgram, error) {
	if construct := UnportableRegexConstruct(pattern); construct != "" {
		return nil, &InvalidError{Reason: fmt.Sprintf(
			"regex %s is not supported, write the pattern without it", construct)}
	}
	parser := &regexParser{characters: []rune(pattern), caseInsensitive: caseInsensitive}
	root, err := parser.parse()
	if err != nil {
		return nil, err
	}
	if root.depth() > regexNestLimit {
		return nil, regexError(fmt.Sprintf("exceeds the nest limit of %d", regexNestLimit))
	}
	if root.cost()+regexBaseCost > regexSizeLimit {
		return nil, regexError(fmt.Sprintf(
			"compiled regex exceeds size limit of %d bytes", regexSizeLimit))
	}
	compiler := &regexCompiler{}
	if err := compiler.emit(root); err != nil {
		return nil, err
	}
	if _, err := compiler.push(regexInst{op: regexMatch}); err != nil {
		return nil, err
	}
	return &compiledRegex{program: compiler.program, classes: compiler.classes}, nil
}

// regexLook is a zero-width assertion.
type regexLook uint8

const (
	lookStart regexLook = iota
	lookEnd
	lookWord
	lookNotWord
	lookWordStart
	lookWordEnd
	lookWordStartHalf
	lookWordEndHalf
)

// regexNodeKind discriminates a parsed node.
type regexNodeKind uint8

const (
	nodeEmpty regexNodeKind = iota
	nodeAtom
	nodeLook
	nodeConcat
	nodeAlt
	nodeGroup
	nodeRepeat
)

// regexNode is one parsed expression.
type regexNode struct {
	kind  regexNodeKind
	items []regexNode
	look  regexLook
	// atom is the set of code points one character position accepts.
	atom      runeSet
	atomCost  int
	atomDepth int
	min       int
	max       int
	bounded   bool
}

func (node regexNode) depth() int {
	switch node.kind {
	case nodeEmpty, nodeLook:
		return 0
	case nodeAtom:
		return node.atomDepth
	case nodeConcat, nodeAlt:
		deepest := 0
		for _, item := range node.items {
			if child := item.depth(); child > deepest {
				deepest = child
			}
		}
		return 1 + deepest
	default:
		return 1 + node.items[0].depth()
	}
}

func (node regexNode) cost() int {
	switch node.kind {
	case nodeEmpty, nodeLook:
		return 0
	case nodeAtom:
		return node.atomCost
	case nodeConcat:
		total := 0
		for _, item := range node.items {
			total += item.cost()
		}
		return total
	case nodeAlt:
		// The reference engine merges and factors alternatives, so only the
		// largest branch is certain to survive.
		largest := 0
		for _, item := range node.items {
			if child := item.cost(); child > largest {
				largest = child
			}
		}
		return largest
	case nodeGroup:
		return node.items[0].cost()
	default:
		body := node.items[0].cost()
		if body == 0 || (node.bounded && node.max == 0) {
			return 0
		}
		if !node.bounded {
			if node.min > 0 {
				return node.min * body
			}
			return body + regexRepeatCost
		}
		return node.min*body + (node.max-node.min)*(body+regexRepeatCost)
	}
}

// zeroWidth reports whether the node consumes no input, so a repetition of it
// matches the same once as any number of times.
func (node regexNode) zeroWidth() bool {
	switch node.kind {
	case nodeEmpty, nodeLook:
		return true
	case nodeAtom:
		return false
	case nodeConcat, nodeAlt:
		for _, item := range node.items {
			if !item.zeroWidth() {
				return false
			}
		}
		return true
	case nodeGroup:
		return node.items[0].zeroWidth()
	default:
		return (node.bounded && node.max == 0) || node.items[0].zeroWidth()
	}
}

// regexOp is one instruction of the compiled program.
type regexOp uint8

const (
	regexChar regexOp = iota
	regexLookOp
	regexSplit
	regexJump
	regexMatch
)

type regexInst struct {
	op    regexOp
	class int
	look  regexLook
	x, y  int
}

type regexCompiler struct {
	program []regexInst
	classes []runeSet
}

func (compiler *regexCompiler) push(inst regexInst) (int, error) {
	if len(compiler.program) >= maxRegexInstructions {
		return 0, regexError(fmt.Sprintf(
			"compiled regex exceeds size limit of %d bytes", regexSizeLimit))
	}
	compiler.program = append(compiler.program, inst)
	return len(compiler.program) - 1, nil
}

func (compiler *regexCompiler) emit(node regexNode) error {
	switch node.kind {
	case nodeEmpty:
		return nil
	case nodeAtom:
		compiler.classes = append(compiler.classes, node.atom)
		_, err := compiler.push(regexInst{op: regexChar, class: len(compiler.classes) - 1})
		return err
	case nodeLook:
		_, err := compiler.push(regexInst{op: regexLookOp, look: node.look})
		return err
	case nodeConcat:
		for _, item := range node.items {
			if err := compiler.emit(item); err != nil {
				return err
			}
		}
		return nil
	case nodeGroup:
		return compiler.emit(node.items[0])
	case nodeAlt:
		return compiler.emitAlternation(node.items)
	default:
		return compiler.emitRepeat(node)
	}
}

func (compiler *regexCompiler) emitAlternation(items []regexNode) error {
	var exits []int
	for index, item := range items {
		if index == len(items)-1 {
			if err := compiler.emit(item); err != nil {
				return err
			}
			break
		}
		split, err := compiler.push(regexInst{
			op: regexSplit, x: len(compiler.program) + 1, y: -1})
		if err != nil {
			return err
		}
		if err := compiler.emit(item); err != nil {
			return err
		}
		exit, err := compiler.push(regexInst{op: regexJump, x: -1})
		if err != nil {
			return err
		}
		exits = append(exits, exit)
		compiler.program[split].y = len(compiler.program)
	}
	for _, exit := range exits {
		compiler.program[exit].x = len(compiler.program)
	}
	return nil
}

func (compiler *regexCompiler) emitRepeat(node regexNode) error {
	child := node.items[0]
	// An empty-width body matches the same once as any number of times, which
	// keeps a counted repetition of a word boundary small.
	if child.zeroWidth() {
		switch {
		case node.bounded && node.max == 0:
			return nil
		case node.min > 0:
			return compiler.emit(child)
		default:
			return compiler.emitOptional(child)
		}
	}
	for index := 0; index < node.min; index++ {
		if err := compiler.emit(child); err != nil {
			return err
		}
	}
	if !node.bounded {
		loop, err := compiler.push(regexInst{
			op: regexSplit, x: len(compiler.program) + 1, y: -1})
		if err != nil {
			return err
		}
		if err := compiler.emit(child); err != nil {
			return err
		}
		if _, err := compiler.push(regexInst{op: regexJump, x: loop}); err != nil {
			return err
		}
		compiler.program[loop].y = len(compiler.program)
		return nil
	}
	var splits []int
	for index := node.min; index < node.max; index++ {
		split, err := compiler.push(regexInst{
			op: regexSplit, x: len(compiler.program) + 1, y: -1})
		if err != nil {
			return err
		}
		splits = append(splits, split)
		if err := compiler.emit(child); err != nil {
			return err
		}
	}
	for _, split := range splits {
		compiler.program[split].y = len(compiler.program)
	}
	return nil
}

func (compiler *regexCompiler) emitOptional(child regexNode) error {
	split, err := compiler.push(regexInst{op: regexSplit, x: len(compiler.program) + 1, y: -1})
	if err != nil {
		return err
	}
	if err := compiler.emit(child); err != nil {
		return err
	}
	compiler.program[split].y = len(compiler.program)
	return nil
}

// compiledRegex searches a value in time linear in its length.
//
// It holds no mutable state, so one compiled filter may be evaluated
// concurrently.
type compiledRegex struct {
	program []regexInst
	classes []runeSet
}

// MatchString reports whether the value contains a match.
func (regex *compiledRegex) MatchString(value string) bool {
	text := []rune(value)
	marks := make([]int, len(regex.program))
	// A thread is added to a list at most once per position, which is what
	// keeps the simulation linear. The generation marks that position.
	generation := 1
	current := make([]int, 0, len(regex.program))
	next := make([]int, 0, len(regex.program))
	stack := make([]int, 0, len(regex.program))

	// add follows the empty transitions from one instruction, queuing the
	// character steps, and reports reaching the match state.
	add := func(list *[]int, start, at int) bool {
		stack = append(stack[:0], start)
		for len(stack) > 0 {
			counter := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if counter < 0 || counter >= len(regex.program) || marks[counter] == generation {
				continue
			}
			marks[counter] = generation
			switch inst := regex.program[counter]; inst.op {
			case regexChar:
				*list = append(*list, counter)
			case regexLookOp:
				if lookHolds(inst.look, text, at) {
					stack = append(stack, counter+1)
				}
			case regexJump:
				stack = append(stack, inst.x)
			case regexSplit:
				stack = append(stack, inst.y, inst.x)
			case regexMatch:
				return true
			}
		}
		return false
	}

	for at := 0; ; at++ {
		// Seeding a thread at every position is what makes this a search
		// rather than an anchored match, as the reference engine's is_match
		// is. The threads already carried over from the previous position stay
		// in the list alongside it.
		if add(&current, 0, at) {
			return true
		}
		if at == len(text) {
			return false
		}
		generation++
		next = next[:0]
		for _, counter := range current {
			inst := regex.program[counter]
			if regex.classes[inst.class].contains(text[at]) &&
				add(&next, counter+1, at+1) {
				return true
			}
		}
		current, next = next, current
	}
}

func lookHolds(look regexLook, text []rune, at int) bool {
	switch look {
	case lookStart:
		return at == 0
	case lookEnd:
		return at == len(text)
	}
	before := at > 0 && classWord.contains(text[at-1])
	after := at < len(text) && classWord.contains(text[at])
	switch look {
	case lookWord:
		return before != after
	case lookNotWord:
		return before == after
	case lookWordStart:
		return !before && after
	case lookWordEnd:
		return before && !after
	case lookWordStartHalf:
		return !before
	default:
		return !after
	}
}

func utf8Width(character rune) int {
	switch {
	case character < 0x80:
		return 1
	case character < 0x800:
		return 2
	case character < 0x10000:
		return 3
	default:
		return 4
	}
}

// setCost is the lower bound on what the reference engine spends on a class.
func setCost(set runeSet) int {
	if len(set) == 0 {
		return 0
	}
	if len(set) == 1 && set[0].lo == set[0].hi {
		return regexLiteralByteCost * utf8Width(set[0].lo)
	}
	for _, span := range set {
		if span.hi >= 0x80 {
			return regexClassCost
		}
	}
	return regexClassCost + regexClassRangeCost*len(set)
}

// asciiCaseVariants adds the ASCII case variants of a set, which is what the
// reference engine's cost model folds before sizing a class.
func asciiCaseVariants(set runeSet) runeSet {
	variants := append([]runeRange{}, set...)
	for _, span := range set {
		low, high := span.lo, span.hi
		if low < 'A' {
			low = 'A'
		}
		if high > 'z' {
			high = 'z'
		}
		for character := low; character <= high; character++ {
			switch {
			case character <= 'Z':
				variants = append(variants, runeRange{character + 0x20, character + 0x20})
			case character >= 'a':
				variants = append(variants, runeRange{character - 0x20, character - 0x20})
			}
		}
	}
	return newRuneSet(variants)
}

// regexParser reads the reference engine's syntax.
type regexParser struct {
	characters      []rune
	index           int
	caseInsensitive bool
}

func (parser *regexParser) eof() bool { return parser.index >= len(parser.characters) }

// current is the character under the cursor, and zero at the end of the
// pattern.
func (parser *regexParser) current() rune {
	if parser.eof() {
		return 0
	}
	return parser.characters[parser.index]
}

func (parser *regexParser) peek() rune {
	if parser.index+1 >= len(parser.characters) {
		return 0
	}
	return parser.characters[parser.index+1]
}

type regexGroup struct {
	concat     []regexNode
	alternates []regexNode
	open       bool
}

func (parser *regexParser) parse() (regexNode, error) {
	var groups []regexGroup
	var concat []regexNode
	var alternates []regexNode
	haveAlternates := false

	for !parser.eof() {
		switch parser.current() {
		case '(':
			parser.index++
			if parser.current() == '?' {
				if parser.peek() != ':' {
					return regexNode{}, regexError(
						"groups starting with (? other than (?: are not supported")
				}
				parser.index += 2
			}
			groups = append(groups, regexGroup{concat, alternates, haveAlternates})
			concat, alternates, haveAlternates = nil, nil, false
		case ')':
			if len(groups) == 0 {
				return regexNode{}, regexError("unopened group")
			}
			child := finishBranch(concat, alternates, haveAlternates)
			outer := groups[len(groups)-1]
			groups = groups[:len(groups)-1]
			concat, alternates, haveAlternates = outer.concat, outer.alternates, outer.open
			concat = append(concat, regexNode{kind: nodeGroup, items: []regexNode{child}})
			parser.index++
		case '|':
			alternates = append(alternates, intoConcat(concat))
			haveAlternates = true
			concat = nil
			parser.index++
		case '[':
			class, err := parser.parseClass()
			if err != nil {
				return regexNode{}, err
			}
			concat = append(concat, class)
		case '?':
			repeated, err := parser.repeat(concat, 0, 1, true)
			if err != nil {
				return regexNode{}, err
			}
			concat = repeated
		case '*':
			repeated, err := parser.repeat(concat, 0, 0, false)
			if err != nil {
				return regexNode{}, err
			}
			concat = repeated
		case '+':
			repeated, err := parser.repeat(concat, 1, 0, false)
			if err != nil {
				return regexNode{}, err
			}
			concat = repeated
		case '{':
			repeated, err := parser.countedRepeat(concat)
			if err != nil {
				return regexNode{}, err
			}
			concat = repeated
		default:
			primitive, err := parser.parsePrimitive()
			if err != nil {
				return regexNode{}, err
			}
			concat = append(concat, primitive)
		}
	}
	if len(groups) > 0 {
		return regexNode{}, regexError("unclosed group")
	}
	return finishBranch(concat, alternates, haveAlternates), nil
}

func intoConcat(items []regexNode) regexNode {
	switch len(items) {
	case 0:
		return regexNode{kind: nodeEmpty}
	case 1:
		return items[0]
	default:
		return regexNode{kind: nodeConcat, items: items}
	}
}

func finishBranch(concat, alternates []regexNode, haveAlternates bool) regexNode {
	if !haveAlternates {
		return intoConcat(concat)
	}
	return regexNode{kind: nodeAlt, items: append(alternates, intoConcat(concat))}
}

func (parser *regexParser) repeat(
	concat []regexNode, min, max int, bounded bool,
) ([]regexNode, error) {
	if len(concat) == 0 {
		return nil, regexError("repetition operator missing expression")
	}
	child := concat[len(concat)-1]
	parser.index++
	// A lazy repetition matches the same set of values as a greedy one, and
	// only the verdict matters here.
	if parser.current() == '?' {
		parser.index++
	}
	return append(concat[:len(concat)-1], regexNode{
		kind: nodeRepeat, items: []regexNode{child}, min: min, max: max, bounded: bounded,
	}), nil
}

func (parser *regexParser) countedRepeat(concat []regexNode) ([]regexNode, error) {
	if len(concat) == 0 {
		return nil, regexError("repetition operator missing expression")
	}
	child := concat[len(concat)-1]
	unclosed := regexError("unclosed counted repetition")
	parser.index++
	if parser.eof() {
		return nil, unclosed
	}
	start, err := parser.decimal()
	if err != nil {
		return nil, err
	}
	if parser.eof() {
		return nil, unclosed
	}
	min, max, bounded := start, start, true
	if parser.current() == ',' {
		parser.index++
		if parser.eof() {
			return nil, unclosed
		}
		if parser.current() == '}' {
			bounded = false
			max = 0
		} else {
			max, err = parser.decimal()
			if err != nil {
				return nil, err
			}
		}
	}
	if parser.current() != '}' {
		return nil, unclosed
	}
	parser.index++
	if parser.current() == '?' {
		parser.index++
	}
	if bounded && min > max {
		return nil, regexError("invalid repetition count range")
	}
	return append(concat[:len(concat)-1], regexNode{
		kind: nodeRepeat, items: []regexNode{child}, min: min, max: max, bounded: bounded,
	}), nil
}

// decimal reads a repetition count, which may be surrounded by whitespace.
func (parser *regexParser) decimal() (int, error) {
	for !parser.eof() && unicode.IsSpace(parser.current()) {
		parser.index++
	}
	var digits strings.Builder
	for !parser.eof() && parser.current() >= '0' && parser.current() <= '9' {
		digits.WriteRune(parser.current())
		parser.index++
	}
	for !parser.eof() && unicode.IsSpace(parser.current()) {
		parser.index++
	}
	if digits.Len() == 0 {
		return 0, regexError("repetition quantifier expects a valid decimal")
	}
	value, err := strconv.ParseUint(digits.String(), 10, 64)
	if err != nil || value > 0xffff_ffff {
		return 0, regexError("decimal literal invalid")
	}
	return int(value), nil
}
