import { InvalidError } from "../client/errors.js"
import {
  BINARY_PROPERTIES,
  GENERAL_CATEGORY_VALUES,
  PROPERTY_NAMES,
  SCRIPT_VALUES
} from "./regex-unicode.js"

// A port of the Rust `regex` syntax (regex-syntax 0.8) the server compiles
// filter regexes with, run as a Thompson NFA simulation so matching stays
// linear in the input like the Rust engine. Each character class is a one
// character JavaScript regex with the `v` flag, tested one code point at a
// time, so V8 never backtracks over the input. V8 supplies the Unicode
// properties and simple case folding, and its tables can differ from the
// Unicode version regex-syntax was built with.

const NEST_LIMIT = 64
const SIZE_LIMIT = 256 << 10
// The Rust size limit bounds the compiled NFA. These lower bounds on its byte
// cost were measured against regex 1.13.1, so a pattern the server accepts is
// never refused here. Large Unicode classes cost far more in Rust than the
// generic class bound, so some patterns pass here and fail on the server.
const BASE_COST = 200
const LITERAL_BYTE_COST = 32
const CLASS_COST = 64
const CLASS_RANGE_COST = 8
const REPEAT_COST = 40
const DOT_COST = 999
const PERL_COST: Readonly<Record<string, number>> = {
  d: 5136,
  D: 13097,
  s: 680,
  S: 1926,
  w: 52388,
  W: 52388
}
// A guard on this engine's own program, above anything the size limit admits.
const MAX_INSTRUCTIONS = 1 << 16
const MAX_SCALAR = 0x10ffff

const WORD_SOURCE = "\\p{Alphabetic}\\p{M}\\p{Nd}\\p{Pc}\\p{Join_Control}"
const WORD = new RegExp(`^[${WORD_SOURCE}]$`, "v")
const WHITE_SPACE = new RegExp("^\\p{White_Space}$", "v")
const ASCII_CLASSES: Readonly<Record<string, readonly (readonly [number, number])[]>> = {
  alnum: [
    [0x30, 0x39],
    [0x41, 0x5a],
    [0x61, 0x7a]
  ],
  alpha: [
    [0x41, 0x5a],
    [0x61, 0x7a]
  ],
  ascii: [[0x00, 0x7f]],
  blank: [
    [0x09, 0x09],
    [0x20, 0x20]
  ],
  cntrl: [
    [0x00, 0x1f],
    [0x7f, 0x7f]
  ],
  digit: [[0x30, 0x39]],
  graph: [[0x21, 0x7e]],
  lower: [[0x61, 0x7a]],
  print: [[0x20, 0x7e]],
  punct: [
    [0x21, 0x2f],
    [0x3a, 0x40],
    [0x5b, 0x60],
    [0x7b, 0x7e]
  ],
  space: [
    [0x09, 0x0d],
    [0x20, 0x20]
  ],
  upper: [[0x41, 0x5a]],
  word: [
    [0x30, 0x39],
    [0x41, 0x5a],
    [0x5f, 0x5f],
    [0x61, 0x7a]
  ],
  xdigit: [
    [0x30, 0x39],
    [0x41, 0x46],
    [0x61, 0x66]
  ]
}

type Look =
  | "start"
  | "end"
  | "word"
  | "not_word"
  | "word_start"
  | "word_end"
  | "word_start_half"
  | "word_end_half"

type Range = readonly [number, number]

// One character class item, kept in the shape regex-syntax builds so nesting
// depth counts the same way.
type ClassItem =
  | { readonly kind: "empty" }
  | { readonly kind: "literal"; readonly value: number }
  | { readonly kind: "range"; readonly from: number; readonly to: number }
  | { readonly kind: "primitive"; readonly source: string; readonly ascii?: readonly Range[] }
  | { readonly kind: "bracketed"; readonly negated: boolean; readonly set: ClassSet }
  | { readonly kind: "union"; readonly items: readonly ClassItem[] }

type ClassSet =
  | { readonly kind: "item"; readonly item: ClassItem }
  | {
      readonly kind: "op"
      readonly op: "&&" | "--" | "~~"
      readonly lhs: ClassSet
      readonly rhs: ClassSet
    }

interface Atom {
  readonly source: string
  readonly cost: number
  readonly depth: number
  // A case-sensitive literal compares directly.
  readonly literal?: string
}

type Node =
  | { readonly kind: "empty" }
  | { readonly kind: "atom"; readonly atom: Atom }
  | { readonly kind: "look"; readonly look: Look }
  | { readonly kind: "concat"; readonly items: readonly Node[] }
  | { readonly kind: "alt"; readonly items: readonly Node[] }
  | { readonly kind: "group"; readonly child: Node }
  | {
      readonly kind: "repeat"
      readonly min: number
      readonly max: number | undefined
      readonly child: Node
    }

type Escape =
  | { readonly kind: "literal"; readonly value: number }
  | { readonly kind: "primitive"; readonly item: ClassItem; readonly cost: number }
  | { readonly kind: "look"; readonly look: Look }

type Inst =
  | { readonly op: "char"; readonly atom: number }
  | { readonly op: "look"; readonly look: Look }
  | { op: "split"; x: number; y: number }
  | { op: "jmp"; x: number }
  | { readonly op: "match" }

function invalid(reason: string): InvalidError {
  return new InvalidError(`invalid regex: ${reason}`)
}

/**
 * Inline flags, lookaround, and backreferences have no shared meaning across
 * the SDK regex engines, so every SDK refuses them before compiling.
 */
export function unportableRegexConstruct(pattern: string): string | undefined {
  const characters = Array.from(pattern)
  let inClass = false
  for (let index = 0; index < characters.length; index += 1) {
    const character = characters[index]
    if (character === "\\") {
      const escaped = characters[index + 1] ?? ""
      if (escaped >= "1" && escaped <= "9") return "backreferences"
      if (escaped === "k") return "named backreferences"
      index += 1
    } else if (character === "[") inClass = true
    else if (character === "]") inClass = false
    else if (character === "(" && !inClass && characters[index + 1] === "?") {
      if (characters[index + 2] !== ":") return "groups starting with (? other than (?:"
      index += 1
    }
  }
  return undefined
}

/** A regex compiled with the server's syntax and bounds, matched in linear time. */
export class RustRegex {
  private constructor(
    private readonly program: readonly Inst[],
    private readonly atoms: readonly AtomMatcher[]
  ) {}

  /** Refuses what the Rust engine refuses, with its portability rules first. */
  static compile(pattern: string, caseInsensitive: boolean): RustRegex {
    const construct = unportableRegexConstruct(pattern)
    if (construct !== undefined) {
      throw new InvalidError(`regex ${construct} is not supported, write the pattern without it`)
    }
    const root = new Parser(pattern, caseInsensitive).parse()
    if (depth(root) > NEST_LIMIT) {
      throw invalid(`exceeds the nest limit of ${String(NEST_LIMIT)}`)
    }
    if (cost(root) + BASE_COST > SIZE_LIMIT) {
      throw invalid(`compiled regex exceeds size limit of ${String(SIZE_LIMIT)} bytes`)
    }
    const compiler = new Compiler(caseInsensitive)
    compiler.emit(root)
    compiler.push({ op: "match" })
    return new RustRegex(compiler.program, compiler.atoms)
  }

  /** True when the value contains a match, like Rust `Regex::is_match`. */
  matches(value: string): boolean {
    const text = Array.from(value)
    const program = this.program
    const marks = new Uint32Array(program.length)
    const stack: number[] = []
    let generation = 1
    let current: number[] = []
    let next: number[] = []
    const holds = (look: Look, at: number): boolean => {
      if (look === "start") return at === 0
      if (look === "end") return at === text.length
      const before = at > 0 && isWord(text[at - 1])
      const after = at < text.length && isWord(text[at])
      switch (look) {
        case "word":
          return before !== after
        case "not_word":
          return before === after
        case "word_start":
          return !before && after
        case "word_end":
          return before && !after
        case "word_start_half":
          return !before
        case "word_end_half":
          return !after
      }
    }
    // Follows empty transitions from `start`, queuing character steps.
    // Returns true on reaching the match state.
    const add = (list: number[], start: number, at: number): boolean => {
      stack.push(start)
      for (let pc = stack.pop(); pc !== undefined; pc = stack.pop()) {
        const inst = program[pc]
        if (inst === undefined || marks[pc] === generation) continue
        marks[pc] = generation
        switch (inst.op) {
          case "char":
            list.push(pc)
            break
          case "look":
            if (holds(inst.look, at)) stack.push(pc + 1)
            break
          case "jmp":
            stack.push(inst.x)
            break
          case "split":
            stack.push(inst.y, inst.x)
            break
          case "match":
            stack.length = 0
            return true
        }
      }
      return false
    }
    for (let at = 0; ; at += 1) {
      if (add(current, 0, at)) return true
      if (at === text.length) return false
      const character = text[at] ?? ""
      generation += 1
      next.length = 0
      for (const pc of current) {
        const inst = program[pc]
        const atom = inst?.op === "char" ? this.atoms[inst.atom] : undefined
        if (atom?.test(character) === true && add(next, pc + 1, at + 1)) return true
      }
      ;[current, next] = [next, current]
    }
  }
}

class AtomMatcher {
  private readonly ascii = new Uint8Array(128)
  private readonly regex: RegExp | undefined

  constructor(
    private readonly literal: string | undefined,
    source: string,
    caseInsensitive: boolean
  ) {
    if (literal === undefined) {
      try {
        this.regex = new RegExp(`^[${source}]$`, caseInsensitive ? "iv" : "v")
      } catch {
        throw invalid("a character class is not supported by this JavaScript runtime")
      }
    }
  }

  test(character: string): boolean {
    if (this.literal !== undefined) return character === this.literal
    const point = character.codePointAt(0) ?? 0
    if (this.regex === undefined) return false
    if (point >= 128) return this.regex.test(character)
    const cached = this.ascii[point]
    if (cached !== 0) return cached === 2
    const result = this.regex.test(character)
    this.ascii[point] = result ? 2 : 1
    return result
  }
}

const WORD_ASCII = new Uint8Array(128)
function isWord(character: string | undefined): boolean {
  if (character === undefined) return false
  const point = character.codePointAt(0) ?? 0
  if (point >= 128) return WORD.test(character)
  const cached = WORD_ASCII[point]
  if (cached !== 0) return cached === 2
  const result = WORD.test(character)
  WORD_ASCII[point] = result ? 2 : 1
  return result
}

class Compiler {
  readonly program: Inst[] = []
  readonly atoms: AtomMatcher[] = []

  constructor(private readonly caseInsensitive: boolean) {}

  push(inst: Inst): number {
    if (this.program.length >= MAX_INSTRUCTIONS) {
      throw invalid(`compiled regex exceeds size limit of ${String(SIZE_LIMIT)} bytes`)
    }
    this.program.push(inst)
    return this.program.length - 1
  }

  emit(node: Node): void {
    switch (node.kind) {
      case "empty":
        return
      case "atom":
        this.atoms.push(new AtomMatcher(node.atom.literal, node.atom.source, this.caseInsensitive))
        this.push({ op: "char", atom: this.atoms.length - 1 })
        return
      case "look":
        this.push({ op: "look", look: node.look })
        return
      case "concat":
        for (const item of node.items) this.emit(item)
        return
      case "group":
        this.emit(node.child)
        return
      case "alt": {
        const exits: number[] = []
        node.items.forEach((item, index) => {
          if (index === node.items.length - 1) {
            this.emit(item)
            return
          }
          const split = this.push({ op: "split", x: this.program.length + 1, y: -1 })
          this.emit(item)
          exits.push(this.push({ op: "jmp", x: -1 }))
          this.patch(split, this.program.length)
        })
        for (const exit of exits) (this.program[exit] as { x: number }).x = this.program.length
        return
      }
      case "repeat": {
        const { min, max, child } = node
        // An empty-width body matches the same once as any number of times,
        // which keeps `\b{1000000}` small, as in Rust.
        if (zeroWidth(child)) {
          if (max === 0) return
          if (min > 0) this.emit(child)
          else this.optional(child)
          return
        }
        for (let index = 0; index < min; index += 1) this.emit(child)
        if (max === undefined) {
          const loop = this.push({ op: "split", x: this.program.length + 1, y: -1 })
          this.emit(child)
          this.push({ op: "jmp", x: loop })
          this.patch(loop, this.program.length)
          return
        }
        const splits: number[] = []
        for (let index = min; index < max; index += 1) {
          splits.push(this.push({ op: "split", x: this.program.length + 1, y: -1 }))
          this.emit(child)
        }
        for (const split of splits) this.patch(split, this.program.length)
        return
      }
    }
  }

  private optional(child: Node): void {
    const split = this.push({ op: "split", x: this.program.length + 1, y: -1 })
    this.emit(child)
    this.patch(split, this.program.length)
  }

  private patch(split: number, target: number): void {
    ;(this.program[split] as { y: number }).y = target
  }
}

function zeroWidth(node: Node): boolean {
  switch (node.kind) {
    case "empty":
    case "look":
      return true
    case "atom":
      return false
    case "concat":
    case "alt":
      return node.items.every(zeroWidth)
    case "group":
      return zeroWidth(node.child)
    case "repeat":
      return node.max === 0 || zeroWidth(node.child)
  }
}

function depth(node: Node): number {
  switch (node.kind) {
    case "empty":
    case "look":
      return 0
    case "atom":
      return node.atom.depth
    case "concat":
    case "alt":
      return 1 + Math.max(0, ...node.items.map(depth))
    case "group":
    case "repeat":
      return 1 + depth(node.child)
  }
}

function cost(node: Node): number {
  switch (node.kind) {
    case "empty":
    case "look":
      return 0
    case "atom":
      return node.atom.cost
    case "concat":
      return node.items.reduce((sum, item) => sum + cost(item), 0)
    // Rust merges and factors alternatives, so only the largest is certain.
    case "alt":
      return Math.max(0, ...node.items.map(cost))
    case "group":
      return cost(node.child)
    case "repeat": {
      const body = cost(node.child)
      if (body === 0 || node.max === 0) return 0
      if (node.max === undefined) return node.min > 0 ? node.min * body : body + REPEAT_COST
      return node.min * body + (node.max - node.min) * (body + REPEAT_COST)
    }
  }
}

function classDepth(set: ClassSet): number {
  if (set.kind === "op") return 1 + Math.max(classDepth(set.lhs), classDepth(set.rhs))
  return itemDepth(set.item)
}

function itemDepth(item: ClassItem): number {
  switch (item.kind) {
    case "bracketed":
      return 1 + classDepth(item.set)
    case "union":
      return 1 + Math.max(0, ...item.items.map(itemDepth))
    case "empty":
    case "literal":
    case "range":
    case "primitive":
      return 0
  }
}

function hex(point: number): string {
  return `\\u{${point.toString(16)}}`
}

function rangesSource(ranges: readonly Range[]): string {
  return ranges.map(([from, to]) => (from === to ? hex(from) : `${hex(from)}-${hex(to)}`)).join("")
}

function setSource(set: ClassSet): string {
  if (set.kind === "item") return itemSource(set.item)
  const lhs = `[${setSource(set.lhs)}]`
  const rhs = `[${setSource(set.rhs)}]`
  if (set.op === "~~") return `[${lhs}--${rhs}][${rhs}--${lhs}]`
  return `${lhs}${set.op}${rhs}`
}

function itemSource(item: ClassItem): string {
  switch (item.kind) {
    case "empty":
      return "[]"
    case "literal":
      return hex(item.value)
    case "range":
      return `${hex(item.from)}-${hex(item.to)}`
    case "primitive":
      return item.source
    case "bracketed":
      return `[${item.negated ? "^" : ""}${setSource(item.set)}]`
    case "union":
      return item.items.map(itemSource).join("")
  }
}

function utf8Length(point: number): number {
  return point < 0x80 ? 1 : point < 0x800 ? 2 : point < 0x10000 ? 3 : 4
}

// The plain ranges a class lists, or undefined when it holds a negation or a
// Unicode class.
function plainRanges(set: ClassSet): Range[] | undefined {
  if (set.kind === "op") {
    const lhs = plainRanges(set.lhs)
    const rhs = plainRanges(set.rhs)
    if (lhs === undefined || rhs === undefined) return undefined
    const inLhs = (point: number): boolean => lhs.some(([from, to]) => point >= from && point <= to)
    const inRhs = (point: number): boolean => rhs.some(([from, to]) => point >= from && point <= to)
    const keep = (point: number): boolean =>
      set.op === "&&"
        ? inLhs(point) && inRhs(point)
        : set.op === "--"
          ? inLhs(point) && !inRhs(point)
          : inLhs(point) !== inRhs(point)
    // Membership only changes at range edges, so testing each edge decides
    // every span between them.
    const edges = [...new Set([...lhs, ...rhs].flatMap(([from, to]) => [from, to + 1]))].sort(
      (left, right) => left - right
    )
    const ranges: Range[] = []
    edges.forEach((edge, index) => {
      const end = (edges[index + 1] ?? edge + 1) - 1
      if (keep(edge)) ranges.push([edge, end])
    })
    return ranges
  }
  const collect = (item: ClassItem): Range[] | undefined => {
    switch (item.kind) {
      case "empty":
        return []
      case "literal":
        return [[item.value, item.value]]
      case "range":
        return [[item.from, item.to]]
      case "primitive":
        return item.ascii === undefined ? undefined : [...item.ascii]
      case "bracketed":
        return item.negated ? undefined : plainRanges(item.set)
      case "union": {
        const ranges: Range[] = []
        for (const child of item.items) {
          const found = collect(child)
          if (found === undefined) return undefined
          ranges.push(...found)
        }
        return ranges
      }
    }
  }
  return collect(set.item)
}

function mergeRanges(ranges: readonly Range[]): Range[] {
  const sorted = [...ranges].sort((left, right) => left[0] - right[0])
  const merged: [number, number][] = []
  for (const [from, to] of sorted) {
    const last = merged[merged.length - 1]
    if (last !== undefined && from <= last[1] + 1) last[1] = Math.max(last[1], to)
    else merged.push([from, to])
  }
  return merged
}

function asciiCaseVariants(ranges: readonly Range[]): Range[] {
  const variants: Range[] = [...ranges]
  for (const [from, to] of ranges) {
    for (let point = Math.max(from, 0x41); point <= Math.min(to, 0x7a); point += 1) {
      if (point <= 0x5a) variants.push([point + 0x20, point + 0x20])
      else if (point >= 0x61) variants.push([point - 0x20, point - 0x20])
    }
  }
  return variants
}

function rangesCost(ranges: readonly Range[]): number {
  const merged = mergeRanges(ranges)
  if (merged.length === 0) return 0
  const first = merged[0]
  if (merged.length === 1 && first !== undefined && first[0] === first[1]) {
    return LITERAL_BYTE_COST * utf8Length(first[0])
  }
  if (merged.some(([, to]) => to >= 0x80)) return CLASS_COST
  return CLASS_COST + CLASS_RANGE_COST * merged.length
}

function literalCost(point: number, caseInsensitive: boolean): number {
  if (!caseInsensitive) return LITERAL_BYTE_COST * utf8Length(point)
  return rangesCost(asciiCaseVariants([[point, point]]))
}

function normalizeName(name: string): string {
  const prefixed = name.length >= 2 && name.slice(0, 2).toLowerCase() === "is"
  let normalized = ""
  for (const character of prefixed ? name.slice(2) : name) {
    if (character === " " || character === "_" || character === "-") continue
    if (character >= "A" && character <= "Z") normalized += character.toLowerCase()
    else if ((character.codePointAt(0) ?? 0x80) < 0x80) normalized += character
  }
  return prefixed && normalized === "c" ? "isc" : normalized
}

function aliasTable(packed: string): ReadonlyMap<string, string> {
  const table = new Map<string, string>()
  for (const entry of packed.split(",")) {
    const [canonical = "", aliases] = entry.split("=")
    table.set(normalizeName(canonical), canonical)
    for (const alias of aliases?.split("|") ?? []) table.set(alias, canonical)
  }
  return table
}

let tables:
  | {
      readonly names: ReadonlyMap<string, string>
      readonly categories: ReadonlyMap<string, string>
      readonly scripts: ReadonlyMap<string, string>
      readonly binary: ReadonlySet<string>
    }
  | undefined

function unicodeTables(): NonNullable<typeof tables> {
  tables ??= {
    names: aliasTable(PROPERTY_NAMES),
    categories: aliasTable(GENERAL_CATEGORY_VALUES),
    scripts: aliasTable(SCRIPT_VALUES),
    binary: new Set(BINARY_PROPERTIES.split(","))
  }
  return tables
}

function generalCategory(normalized: string): string | undefined {
  if (normalized === "any") return "\\p{Any}"
  if (normalized === "assigned") return "\\p{Assigned}"
  // V8 leaves the long s and the Kelvin sign out of a case-folded \p{ASCII}.
  if (normalized === "ascii") return "[\\u{0}-\\u{7f}]"
  const canonical = unicodeTables().categories.get(normalized)
  return canonical === undefined ? undefined : `\\p{General_Category=${canonical}}`
}

// Resolves a property query the way regex-syntax does, to its JavaScript
// spelling. Age and the segmentation properties have no JavaScript peer and
// are refused.
function unicodeProperty(name: string, value: string | undefined): string {
  const { names, scripts, binary } = unicodeTables()
  if (value === undefined) {
    const normalized = normalizeName(name)
    if (normalized !== "cf" && normalized !== "sc" && normalized !== "lc") {
      const canonical = names.get(normalized)
      if (canonical !== undefined) {
        if (binary.has(canonical)) return `\\p{${canonical}}`
        throw invalid(`Unicode property not found: ${name}`)
      }
    }
    const category = generalCategory(normalized)
    if (category !== undefined) return category
    const script = scripts.get(normalized)
    if (script !== undefined) return `\\p{Script=${script}}`
    throw invalid(`Unicode property not found: ${name}`)
  }
  const property = names.get(normalizeName(name))
  const normalized = normalizeName(value)
  switch (property) {
    case "General_Category": {
      const category = generalCategory(normalized)
      if (category !== undefined) return category
      break
    }
    case "Script":
    case "Script_Extensions": {
      const script = scripts.get(normalized)
      if (script !== undefined) return `\\p{${property}=${script}}`
      break
    }
    case "Age":
    case "Grapheme_Cluster_Break":
    case "Sentence_Break":
    case "Word_Break":
      throw invalid(`the Unicode property ${property} is not supported in TypeScript`)
    case undefined:
      throw invalid(`Unicode property not found: ${name}`)
  }
  throw invalid(`Unicode property value not found: ${value}`)
}

function negate(source: string): string {
  return source.startsWith("\\p{") ? `\\P${source.slice(2)}` : `[^${source}]`
}

class Parser {
  private index = 0
  private readonly characters: readonly string[]

  constructor(
    pattern: string,
    private readonly caseInsensitive: boolean
  ) {
    this.characters = Array.from(pattern)
  }

  parse(): Node {
    const groups: { readonly concat: Node[]; readonly alternates: Node[] | undefined }[] = []
    let concat: Node[] = []
    let alternates: Node[] | undefined
    while (!this.eof()) {
      switch (this.char()) {
        case "(":
          this.index += 1
          if (this.char() === "?") {
            if (this.peek() !== ":") {
              throw invalid("groups starting with (? other than (?: are not supported")
            }
            this.index += 2
          }
          groups.push({ concat, alternates })
          concat = []
          alternates = undefined
          break
        case ")": {
          const group = groups.pop()
          if (group === undefined) throw invalid("unopened group")
          const child = finish(concat, alternates)
          concat = group.concat
          alternates = group.alternates
          concat.push({ kind: "group", child })
          this.index += 1
          break
        }
        case "|":
          ;(alternates ??= []).push(intoConcat(concat))
          concat = []
          this.index += 1
          break
        case "[":
          concat.push(this.parseClass())
          break
        case "?":
          this.repeat(concat, 0, 1)
          break
        case "*":
          this.repeat(concat, 0, undefined)
          break
        case "+":
          this.repeat(concat, 1, undefined)
          break
        case "{":
          this.countedRepeat(concat)
          break
        default:
          concat.push(this.parsePrimitive())
      }
    }
    if (groups.length > 0) throw invalid("unclosed group")
    return finish(concat, alternates)
  }

  // The current character, empty at the end of the pattern.
  private char(): string {
    return this.characters[this.index] ?? ""
  }

  private peek(): string {
    return this.characters[this.index + 1] ?? ""
  }

  private eof(): boolean {
    return this.index >= this.characters.length
  }

  private repeat(concat: Node[], min: number, max: number | undefined): void {
    const child = concat.pop()
    if (child === undefined) throw invalid("repetition operator missing expression")
    this.index += 1
    if (this.char() === "?") this.index += 1
    concat.push({ kind: "repeat", min, max, child })
  }

  private countedRepeat(concat: Node[]): void {
    const child = concat.pop()
    if (child === undefined) throw invalid("repetition operator missing expression")
    const unclosed = (): InvalidError => invalid("unclosed counted repetition")
    this.index += 1
    if (this.eof()) throw unclosed()
    const start = this.decimal()
    if (this.eof()) throw unclosed()
    let min: number
    let max: number | undefined
    if (this.char() === ",") {
      this.index += 1
      if (this.eof()) throw unclosed()
      if (this.char() !== "}") {
        min = required(start)
        max = required(this.decimal())
      } else {
        min = required(start)
        max = undefined
      }
    } else {
      min = required(start)
      max = min
    }
    if (this.char() !== "}") throw unclosed()
    this.index += 1
    if (this.char() === "?") this.index += 1
    if (max !== undefined && min > max) throw invalid("invalid repetition count range")
    concat.push({ kind: "repeat", min, max, child })
  }

  // Digits with surrounding whitespace, or an error message when there are none.
  private decimal(): number | string {
    while (!this.eof() && WHITE_SPACE.test(this.char())) this.index += 1
    let digits = ""
    while (!this.eof() && this.char() >= "0" && this.char() <= "9") {
      digits += this.char()
      this.index += 1
    }
    while (!this.eof() && WHITE_SPACE.test(this.char())) this.index += 1
    if (digits === "") return "repetition quantifier expects a valid decimal"
    const value = Number(digits)
    if (value > 0xffff_ffff) return "decimal literal invalid"
    return value
  }

  private parsePrimitive(): Node {
    const character = this.char()
    if (character === "\\") {
      const escape = this.parseEscape()
      switch (escape.kind) {
        case "literal":
          return this.literal(escape.value)
        case "look":
          return { kind: "look", look: escape.look }
        case "primitive":
          return {
            kind: "atom",
            atom: { source: itemSource(escape.item), cost: escape.cost, depth: 0 }
          }
      }
    }
    switch (character) {
      case ".":
        this.index += 1
        return { kind: "atom", atom: { source: "^\\n", cost: DOT_COST, depth: 0 } }
      case "^":
        this.index += 1
        return { kind: "look", look: "start" }
      case "$":
        this.index += 1
        return { kind: "look", look: "end" }
    }
    this.index += 1
    return this.literal(character.codePointAt(0) ?? 0)
  }

  private literal(point: number): Node {
    const source = hex(point)
    const cost = literalCost(point, this.caseInsensitive)
    return {
      kind: "atom",
      atom: this.caseInsensitive
        ? { source, cost, depth: 0 }
        : { source, cost, depth: 0, literal: String.fromCodePoint(point) }
    }
  }

  private parseEscape(): Escape {
    this.index += 1
    if (this.eof()) throw invalid("incomplete escape sequence")
    const character = this.char()
    if (character >= "0" && character <= "9") throw invalid("backreferences are not supported")
    if (character === "x" || character === "u" || character === "U") return this.parseHex()
    if (character === "p" || character === "P") return this.parseUnicodeClass()
    if ("dswDSW".includes(character)) {
      this.index += 1
      const lower = character.toLowerCase()
      const base =
        lower === "d" ? "\\p{Nd}" : lower === "s" ? "\\p{White_Space}" : `[${WORD_SOURCE}]`
      const source = character === lower ? base : negate(base)
      return {
        kind: "primitive",
        item: { kind: "primitive", source },
        cost: PERL_COST[character] ?? CLASS_COST
      }
    }
    this.index += 1
    if (isEscapeable(character)) {
      return { kind: "literal", value: character.codePointAt(0) ?? 0 }
    }
    switch (character) {
      case "a":
        return { kind: "literal", value: 0x07 }
      case "f":
        return { kind: "literal", value: 0x0c }
      case "t":
        return { kind: "literal", value: 0x09 }
      case "n":
        return { kind: "literal", value: 0x0a }
      case "r":
        return { kind: "literal", value: 0x0d }
      case "v":
        return { kind: "literal", value: 0x0b }
      case "A":
        return { kind: "look", look: "start" }
      case "z":
        return { kind: "look", look: "end" }
      case "b":
        return { kind: "look", look: this.char() === "{" ? this.specialWordBoundary() : "word" }
      case "B":
        return { kind: "look", look: "not_word" }
      case "<":
        return { kind: "look", look: "word_start" }
      case ">":
        return { kind: "look", look: "word_end" }
    }
    throw invalid("unrecognized escape sequence")
  }

  // `\b{start}` and its peers. Anything that cannot be one, such as `\b{5}`,
  // is left for the counted repetition parser.
  private specialWordBoundary(): Look {
    const start = this.index
    this.index += 1
    if (this.eof()) throw invalid("unexpected end of a special word boundary or repetition")
    const valid = (character: string): boolean => /^[A-Za-z-]$/u.test(character)
    if (!valid(this.char())) {
      this.index = start
      return "word"
    }
    let name = ""
    while (!this.eof() && valid(this.char())) {
      name += this.char()
      this.index += 1
    }
    if (this.char() !== "}") throw invalid("unclosed special word boundary")
    this.index += 1
    switch (name) {
      case "start":
        return "word_start"
      case "end":
        return "word_end"
      case "start-half":
        return "word_start_half"
      case "end-half":
        return "word_end_half"
    }
    throw invalid("unrecognized special word boundary")
  }

  private parseHex(): Escape {
    const digits = this.char() === "x" ? 2 : this.char() === "u" ? 4 : 8
    this.index += 1
    if (this.eof()) throw invalid("incomplete escape sequence")
    let text = ""
    if (this.char() === "{") {
      this.index += 1
      while (!this.eof() && this.char() !== "}") {
        if (!isHex(this.char())) throw invalid("hexadecimal literal is not a hexadecimal digit")
        text += this.char()
        this.index += 1
      }
      if (this.eof()) throw invalid("incomplete escape sequence")
      this.index += 1
      if (text === "") throw invalid("hexadecimal literal empty")
    } else {
      for (let index = 0; index < digits; index += 1) {
        if (this.eof()) throw invalid("incomplete escape sequence")
        if (!isHex(this.char())) throw invalid("hexadecimal literal is not a hexadecimal digit")
        text += this.char()
        this.index += 1
      }
    }
    const trimmed = text.replace(/^0+/u, "")
    const value = trimmed.length > 8 ? Number.POSITIVE_INFINITY : Number.parseInt(text, 16)
    if (value > MAX_SCALAR || (value >= 0xd800 && value <= 0xdfff)) {
      throw invalid("hexadecimal literal does not correspond to a Unicode scalar value")
    }
    return { kind: "literal", value }
  }

  private parseUnicodeClass(): Escape {
    let negated = this.char() === "P"
    this.index += 1
    if (this.eof()) throw invalid("incomplete escape sequence")
    let source: string
    if (this.char() === "{") {
      let name = ""
      this.index += 1
      while (!this.eof() && this.char() !== "}") {
        name += this.char()
        this.index += 1
      }
      if (this.eof()) throw invalid("incomplete escape sequence")
      this.index += 1
      const notEqual = name.indexOf("!=")
      const colon = name.indexOf(":")
      const equal = name.indexOf("=")
      if (notEqual >= 0) {
        negated = !negated
        source = unicodeProperty(name.slice(0, notEqual), name.slice(notEqual + 2))
      } else if (colon >= 0) {
        source = unicodeProperty(name.slice(0, colon), name.slice(colon + 1))
      } else if (equal >= 0) {
        source = unicodeProperty(name.slice(0, equal), name.slice(equal + 1))
      } else {
        source = unicodeProperty(name, undefined)
      }
    } else {
      const letter = this.char()
      if (letter === "\\") throw invalid("invalid Unicode character class")
      this.index += 1
      source = unicodeProperty(letter, undefined)
    }
    return {
      kind: "primitive",
      item: { kind: "primitive", source: negated ? negate(source) : source },
      cost: CLASS_COST
    }
  }

  private parseClass(): Node {
    const bracketed = this.parseBracketed()
    const item: ClassItem = bracketed
    const ranges = bracketed.negated ? undefined : plainRanges(bracketed.set)
    // An opaque class can still hold a single character, the cheapest program.
    let classCost = LITERAL_BYTE_COST
    if (ranges !== undefined) {
      classCost = rangesCost(this.caseInsensitive ? asciiCaseVariants(ranges) : ranges)
    } else if (
      bracketed.set.kind === "item" &&
      bracketed.set.item.kind === "primitive" &&
      !bracketed.negated
    ) {
      classCost = perlCostOf(bracketed.set.item.source)
    }
    return {
      kind: "atom",
      atom: { source: setSourceOf(bracketed), cost: classCost, depth: itemDepth(item) }
    }
  }

  // At an opening `[`, through its matching `]`.
  private parseBracketed(): {
    readonly kind: "bracketed"
    readonly negated: boolean
    readonly set: ClassSet
  } {
    const unclosed = (): InvalidError => invalid("unclosed character class")
    this.index += 1
    if (this.eof()) throw unclosed()
    let negated = false
    if (this.char() === "^") {
      negated = true
      this.index += 1
      if (this.eof()) throw unclosed()
    }
    let union: ClassItem[] = []
    while (this.char() === "-") {
      union.push({ kind: "literal", value: 0x2d })
      this.index += 1
      if (this.eof()) throw unclosed()
    }
    if (union.length === 0 && this.char() === "]") {
      union.push({ kind: "literal", value: 0x5d })
      this.index += 1
      if (this.eof()) throw unclosed()
    }
    let lhs: ClassSet | undefined
    let op: "&&" | "--" | "~~" | undefined
    const close = (): ClassSet => {
      const rhs: ClassSet = { kind: "item", item: intoItem(union) }
      return lhs === undefined || op === undefined ? rhs : { kind: "op", op, lhs, rhs }
    }
    for (;;) {
      if (this.eof()) throw unclosed()
      const character = this.char()
      if (character === "[") {
        union.push(this.asciiClass() ?? this.parseBracketed())
        continue
      }
      if (character === "]") {
        this.index += 1
        return { kind: "bracketed", negated, set: close() }
      }
      const pair = `${character}${this.peek()}`
      if (pair === "&&" || pair === "--" || pair === "~~") {
        this.index += 2
        lhs = close()
        op = pair
        union = []
        continue
      }
      union.push(this.classRange())
    }
  }

  private classRange(): ClassItem {
    const first = this.classPrimitive()
    if (this.eof()) throw invalid("unclosed character class")
    if (this.char() !== "-" || this.peek() === "]" || this.peek() === "-") {
      return classItem(first)
    }
    this.index += 1
    if (this.eof()) throw invalid("unclosed character class")
    const second = this.classPrimitive()
    if (first.kind !== "literal" || second.kind !== "literal") {
      throw invalid("invalid range boundary, must be a literal")
    }
    if (first.value > second.value) throw invalid("invalid character class range")
    return { kind: "range", from: first.value, to: second.value }
  }

  private classPrimitive(): Escape {
    if (this.char() === "\\") return this.parseEscape()
    const point = this.char().codePointAt(0) ?? 0
    this.index += 1
    return { kind: "literal", value: point }
  }

  // `[:name:]` inside a class. Anything else leaves the parser where it was,
  // so the `[` opens a nested class instead.
  private asciiClass(): ClassItem | undefined {
    const start = this.index
    const item = this.asciiClassItem()
    if (item === undefined) this.index = start
    return item
  }

  private asciiClassItem(): ClassItem | undefined {
    this.index += 1
    if (this.eof() || this.char() !== ":") return undefined
    this.index += 1
    if (this.eof()) return undefined
    let negated = false
    if (this.char() === "^") {
      negated = true
      this.index += 1
      if (this.eof()) return undefined
    }
    const nameStart = this.index
    while (!this.eof() && this.char() !== ":") this.index += 1
    if (this.eof()) return undefined
    const name = this.characters.slice(nameStart, this.index).join("")
    if (this.char() !== ":" || this.peek() !== "]") return undefined
    this.index += 2
    const ranges = ASCII_CLASSES[name]
    if (ranges === undefined) return undefined
    const source = `[${negated ? "^" : ""}${rangesSource(ranges)}]`
    return negated ? { kind: "primitive", source } : { kind: "primitive", source, ascii: ranges }
  }
}

function setSourceOf(bracketed: { readonly negated: boolean; readonly set: ClassSet }): string {
  return `${bracketed.negated ? "^" : ""}${setSource(bracketed.set)}`
}

function perlCostOf(source: string): number {
  for (const [name, cost] of Object.entries(PERL_COST)) {
    const lower = name.toLowerCase()
    const base = lower === "d" ? "\\p{Nd}" : lower === "s" ? "\\p{White_Space}" : `[${WORD_SOURCE}]`
    if (source === (name === lower ? base : negate(base))) return cost
  }
  return CLASS_COST
}

function classItem(escape: Escape): ClassItem {
  switch (escape.kind) {
    case "literal":
      return { kind: "literal", value: escape.value }
    case "primitive":
      return escape.item
    case "look":
      throw invalid("escape sequence is not valid in a character class")
  }
}

function intoItem(union: readonly ClassItem[]): ClassItem {
  if (union.length === 0) return { kind: "empty" }
  const [only] = union
  if (union.length === 1 && only !== undefined) return only
  return { kind: "union", items: union }
}

function intoConcat(items: readonly Node[]): Node {
  if (items.length === 0) return { kind: "empty" }
  const [only] = items
  if (items.length === 1 && only !== undefined) return only
  return { kind: "concat", items }
}

function finish(concat: readonly Node[], alternates: readonly Node[] | undefined): Node {
  if (alternates === undefined) return intoConcat(concat)
  return { kind: "alt", items: [...alternates, intoConcat(concat)] }
}

function required(value: number | string): number {
  if (typeof value === "string") throw invalid(value)
  return value
}

function isHex(character: string | undefined): boolean {
  return character !== undefined && /^[0-9A-Fa-f]$/u.test(character)
}

// regex-syntax lets any ASCII character other than a letter, a digit, `<`,
// or `>` be escaped to itself.
function isEscapeable(character: string): boolean {
  const point = character.codePointAt(0) ?? 0x80
  if (point >= 0x80) return false
  return !/^[0-9A-Za-z<>]$/u.test(character)
}
