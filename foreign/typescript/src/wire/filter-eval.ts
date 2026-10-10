import { MAX_FILTER_PARSE_DEPTH } from "./limits.js"
import { PayloadDecoders, PayloadFault } from "./filter-codecs.js"
import type { SchemaDef } from "./control.js"
import { InvalidError } from "../client/errors.js"
import {
  type Coerce,
  type ConsumerFilter,
  ExactDecimal,
  type ExplainNode,
  type FaultPolicy,
  type FaultReason,
  FieldPath,
  type FilterExplanation,
  type FilterExpr,
  type TimestampFormat,
  type RecordPolicy,
  type TextMatch,
  type TextPredicate,
  type Truth,
  type Verdict,
  consumerFilterDigest,
  faultReasonIsForeign,
  globTokens,
  type GlobToken,
  parseCanonicalJson,
  timestampFormatMicrosFromInteger,
  timestampFormatMicrosFromText,
  validateConsumerFilter
} from "./filter.js"
import type { CmpOp } from "./query.js"
import { RustRegex } from "./regex.js"
import type { TypedValue } from "./schema.js"

const UTF8_STRICT = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true })
const CONTENT_TYPE_HEADER = "agdx.ct"
// agdx.ct codes of the payload codecs a filter decodes, and `any`, which never makes a record foreign.
const CODEC_CONTENT_TYPES: Readonly<Record<string, number>> = {
  json: 1,
  cbor: 3,
  avro: 5,
  protobuf: 6
}
const ANY_CONTENT_TYPE = 255
const KNOWN_CONTENT_TYPES: ReadonlySet<number> = new Set([
  0,
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  ANY_CONTENT_TYPE
])

/** Bounds on decoding one payload, checked before the payload is parsed. */
export interface DecodeLimits {
  readonly maxPayloadBytes: number
  readonly maxDepth: number
}

export const DEFAULT_DECODE_LIMITS: DecodeLimits = {
  maxPayloadBytes: 1024 * 1024,
  maxDepth: 64
}

/** One record as the evaluator sees it. */
export interface FilterRecord {
  readonly payload: Uint8Array
  readonly headers: readonly HeaderRef[]
}

/** One user header of a record as the evaluator reads it. */
export interface HeaderRef {
  readonly key: string
  readonly value: HeaderValueRef
}

/** A typed user header value. Mirrors `HeaderScalar`. */
export type HeaderValueRef =
  | { readonly kind: "bool"; readonly value: boolean }
  | { readonly kind: "int"; readonly value: bigint }
  | { readonly kind: "uint"; readonly value: bigint }
  | { readonly kind: "float"; readonly value: number }
  | { readonly kind: "string"; readonly value: string }
  | { readonly kind: "raw"; readonly value: Uint8Array }

/** What a caller decodes from a record's header block before evaluating. */
export type HeaderNeed =
  /** No header is read. */
  | "none"
  /** Only `agdx.ct`, to keep a record in another codec out of the decoder. */
  | "content_type"
  | "all"

/**
 * A decoded payload value. Objects are maps, integers that fit i64 or u64 are
 * bigints, and every other number is a double, exactly as the server decodes.
 */
export type JsonValue =
  null | boolean | bigint | number | string | readonly JsonValue[] | ReadonlyMap<string, JsonValue>

type Literal =
  | { readonly kind: "null" }
  | { readonly kind: "bool"; readonly value: boolean }
  | { readonly kind: "int"; readonly value: bigint }
  | { readonly kind: "float"; readonly value: number }
  | { readonly kind: "text"; readonly value: string }
  | { readonly kind: "list"; readonly items: readonly Literal[] }

type Comparable =
  | { readonly kind: "instant"; readonly micros: bigint }
  | { readonly kind: "decimal"; readonly decimal: ExactDecimal }

type CoercedLiteral =
  Comparable | { readonly kind: "list"; readonly items: readonly CoercedLiteral[] }

type Scalar =
  | { readonly kind: "null" }
  | { readonly kind: "bool"; readonly value: boolean }
  | { readonly kind: "int"; readonly value: bigint }
  | { readonly kind: "float"; readonly value: number }
  | { readonly kind: "text"; readonly value: string }
  | { readonly kind: "composite" }

type NodeKind =
  | {
      readonly kind: "all" | "any"
      readonly children: readonly Node[]
      // Header children first, so headers decide before the payload decodes.
      readonly ordered: readonly Node[]
    }
  | { readonly kind: "not"; readonly child: Node }
  | {
      readonly kind: "compare"
      readonly path: FieldPath
      readonly op: CmpOp
      readonly literal: Literal
    }
  | {
      readonly kind: "coerced"
      readonly path: FieldPath
      readonly op: CmpOp
      readonly coerce: Coerce
      readonly literal: CoercedLiteral
    }
  | { readonly kind: "present" | "absent"; readonly path: FieldPath }
  | { readonly kind: "header"; readonly key: string; readonly op: CmpOp; readonly literal: Literal }
  | { readonly kind: "text"; readonly path: FieldPath; readonly matcher: TextMatcher }
  | { readonly kind: "header_text"; readonly key: string; readonly matcher: TextMatcher }

interface Node {
  readonly node: NodeKind
  readonly readsPayload: boolean
  readonly readsHeaders: boolean
}

/** A validated filter compiled for repeated evaluation. Compile once, evaluate per record. */
export class CompiledFilter {
  private constructor(
    readonly filter: ConsumerFilter,
    readonly digest: Uint8Array,
    private readonly root: Node,
    private readonly decoders: PayloadDecoders
  ) {}

  get readsHeaders(): boolean {
    return this.root.readsHeaders || this.filter.schemaRefs.length > 0
  }

  /**
   * Which headers a caller has to decode before evaluating. A payload filter
   * without header predicates needs only `agdx.ct`.
   */
  get headerNeed(): HeaderNeed {
    if (this.readsHeaders) return "all"
    return this.root.readsPayload ? "content_type" : "none"
  }

  static compile(filter: ConsumerFilter, schemas: readonly SchemaDef[] = []): CompiledFilter {
    validateConsumerFilter(filter)
    return new CompiledFilter(
      filter,
      consumerFilterDigest(filter),
      compileNode(filter.expr),
      new PayloadDecoders(filter, schemas)
    )
  }

  get faultPolicy(): FaultPolicy {
    return this.filter.faultPolicy
  }

  get readsPayload(): boolean {
    return this.root.readsPayload
  }

  /**
   * Whether the record is selected, rejected, or could not be decoded. Header
   * predicates run before the payload is touched, and the payload is decoded at
   * most once.
   */
  evaluate(record: FilterRecord, limits: DecodeLimits = DEFAULT_DECODE_LIMITS): Verdict {
    return this.outcome(this.context(record, limits)).verdict
  }

  /** The verdict and, for a fault or a policy rejection, its reason, from one decode. */
  evaluateWithFault(
    record: FilterRecord,
    limits: DecodeLimits = DEFAULT_DECODE_LIMITS
  ): { verdict: Verdict; fault?: FaultReason } {
    return this.outcome(this.context(record, limits))
  }

  /**
   * The policy a record that faulted for `reason` follows: a record in another
   * format the foreign policy, a type mismatch the mismatch policy, a broken
   * payload the fault policy. A `reject` record policy maps to `drop`.
   */
  policyFor(reason: FaultReason): FaultPolicy {
    const policy = this.recordPolicy(reason)
    if (policy === undefined) return this.filter.faultPolicy
    return policy === "pass" ? "pass" : "drop"
  }

  /** The record policy that covers `reason`, `undefined` for a decode fault. */
  recordPolicy(reason: FaultReason): RecordPolicy | undefined {
    if (faultReasonIsForeign(reason)) return this.filter.foreignPolicy ?? "reject"
    if (reason === "type_mismatch") return this.filter.mismatchPolicy ?? "reject"
    return undefined
  }

  /** Each node's truth, for sample tests. The verdict is the delivery verdict of `evaluate`. */
  explain(record: FilterRecord, limits: DecodeLimits = DEFAULT_DECODE_LIMITS): FilterExplanation {
    const context = this.context(record, limits)
    const { verdict, fault } = this.outcome(context)
    const root = context.explain(this.root)
    return { verdict, ...(fault !== undefined ? { fault } : {}), root }
  }

  private context(record: FilterRecord, limits: DecodeLimits): Context {
    return new Context(
      record,
      { ...limits, maxDepth: Math.min(limits.maxDepth, MAX_FILTER_PARSE_DEPTH) },
      this.filter.codec,
      this.decoders
    )
  }

  // A `reject` record policy decides here, so the record is rejected and never
  // a fault. `pass` surfaces as a fault with its reason.
  private outcome(context: Context): { verdict: Verdict; fault?: FaultReason } {
    let reason: FaultReason
    try {
      const truth = context.truth(this.root)
      if (truth === "match") return { verdict: "selected" }
      if (truth === "no_match" || !context.mismatched) return { verdict: "rejected" }
      reason = "type_mismatch"
    } catch (error) {
      if (!(error instanceof PayloadFault)) throw error
      reason = error.reason
    }
    if (this.recordPolicy(reason) === "reject") return { verdict: "rejected", fault: reason }
    return { verdict: "fault", fault: reason }
  }
}

/** A validated text predicate ready to run per record. */
export class TextMatcher {
  private readonly pattern: string
  private readonly glob: GlobProgram | undefined
  private readonly regex: RustRegex | undefined

  // Regex runs the Rust syntax with the engine's own case folding, the other
  // kinds lowercase both sides, as in Rust.
  constructor(
    readonly kind: TextMatch,
    readonly source: string,
    private readonly caseInsensitive: boolean
  ) {
    if (kind === "regex") this.regex = RustRegex.compile(source, caseInsensitive)
    this.pattern = caseInsensitive ? source.toLowerCase() : source
    if (kind === "glob") {
      const tokens = globTokens(source)
      if (tokens === undefined) throw new InvalidError("the glob has a trailing escape")
      this.glob = new GlobProgram(
        tokens.flatMap((token) =>
          token.kind === "literal" && caseInsensitive
            ? Array.from(token.value.toLowerCase(), (value) => ({
                kind: "literal" as const,
                value
              }))
            : [token]
        )
      )
    }
  }

  static of(predicate: TextPredicate): TextMatcher {
    return new TextMatcher(predicate.kind, predicate.pattern, predicate.caseInsensitive === true)
  }

  matches(text: string): boolean {
    if (this.regex !== undefined) return this.regex.matches(text)
    const value = this.caseInsensitive ? text.toLowerCase() : text
    switch (this.kind) {
      case "equals":
        return value === this.pattern
      case "prefix":
        return value.startsWith(this.pattern)
      case "suffix":
        return value.endsWith(this.pattern)
      case "contains":
        return value.includes(this.pattern)
      case "glob":
        return this.glob?.matches(value) === true
      case "regex":
        return false
    }
  }

  label(): string {
    const pattern = JSON.stringify(this.source)
    return this.caseInsensitive ? `${pattern} ignoring case` : pattern
  }
}

// A bitset holds all active glob states. Each character advances them once,
// without rescanning an already matched literal prefix after a star.
class GlobProgram {
  private runs = 0n
  private ones = 0n
  private readonly literals = new Map<string, bigint>()
  private readonly accept: bigint

  constructor(tokens: readonly GlobToken[]) {
    let count = 0n
    let previousRun = false
    for (const token of tokens) {
      if (token.kind === "run" && previousRun) continue
      const state = 1n << count++
      previousRun = token.kind === "run"
      if (token.kind === "run") this.runs |= state
      else if (token.kind === "one") this.ones |= state
      else this.literals.set(token.value, (this.literals.get(token.value) ?? 0n) | state)
    }
    this.accept = 1n << count
  }

  matches(value: string): boolean {
    let active = 1n | ((1n & this.runs) << 1n)
    for (const character of value) {
      active =
        ((active & (this.ones | (this.literals.get(character) ?? 0n))) << 1n) | (active & this.runs)
      active |= (active & this.runs) << 1n
      if (active === 0n) return false
    }
    return (active & this.accept) !== 0n
  }
}

/**
 * Decode a JSON payload within `limits`. The depth is checked by a scan over
 * the raw bytes before any value is built.
 */
export function decodeJsonPayload(
  payload: Uint8Array,
  limits: DecodeLimits
): JsonValue | PayloadFault {
  if (payload.byteLength > limits.maxPayloadBytes) return new PayloadFault("too_large")
  if (!jsonDepthWithin(payload, limits.maxDepth)) return new PayloadFault("too_deep")
  try {
    return parseCanonicalJson(UTF8_STRICT.decode(payload), "payload") as JsonValue
  } catch {
    return new PayloadFault("malformed")
  }
}

function jsonDepthWithin(payload: Uint8Array, maxDepth: number): boolean {
  let depth = 0
  let inString = false
  let escaped = false
  for (const byte of payload) {
    if (inString) {
      if (escaped) escaped = false
      else if (byte === 0x5c) escaped = true
      else if (byte === 0x22) inString = false
      continue
    }
    if (byte === 0x22) inString = true
    else if (byte === 0x5b || byte === 0x7b) {
      depth += 1
      if (depth > maxDepth) return false
    } else if (byte === 0x5d || byte === 0x7d) {
      depth = Math.max(0, depth - 1)
    }
  }
  return true
}

// Built bottom up, so what a node reads is known from its children and no
// subtree is walked twice.
function compileNode(expr: FilterExpr): Node {
  switch (expr.kind) {
    case "all":
    case "any": {
      const children = expr.children.map(compileNode)
      return {
        node: {
          kind: expr.kind,
          children,
          ordered: [
            ...children.filter((child) => !child.readsPayload),
            ...children.filter((child) => child.readsPayload)
          ]
        },
        readsPayload: children.some((child) => child.readsPayload),
        readsHeaders: children.some((child) => child.readsHeaders)
      }
    }
    case "not": {
      const child = compileNode(expr.child)
      return {
        node: { kind: "not", child },
        readsPayload: child.readsPayload,
        readsHeaders: child.readsHeaders
      }
    }
    case "pred":
      return {
        node: {
          kind: "compare",
          path: FieldPath.parse(expr.predicate.field),
          op: expr.predicate.op,
          literal: compileLiteral(expr.predicate.value)
        },
        readsPayload: true,
        readsHeaders: false
      }
    case "pred_as":
      return {
        node: {
          kind: "coerced",
          path: FieldPath.parse(expr.predicate.pred.field),
          op: expr.predicate.pred.op,
          coerce: expr.predicate.coerce,
          literal: compileCoercedLiteral(expr.predicate.coerce, expr.predicate.pred.value)
        },
        readsPayload: true,
        readsHeaders: false
      }
    case "present":
    case "absent":
      return {
        node: { kind: expr.kind, path: expr.path },
        readsPayload: true,
        readsHeaders: false
      }
    case "header":
      return {
        node: {
          kind: "header",
          key: expr.predicate.key,
          op: expr.predicate.op,
          literal: compileLiteral(expr.predicate.value)
        },
        readsPayload: false,
        readsHeaders: true
      }
    case "text":
      return {
        node: {
          kind: "text",
          path: FieldPath.parse(expr.predicate.field),
          matcher: TextMatcher.of(expr.predicate)
        },
        readsPayload: true,
        readsHeaders: false
      }
    case "header_text":
      return {
        node: {
          kind: "header_text",
          key: expr.predicate.field,
          matcher: TextMatcher.of(expr.predicate)
        },
        readsPayload: false,
        readsHeaders: true
      }
  }
}

function compileLiteral(value: TypedValue): Literal {
  switch (value.kind) {
    case "null":
      return { kind: "null" }
    case "boolean":
      return { kind: "bool", value: value.value }
    case "int":
      return { kind: "int", value: BigInt(value.value) }
    case "long":
      return { kind: "int", value: value.value }
    case "double":
      return { kind: "float", value: value.value }
    case "string":
      return { kind: "text", value: value.value }
    case "list":
      return { kind: "list", items: value.value.map(compileLiteral) }
    case "float":
    case "time_micros":
    case "timestamp_micros":
    case "timestamp_tz_micros":
    case "date":
    case "decimal":
    case "uuid":
    case "fixed":
    case "binary":
    case "struct":
    case "map":
      throw new InvalidError(`unsupported filter literal ${value.kind}`)
  }
}

function compileCoercedLiteral(coerce: Coerce, value: TypedValue): CoercedLiteral {
  if (value.kind === "list") {
    return { kind: "list", items: value.value.map((item) => compileCoercedLiteral(coerce, item)) }
  }
  let comparable: Comparable | undefined
  if (coerce.kind === "timestamp") {
    const micros =
      value.kind === "string"
        ? timestampFormatMicrosFromText(coerce.format, value.value)
        : value.kind === "int" || value.kind === "long"
          ? timestampFormatMicrosFromInteger(coerce.format, BigInt(value.value))
          : undefined
    comparable = micros === undefined ? undefined : { kind: "instant", micros }
  } else {
    const decimal =
      value.kind === "string"
        ? ExactDecimal.parse(value.value)
        : value.kind === "int" || value.kind === "long"
          ? ExactDecimal.fromInteger(BigInt(value.value))
          : undefined
    comparable = decimal === undefined ? undefined : { kind: "decimal", decimal }
  }
  if (comparable === undefined) {
    throw new InvalidError(`literal ${value.kind} does not parse under its coercion`)
  }
  return comparable
}

class Context {
  private payload: JsonValue | PayloadFault | undefined
  /** A predicate saw a value of a type it cannot compare. */
  mismatched = false

  constructor(
    private readonly record: FilterRecord,
    private readonly limits: DecodeLimits,
    private readonly codec: ConsumerFilter["codec"],
    private readonly decoders: PayloadDecoders
  ) {}

  private decoded(): JsonValue {
    if (this.payload === undefined) {
      this.payload = this.declaresForeignCodec()
        ? new PayloadFault("foreign_codec")
        : this.codec === "json"
          ? decodeJsonPayload(this.record.payload, this.limits)
          : this.decoders.decode(this.record, this.limits)
    }
    if (this.payload instanceof PayloadFault) throw this.payload
    return this.payload
  }

  // `any` and codes this build does not know decode as usual.
  private declaresForeignCodec(): boolean {
    const header = this.record.headers.findLast(
      (candidate) => candidate.key === CONTENT_TYPE_HEADER
    )
    const expected = CODEC_CONTENT_TYPES[this.codec]
    if (header === undefined || expected === undefined) return false
    if (header.value.kind !== "uint" && header.value.kind !== "int") return false
    const code = Number(header.value.value)
    if (!KNOWN_CONTENT_TYPES.has(code) || code === ANY_CONTENT_TYPE) return false
    return code !== expected
  }

  payloadFault(): FaultReason | undefined {
    return this.payload instanceof PayloadFault ? this.payload.reason : undefined
  }

  truth(node: Node): Truth {
    const kind = node.node
    switch (kind.kind) {
      case "all":
        return this.combine(kind.ordered, "no_match", "match")
      case "any":
        return this.combine(kind.ordered, "match", "no_match")
      case "not":
        return negate(this.truth(kind.child))
      case "header":
        return this.noted(this.headerTruth(kind.key, kind.op, kind.literal))
      case "header_text":
        return this.noted(this.headerTextTruth(kind.key, kind.matcher))
      case "compare":
      case "coerced":
      case "present":
      case "absent":
      case "text":
        return this.noted(payloadTruth(kind, this.decoded()))
    }
  }

  private noted([truth, mismatched]: readonly [Truth, boolean]): Truth {
    if (mismatched) this.mismatched = true
    return truth
  }

  // `ordered` runs the children that read no payload first, so headers can
  // decide the node before the payload is ever decoded.
  private combine(ordered: readonly Node[], decisive: Truth, otherwise: Truth): Truth {
    let unknown = false
    for (const child of ordered) {
      const truth = this.truth(child)
      if (truth === decisive) return decisive
      if (truth === "unknown") unknown = true
    }
    return unknown ? "unknown" : otherwise
  }

  // The truth, and whether the header exists with a type the predicate cannot compare.
  private headerTruth(key: string, op: CmpOp, literal: Literal): readonly [Truth, boolean] {
    const header = this.record.headers.findLast((candidate) => candidate.key === key)
    if (header === undefined) return ["unknown", false]
    const scalar = headerScalar(header)
    const truth = compareScalar(scalar, undefined, op, literal)
    return [truth, truth === "unknown" && !comparable(scalar, undefined, op, literal)]
  }

  private headerTextTruth(key: string, matcher: TextMatcher): readonly [Truth, boolean] {
    const header = this.record.headers.findLast((candidate) => candidate.key === key)
    if (header === undefined) return ["unknown", false]
    const scalar = headerScalar(header)
    return scalar.kind === "text"
      ? [truthOf(matcher.matches(scalar.value)), false]
      : ["unknown", true]
  }

  explain(node: Node): ExplainNode {
    const kind = node.node
    if (kind.kind === "all" || kind.kind === "any" || kind.kind === "not") {
      const children = kind.kind === "not" ? [kind.child] : kind.children
      const explained = children.map((child) => this.explain(child))
      const truths = explained.flatMap((child) => (child.truth === undefined ? [] : [child.truth]))
      const complete = truths.length === explained.length
      const first = truths[0] ?? "unknown"
      const truth = !complete
        ? undefined
        : kind.kind === "all"
          ? fold(truths, "no_match", "match")
          : kind.kind === "any"
            ? fold(truths, "match", "no_match")
            : negate(first)
      return { label: kind.kind, ...(truth !== undefined ? { truth } : {}), children: explained }
    }
    let truth: Truth | undefined
    if (kind.kind === "header") {
      truth = this.headerTruth(kind.key, kind.op, kind.literal)[0]
    } else if (kind.kind === "header_text") {
      truth = this.headerTextTruth(kind.key, kind.matcher)[0]
    } else {
      try {
        truth = payloadTruth(kind as PayloadLeaf, this.decoded())[0]
      } catch (error) {
        if (!(error instanceof PayloadFault)) throw error
      }
    }
    return { label: leafLabel(kind), ...(truth !== undefined ? { truth } : {}), children: [] }
  }
}

function headerScalar(header: HeaderRef): Scalar {
  switch (header.value.kind) {
    case "bool":
      return { kind: "bool", value: header.value.value }
    case "int":
    case "uint":
      return { kind: "int", value: header.value.value }
    case "float":
      return { kind: "float", value: header.value.value }
    case "string":
      return { kind: "text", value: header.value.value }
    case "raw":
      try {
        return { kind: "text", value: UTF8_STRICT.decode(header.value.value) }
      } catch {
        return { kind: "composite" }
      }
  }
}

function fold(truths: readonly Truth[], decisive: Truth, otherwise: Truth): Truth {
  if (truths.includes(decisive)) return decisive
  if (truths.includes("unknown")) return "unknown"
  return otherwise
}

function negate(truth: Truth): Truth {
  return truth === "match" ? "no_match" : truth === "no_match" ? "match" : "unknown"
}

function truthOf(matched: boolean): Truth {
  return matched ? "match" : "no_match"
}

type PayloadLeaf = Extract<
  NodeKind,
  { readonly kind: "compare" | "coerced" | "present" | "absent" | "text" }
>

// The truth, and whether the field exists, is not null, and has a type the
// predicate cannot compare. A missing or null field is plain unknown.
function payloadTruth(kind: PayloadLeaf, value: JsonValue): readonly [Truth, boolean] {
  switch (kind.kind) {
    case "compare": {
      const found = resolve(value, kind.path)
      if (found === undefined) return [missingTruth(kind.op, kind.literal), false]
      const scalar = scalarOf(found)
      const truth = compareScalar(scalar, found, kind.op, kind.literal)
      return [truth, truth === "unknown" && !comparable(scalar, found, kind.op, kind.literal)]
    }
    case "coerced": {
      const found = resolve(value, kind.path)
      if (found === undefined) return ["unknown", false]
      const comparable = coerceValue(kind.coerce, found)
      return comparable === undefined
        ? ["unknown", found !== null]
        : [compareCoerced(comparable, kind.op, kind.literal), false]
    }
    case "text": {
      const found = resolve(value, kind.path)
      if (found === undefined || found === null) return ["unknown", false]
      return typeof found === "string"
        ? [truthOf(kind.matcher.matches(found)), false]
        : ["unknown", true]
    }
    case "present":
      return [truthOf(resolve(value, kind.path) !== undefined), false]
    case "absent":
      return [truthOf(resolve(value, kind.path) === undefined), false]
  }
}

// A missing field is unknown, except for the null tests: `eq null` matches a
// missing field and `ne null` does not.
function missingTruth(op: CmpOp, literal: Literal): Truth {
  if (literal.kind === "null" && op === "eq") return "match"
  if (literal.kind === "null" && op === "ne") return "no_match"
  return "unknown"
}

function comparable(
  scalar: Scalar,
  found: JsonValue | undefined,
  op: CmpOp,
  literal: Literal
): boolean {
  if (scalar.kind === "null" || literal.kind === "null") return true
  if (
    (scalar.kind === "int" || scalar.kind === "float") &&
    (literal.kind === "int" || literal.kind === "float")
  )
    return true
  if (
    (scalar.kind === "text" && literal.kind === "text") ||
    (scalar.kind === "bool" && literal.kind === "bool")
  )
    return true
  if (literal.kind === "list")
    return literal.items.some((item) => comparable(scalar, found, op, item))
  return scalar.kind === "composite" && op === "contains" && Array.isArray(found)
}

function compareScalar(
  scalar: Scalar,
  found: JsonValue | undefined,
  op: CmpOp,
  literal: Literal
): Truth {
  if (scalar.kind === "null") return literal.kind === "null" ? truthOf(op === "eq") : "unknown"
  if (literal.kind === "null") return truthOf(op === "ne")
  switch (op) {
    case "eq": {
      const equal = scalarEq(scalar, literal)
      return equal === undefined ? "unknown" : truthOf(equal)
    }
    case "ne": {
      const equal = scalarEq(scalar, literal)
      return equal === undefined ? "unknown" : truthOf(!equal)
    }
    case "lt":
    case "lte":
    case "gt":
    case "gte": {
      const ordering = scalarCmp(scalar, literal)
      if (ordering === undefined) return "unknown"
      return truthOf(
        op === "lt"
          ? ordering < 0
          : op === "lte"
            ? ordering <= 0
            : op === "gt"
              ? ordering > 0
              : ordering >= 0
      )
    }
    case "in": {
      if (literal.kind !== "list") return "unknown"
      let unknown = false
      for (const item of literal.items) {
        const equal = scalarEq(scalar, item)
        if (equal === true) return "match"
        if (equal === undefined) unknown = true
      }
      return unknown ? "unknown" : "no_match"
    }
    case "contains":
      if (scalar.kind === "text" && literal.kind === "text") {
        return truthOf(scalar.value.includes(literal.value))
      }
      if (scalar.kind === "composite" && Array.isArray(found)) {
        return truthOf(
          (found as readonly JsonValue[]).some(
            (element) => scalarEq(scalarOf(element), literal) === true
          )
        )
      }
      return "unknown"
    case "prefix":
      return scalar.kind === "text" && literal.kind === "text"
        ? truthOf(scalar.value.startsWith(literal.value))
        : "unknown"
  }
}

function scalarEq(scalar: Scalar, literal: Literal): boolean | undefined {
  if (scalar.kind === "bool" && literal.kind === "bool") return scalar.value === literal.value
  if (scalar.kind === "text" && literal.kind === "text") return scalar.value === literal.value
  const ordering = scalarCmp(scalar, literal)
  return ordering === undefined ? undefined : ordering === 0
}

function scalarCmp(scalar: Scalar, literal: Literal): number | undefined {
  if (scalar.kind === "int" && literal.kind === "int")
    return compareBigInt(scalar.value, literal.value)
  if (scalar.kind === "float" && literal.kind === "float")
    return compareDouble(scalar.value, literal.value)
  if (scalar.kind === "int" && literal.kind === "float") {
    const left = exactDouble(scalar.value)
    return left === undefined ? undefined : compareDouble(left, literal.value)
  }
  if (scalar.kind === "float" && literal.kind === "int") {
    const right = exactDouble(literal.value)
    return right === undefined ? undefined : compareDouble(scalar.value, right)
  }
  if (scalar.kind === "text" && literal.kind === "text")
    return compareCodePoints(scalar.value, literal.value)
  return undefined
}

function compareBigInt(left: bigint, right: bigint): number {
  return left < right ? -1 : left > right ? 1 : 0
}

function compareDouble(left: number, right: number): number | undefined {
  if (Number.isNaN(left) || Number.isNaN(right)) return undefined
  return left < right ? -1 : left > right ? 1 : 0
}

function exactDouble(value: bigint): number | undefined {
  const converted = Number(value)
  return Number.isFinite(converted) && BigInt(converted) === value ? converted : undefined
}

// Code-point order equals the UTF-8 byte order the server compares in.
function compareCodePoints(left: string, right: string): number {
  let leftIndex = 0
  let rightIndex = 0
  while (leftIndex < left.length && rightIndex < right.length) {
    const leftPoint = left.codePointAt(leftIndex) ?? 0
    const rightPoint = right.codePointAt(rightIndex) ?? 0
    if (leftPoint !== rightPoint) return leftPoint < rightPoint ? -1 : 1
    leftIndex += leftPoint > 0xffff ? 2 : 1
    rightIndex += rightPoint > 0xffff ? 2 : 1
  }
  const leftDone = leftIndex >= left.length
  const rightDone = rightIndex >= right.length
  return leftDone && rightDone ? 0 : leftDone ? -1 : 1
}

function compareCoerced(comparable: Comparable, op: CmpOp, literal: CoercedLiteral): Truth {
  const ordering = (target: CoercedLiteral): number | undefined => {
    if (comparable.kind === "instant" && target.kind === "instant") {
      return compareBigInt(comparable.micros, target.micros)
    }
    if (comparable.kind === "decimal" && target.kind === "decimal") {
      return comparable.decimal.compare(target.decimal)
    }
    return undefined
  }
  if (op === "in" && literal.kind === "list") {
    return truthOf(literal.items.some((item) => ordering(item) === 0))
  }
  const result = ordering(literal)
  if (result === undefined) return "unknown"
  switch (op) {
    case "eq":
      return truthOf(result === 0)
    case "ne":
      return truthOf(result !== 0)
    case "lt":
      return truthOf(result < 0)
    case "lte":
      return truthOf(result <= 0)
    case "gt":
      return truthOf(result > 0)
    case "gte":
      return truthOf(result >= 0)
    case "in":
    case "contains":
    case "prefix":
      return "unknown"
  }
}

function coerceValue(coerce: Coerce, value: JsonValue): Comparable | undefined {
  if (coerce.kind === "timestamp") {
    const micros = timestampOfValue(coerce.format, value)
    return micros === undefined ? undefined : { kind: "instant", micros }
  }
  let decimal: ExactDecimal | undefined
  if (typeof value === "string") decimal = ExactDecimal.parse(value)
  else if (typeof value === "bigint") decimal = ExactDecimal.fromInteger(value)
  else if (typeof value === "number") decimal = ExactDecimal.fromF64(value)
  return decimal === undefined ? undefined : { kind: "decimal", decimal }
}

function timestampOfValue(format: TimestampFormat, value: JsonValue): bigint | undefined {
  if (typeof value === "string") return timestampFormatMicrosFromText(format, value)
  if (format === "rfc3339") return undefined
  return typeof value === "bigint" ? timestampFormatMicrosFromInteger(format, value) : undefined
}

function scalarOf(value: JsonValue): Scalar {
  if (value === null) return { kind: "null" }
  if (typeof value === "boolean") return { kind: "bool", value }
  if (typeof value === "bigint") return { kind: "int", value }
  if (typeof value === "number") return { kind: "float", value }
  if (typeof value === "string") return { kind: "text", value }
  return { kind: "composite" }
}

function resolve(value: JsonValue, path: FieldPath): JsonValue | undefined {
  let current: JsonValue | undefined = value
  for (const segment of path.segments) {
    if (current === undefined) return undefined
    if (segment.kind === "key") {
      current =
        current instanceof Map
          ? (current as ReadonlyMap<string, JsonValue>).get(segment.key)
          : undefined
    } else {
      current = Array.isArray(current)
        ? (current as readonly JsonValue[])[segment.index]
        : undefined
    }
  }
  return current
}

function leafLabel(kind: NodeKind): string {
  switch (kind.kind) {
    case "compare":
      return `${kind.path.toString()} ${kind.op} ${literalLabel(kind.literal)}`
    case "coerced": {
      const coerce =
        kind.coerce.kind === "timestamp" ? `timestamp(${kind.coerce.format})` : "number"
      return `${coerce}(${kind.path.toString()}) ${kind.op} ${coercedLabel(kind.literal)}`
    }
    case "present":
      return `${kind.path.toString()} is present`
    case "absent":
      return `${kind.path.toString()} is absent`
    case "header":
      return `header ${kind.key} ${kind.op} ${literalLabel(kind.literal)}`
    case "text":
      return `${kind.path.toString()} ${kind.matcher.kind} ${kind.matcher.label()}`
    case "header_text":
      return `header ${kind.key} ${kind.matcher.kind} ${kind.matcher.label()}`
    case "all":
    case "any":
    case "not":
      return kind.kind
  }
}

function literalLabel(literal: Literal): string {
  switch (literal.kind) {
    case "null":
      return "null"
    case "bool":
    case "int":
    case "float":
      return String(literal.value)
    case "text":
      return JSON.stringify(literal.value)
    case "list":
      return `[${literal.items.map(literalLabel).join(", ")}]`
  }
}

function coercedLabel(literal: CoercedLiteral): string {
  switch (literal.kind) {
    case "instant":
      return `${literal.micros.toString()}us`
    case "decimal":
      return literal.decimal.toString()
    case "list":
      return `[${literal.items.map(coercedLabel).join(", ")}]`
  }
}
