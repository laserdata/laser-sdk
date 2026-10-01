import { sha256 } from "@noble/hashes/sha2.js"
import { Token, Type } from "cborg"
import { CodecError, InvalidError } from "../client/errors.js"
import {
  type CborMap,
  expectArray,
  expectBytes,
  expectMap,
  expectString,
  expectU32,
  expectU64,
  field,
  singleVariantTag
} from "./cbor.js"
import { type Grant, decodeGrant, encodeGrant } from "./authz.js"
import { FILTER_OP_VERSION } from "./codes.js"
import {
  MAX_FILTER_BYTES,
  MAX_FILTER_DEPTH,
  MAX_FILTER_DESCRIPTION_BYTES,
  MAX_FILTER_LIST_ITEMS,
  MAX_FILTER_NAME_BYTES,
  MAX_FILTER_NODES,
  MAX_FILTER_PATH_BYTES,
  MAX_FILTER_PATH_SEGMENTS,
  MAX_FILTER_PREVIEW_EXAMINED,
  MAX_FILTER_PREVIEW_RECORDS,
  MAX_FILTER_SAMPLE_BYTES,
  MAX_FILTER_SAMPLE_HEADERS,
  MAX_FILTER_SOURCE_NAME_BYTES,
  MAX_FILTER_STRING_BYTES,
  MAX_FILTER_CATALOG_PAGE,
  MAX_FILTERED_PAGE_BYTES,
  MAX_FILTERED_PAGE_RECORDS
} from "./limits.js"
import type { CmpOp, Predicate } from "./query.js"
import { type ResultCode, resultCodeFromWord, resultCodeWord } from "./result.js"
import { type TypedValue, decodeTypedValue } from "./schema.js"

/** The catalog byte limit a command logged before the field existed replays with. */
const DEFAULT_MAX_CATALOG_BYTES = 64n * 1024n * 1024n

export const FILTER_EVALUATOR_VERSION = 1
export const MAX_REGEX_PREDICATES = 4

const DIGEST_DOMAIN = new TextEncoder().encode("agdx.consumer-filter.v1\0")
const UTF8 = new TextEncoder()
const CMP_OPS: ReadonlySet<CmpOp> = new Set<CmpOp>([
  "eq",
  "ne",
  "lt",
  "lte",
  "gt",
  "gte",
  "in",
  "contains",
  "prefix"
])
const TIMESTAMP_FORMATS: ReadonlySet<TimestampFormat> = new Set<TimestampFormat>([
  "rfc3339",
  "epoch_seconds",
  "epoch_millis",
  "epoch_micros"
])
const STOP_REASONS: ReadonlySet<StopReason> = new Set<StopReason>([
  "filled",
  "budget",
  "end_of_visible",
  "fault",
  "oversized_record"
])
const FAULT_REASONS: ReadonlySet<FaultReason> = new Set<FaultReason>([
  "malformed",
  "too_large",
  "too_deep",
  "missing_schema",
  "schema_not_allowed",
  "schema_mismatch",
  "foreign_codec",
  "type_mismatch"
])
const VERDICTS: ReadonlySet<Verdict> = new Set<Verdict>(["selected", "rejected", "fault"])
const TRUTHS: ReadonlySet<Truth> = new Set<Truth>(["match", "no_match", "unknown"])
const FILTER_STATES: ReadonlySet<FilterState> = new Set<FilterState>([
  "active",
  "archived",
  "dropped"
])
const FILTER_ERROR_REASONS: ReadonlySet<FilterErrorReason> = new Set<FilterErrorReason>([
  "invalid_request",
  "unsupported",
  "version_skew",
  "not_found",
  "conflict",
  "source_changed",
  "membership_stale",
  "not_primary",
  "catalog_unavailable",
  "revision_disabled",
  "too_large",
  "unauthenticated",
  "forbidden",
  "unavailable",
  "capacity_exhausted",
  "backend",
  "unknown"
])

/** One step of a {@link FieldPath}: an object key or an array index. */
export type PathSegment =
  | { readonly kind: "key"; readonly key: string }
  | { readonly kind: "index"; readonly index: number }

/**
 * A validated location inside a decoded payload. Keys are separated by `.`,
 * array elements are selected with `[n]`, and a literal `.`, `[`, `]`, or `\`
 * inside a key is escaped with `\`. The text form is canonical.
 */
export class FieldPath {
  private constructor(readonly segments: readonly PathSegment[]) {}

  static parse(text: string): FieldPath {
    if (text.length === 0) throw new InvalidError("field path must not be empty")
    const bytes = UTF8.encode(text).byteLength
    if (bytes > MAX_FILTER_PATH_BYTES) {
      throw new InvalidError(
        `field path is ${String(bytes)}B, exceeds cap ${String(MAX_FILTER_PATH_BYTES)}B`
      )
    }
    const segments = new PathParser(text).segments()
    if (segments.length > MAX_FILTER_PATH_SEGMENTS) {
      throw new InvalidError(
        `field path \`${text}\` has ${String(segments.length)} segments, exceeds cap ${String(MAX_FILTER_PATH_SEGMENTS)}`
      )
    }
    return new FieldPath(segments)
  }

  toString(): string {
    return this.segments
      .map((segment, position) => {
        if (segment.kind === "index") return `[${String(segment.index)}]`
        const escaped = segment.key.replace(/[.[\]\\]/g, (character) => `\\${character}`)
        return position > 0 ? `.${escaped}` : escaped
      })
      .join("")
  }
}

class PathParser {
  private position = 0

  constructor(private readonly text: string) {}

  segments(): PathSegment[] {
    const segments: PathSegment[] = [this.peek() === "[" ? this.index() : this.key()]
    while (this.position < this.text.length) {
      const character = this.peek()
      if (character === ".") {
        this.position += 1
        segments.push(this.key())
      } else if (character === "[") {
        segments.push(this.index())
      } else {
        throw this.invalid("expected `.` or `[` after a segment")
      }
    }
    return segments
  }

  private key(): PathSegment {
    let key = ""
    while (this.position < this.text.length) {
      const character = this.peek()
      if (character === "." || character === "[") break
      if (character === "]") throw this.invalid("unescaped `]` inside a key")
      if (character === "\\") {
        this.position += 1
        const escaped = this.peek()
        if (escaped !== "." && escaped !== "[" && escaped !== "]" && escaped !== "\\") {
          throw this.invalid("`\\` must escape `.`, `[`, `]`, or `\\`")
        }
        key += escaped
        this.position += 1
        continue
      }
      const codePoint = this.text.codePointAt(this.position) ?? 0
      const next = String.fromCodePoint(codePoint)
      key += next
      this.position += next.length
    }
    if (key.length === 0) throw this.invalid("empty key")
    return { kind: "key", key }
  }

  private index(): PathSegment {
    this.position += 1
    const start = this.position
    while (/[0-9]/.test(this.peek() ?? "")) this.position += 1
    const digits = this.text.slice(start, this.position)
    if (this.peek() !== "]") throw this.invalid("an index must be digits closed by `]`")
    this.position += 1
    if (digits.length === 0 || (digits.length > 1 && digits.startsWith("0"))) {
      throw this.invalid("an index must be a canonical decimal number")
    }
    const index = Number(digits)
    if (!Number.isSafeInteger(index) || index > 0xffff_ffff) {
      throw this.invalid("an index must fit in 32 bits")
    }
    return { kind: "index", index }
  }

  private peek(): string | undefined {
    return this.text[this.position]
  }

  private invalid(reason: string): InvalidError {
    const byte = UTF8.encode(this.text.slice(0, this.position)).byteLength
    return new InvalidError(
      `field path \`${this.text}\` is invalid at byte ${String(byte)}: ${reason}`
    )
  }
}

/** How a timestamp is written in the payload or literal. */
export type TimestampFormat = "rfc3339" | "epoch_seconds" | "epoch_millis" | "epoch_micros"

/** An explicit value coercion. A value that does not parse under it is unknown. */
export type Coerce =
  { readonly kind: "timestamp"; readonly format: TimestampFormat } | { readonly kind: "number" }

export interface CoercedPredicate {
  readonly pred: Predicate
  readonly coerce: Coerce
}

export interface HeaderPredicate {
  readonly key: string
  readonly op: CmpOp
  readonly value: TypedValue
}

/**
 * How the payload is decoded before payload predicates run. `unknown` is a
 * codec this build does not know, for example one a newer catalog saved: it
 * decodes so listings stay readable, but it never validates or compiles.
 */
export type FilterCodec = "json" | "cbor" | "avro" | "protobuf" | "headers_only" | "unknown"

/** What happens to a record whose payload cannot be decoded. */
export type FaultPolicy = "stop" | "pass" | "drop"

/**
 * What happens to a record the filter cannot judge on its own terms: one in
 * another format (`foreignPolicy`), or one where a predicate's value has a
 * type it cannot compare (`mismatchPolicy`). `reject` skips the record,
 * `pass` delivers it marked unevaluated so the consumer decides.
 */
export type RecordPolicy = "reject" | "pass"

/** How a text predicate matches, from cheapest to most expensive. */
export type TextMatch = "equals" | "prefix" | "suffix" | "contains" | "glob" | "regex"

/**
 * A text match on a payload field, or on a header for `header_text`. The value
 * must be text: a missing value is unknown, another type is a type mismatch.
 */
export interface TextPredicate {
  readonly field: string
  readonly kind: TextMatch
  readonly pattern: string
  readonly caseInsensitive?: boolean
}

/**
 * A declarative predicate over one record. Comparison truth is three-valued: a
 * missing field or a type mismatch is unknown, and an unknown root selects
 * nothing.
 */
export type FilterExpr =
  | { readonly kind: "all"; readonly children: readonly FilterExpr[] }
  | { readonly kind: "any"; readonly children: readonly FilterExpr[] }
  | { readonly kind: "not"; readonly child: FilterExpr }
  | { readonly kind: "pred"; readonly predicate: Predicate }
  | { readonly kind: "pred_as"; readonly predicate: CoercedPredicate }
  | { readonly kind: "present"; readonly path: FieldPath }
  | { readonly kind: "absent"; readonly path: FieldPath }
  | { readonly kind: "header"; readonly predicate: HeaderPredicate }
  | { readonly kind: "text"; readonly predicate: TextPredicate }
  | { readonly kind: "header_text"; readonly predicate: TextPredicate }

/**
 * A literal in the builders. An integral `number` or a `bigint` is a `long`, a
 * fractional `number` a `double`. Pass a {@link TypedValue} for another kind.
 */
export type FilterLiteral =
  string | number | bigint | boolean | null | TypedValue | readonly FilterLiteral[]

/** Builders for the `FilterExpr` type. */
export const FilterExpr = {
  all(children: readonly FilterExpr[]): FilterExpr {
    return { kind: "all", children }
  },
  any(children: readonly FilterExpr[]): FilterExpr {
    return { kind: "any", children }
  },
  negate(child: FilterExpr): FilterExpr {
    return { kind: "not", child }
  },
  pred(fieldName: string, op: CmpOp, value: FilterLiteral): FilterExpr {
    return { kind: "pred", predicate: { field: fieldName, op, value: typedLiteral(value) } }
  },
  predAs(fieldName: string, op: CmpOp, value: FilterLiteral, coerce: Coerce): FilterExpr {
    return {
      kind: "pred_as",
      predicate: { pred: { field: fieldName, op, value: typedLiteral(value) }, coerce }
    }
  },
  present(path: string): FilterExpr {
    return { kind: "present", path: FieldPath.parse(path) }
  },
  absent(path: string): FilterExpr {
    return { kind: "absent", path: FieldPath.parse(path) }
  },
  header(key: string, op: CmpOp, value: FilterLiteral): FilterExpr {
    return { kind: "header", predicate: { key, op, value: typedLiteral(value) } }
  },
  text(fieldName: string, kind: TextMatch, pattern: string, caseInsensitive = false): FilterExpr {
    return { kind: "text", predicate: textPredicate(fieldName, kind, pattern, caseInsensitive) }
  },
  headerText(key: string, kind: TextMatch, pattern: string, caseInsensitive = false): FilterExpr {
    return { kind: "header_text", predicate: textPredicate(key, kind, pattern, caseInsensitive) }
  }
} as const

function textPredicate(
  fieldName: string,
  kind: TextMatch,
  pattern: string,
  caseInsensitive: boolean
): TextPredicate {
  return caseInsensitive
    ? { field: fieldName, kind, pattern, caseInsensitive: true }
    : { field: fieldName, kind, pattern }
}

/** One consumer filter: the expression plus everything that changes what it selects. */
export interface ConsumerFilter {
  readonly v: number
  readonly evaluatorVersion: number
  readonly expr: FilterExpr
  readonly codec: FilterCodec
  readonly faultPolicy: FaultPolicy
  /** Default `reject`. */
  readonly foreignPolicy?: RecordPolicy
  /** Default `reject`. */
  readonly mismatchPolicy?: RecordPolicy
  readonly schemaRefs: readonly number[]
}

/** Builders for the `ConsumerFilter` type. */
export const ConsumerFilter = {
  /** Decode the payload as JSON, with the default `stop` fault policy. */
  json(expr: FilterExpr, faultPolicy: FaultPolicy = "stop"): ConsumerFilter {
    return newFilter(expr, "json", faultPolicy)
  },
  cbor(expr: FilterExpr, faultPolicy: FaultPolicy = "stop"): ConsumerFilter {
    return newFilter(expr, "cbor", faultPolicy)
  },
  avro(
    expr: FilterExpr,
    schemaRefs: readonly number[],
    faultPolicy: FaultPolicy = "stop"
  ): ConsumerFilter {
    return {
      ...newFilter(expr, "avro", faultPolicy),
      schemaRefs: [...new Set(schemaRefs)].sort((left, right) => left - right)
    }
  },
  protobuf(
    expr: FilterExpr,
    schemaRefs: readonly number[],
    faultPolicy: FaultPolicy = "stop"
  ): ConsumerFilter {
    return {
      ...newFilter(expr, "protobuf", faultPolicy),
      schemaRefs: [...new Set(schemaRefs)].sort((left, right) => left - right)
    }
  },
  /** Never decode the payload. Only header predicates are allowed. */
  headersOnly(expr: FilterExpr, faultPolicy: FaultPolicy = "stop"): ConsumerFilter {
    return newFilter(expr, "headers_only", faultPolicy)
  },
  /** The same filter under another policy for records that do not decode. */
  withFaultPolicy(filter: ConsumerFilter, faultPolicy: FaultPolicy): ConsumerFilter {
    return { ...filter, faultPolicy }
  },
  /** The same filter under another policy for records of another codec. */
  withForeignPolicy(filter: ConsumerFilter, foreignPolicy: RecordPolicy): ConsumerFilter {
    return { ...filter, foreignPolicy }
  },
  /** The same filter under another policy for a field of an unexpected type. */
  withMismatchPolicy(filter: ConsumerFilter, mismatchPolicy: RecordPolicy): ConsumerFilter {
    return { ...filter, mismatchPolicy }
  }
} as const

function newFilter(expr: FilterExpr, codec: FilterCodec, faultPolicy: FaultPolicy): ConsumerFilter {
  return {
    v: FILTER_OP_VERSION,
    evaluatorVersion: FILTER_EVALUATOR_VERSION,
    expr,
    codec,
    faultPolicy,
    schemaRefs: []
  }
}

function typedLiteral(value: FilterLiteral): TypedValue {
  if (value === null) return { kind: "null" }
  if (typeof value === "string") return { kind: "string", value }
  if (typeof value === "boolean") return { kind: "boolean", value }
  if (typeof value === "bigint") return { kind: "long", value }
  if (typeof value === "number") {
    return Number.isInteger(value)
      ? { kind: "long", value: BigInt(value) }
      : { kind: "double", value }
  }
  if (Array.isArray(value)) {
    return { kind: "list", value: (value as readonly FilterLiteral[]).map(typedLiteral) }
  }
  return value as TypedValue
}

export function filterExprReadsPayload(expr: FilterExpr): boolean {
  switch (expr.kind) {
    case "all":
    case "any":
      return expr.children.some(filterExprReadsPayload)
    case "not":
      return filterExprReadsPayload(expr.child)
    case "header":
    case "header_text":
      return false
    case "pred":
    case "pred_as":
    case "present":
    case "absent":
    case "text":
      return true
  }
}

export function filterExprReadsHeaders(expr: FilterExpr): boolean {
  switch (expr.kind) {
    case "all":
    case "any":
      return expr.children.some(filterExprReadsHeaders)
    case "not":
      return filterExprReadsHeaders(expr.child)
    case "header":
    case "header_text":
      return true
    case "pred":
    case "pred_as":
    case "present":
    case "absent":
    case "text":
      return false
  }
}

/**
 * SHA-256 over a domain tag and the canonical JSON encoding of the filter, byte
 * for byte what every server and SDK computes.
 */
export function consumerFilterDigest(filter: ConsumerFilter): Uint8Array {
  const encoded = UTF8.encode(consumerFilterJson(filter))
  const input = new Uint8Array(DIGEST_DOMAIN.byteLength + encoded.byteLength)
  input.set(DIGEST_DOMAIN, 0)
  input.set(encoded, DIGEST_DOMAIN.byteLength)
  return sha256(input)
}

/** The canonical JSON text of a filter, the encoding its digest covers. */
export function consumerFilterJson(filter: ConsumerFilter): string {
  return writeJson(encodeConsumerFilterTree(filter, "json"))
}

/** Parse a filter from its canonical JSON text. */
export function decodeConsumerFilterJson(text: string): ConsumerFilter {
  return decodeConsumerFilter(parseCanonicalJson(text), "ConsumerFilter")
}

export function validateConsumerFilter(filter: ConsumerFilter): void {
  if (filter.v !== FILTER_OP_VERSION) {
    throw new InvalidError(
      `filter version ${String(filter.v)} is not supported, expected ${String(FILTER_OP_VERSION)}`
    )
  }
  if (filter.evaluatorVersion !== FILTER_EVALUATOR_VERSION) {
    throw new InvalidError(
      `evaluator version ${String(filter.evaluatorVersion)} is not supported, expected ${String(FILTER_EVALUATOR_VERSION)}`
    )
  }
  const schemaCodec = filter.codec === "avro" || filter.codec === "protobuf"
  if (schemaCodec === (filter.schemaRefs.length === 0)) {
    throw new InvalidError(
      "Avro and Protobuf require schema_refs, other codecs require no schema_refs"
    )
  }
  filter.schemaRefs.forEach((id, index) => {
    if (
      !Number.isInteger(id) ||
      id < 0 ||
      id > 0xffffffff ||
      (index > 0 && id <= (filter.schemaRefs[index - 1] ?? -1))
    ) {
      throw new InvalidError("schema_refs must be sorted distinct uint32 values")
    }
  })
  const encodedBytes = UTF8.encode(consumerFilterJson(filter)).byteLength
  if (encodedBytes > MAX_FILTER_BYTES) {
    throw new InvalidError(
      `filter is ${String(encodedBytes)}B, exceeds cap ${String(MAX_FILTER_BYTES)}B`
    )
  }
  if (filter.codec === "unknown") {
    throw new InvalidError("the filter codec is not supported by this build")
  }
  if (filter.codec === "headers_only" && filterExprReadsPayload(filter.expr)) {
    throw new InvalidError("a headers_only filter cannot use payload predicates")
  }
  const nodes = { count: 0 }
  validateNode(filter.expr, 1, nodes)
  if (compiledTextPredicates(filter.expr) > MAX_REGEX_PREDICATES) {
    throw new InvalidError(
      `a filter holds at most ${String(MAX_REGEX_PREDICATES)} compiled text predicates (glob or regex)`
    )
  }
}

function compiledTextPredicates(expr: FilterExpr): number {
  switch (expr.kind) {
    case "all":
    case "any":
      return expr.children.reduce((total, child) => total + compiledTextPredicates(child), 0)
    case "not":
      return compiledTextPredicates(expr.child)
    case "text":
    case "header_text":
      return Number(expr.predicate.kind === "regex" || expr.predicate.kind === "glob")
    case "header":
    case "absent":
    case "pred":
    case "present":
    case "pred_as":
      return 0
  }
}

function validateNode(expr: FilterExpr, depth: number, nodes: { count: number }): void {
  if (depth > MAX_FILTER_DEPTH) {
    throw new InvalidError(`filter nesting exceeds depth ${String(MAX_FILTER_DEPTH)}`)
  }
  countNodes(nodes, 1)
  switch (expr.kind) {
    case "all":
    case "any":
      if (expr.children.length === 0) {
        throw new InvalidError("`all` and `any` need at least one child")
      }
      for (const child of expr.children) validateNode(child, depth + 1, nodes)
      return
    case "not":
      validateNode(expr.child, depth + 1, nodes)
      return
    case "pred":
      FieldPath.parse(expr.predicate.field)
      validateLiteral(expr.predicate.op, expr.predicate.value, "payload", nodes)
      return
    case "pred_as":
      FieldPath.parse(expr.predicate.pred.field)
      validateCoerced(expr.predicate, nodes)
      return
    case "present":
    case "absent":
      return
    case "header":
      validateHeaderKey(expr.predicate.key)
      validateLiteral(expr.predicate.op, expr.predicate.value, "header", nodes)
      return
    case "text":
      FieldPath.parse(expr.predicate.field)
      validateTextPredicate(expr.predicate)
      return
    case "header_text":
      validateHeaderKey(expr.predicate.field)
      validateTextPredicate(expr.predicate)
  }
}

function validateHeaderKey(key: string): void {
  const keyBytes = UTF8.encode(key).byteLength
  if (keyBytes === 0 || keyBytes > MAX_FILTER_PATH_BYTES) {
    throw new InvalidError(`header key must be 1..=${String(MAX_FILTER_PATH_BYTES)} bytes`)
  }
}

// The server compiles regexes with the Rust engine and is the authority on
// their syntax. These checks refuse what no SDK accepts, so a filter fails here
// before it travels.
function validateTextPredicate(predicate: TextPredicate): void {
  if (UTF8.encode(predicate.pattern).byteLength > MAX_FILTER_STRING_BYTES) {
    throw new InvalidError(`a text pattern exceeds ${String(MAX_FILTER_STRING_BYTES)}B`)
  }
  if (predicate.kind === "glob" && globTokens(predicate.pattern) === undefined) {
    throw new InvalidError("a glob pattern cannot end with an unescaped backslash")
  }
  if (predicate.kind === "regex") {
    const construct = unportableRegexConstruct(predicate.pattern)
    if (construct !== undefined) {
      throw new InvalidError(`regex ${construct} is not supported, write the pattern without it`)
    }
  }
}

/** One glob element after escapes are resolved: any run, any one, or a literal. */
export type GlobToken =
  | { readonly kind: "run" }
  | { readonly kind: "one" }
  | { readonly kind: "literal"; readonly value: string }

export function globTokens(pattern: string): GlobToken[] | undefined {
  const tokens: GlobToken[] = []
  const characters = Array.from(pattern)
  for (let index = 0; index < characters.length; index += 1) {
    const character = characters[index]
    if (character === "*") tokens.push({ kind: "run" })
    else if (character === "?") tokens.push({ kind: "one" })
    else if (character === "\\") {
      const escaped = characters[index + 1]
      if (escaped === undefined) return undefined
      tokens.push({ kind: "literal", value: escaped })
      index += 1
    } else tokens.push({ kind: "literal", value: character ?? "" })
  }
  return tokens
}

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

function countNodes(nodes: { count: number }, added: number): void {
  nodes.count += added
  if (nodes.count > MAX_FILTER_NODES) {
    throw new InvalidError(`filter has more than ${String(MAX_FILTER_NODES)} nodes`)
  }
}

function validateLiteral(
  op: CmpOp,
  value: TypedValue,
  site: "payload" | "header",
  nodes: { count: number }
): void {
  if (value.kind === "null") {
    if ((op === "eq" || op === "ne") && site === "payload") return
    throw new InvalidError(
      site === "payload"
        ? "a null literal only supports eq and ne"
        : "a header predicate cannot compare against null"
    )
  }
  if (op === "in") {
    if (value.kind !== "list") throw new InvalidError("`in` expects a list literal")
    validateListSize(value.value.length)
    countNodes(nodes, value.value.length)
    for (const item of value.value) {
      if (item.kind === "null" || item.kind === "list") {
        throw new InvalidError("`in` list items must be non-null scalars")
      }
      validateScalar(item)
    }
    return
  }
  if (value.kind === "list") throw new InvalidError("a list literal only supports `in`")
  if ((op === "lt" || op === "lte" || op === "gt" || op === "gte") && value.kind === "boolean") {
    throw new InvalidError("ordered comparisons need a number or string literal")
  }
  if (op === "prefix" && value.kind !== "string") {
    throw new InvalidError("`prefix` expects a string literal")
  }
  validateScalar(value)
}

function validateListSize(length: number): void {
  if (length === 0 || length > MAX_FILTER_LIST_ITEMS) {
    throw new InvalidError(`an \`in\` list needs 1..=${String(MAX_FILTER_LIST_ITEMS)} items`)
  }
}

function validateScalar(value: TypedValue): void {
  switch (value.kind) {
    case "boolean":
    case "int":
    case "long":
      return
    case "double":
      if (!Number.isFinite(value.value)) throw new InvalidError("a double literal must be finite")
      return
    case "string":
      if (UTF8.encode(value.value).byteLength > MAX_FILTER_STRING_BYTES) {
        throw new InvalidError(`a string literal exceeds ${String(MAX_FILTER_STRING_BYTES)}B`)
      }
      return
    case "null":
    case "float":
    case "list":
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
      throw new InvalidError(
        "supported literals are null, boolean, int, long, double, string, and list"
      )
  }
}

function validateCoerced(coerced: CoercedPredicate, nodes: { count: number }): void {
  const op = coerced.pred.op
  if (op === "contains" || op === "prefix") {
    throw new InvalidError("a coerced predicate supports eq, ne, lt, lte, gt, gte, and in")
  }
  let items: readonly TypedValue[]
  if (coerced.pred.value.kind === "list" && op === "in") {
    validateListSize(coerced.pred.value.value.length)
    countNodes(nodes, coerced.pred.value.value.length)
    items = coerced.pred.value.value
  } else if (op === "in") {
    throw new InvalidError("`in` expects a list literal")
  } else if (coerced.pred.value.kind === "list") {
    throw new InvalidError("a list literal only supports `in`")
  } else {
    items = [coerced.pred.value]
  }
  for (const item of items) {
    if (coercedLiteralParses(coerced.coerce, item)) continue
    throw new InvalidError(`literal ${typedValueLabel(item)} does not parse under its coercion`)
  }
}

// Mirrors the evaluator's compile step, so validation and evaluation agree on
// which literals a coercion accepts.
function coercedLiteralParses(coerce: Coerce, value: TypedValue): boolean {
  if (coerce.kind === "number") {
    if (value.kind === "int" || value.kind === "long") return true
    return value.kind === "string" && ExactDecimal.parse(value.value) !== undefined
  }
  if (value.kind === "string") {
    return timestampFromText(coerce.format, value.value) !== undefined
  }
  if (value.kind === "int" || value.kind === "long") {
    return timestampFromInteger(coerce.format, BigInt(value.value)) !== undefined
  }
  return false
}

function typedValueLabel(value: TypedValue): string {
  switch (value.kind) {
    case "string":
      return JSON.stringify(value.value)
    case "null":
      return "null"
    case "boolean":
    case "int":
    case "long":
    case "double":
    case "float":
      return String(value.value)
    case "list":
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
      return value.kind
  }
}

const MAX_DECIMAL_DIGITS = 512

/**
 * An exact decimal number, compared without floating-point rounding. Leading
 * and trailing zeros are normalized away, so `1.50` equals `1.5` and `-0`
 * equals `0`.
 */
export class ExactDecimal {
  private constructor(
    private readonly negative: boolean,
    private readonly integer: string,
    private readonly fraction: string
  ) {}

  /** Parse `[+-]digits[.digits]`: no exponent, no whitespace, at most 512 digits. */
  static parse(text: string): ExactDecimal | undefined {
    const match = /^([+-]?)(\d*)(?:\.(\d+))?$/.exec(text)
    if (match === null) return undefined
    const [, sign = "", integer = "", fraction = ""] = match
    if (integer.length === 0 && fraction.length === 0) return undefined
    if (integer.length + fraction.length > MAX_DECIMAL_DIGITS) return undefined
    return ExactDecimal.normalized(sign === "-", integer, fraction)
  }

  static fromInteger(value: bigint): ExactDecimal {
    const negative = value < 0n
    return ExactDecimal.normalized(negative, (negative ? -value : value).toString(), "")
  }

  /** The exact value of a finite double, `undefined` for infinities and NaN. */
  static fromDouble(value: number): ExactDecimal | undefined {
    if (!Number.isFinite(value)) return undefined
    if (value === 0) return ExactDecimal.normalized(false, "", "")
    const [mantissa = "", exponentText = "0"] = Math.abs(value).toExponential().split("e")
    const digits = mantissa.replace(".", "")
    const point = Number(exponentText) + 1
    const [integer, fraction] =
      point <= 0
        ? ["", "0".repeat(-point) + digits]
        : point >= digits.length
          ? [digits + "0".repeat(point - digits.length), ""]
          : [digits.slice(0, point), digits.slice(point)]
    return ExactDecimal.normalized(value < 0, integer, fraction)
  }

  private static normalized(negative: boolean, integer: string, fraction: string): ExactDecimal {
    // Linear scans, not `/^0+/` and `/0+$/`: a suffix-anchored run on library
    // input backtracks quadratically on long zero runs.
    let start = 0
    while (start < integer.length && integer.charCodeAt(start) === 48) start += 1
    const trimmedInteger = integer.slice(start)
    let end = fraction.length
    while (end > 0 && fraction.charCodeAt(end - 1) === 48) end -= 1
    const trimmedFraction = fraction.slice(0, end)
    const zero = trimmedInteger.length === 0 && trimmedFraction.length === 0
    return new ExactDecimal(negative && !zero, trimmedInteger, trimmedFraction)
  }

  /** `-1`, `0`, or `1` as this value is below, equal to, or above `other`. */
  compare(other: ExactDecimal): number {
    if (!this.negative && other.negative) return 1
    if (this.negative && !other.negative) return -1
    const magnitude = ExactDecimal.magnitude(this, other)
    return this.negative ? -magnitude : magnitude
  }

  private static magnitude(left: ExactDecimal, right: ExactDecimal): number {
    if (left.integer.length !== right.integer.length) {
      return left.integer.length < right.integer.length ? -1 : 1
    }
    if (left.integer !== right.integer) return left.integer < right.integer ? -1 : 1
    if (left.fraction !== right.fraction) return left.fraction < right.fraction ? -1 : 1
    return 0
  }

  toString(): string {
    const integer = this.integer.length === 0 ? "0" : this.integer
    const fraction = this.fraction.length === 0 ? "" : `.${this.fraction}`
    return `${this.negative ? "-" : ""}${integer}${fraction}`
  }
}

const MICROS_PER_SECOND = 1_000_000n
const MICROS_PER_MILLI = 1_000n
const SECONDS_PER_DAY = 86_400n
const I64_MIN = -(1n << 63n)
const I64_MAX = (1n << 63n) - 1n

function inI64(value: bigint): bigint | undefined {
  return value >= I64_MIN && value <= I64_MAX ? value : undefined
}

/** Microseconds since the epoch for a text value, `undefined` when it does not parse exactly. */
export function timestampFromText(format: TimestampFormat, text: string): bigint | undefined {
  if (format === "rfc3339") return rfc3339Micros(text)
  if (!/^[+-]?\d+$/.test(text)) return undefined
  return timestampFromInteger(format, BigInt(text))
}

/** Microseconds since the epoch for an integer value, `undefined` on overflow or for RFC 3339. */
export function timestampFromInteger(format: TimestampFormat, value: bigint): bigint | undefined {
  if (inI64(value) === undefined) return undefined
  switch (format) {
    case "rfc3339":
      return undefined
    case "epoch_seconds":
      return inI64(value * MICROS_PER_SECOND)
    case "epoch_millis":
      return inI64(value * MICROS_PER_MILLI)
    case "epoch_micros":
      return value
  }
}

function rfc3339Micros(text: string): bigint | undefined {
  if (text.length < 20 || /[^ -~]/.test(text)) return undefined
  const digitsAt = (start: number, length: number): number | undefined => {
    const slice = text.slice(start, start + length)
    return slice.length === length && /^\d+$/.test(slice) ? Number(slice) : undefined
  }
  const year = digitsAt(0, 4)
  const month = digitsAt(5, 2)
  const day = digitsAt(8, 2)
  const hour = digitsAt(11, 2)
  const minute = digitsAt(14, 2)
  const second = digitsAt(17, 2)
  if (
    year === undefined ||
    month === undefined ||
    day === undefined ||
    hour === undefined ||
    minute === undefined ||
    second === undefined
  ) {
    return undefined
  }
  if (
    text[4] !== "-" ||
    text[7] !== "-" ||
    (text[10] !== "T" && text[10] !== "t") ||
    text[13] !== ":" ||
    text[16] !== ":"
  ) {
    return undefined
  }
  if (
    month < 1 ||
    month > 12 ||
    day === 0 ||
    day > daysInMonth(year, month) ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  ) {
    return undefined
  }
  let position = 19
  let fractionMicros = 0n
  if (text[position] === ".") {
    position += 1
    const start = position
    while (/\d/.test(text[position] ?? "")) position += 1
    const fraction = text.slice(start, position)
    if (fraction.length === 0 || fraction.length > 9) return undefined
    const kept = fraction.slice(0, 6)
    if (/[1-9]/.test(fraction.slice(6))) return undefined
    fractionMicros = BigInt(kept) * 10n ** BigInt(6 - kept.length)
  }
  let offsetSeconds: number
  const marker = text[position]
  if ((marker === "Z" || marker === "z") && position + 1 === text.length) {
    offsetSeconds = 0
  } else if ((marker === "+" || marker === "-") && position + 6 === text.length) {
    const offsetHour = digitsAt(position + 1, 2)
    const offsetMinute = digitsAt(position + 4, 2)
    if (
      offsetHour === undefined ||
      offsetMinute === undefined ||
      text[position + 3] !== ":" ||
      offsetHour > 23 ||
      offsetMinute > 59
    ) {
      return undefined
    }
    const magnitude = offsetHour * 3600 + offsetMinute * 60
    offsetSeconds = marker === "-" ? -magnitude : magnitude
  } else {
    return undefined
  }
  const days = daysFromCivil(BigInt(year), BigInt(month), BigInt(day))
  const seconds =
    days * SECONDS_PER_DAY + BigInt(hour * 3600 + minute * 60 + second) - BigInt(offsetSeconds)
  return inI64(seconds * MICROS_PER_SECOND + fractionMicros)
}

function isLeapYear(year: number): boolean {
  return (year % 4 === 0 && year % 100 !== 0) || year % 400 === 0
}

function daysInMonth(year: number, month: number): number {
  if ([1, 3, 5, 7, 8, 10, 12].includes(month)) return 31
  if ([4, 6, 9, 11].includes(month)) return 30
  return isLeapYear(year) ? 29 : 28
}

// Days since 1970-01-01 for a proleptic Gregorian date (H. Hinnant's algorithm).
function daysFromCivil(yearInput: bigint, month: bigint, day: bigint): bigint {
  const year = month <= 2n ? yearInput - 1n : yearInput
  const era = (year >= 0n ? year : year - 399n) / 400n
  const yearOfEra = year - era * 400n
  const shiftedMonth = month > 2n ? month - 3n : month + 9n
  const dayOfYear = (153n * shiftedMonth + 2n) / 5n + day - 1n
  const dayOfEra = yearOfEra * 365n + yearOfEra / 4n - yearOfEra / 100n + dayOfYear
  return era * 146_097n + dayOfEra - 719_468n
}

/** The stream and topic a filtered read or preview addresses, by name. */
export interface FilterSource {
  readonly stream: string
  readonly topic: string
}

/** Whose progress a filtered read follows. */
export type FilterConsumer =
  | { readonly kind: "group_id"; readonly id: bigint }
  | { readonly kind: "consumer"; readonly name: string }
  | { readonly kind: "group"; readonly name: string }

/** Which filter a read executes. */
export type FilterRef =
  | { readonly kind: "inline"; readonly filter: ConsumerFilter }
  | { readonly kind: "revision"; readonly filterId: number; readonly revision: number }
  | { readonly kind: "bound" }

/** Which replica serves the read. */
export type ReadMode = "primary" | "local"

/** The exact source history a page was read from. */
export interface SourceGeneration {
  readonly streamId: number
  readonly streamCreatedAtMicros: bigint
  readonly topicId: number
  readonly topicCreatedAtMicros: bigint
  readonly partitionId: number
  readonly partitionCreatedRevision: bigint
  readonly purgeGeneration: bigint
}

export interface Continuation {
  readonly groupId?: bigint
  readonly nextScanOffset: bigint
  readonly generation: SourceGeneration
  readonly digest: Uint8Array
  /**
   * The route the page was read on. A continuation from a local page never
   * resumes a primary read, because a local replica can lag a purge.
   */
  readonly readMode: ReadMode
}

/** Where a filtered read starts. */
export type FilteredStart =
  | { readonly kind: "next" }
  | { readonly kind: "first" }
  | { readonly kind: "last" }
  | { readonly kind: "offset"; readonly offset: bigint }
  | { readonly kind: "timestamp"; readonly micros: bigint }
  | { readonly kind: "continue"; readonly continuation: Continuation }

export interface FilteredPollRequest {
  readonly v: number
  readonly source: FilterSource
  readonly partitionId: number
  readonly consumer: FilterConsumer
  readonly filter: FilterRef
  readonly start: FilteredStart
  readonly count: number
  readonly maxReplyBytes: number
  readonly readMode: ReadMode
}

export type StopReason = "filled" | "budget" | "end_of_visible" | "fault" | "oversized_record"
export type FaultReason =
  | "malformed"
  | "too_large"
  | "too_deep"
  | "missing_schema"
  | "schema_not_allowed"
  | "schema_mismatch"
  | "foreign_codec"
  | "type_mismatch"

export interface RecordFault {
  readonly offset: bigint
  readonly reason: FaultReason
}

export interface AppliedPolicy {
  readonly groupId?: bigint
  readonly digest: Uint8Array
  readonly filterId?: number
  readonly revision?: number
}

/** One page of a filtered read. `records` is a standard polled-messages body. */
export interface FilteredPage {
  readonly v: number
  readonly partitionId: number
  readonly policy: AppliedPolicy
  readonly generation: SourceGeneration
  readonly readMode: ReadMode
  readonly nextScanOffset?: bigint
  readonly safeAckOffset?: bigint
  readonly frontier: bigint
  readonly examined: number
  readonly matched: number
  readonly stop: StopReason
  readonly fault?: RecordFault
  readonly evaluationLimits?: { readonly maxPayloadBytes: number; readonly maxDepth: number }
  readonly unevaluated: readonly bigint[]
  readonly records: Uint8Array
}

/** The start of the next request: continue after `page`, or repeat `original`. */
export function nextFilteredStart(page: FilteredPage, original: FilteredStart): FilteredStart {
  if (page.nextScanOffset === undefined) return original
  return {
    kind: "continue",
    continuation: {
      nextScanOffset: page.nextScanOffset,
      generation: page.generation,
      digest: page.policy.digest,
      readMode: page.readMode,
      ...(page.policy.groupId !== undefined ? { groupId: page.policy.groupId } : {})
    }
  }
}

export interface FilteredAck {
  readonly groupId?: bigint
  readonly v: number
  readonly source: FilterSource
  readonly partitionId: number
  readonly consumer: FilterConsumer
  readonly generation: SourceGeneration
  readonly digest: Uint8Array
  readonly offset: bigint
}

export interface AckReceipt {
  readonly partitionId: number
  readonly offset: bigint
  readonly generation: SourceGeneration
}

export interface FilterPreviewRequest {
  readonly v: number
  readonly source: FilterSource
  readonly partitionId: number
  readonly filter: FilterRef
  readonly fromOffset: bigint
  readonly maxExamined: number
  readonly maxRecords: number
  readonly explain: boolean
}

export type Verdict = "selected" | "rejected" | "fault"
export type Truth = "match" | "no_match" | "unknown"

export interface ExplainNode {
  readonly label: string
  readonly truth?: Truth
  readonly children: readonly ExplainNode[]
}

export interface FilterExplanation {
  readonly verdict: Verdict
  readonly fault?: FaultReason
  readonly root: ExplainNode
}

export interface PreviewRecord {
  readonly offset: bigint
  readonly timestampMicros: bigint
  readonly verdict: Verdict
  readonly fault?: FaultReason
  readonly payloadText: string
  readonly payloadTruncated: boolean
  readonly explanation?: FilterExplanation
}

export interface FilterPreview {
  readonly v: number
  readonly partitionId: number
  readonly policy: AppliedPolicy
  readonly readMode: ReadMode
  readonly examined: number
  readonly matched: number
  readonly faults: number
  readonly stop: StopReason
  readonly nextOffset?: bigint
  readonly frontier: bigint
  readonly records: readonly PreviewRecord[]
}

/** A typed user header value as the evaluator reads it. */
export type HeaderScalar =
  | { readonly kind: "bool"; readonly value: boolean }
  | { readonly kind: "int"; readonly value: bigint }
  | { readonly kind: "uint"; readonly value: bigint }
  | { readonly kind: "float"; readonly value: number }
  | { readonly kind: "string"; readonly value: string }
  | { readonly kind: "raw"; readonly value: Uint8Array }

export interface FilterHeader {
  readonly key: string
  readonly value: HeaderScalar
}

export interface FilterTestRequest {
  readonly v: number
  readonly filter: FilterRef
  readonly payload: Uint8Array
  readonly headers: readonly FilterHeader[]
}

export interface FilterTestResult {
  readonly v: number
  readonly policy: AppliedPolicy
  readonly explanation: FilterExplanation
}

export interface FilterValidation {
  readonly v: number
  readonly digest: Uint8Array
  readonly readsPayload: boolean
  readonly readsHeaders: boolean
}

export type FilterErrorReason =
  | "invalid_request"
  | "unsupported"
  | "version_skew"
  | "not_found"
  | "conflict"
  | "source_changed"
  | "membership_stale"
  | "not_primary"
  | "catalog_unavailable"
  | "revision_disabled"
  | "too_large"
  | "unauthenticated"
  | "forbidden"
  | "unavailable"
  | "capacity_exhausted"
  | "backend"
  /** A reason this build does not know. The error's `code` still classifies it. */
  | "unknown"

/** A consumer-filter failure as the server reports it. */
export interface FilterError {
  readonly code: ResultCode
  readonly reason: FilterErrorReason
  readonly message: string
}

export type FilterOutcome =
  | { readonly kind: "page"; readonly page: FilteredPage }
  | { readonly kind: "acknowledged"; readonly receipt: AckReceipt }
  | { readonly kind: "preview"; readonly preview: FilterPreview }
  | { readonly kind: "tested"; readonly result: FilterTestResult }
  | { readonly kind: "validated"; readonly validation: FilterValidation }

export type FilterReply =
  | { readonly kind: "ok"; readonly outcome: FilterOutcome }
  | { readonly kind: "err"; readonly error: FilterError }

export type FilterState = "active" | "archived" | "dropped"

export interface FilterGroupIdentity {
  readonly streamId: number
  readonly streamCreatedAtMicros: bigint
  readonly topicId: number
  readonly topicCreatedAtMicros: bigint
  readonly groupId: bigint
}

export interface FilterGroupRef {
  readonly stream: string
  readonly topic: string
  readonly group: string
}

export interface FilterRevisionInfo {
  readonly enabled: boolean
  readonly revision: number
  readonly digest: Uint8Array
  readonly filter: ConsumerFilter
  readonly createdAtMicros: bigint
}

export interface FilterSummary {
  readonly id: number
  readonly name: string
  readonly description: string
  readonly state: FilterState
  readonly latestRevision: number
  readonly latestDigest: Uint8Array
  readonly codec: FilterCodec
  readonly bindings: number
  readonly createdAtMicros: bigint
  readonly updatedAtMicros: bigint
}

export interface FilterBinding {
  readonly group: FilterGroupRef
  readonly identity: FilterGroupIdentity
  readonly filterId: number
  readonly revision: number
  readonly digest: Uint8Array
  readonly boundAtMicros: bigint
}

export interface FilterDetail {
  readonly summary: FilterSummary
  readonly latest: FilterRevisionInfo
  readonly bindings: readonly FilterBinding[]
}

export type FilterMutation =
  | {
      readonly kind: "set_revision_enabled"
      readonly filterId: number
      readonly revision: number
      readonly enabled: boolean
    }
  | {
      readonly kind: "register"
      readonly name: string
      readonly description: string
      readonly filter: ConsumerFilter
    }
  | {
      readonly kind: "revise"
      readonly filterId: number
      readonly expectedRevision: number
      readonly filter: ConsumerFilter
    }
  | { readonly kind: "describe"; readonly filterId: number; readonly description: string }
  | { readonly kind: "archive"; readonly filterId: number }
  | { readonly kind: "drop"; readonly filterId: number }
  | {
      readonly kind: "bind"
      readonly group: FilterGroupRef
      readonly filterId: number
      readonly revision: number
    }
  | {
      readonly kind: "unbind"
      readonly group: FilterGroupRef
      readonly expectedDigest: Uint8Array
      readonly expectedIdentity?: FilterGroupIdentity
    }

export interface FilterMutationRequest {
  readonly v: number
  readonly operationId: bigint
  readonly mutation: FilterMutation
}

/**
 * The mutation the catalog appends to its control log, stamped by the
 * streaming server with the actor and group identity, then by the plane with
 * the caller's grants and, for a registration, the allocated filter id.
 */
export interface FilterCatalogLimits {
  readonly maxDefinitions: number
  readonly maxTombstones: number
  readonly maxRevisions: number
  readonly maxGroupIdentities: number
  /** Encoded bytes of every stored filter revision together. */
  readonly maxCatalogBytes: bigint
}

export interface FilterCatalogCommand {
  readonly limits?: FilterCatalogLimits
  readonly operationId: bigint
  readonly actorUserId: number
  readonly mutation: FilterMutation
  readonly identity?: FilterGroupIdentity
  /** The id a registration takes, allocated before the append. */
  readonly filterId?: number
  /** The grants the mutation is authorized with again when it applies. */
  readonly grants?: readonly Grant[]
}

export interface FilterRevisionRef {
  readonly filterId: number
  readonly revision: number
  readonly digest: Uint8Array
}

export type FilterMutationResult =
  | {
      readonly kind: "revision_state"
      readonly filterId: number
      readonly revision: number
      readonly enabled: boolean
    }
  | { readonly kind: "registered"; readonly revision: FilterRevisionRef }
  | { readonly kind: "revised"; readonly revision: FilterRevisionRef }
  | { readonly kind: "described"; readonly filterId: number }
  | { readonly kind: "archived"; readonly filterId: number }
  | { readonly kind: "dropped"; readonly filterId: number }
  | { readonly kind: "bound"; readonly binding: FilterBinding }
  | { readonly kind: "unbound"; readonly binding: FilterBinding }

export type FilterMutationStatus =
  | { readonly kind: "pending" }
  | { readonly kind: "applied"; readonly result: FilterMutationResult }
  | { readonly kind: "rejected"; readonly error: FilterError }

export interface FilterMutationOutcome {
  readonly v: number
  readonly operationId: bigint
  readonly status: FilterMutationStatus
}

export interface GetFilter {
  readonly v: number
  readonly filterId: number
}

export interface ListFilters {
  readonly v: number
  readonly nameContains?: string
  readonly state?: FilterState
  /** Only filters with a lower id, so paging stays stable while the catalog changes. */
  readonly beforeId?: number
  readonly page: number
  readonly pageSize: number
}

export interface FilterPage {
  readonly items: readonly FilterSummary[]
  readonly page: number
  readonly pageSize: number
  readonly total: number
}

export interface ListFilterRevisions {
  readonly v: number
  readonly filterId: number
  readonly page: number
  readonly pageSize: number
}

export interface FilterRevisionPage {
  readonly filterId: number
  readonly items: readonly FilterRevisionInfo[]
  readonly page: number
  readonly pageSize: number
  readonly total: number
}

export interface GetFilterBinding {
  readonly v: number
  readonly group: FilterGroupRef
  readonly identity?: FilterGroupIdentity
}

export interface ListFilterBindings {
  readonly v: number
  readonly filterId?: number
  readonly stream?: string
  readonly topic?: string
  readonly page: number
  readonly pageSize: number
}

export interface FilterBindingPage {
  readonly items: readonly FilterBinding[]
  readonly page: number
  readonly pageSize: number
  readonly total: number
}

export interface GetFilterOperation {
  readonly v: number
  readonly operationId: bigint
}

export type FilterPolicyRef =
  | { readonly kind: "revision"; readonly filterId: number; readonly revision: number }
  | { readonly kind: "binding"; readonly identity: FilterGroupIdentity }

export interface ResolveFilterPolicy {
  readonly allowDisabled?: boolean
  readonly v: number
  readonly policy: FilterPolicyRef
}

export interface ResolvedFilterPolicy {
  readonly filterId: number
  readonly revision: number
  readonly digest: Uint8Array
  readonly state: FilterState
  readonly filter: ConsumerFilter
}

export type FilterCatalogOutcome =
  | { readonly kind: "filter"; readonly detail: FilterDetail }
  | { readonly kind: "filters"; readonly page: FilterPage }
  | { readonly kind: "revisions"; readonly page: FilterRevisionPage }
  | { readonly kind: "binding"; readonly binding: FilterBinding }
  | { readonly kind: "bindings"; readonly page: FilterBindingPage }
  | { readonly kind: "mutation"; readonly outcome: FilterMutationOutcome }
  | { readonly kind: "policy"; readonly policy: ResolvedFilterPolicy }

export type FilterCatalogReply =
  | { readonly kind: "ok"; readonly outcome: FilterCatalogOutcome }
  | { readonly kind: "err"; readonly error: FilterError }

function validateVersion(v: number): void {
  if (v !== FILTER_OP_VERSION) {
    throw new InvalidError(
      `filter request version ${String(v)} is not supported, expected ${String(FILTER_OP_VERSION)}`
    )
  }
}

function validateName(label: string, name: string): void {
  const bytes = UTF8.encode(name).byteLength
  if (bytes === 0 || bytes > MAX_FILTER_SOURCE_NAME_BYTES) {
    throw new InvalidError(`${label} must be 1..=${String(MAX_FILTER_SOURCE_NAME_BYTES)} bytes`)
  }
}

function validateSource(source: FilterSource): void {
  validateName("stream", source.stream)
  validateName("topic", source.topic)
}

function validateConsumer(consumer: FilterConsumer): void {
  if (consumer.kind === "group_id") {
    if (consumer.id < 0n || consumer.id > 0xffff_ffffn)
      throw new InvalidError("consumer group id exceeds the native offset-key range")
    return
  }
  validateName(consumer.kind === "group" ? "consumer group" : "consumer", consumer.name)
}

function validateFilterRef(filter: FilterRef): void {
  if (filter.kind === "inline") validateConsumerFilter(filter.filter)
}

function validateDigest(digest: Uint8Array): void {
  if (digest.byteLength !== 32) throw new InvalidError("a digest must be 32 bytes")
}

export function validateFilteredPollRequest(request: FilteredPollRequest): void {
  validateVersion(request.v)
  validateSource(request.source)
  validateConsumer(request.consumer)
  validateFilterRef(request.filter)
  if (request.filter.kind === "bound" && request.consumer.kind === "consumer") {
    throw new InvalidError("a `bound` filter needs a consumer group")
  }
  if (
    !Number.isInteger(request.count) ||
    request.count < 1 ||
    request.count > MAX_FILTERED_PAGE_RECORDS
  ) {
    throw new InvalidError(`count must be 1..=${String(MAX_FILTERED_PAGE_RECORDS)}`)
  }
  if (
    !Number.isInteger(request.maxReplyBytes) ||
    request.maxReplyBytes < 1 ||
    request.maxReplyBytes > MAX_FILTERED_PAGE_BYTES
  ) {
    throw new InvalidError(`max_reply_bytes must be 1..=${String(MAX_FILTERED_PAGE_BYTES)}`)
  }
  if (request.start.kind === "continue") {
    validateDigest(request.start.continuation.digest)
    if (
      (request.consumer.kind !== "consumer") !==
      (request.start.continuation.groupId !== undefined)
    )
      throw new InvalidError("a group continuation must retain its group identity")
    if (request.start.continuation.generation.partitionId !== request.partitionId) {
      throw new InvalidError("a continuation belongs to another partition")
    }
  }
}

export function validateFilteredAck(ack: FilteredAck): void {
  validateVersion(ack.v)
  validateSource(ack.source)
  validateConsumer(ack.consumer)
  validateDigest(ack.digest)
  if ((ack.consumer.kind !== "consumer") !== (ack.groupId !== undefined))
    throw new InvalidError("a group acknowledgment must retain its group identity")
  if (ack.generation.partitionId !== ack.partitionId) {
    throw new InvalidError("an acknowledgment names another partition's generation")
  }
}

export function validateFilterPreviewRequest(request: FilterPreviewRequest): void {
  validateVersion(request.v)
  validateSource(request.source)
  validateFilterRef(request.filter)
  if (request.filter.kind === "bound") {
    throw new InvalidError("a preview executes an inline filter or a saved revision")
  }
  if (request.maxExamined === 0 || request.maxExamined > MAX_FILTER_PREVIEW_EXAMINED) {
    throw new InvalidError(`max_examined must be 1..=${String(MAX_FILTER_PREVIEW_EXAMINED)}`)
  }
  if (request.maxRecords === 0 || request.maxRecords > MAX_FILTER_PREVIEW_RECORDS) {
    throw new InvalidError(`max_records must be 1..=${String(MAX_FILTER_PREVIEW_RECORDS)}`)
  }
}

export function validateFilterTestRequest(request: FilterTestRequest): void {
  validateVersion(request.v)
  validateFilterRef(request.filter)
  if (request.filter.kind === "bound") {
    throw new InvalidError("a sample test executes an inline filter or a saved revision")
  }
  if (request.payload.byteLength > MAX_FILTER_SAMPLE_BYTES) {
    throw new InvalidError(`a sample payload exceeds ${String(MAX_FILTER_SAMPLE_BYTES)}B`)
  }
  if (request.headers.length > MAX_FILTER_SAMPLE_HEADERS) {
    throw new InvalidError(
      `a sample carries more than ${String(MAX_FILTER_SAMPLE_HEADERS)} headers`
    )
  }
  for (const header of request.headers) {
    validateName("sample header key", header.key)
    const bytes =
      header.value.kind === "string"
        ? UTF8.encode(header.value.value).byteLength
        : header.value.kind === "raw"
          ? header.value.value.byteLength
          : 0
    if (bytes > MAX_FILTER_STRING_BYTES)
      throw new InvalidError("a sample header value exceeds its byte limit")
  }
}

export function validateFilterGroupRef(group: FilterGroupRef): void {
  validateName("stream", group.stream)
  validateName("topic", group.topic)
  validateName("consumer group", group.group)
}

export function validateFilterMutation(mutation: FilterMutation): void {
  switch (mutation.kind) {
    case "register":
      validateFilterName(mutation.name)
      validateDescription(mutation.description)
      validateConsumerFilter(mutation.filter)
      return
    case "revise":
      validateConsumerFilter(mutation.filter)
      return
    case "describe":
      validateDescription(mutation.description)
      return
    case "set_revision_enabled":
      if (
        !Number.isSafeInteger(mutation.revision) ||
        mutation.revision <= 0 ||
        mutation.revision > 0xffff_ffff
      )
        throw new InvalidError("revision must be positive and fit u32")
      return
    case "archive":
    case "drop":
      return
    case "bind":
      validateFilterGroupRef(mutation.group)
      return
    case "unbind":
      validateFilterGroupRef(mutation.group)
      validateDigest(mutation.expectedDigest)
  }
}

function validateFilterName(name: string): void {
  const bytes = UTF8.encode(name).byteLength
  if (bytes === 0 || bytes > MAX_FILTER_NAME_BYTES) {
    throw new InvalidError(`a filter name must be 1..=${String(MAX_FILTER_NAME_BYTES)} bytes`)
  }
  if (!/^[A-Za-z0-9._-]+$/.test(name)) {
    throw new InvalidError(
      "a filter name has a disallowed byte: allowed are ASCII letters, digits, '-', '_', '.'"
    )
  }
}

function validateDescription(description: string): void {
  if (UTF8.encode(description).byteLength > MAX_FILTER_DESCRIPTION_BYTES) {
    throw new InvalidError(`a filter description exceeds ${String(MAX_FILTER_DESCRIPTION_BYTES)}B`)
  }
}

export function validateFilterMutationRequest(request: FilterMutationRequest): void {
  validateVersion(request.v)
  if (request.operationId === 0n) throw new InvalidError("operation_id must not be zero")
  validateFilterMutation(request.mutation)
}

export function validateCatalogPage(page: number, pageSize: number): void {
  if (!Number.isInteger(page) || page < 0 || page > 0xffff_ffff)
    throw new InvalidError("page must be a whole number that fits in 32 bits")
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > MAX_FILTER_CATALOG_PAGE) {
    throw new InvalidError(`page_size must be 1..=${String(MAX_FILTER_CATALOG_PAGE)}`)
  }
}

// Encoding. `mode` picks the JSON tree (doubles marked for Rust formatting,
// bytes as number arrays) or the CBOR tree (doubles as float tokens).
type TreeMode = "cbor" | "json"

class JsonDouble {
  constructor(readonly value: number) {}
}

function mapOf(entries: readonly (readonly [string, unknown])[]): Map<string, unknown> {
  return new Map(entries)
}

function setIf(map: Map<string, unknown>, key: string, value: unknown): void {
  if (value !== undefined) map.set(key, value)
}

function encodeTypedLiteral(value: TypedValue, mode: TreeMode): Map<string, unknown> {
  const map = new Map<string, unknown>([["kind", value.kind]])
  switch (value.kind) {
    case "null":
      return map
    case "double":
    case "float":
      map.set(
        "value",
        mode === "json" ? new JsonDouble(value.value) : new Token(Type.float, value.value)
      )
      return map
    case "list":
      map.set(
        "value",
        value.value.map((item) => encodeTypedLiteral(item, mode))
      )
      return map
    case "boolean":
    case "int":
    case "long":
    case "string":
      map.set("value", value.value)
      return map
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
      throw new InvalidError(
        "supported literals are null, boolean, int, long, double, string, and list"
      )
  }
}

function encodePredicateTree(predicate: Predicate, mode: TreeMode): Map<string, unknown> {
  return mapOf([
    ["field", predicate.field],
    ["op", predicate.op],
    ["value", encodeTypedLiteral(predicate.value, mode)]
  ])
}

function encodeCoerce(coerce: Coerce): Map<string, unknown> {
  return coerce.kind === "number"
    ? mapOf([["kind", "number"]])
    : mapOf([
        ["kind", "timestamp"],
        ["format", coerce.format]
      ])
}

function encodeExprTree(expr: FilterExpr, mode: TreeMode): Map<string, unknown> {
  switch (expr.kind) {
    case "all":
    case "any":
      return mapOf([[expr.kind, expr.children.map((child) => encodeExprTree(child, mode))]])
    case "not":
      return mapOf([["not", encodeExprTree(expr.child, mode)]])
    case "pred":
      return mapOf([["pred", encodePredicateTree(expr.predicate, mode)]])
    case "pred_as":
      return mapOf([
        [
          "pred_as",
          mapOf([
            ["pred", encodePredicateTree(expr.predicate.pred, mode)],
            ["coerce", encodeCoerce(expr.predicate.coerce)]
          ])
        ]
      ])
    case "present":
    case "absent":
      return mapOf([[expr.kind, expr.path.toString()]])
    case "header":
      return mapOf([
        [
          "header",
          mapOf([
            ["key", expr.predicate.key],
            ["op", expr.predicate.op],
            ["value", encodeTypedLiteral(expr.predicate.value, mode)]
          ])
        ]
      ])
    case "text":
    case "header_text": {
      const predicate = mapOf([
        ["field", expr.predicate.field],
        ["kind", expr.predicate.kind],
        ["pattern", expr.predicate.pattern]
      ])
      if (expr.predicate.caseInsensitive === true) predicate.set("case_insensitive", true)
      return mapOf([[expr.kind, predicate]])
    }
  }
}

// Field order and omitted defaults follow the Rust encoding, which the digest covers.
function encodeConsumerFilterTree(filter: ConsumerFilter, mode: TreeMode): Map<string, unknown> {
  const map = mapOf([
    ["v", filter.v],
    ["evaluator_version", filter.evaluatorVersion],
    ["expr", encodeExprTree(filter.expr, mode)],
    ["codec", filter.codec],
    ["fault_policy", filter.faultPolicy]
  ])
  if (filter.foreignPolicy === "pass") map.set("foreign_policy", "pass")
  if (filter.mismatchPolicy === "pass") map.set("mismatch_policy", "pass")
  if (filter.schemaRefs.length > 0) map.set("schema_refs", [...filter.schemaRefs])
  return map
}

export function encodeFilterExpr(expr: FilterExpr): Map<string, unknown> {
  return encodeExprTree(expr, "cbor")
}

export function encodeConsumerFilter(filter: ConsumerFilter): Map<string, unknown> {
  return encodeConsumerFilterTree(filter, "cbor")
}

export function encodeFilterSource(source: FilterSource): Map<string, unknown> {
  return mapOf([
    ["stream", source.stream],
    ["topic", source.topic]
  ])
}

export function encodeFilterConsumer(consumer: FilterConsumer): Map<string, unknown> {
  if (consumer.kind === "group_id")
    return mapOf([
      ["kind", "group_id"],
      ["id", consumer.id]
    ])
  return mapOf([
    ["kind", consumer.kind],
    ["name", consumer.name]
  ])
}

export function encodeFilterRef(filter: FilterRef): unknown {
  switch (filter.kind) {
    case "inline":
      return mapOf([["inline", encodeConsumerFilter(filter.filter)]])
    case "revision":
      return mapOf([
        [
          "revision",
          mapOf([
            ["filter_id", filter.filterId],
            ["revision", filter.revision]
          ])
        ]
      ])
    case "bound":
      return "bound"
  }
}

export function encodeSourceGeneration(generation: SourceGeneration): Map<string, unknown> {
  return mapOf([
    ["stream_id", generation.streamId],
    ["stream_created_at_micros", generation.streamCreatedAtMicros],
    ["topic_id", generation.topicId],
    ["topic_created_at_micros", generation.topicCreatedAtMicros],
    ["partition_id", generation.partitionId],
    ["partition_created_revision", generation.partitionCreatedRevision],
    ["purge_generation", generation.purgeGeneration]
  ])
}

export function encodeFilteredStart(start: FilteredStart): unknown {
  switch (start.kind) {
    case "next":
    case "first":
    case "last":
      return start.kind
    case "offset":
      return mapOf([["offset", start.offset]])
    case "timestamp":
      return mapOf([["timestamp", start.micros]])
    case "continue":
      return mapOf([
        [
          "continue",
          mapOf([
            ...(start.continuation.groupId === undefined
              ? []
              : [["group_id", start.continuation.groupId] as const]),
            ["next_scan_offset", start.continuation.nextScanOffset],
            ["generation", encodeSourceGeneration(start.continuation.generation)],
            ["digest", start.continuation.digest],
            ...(start.continuation.readMode === "primary"
              ? []
              : [["read_mode", start.continuation.readMode] as const])
          ])
        ]
      ])
  }
}

export function encodeFilteredPollRequest(request: FilteredPollRequest): Map<string, unknown> {
  return mapOf([
    ["v", request.v],
    ["source", encodeFilterSource(request.source)],
    ["partition_id", request.partitionId],
    ["consumer", encodeFilterConsumer(request.consumer)],
    ["filter", encodeFilterRef(request.filter)],
    ["start", encodeFilteredStart(request.start)],
    ["count", request.count],
    ["max_reply_bytes", request.maxReplyBytes],
    ["read_mode", request.readMode]
  ])
}

export function encodeFilteredAck(ack: FilteredAck): Map<string, unknown> {
  return mapOf([
    ...(ack.groupId === undefined ? [] : [["group_id", ack.groupId] as const]),
    ["v", ack.v],
    ["source", encodeFilterSource(ack.source)],
    ["partition_id", ack.partitionId],
    ["consumer", encodeFilterConsumer(ack.consumer)],
    ["generation", encodeSourceGeneration(ack.generation)],
    ["digest", ack.digest],
    ["offset", ack.offset]
  ])
}

export function encodeFilterPreviewRequest(request: FilterPreviewRequest): Map<string, unknown> {
  return mapOf([
    ["v", request.v],
    ["source", encodeFilterSource(request.source)],
    ["partition_id", request.partitionId],
    ["filter", encodeFilterRef(request.filter)],
    ["from_offset", request.fromOffset],
    ["max_examined", request.maxExamined],
    ["max_records", request.maxRecords],
    ["explain", request.explain]
  ])
}

export function encodeHeaderScalar(value: HeaderScalar): Map<string, unknown> {
  return mapOf([
    ["kind", value.kind],
    ["value", value.kind === "float" ? new Token(Type.float, value.value) : value.value]
  ])
}

export function encodeFilterTestRequest(request: FilterTestRequest): Map<string, unknown> {
  const map = mapOf([
    ["v", request.v],
    ["filter", encodeFilterRef(request.filter)],
    ["payload", request.payload]
  ])
  if (request.headers.length > 0) {
    map.set(
      "headers",
      request.headers.map((header) =>
        mapOf([
          ["key", header.key],
          ["value", encodeHeaderScalar(header.value)]
        ])
      )
    )
  }
  return map
}

function encodeAppliedPolicy(policy: AppliedPolicy): Map<string, unknown> {
  const map = mapOf([])
  setIf(map, "group_id", policy.groupId)
  map.set("digest", policy.digest)
  setIf(map, "filter_id", policy.filterId)
  setIf(map, "revision", policy.revision)
  return map
}

export function encodeFilteredPage(page: FilteredPage): Map<string, unknown> {
  const map = mapOf([
    ["v", page.v],
    ["partition_id", page.partitionId],
    ["policy", encodeAppliedPolicy(page.policy)],
    ["generation", encodeSourceGeneration(page.generation)],
    ["read_mode", page.readMode]
  ])
  setIf(map, "next_scan_offset", page.nextScanOffset)
  setIf(map, "safe_ack_offset", page.safeAckOffset)
  map.set("frontier", page.frontier)
  map.set("examined", page.examined)
  map.set("matched", page.matched)
  map.set("stop", page.stop)
  if (page.fault !== undefined) {
    map.set(
      "fault",
      mapOf([
        ["offset", page.fault.offset],
        ["reason", page.fault.reason]
      ])
    )
  }
  if (page.unevaluated.length > 0) map.set("unevaluated", [...page.unevaluated])
  if (page.evaluationLimits !== undefined)
    map.set(
      "evaluation_limits",
      mapOf([
        ["max_payload_bytes", page.evaluationLimits.maxPayloadBytes],
        ["max_depth", page.evaluationLimits.maxDepth]
      ])
    )
  map.set("records", page.records)
  return map
}

function encodeAckReceipt(receipt: AckReceipt): Map<string, unknown> {
  return mapOf([
    ["partition_id", receipt.partitionId],
    ["offset", receipt.offset],
    ["generation", encodeSourceGeneration(receipt.generation)]
  ])
}

export function encodeFilterError(error: FilterError): Map<string, unknown> {
  return mapOf([
    ["code", resultCodeWord(error.code) ?? "backend"],
    ["reason", error.reason],
    ["message", error.message]
  ])
}

function encodeExplainNode(node: ExplainNode): Map<string, unknown> {
  const map = mapOf([["label", node.label]])
  setIf(map, "truth", node.truth)
  if (node.children.length > 0) map.set("children", node.children.map(encodeExplainNode))
  return map
}

function encodeExplanation(explanation: FilterExplanation): Map<string, unknown> {
  const map = mapOf([["verdict", explanation.verdict]])
  setIf(map, "fault", explanation.fault)
  map.set("root", encodeExplainNode(explanation.root))
  return map
}

function encodeFilterOutcome(outcome: FilterOutcome): Map<string, unknown> {
  switch (outcome.kind) {
    case "page":
      return mapOf([["page", encodeFilteredPage(outcome.page)]])
    case "acknowledged":
      return mapOf([["acknowledged", encodeAckReceipt(outcome.receipt)]])
    case "tested":
      return mapOf([
        [
          "tested",
          mapOf([
            ["v", outcome.result.v],
            ["policy", encodeAppliedPolicy(outcome.result.policy)],
            ["explanation", encodeExplanation(outcome.result.explanation)]
          ])
        ]
      ])
    case "validated":
      return mapOf([
        [
          "validated",
          mapOf([
            ["v", outcome.validation.v],
            ["digest", outcome.validation.digest],
            ["reads_payload", outcome.validation.readsPayload],
            ["reads_headers", outcome.validation.readsHeaders]
          ])
        ]
      ])
    case "preview":
      return mapOf([["preview", encodeFilterPreview(outcome.preview)]])
  }
}

function encodeFilterPreview(preview: FilterPreview): Map<string, unknown> {
  const map = mapOf([
    ["v", preview.v],
    ["partition_id", preview.partitionId],
    ["policy", encodeAppliedPolicy(preview.policy)],
    ["read_mode", preview.readMode],
    ["examined", preview.examined],
    ["matched", preview.matched],
    ["faults", preview.faults],
    ["stop", preview.stop]
  ])
  setIf(map, "next_offset", preview.nextOffset)
  map.set("frontier", preview.frontier)
  map.set(
    "records",
    preview.records.map((record) => {
      const entry = mapOf([
        ["offset", record.offset],
        ["timestamp_micros", record.timestampMicros],
        ["verdict", record.verdict]
      ])
      setIf(entry, "fault", record.fault)
      entry.set("payload_text", record.payloadText)
      entry.set("payload_truncated", record.payloadTruncated)
      if (record.explanation !== undefined) {
        entry.set("explanation", encodeExplanation(record.explanation))
      }
      return entry
    })
  )
  return map
}

export function encodeFilterReply(reply: FilterReply): Map<string, unknown> {
  return reply.kind === "ok"
    ? mapOf([["ok", encodeFilterOutcome(reply.outcome)]])
    : mapOf([["err", encodeFilterError(reply.error)]])
}

export function encodeFilterGroupRef(group: FilterGroupRef): Map<string, unknown> {
  return mapOf([
    ["stream", group.stream],
    ["topic", group.topic],
    ["group", group.group]
  ])
}

export function encodeFilterGroupIdentity(identity: FilterGroupIdentity): Map<string, unknown> {
  return mapOf([
    ["stream_id", identity.streamId],
    ["stream_created_at_micros", identity.streamCreatedAtMicros],
    ["topic_id", identity.topicId],
    ["topic_created_at_micros", identity.topicCreatedAtMicros],
    ["group_id", identity.groupId]
  ])
}

export function encodeFilterMutation(mutation: FilterMutation): Map<string, unknown> {
  switch (mutation.kind) {
    case "register":
      return mapOf([
        [
          "register",
          mapOf([
            ["name", mutation.name],
            ["description", mutation.description],
            ["filter", encodeConsumerFilter(mutation.filter)]
          ])
        ]
      ])
    case "revise":
      return mapOf([
        [
          "revise",
          mapOf([
            ["filter_id", mutation.filterId],
            ["expected_revision", mutation.expectedRevision],
            ["filter", encodeConsumerFilter(mutation.filter)]
          ])
        ]
      ])
    case "describe":
      return mapOf([
        [
          "describe",
          mapOf([
            ["filter_id", mutation.filterId],
            ["description", mutation.description]
          ])
        ]
      ])
    case "set_revision_enabled":
      return mapOf([
        [
          mutation.kind,
          mapOf([
            ["filter_id", mutation.filterId],
            ["revision", mutation.revision],
            ["enabled", mutation.enabled]
          ])
        ]
      ])
    case "archive":
    case "drop":
      return mapOf([[mutation.kind, mapOf([["filter_id", mutation.filterId]])]])
    case "bind":
      return mapOf([
        [
          "bind",
          mapOf([
            ["group", encodeFilterGroupRef(mutation.group)],
            ["filter_id", mutation.filterId],
            ["revision", mutation.revision]
          ])
        ]
      ])
    case "unbind":
      return mapOf([
        [
          "unbind",
          mapOf([
            ["group", encodeFilterGroupRef(mutation.group)],
            ["expected_digest", mutation.expectedDigest],
            ...(mutation.expectedIdentity === undefined
              ? []
              : [
                  [
                    "expected_identity",
                    encodeFilterGroupIdentity(mutation.expectedIdentity)
                  ] as const
                ])
          ])
        ]
      ])
  }
}

export function encodeFilterMutationRequest(request: FilterMutationRequest): Map<string, unknown> {
  return mapOf([
    ["v", request.v],
    ["operation_id", request.operationId],
    ["mutation", encodeFilterMutation(request.mutation)]
  ])
}

export function encodeFilterCatalogCommand(command: FilterCatalogCommand): Map<string, unknown> {
  const map = mapOf([
    ["operation_id", command.operationId],
    ["actor_user_id", command.actorUserId],
    ["mutation", encodeFilterMutation(command.mutation)]
  ])
  if (command.identity !== undefined) {
    map.set("identity", encodeFilterGroupIdentity(command.identity))
  }
  setIf(map, "filter_id", command.filterId)
  if (command.grants !== undefined && command.grants.length > 0) {
    map.set(
      "grants",
      command.grants.map((grant) => encodeGrant(grant))
    )
  }
  if (command.limits !== undefined) {
    map.set(
      "limits",
      mapOf([
        ["max_definitions", command.limits.maxDefinitions],
        ["max_tombstones", command.limits.maxTombstones],
        ["max_revisions", command.limits.maxRevisions],
        ["max_group_identities", command.limits.maxGroupIdentities],
        ["max_catalog_bytes", command.limits.maxCatalogBytes]
      ])
    )
  }
  return map
}

export function encodeGetFilter(request: GetFilter): Map<string, unknown> {
  return mapOf([
    ["v", request.v],
    ["filter_id", request.filterId]
  ])
}

export function encodeListFilters(request: ListFilters): Map<string, unknown> {
  const map = mapOf([["v", request.v]])
  setIf(map, "name_contains", request.nameContains)
  setIf(map, "state", request.state)
  setIf(map, "before_id", request.beforeId)
  map.set("page", request.page)
  map.set("page_size", request.pageSize)
  return map
}

export function encodeListFilterRevisions(request: ListFilterRevisions): Map<string, unknown> {
  return mapOf([
    ["v", request.v],
    ["filter_id", request.filterId],
    ["page", request.page],
    ["page_size", request.pageSize]
  ])
}

export function encodeGetFilterBinding(request: GetFilterBinding): Map<string, unknown> {
  const map = mapOf([
    ["v", request.v],
    ["group", encodeFilterGroupRef(request.group)]
  ])
  if (request.identity !== undefined) {
    map.set("identity", encodeFilterGroupIdentity(request.identity))
  }
  return map
}

export function encodeListFilterBindings(request: ListFilterBindings): Map<string, unknown> {
  const map = mapOf([["v", request.v]])
  setIf(map, "filter_id", request.filterId)
  setIf(map, "stream", request.stream)
  setIf(map, "topic", request.topic)
  map.set("page", request.page)
  map.set("page_size", request.pageSize)
  return map
}

export function encodeGetFilterOperation(request: GetFilterOperation): Map<string, unknown> {
  return mapOf([
    ["v", request.v],
    ["operation_id", request.operationId]
  ])
}

function encodeRevisionRef(revision: FilterRevisionRef): Map<string, unknown> {
  return mapOf([
    ["filter_id", revision.filterId],
    ["revision", revision.revision],
    ["digest", revision.digest]
  ])
}

function encodeFilterBinding(binding: FilterBinding): Map<string, unknown> {
  return mapOf([
    ["group", encodeFilterGroupRef(binding.group)],
    ["identity", encodeFilterGroupIdentity(binding.identity)],
    ["filter_id", binding.filterId],
    ["revision", binding.revision],
    ["digest", binding.digest],
    ["bound_at_micros", binding.boundAtMicros]
  ])
}

function encodeMutationResult(result: FilterMutationResult): Map<string, unknown> {
  switch (result.kind) {
    case "revision_state":
      return mapOf([
        [
          result.kind,
          mapOf([
            ["filter_id", result.filterId],
            ["revision", result.revision],
            ["enabled", result.enabled]
          ])
        ]
      ])
    case "registered":
    case "revised":
      return mapOf([[result.kind, encodeRevisionRef(result.revision)]])
    case "described":
    case "archived":
    case "dropped":
      return mapOf([[result.kind, mapOf([["filter_id", result.filterId]])]])
    case "bound":
    case "unbound":
      return mapOf([[result.kind, encodeFilterBinding(result.binding)]])
  }
}

function encodeMutationOutcome(outcome: FilterMutationOutcome): Map<string, unknown> {
  const status =
    outcome.status.kind === "pending"
      ? "pending"
      : outcome.status.kind === "applied"
        ? mapOf([["applied", encodeMutationResult(outcome.status.result)]])
        : mapOf([["rejected", encodeFilterError(outcome.status.error)]])
  return mapOf([
    ["v", outcome.v],
    ["operation_id", outcome.operationId],
    ["status", status]
  ])
}

function encodeRevisionInfo(info: FilterRevisionInfo): Map<string, unknown> {
  return mapOf([
    ["enabled", info.enabled],
    ["revision", info.revision],
    ["digest", info.digest],
    ["filter", encodeConsumerFilter(info.filter)],
    ["created_at_micros", info.createdAtMicros]
  ])
}

function encodeSummary(summary: FilterSummary): Map<string, unknown> {
  return mapOf([
    ["id", summary.id],
    ["name", summary.name],
    ["description", summary.description],
    ["state", summary.state],
    ["latest_revision", summary.latestRevision],
    ["latest_digest", summary.latestDigest],
    ["codec", summary.codec],
    ["bindings", summary.bindings],
    ["created_at_micros", summary.createdAtMicros],
    ["updated_at_micros", summary.updatedAtMicros]
  ])
}

function encodeCatalogOutcome(outcome: FilterCatalogOutcome): Map<string, unknown> {
  switch (outcome.kind) {
    case "filter":
      return mapOf([
        [
          "filter",
          mapOf([
            ["summary", encodeSummary(outcome.detail.summary)],
            ["latest", encodeRevisionInfo(outcome.detail.latest)],
            ["bindings", outcome.detail.bindings.map(encodeFilterBinding)]
          ])
        ]
      ])
    case "filters":
      return mapOf([
        [
          "filters",
          mapOf([
            ["items", outcome.page.items.map(encodeSummary)],
            ["page", outcome.page.page],
            ["page_size", outcome.page.pageSize],
            ["total", outcome.page.total]
          ])
        ]
      ])
    case "revisions":
      return mapOf([
        [
          "revisions",
          mapOf([
            ["filter_id", outcome.page.filterId],
            ["items", outcome.page.items.map(encodeRevisionInfo)],
            ["page", outcome.page.page],
            ["page_size", outcome.page.pageSize],
            ["total", outcome.page.total]
          ])
        ]
      ])
    case "binding":
      return mapOf([["binding", encodeFilterBinding(outcome.binding)]])
    case "bindings":
      return mapOf([
        [
          "bindings",
          mapOf([
            ["items", outcome.page.items.map(encodeFilterBinding)],
            ["page", outcome.page.page],
            ["page_size", outcome.page.pageSize],
            ["total", outcome.page.total]
          ])
        ]
      ])
    case "mutation":
      return mapOf([["mutation", encodeMutationOutcome(outcome.outcome)]])
    case "policy":
      return mapOf([
        [
          "policy",
          mapOf([
            ["filter_id", outcome.policy.filterId],
            ["revision", outcome.policy.revision],
            ["digest", outcome.policy.digest],
            ["state", outcome.policy.state],
            ["filter", encodeConsumerFilter(outcome.policy.filter)]
          ])
        ]
      ])
  }
}

export function encodeFilterCatalogReply(reply: FilterCatalogReply): Map<string, unknown> {
  return reply.kind === "ok"
    ? mapOf([["ok", encodeCatalogOutcome(reply.outcome)]])
    : mapOf([["err", encodeFilterError(reply.error)]])
}

// Decoding accepts the CBOR tree and the canonical JSON tree alike: both reach
// here as maps, JSON integers as bigints and JSON bytes as number arrays.

function oneOf<T extends string>(value: unknown, known: ReadonlySet<T>, context: string): T {
  const text = expectString(value, context)
  if (!(known as ReadonlySet<string>).has(text)) {
    throw new CodecError(`\`${text}\` is not recognized in ${context}`, context, "value")
  }
  return text as T
}

function u32(value: unknown, context: string): number {
  return typeof value === "bigint" ? expectU32(Number(value), context) : expectU32(value, context)
}

function requiredU32(map: CborMap, key: string, context: string): number {
  if (!map.has(key))
    throw new CodecError(`missing required field \`${key}\` in ${context}`, context, key)
  return u32(map.get(key), `${context}.${key}`)
}

function optionalU32(map: CborMap, key: string, context: string): number | undefined {
  return map.has(key) ? u32(map.get(key), `${context}.${key}`) : undefined
}

function bytesOf(value: unknown, context: string): Uint8Array {
  if (Array.isArray(value)) {
    return Uint8Array.from(value, (byte: unknown) => {
      const number = u32(byte, context)
      if (number > 0xff) throw new CodecError(`byte out of range in ${context}`, context, "value")
      return number
    })
  }
  return expectBytes(value, context)
}

function requiredBytes(map: CborMap, key: string, context: string): Uint8Array {
  if (!map.has(key))
    throw new CodecError(`missing required field \`${key}\` in ${context}`, context, key)
  return bytesOf(map.get(key), `${context}.${key}`)
}

function decodeTypedLiteral(value: unknown, context: string): TypedValue {
  const map = expectMap(value, context)
  const kind = field.requiredString(map, "kind", context)
  if (kind === "double" || kind === "float") {
    const number = map.get("value")
    if (typeof number === "number") return { kind, value: number }
    if (typeof number === "bigint") return { kind, value: Number(number) }
    throw new CodecError(`field \`value\` in ${context} must be a number`, context, "value")
  }
  if (kind === "int") {
    const number = map.get("value")
    return { kind, value: typeof number === "bigint" ? Number(number) : (number as number) }
  }
  if (kind === "list") {
    return {
      kind,
      value: field.requiredArray(map, "value", context, (item, index) =>
        decodeTypedLiteral(item, `${context}[${String(index)}]`)
      )
    }
  }
  return decodeTypedValue(map, context)
}

function decodePredicateTree(value: unknown, context: string): Predicate {
  const map = expectMap(value, context)
  return {
    field: field.requiredString(map, "field", context),
    op: oneOf<CmpOp>(map.get("op"), CMP_OPS, `${context}.op`),
    value: decodeTypedLiteral(map.get("value"), `${context}.value`)
  }
}

function decodeCoerce(value: unknown, context: string): Coerce {
  const map = expectMap(value, context)
  const kind = field.requiredString(map, "kind", context)
  if (kind === "number") return { kind }
  if (kind === "timestamp") {
    return {
      kind,
      format: oneOf<TimestampFormat>(map.get("format"), TIMESTAMP_FORMATS, `${context}.format`)
    }
  }
  throw new CodecError(`\`${kind}\` is not a recognized coercion`, context, "kind")
}

function fieldPathOf(value: unknown, context: string): FieldPath {
  try {
    return FieldPath.parse(expectString(value, context))
  } catch (cause) {
    throw new CodecError(`invalid field path in ${context}`, context, "path", { cause })
  }
}

export function decodeFilterExpr(value: unknown, context: string): FilterExpr {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "all":
    case "any":
      return {
        kind: tag,
        children: expectArray(inner, context).map((child, index) =>
          decodeFilterExpr(child, `${context}.${tag}[${String(index)}]`)
        )
      }
    case "not":
      return { kind: "not", child: decodeFilterExpr(inner, `${context}.not`) }
    case "pred":
      return { kind: "pred", predicate: decodePredicateTree(inner, `${context}.pred`) }
    case "pred_as": {
      const map = expectMap(inner, `${context}.pred_as`)
      return {
        kind: "pred_as",
        predicate: {
          pred: decodePredicateTree(map.get("pred"), `${context}.pred_as.pred`),
          coerce: decodeCoerce(map.get("coerce"), `${context}.pred_as.coerce`)
        }
      }
    }
    case "present":
    case "absent":
      return { kind: tag, path: fieldPathOf(inner, `${context}.${tag}`) }
    case "header": {
      const map = expectMap(inner, `${context}.header`)
      return {
        kind: "header",
        predicate: {
          key: field.requiredString(map, "key", context),
          op: oneOf<CmpOp>(map.get("op"), CMP_OPS, `${context}.header.op`),
          value: decodeTypedLiteral(map.get("value"), `${context}.header.value`)
        }
      }
    }
    case "text":
    case "header_text": {
      const map = expectMap(inner, `${context}.${tag}`)
      const caseInsensitive = map.get("case_insensitive")
      if (caseInsensitive !== undefined && typeof caseInsensitive !== "boolean") {
        throw new CodecError(
          `${context}.${tag}.case_insensitive must be a boolean`,
          context,
          "expr"
        )
      }
      return {
        kind: tag,
        predicate: textPredicate(
          field.requiredString(map, "field", context),
          oneOf<TextMatch>(map.get("kind"), TEXT_MATCHES, `${context}.${tag}.kind`),
          field.requiredString(map, "pattern", context),
          caseInsensitive === true
        )
      }
    }
    default:
      throw new CodecError(`\`${tag}\` is not a recognized filter expression`, context, "expr")
  }
}

// An operation id is a CBOR integer on the binary carriage and decimal text in
// JSON, where a bare 128-bit number would not survive most parsers.
function operationIdOf(map: CborMap, context: string): bigint {
  const value = map.get("operation_id")
  if (typeof value === "string" && /^[0-9]{1,39}$/.test(value)) {
    const parsed = BigInt(value)
    if (parsed < 1n << 128n) return parsed
  }
  return field.requiredU128(map, "operation_id", context)
}

// A reason this build does not know decodes as `unknown`, so an older client
// still reads the error and its code.
function decodeFilterErrorReason(value: unknown, context: string): FilterErrorReason {
  const reason = expectString(value, context)
  return FILTER_ERROR_REASONS.has(reason as FilterErrorReason)
    ? (reason as FilterErrorReason)
    : "unknown"
}

export function decodeFilterCodec(value: unknown, context: string): FilterCodec {
  const codec = expectString(value, context)
  return codec === "json" ||
    codec === "headers_only" ||
    codec === "cbor" ||
    codec === "avro" ||
    codec === "protobuf"
    ? codec
    : "unknown"
}

const TEXT_MATCHES = new Set<TextMatch>(["equals", "prefix", "suffix", "contains", "glob", "regex"])

// A default `reject` stays absent, so a decoded filter compares equal to one a builder made.
function recordPolicyOf(
  map: CborMap,
  key: string,
  name: "foreignPolicy" | "mismatchPolicy",
  context: string
): Partial<Record<"foreignPolicy" | "mismatchPolicy", RecordPolicy>> {
  if (!map.has(key)) return {}
  const policy = oneOf(map.get(key), new Set<RecordPolicy>(["reject", "pass"]), `${context}.${key}`)
  return policy === "pass" ? { [name]: policy } : {}
}

export function decodeConsumerFilter(value: unknown, context: string): ConsumerFilter {
  const map = expectMap(value, context)
  return {
    v: requiredU32(map, "v", context),
    evaluatorVersion: requiredU32(map, "evaluator_version", context),
    expr: decodeFilterExpr(map.get("expr"), `${context}.expr`),
    codec: decodeFilterCodec(map.get("codec"), `${context}.codec`),
    faultPolicy: map.has("fault_policy")
      ? oneOf(
          map.get("fault_policy"),
          new Set<FaultPolicy>(["stop", "pass", "drop"]),
          `${context}.fault_policy`
        )
      : "stop",
    ...recordPolicyOf(map, "foreign_policy", "foreignPolicy", context),
    ...recordPolicyOf(map, "mismatch_policy", "mismatchPolicy", context),
    schemaRefs: field.optionalArray(map, "schema_refs", context, (item, index) =>
      u32(item, `${context}.schema_refs[${String(index)}]`)
    )
  }
}

export function decodeFilterSource(value: unknown, context: string): FilterSource {
  const map = expectMap(value, context)
  return {
    stream: field.requiredString(map, "stream", context),
    topic: field.requiredString(map, "topic", context)
  }
}

export function decodeFilterConsumer(value: unknown, context: string): FilterConsumer {
  const map = expectMap(value, context)
  const kind = field.requiredString(map, "kind", context)
  if (kind === "group_id") return { kind, id: field.requiredU64(map, "id", context) }
  if (kind !== "consumer" && kind !== "group") {
    throw new CodecError(`\`${kind}\` is not a recognized consumer kind`, context, "kind")
  }
  return { kind, name: field.requiredString(map, "name", context) }
}

export function decodeFilterRef(value: unknown, context: string): FilterRef {
  if (value === "bound") return { kind: "bound" }
  const [tag, inner] = singleVariantTag(value, context)
  if (tag === "inline") {
    return { kind: "inline", filter: decodeConsumerFilter(inner, `${context}.inline`) }
  }
  if (tag === "revision") {
    const map = expectMap(inner, `${context}.revision`)
    return {
      kind: "revision",
      filterId: requiredU32(map, "filter_id", context),
      revision: requiredU32(map, "revision", context)
    }
  }
  throw new CodecError(`\`${tag}\` is not a recognized filter reference`, context, "filter")
}

export function decodeSourceGeneration(value: unknown, context: string): SourceGeneration {
  const map = expectMap(value, context)
  return {
    streamId: requiredU32(map, "stream_id", context),
    streamCreatedAtMicros: field.requiredU64(map, "stream_created_at_micros", context),
    topicId: requiredU32(map, "topic_id", context),
    topicCreatedAtMicros: field.requiredU64(map, "topic_created_at_micros", context),
    partitionId: requiredU32(map, "partition_id", context),
    partitionCreatedRevision: field.requiredU64(map, "partition_created_revision", context),
    purgeGeneration: field.requiredU64(map, "purge_generation", context)
  }
}

export function decodeFilteredStart(value: unknown, context: string): FilteredStart {
  if (value === "next" || value === "first" || value === "last") return { kind: value }
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "offset":
      return {
        kind: "offset",
        offset: field.requiredU64(new Map([["offset", inner]]), "offset", context)
      }
    case "timestamp":
      return {
        kind: "timestamp",
        micros: field.requiredU64(new Map([["micros", inner]]), "micros", context)
      }
    case "continue": {
      const map = expectMap(inner, `${context}.continue`)
      const groupId = field.optionalU64(map, "group_id", context)
      return {
        kind: "continue",
        continuation: {
          ...(groupId !== undefined ? { groupId } : {}),
          nextScanOffset: field.requiredU64(map, "next_scan_offset", context),
          generation: decodeSourceGeneration(map.get("generation"), `${context}.generation`),
          digest: requiredBytes(map, "digest", context),
          readMode: map.has("read_mode")
            ? decodeReadMode(map.get("read_mode"), `${context}.read_mode`)
            : "primary"
        }
      }
    }
    default:
      throw new CodecError(`\`${tag}\` is not a recognized filtered start`, context, "start")
  }
}

function decodeReadMode(value: unknown, context: string): ReadMode {
  return oneOf(value, new Set<ReadMode>(["primary", "local"]), context)
}

export function decodeFilteredPollRequest(value: unknown, context: string): FilteredPollRequest {
  const map = expectMap(value, context)
  return {
    v: requiredU32(map, "v", context),
    source: decodeFilterSource(map.get("source"), `${context}.source`),
    partitionId: requiredU32(map, "partition_id", context),
    consumer: decodeFilterConsumer(map.get("consumer"), `${context}.consumer`),
    filter: decodeFilterRef(map.get("filter"), `${context}.filter`),
    start: decodeFilteredStart(map.get("start"), `${context}.start`),
    count: requiredU32(map, "count", context),
    maxReplyBytes: requiredU32(map, "max_reply_bytes", context),
    readMode: map.has("read_mode")
      ? decodeReadMode(map.get("read_mode"), `${context}.read_mode`)
      : "primary"
  }
}

export function decodeFilteredAck(value: unknown, context: string): FilteredAck {
  const map = expectMap(value, context)
  const groupId = field.optionalU64(map, "group_id", context)
  return {
    ...(groupId !== undefined ? { groupId } : {}),
    v: requiredU32(map, "v", context),
    source: decodeFilterSource(map.get("source"), `${context}.source`),
    partitionId: requiredU32(map, "partition_id", context),
    consumer: decodeFilterConsumer(map.get("consumer"), `${context}.consumer`),
    generation: decodeSourceGeneration(map.get("generation"), `${context}.generation`),
    digest: requiredBytes(map, "digest", context),
    offset: field.requiredU64(map, "offset", context)
  }
}

function decodeAppliedPolicy(value: unknown, context: string): AppliedPolicy {
  const map = expectMap(value, context)
  const filterId = optionalU32(map, "filter_id", context)
  const revision = optionalU32(map, "revision", context)
  const groupId = field.optionalU64(map, "group_id", context)
  return {
    ...(groupId !== undefined ? { groupId } : {}),
    digest: requiredBytes(map, "digest", context),
    ...(filterId !== undefined ? { filterId } : {}),
    ...(revision !== undefined ? { revision } : {})
  }
}

function decodeFaultReason(value: unknown, context: string): FaultReason {
  return oneOf<FaultReason>(value, FAULT_REASONS, context)
}

export function decodeFilteredPage(value: unknown, context: string): FilteredPage {
  const map = expectMap(value, context)
  const nextScanOffset = field.optionalU64(map, "next_scan_offset", context)
  const safeAckOffset = field.optionalU64(map, "safe_ack_offset", context)
  const fault = field.optionalMap(map, "fault", context)
  const evaluationLimits = field.optionalMap(map, "evaluation_limits", context)
  return {
    v: requiredU32(map, "v", context),
    partitionId: requiredU32(map, "partition_id", context),
    policy: decodeAppliedPolicy(map.get("policy"), `${context}.policy`),
    generation: decodeSourceGeneration(map.get("generation"), `${context}.generation`),
    readMode: decodeReadMode(map.get("read_mode"), `${context}.read_mode`),
    ...(nextScanOffset !== undefined ? { nextScanOffset } : {}),
    ...(safeAckOffset !== undefined ? { safeAckOffset } : {}),
    frontier: field.requiredU64(map, "frontier", context),
    examined: requiredU32(map, "examined", context),
    matched: requiredU32(map, "matched", context),
    stop: oneOf<StopReason>(map.get("stop"), STOP_REASONS, `${context}.stop`),
    ...(fault !== undefined
      ? {
          fault: {
            offset: field.requiredU64(fault, "offset", `${context}.fault`),
            reason: decodeFaultReason(fault.get("reason"), `${context}.fault.reason`)
          }
        }
      : {}),
    ...(evaluationLimits === undefined
      ? {}
      : {
          evaluationLimits: {
            maxPayloadBytes: requiredU32(evaluationLimits, "max_payload_bytes", context),
            maxDepth: requiredU32(evaluationLimits, "max_depth", context)
          }
        }),
    unevaluated: field.optionalArray(map, "unevaluated", context, (item, index) =>
      expectU64(item, `${context}.unevaluated[${String(index)}]`)
    ),
    records: requiredBytes(map, "records", context)
  }
}

function decodeAckReceipt(value: unknown, context: string): AckReceipt {
  const map = expectMap(value, context)
  return {
    partitionId: requiredU32(map, "partition_id", context),
    offset: field.requiredU64(map, "offset", context),
    generation: decodeSourceGeneration(map.get("generation"), `${context}.generation`)
  }
}

export function decodeFilterError(value: unknown, context: string): FilterError {
  const map = expectMap(value, context)
  return {
    code: resultCodeOf(field.requiredString(map, "code", context), context),
    reason: decodeFilterErrorReason(map.get("reason"), `${context}.reason`),
    message: field.requiredString(map, "message", context)
  }
}

function resultCodeOf(word: string, context: string): ResultCode {
  const code = resultCodeFromWord(word)
  if (code === undefined) throw new CodecError(`unknown result code \`${word}\``, context, "code")
  return code
}

function decodeExplainNode(value: unknown, context: string): ExplainNode {
  const map = expectMap(value, context)
  const truth = map.has("truth")
    ? oneOf<Truth>(map.get("truth"), TRUTHS, `${context}.truth`)
    : undefined
  return {
    label: field.requiredString(map, "label", context),
    ...(truth !== undefined ? { truth } : {}),
    children: field.optionalArray(map, "children", context, (child, index) =>
      decodeExplainNode(child, `${context}.children[${String(index)}]`)
    )
  }
}

function decodeExplanation(value: unknown, context: string): FilterExplanation {
  const map = expectMap(value, context)
  const fault = map.has("fault")
    ? decodeFaultReason(map.get("fault"), `${context}.fault`)
    : undefined
  return {
    verdict: oneOf<Verdict>(map.get("verdict"), VERDICTS, `${context}.verdict`),
    ...(fault !== undefined ? { fault } : {}),
    root: decodeExplainNode(map.get("root"), `${context}.root`)
  }
}

function decodeFilterPreview(value: unknown, context: string): FilterPreview {
  const map = expectMap(value, context)
  const nextOffset = field.optionalU64(map, "next_offset", context)
  return {
    v: requiredU32(map, "v", context),
    partitionId: requiredU32(map, "partition_id", context),
    policy: decodeAppliedPolicy(map.get("policy"), `${context}.policy`),
    readMode: decodeReadMode(map.get("read_mode"), `${context}.read_mode`),
    examined: requiredU32(map, "examined", context),
    matched: requiredU32(map, "matched", context),
    faults: requiredU32(map, "faults", context),
    stop: oneOf<StopReason>(map.get("stop"), STOP_REASONS, `${context}.stop`),
    ...(nextOffset !== undefined ? { nextOffset } : {}),
    frontier: field.requiredU64(map, "frontier", context),
    records: field.requiredArray(map, "records", context, (item, index) => {
      const recordContext = `${context}.records[${String(index)}]`
      const record = expectMap(item, recordContext)
      const fault = record.has("fault")
        ? decodeFaultReason(record.get("fault"), `${recordContext}.fault`)
        : undefined
      const explanation = record.has("explanation")
        ? decodeExplanation(record.get("explanation"), `${recordContext}.explanation`)
        : undefined
      return {
        offset: field.requiredU64(record, "offset", recordContext),
        timestampMicros: field.requiredU64(record, "timestamp_micros", recordContext),
        verdict: oneOf<Verdict>(record.get("verdict"), VERDICTS, `${recordContext}.verdict`),
        ...(fault !== undefined ? { fault } : {}),
        payloadText: field.requiredString(record, "payload_text", recordContext),
        payloadTruncated:
          field.optionalBoolean(record, "payload_truncated", recordContext) ?? false,
        ...(explanation !== undefined ? { explanation } : {})
      }
    })
  }
}

function decodeFilterOutcome(value: unknown, context: string): FilterOutcome {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "page":
      return { kind: "page", page: decodeFilteredPage(inner, `${context}.page`) }
    case "acknowledged":
      return { kind: "acknowledged", receipt: decodeAckReceipt(inner, `${context}.acknowledged`) }
    case "preview":
      return { kind: "preview", preview: decodeFilterPreview(inner, `${context}.preview`) }
    case "tested": {
      const map = expectMap(inner, `${context}.tested`)
      return {
        kind: "tested",
        result: {
          v: requiredU32(map, "v", context),
          policy: decodeAppliedPolicy(map.get("policy"), `${context}.tested.policy`),
          explanation: decodeExplanation(map.get("explanation"), `${context}.tested.explanation`)
        }
      }
    }
    case "validated": {
      const map = expectMap(inner, `${context}.validated`)
      return {
        kind: "validated",
        validation: {
          v: requiredU32(map, "v", context),
          digest: requiredBytes(map, "digest", context),
          readsPayload: field.requiredBoolean(map, "reads_payload", context),
          readsHeaders: field.requiredBoolean(map, "reads_headers", context)
        }
      }
    }
    default:
      throw new CodecError(`\`${tag}\` is not a recognized filter outcome`, context, "outcome")
  }
}

export function decodeFilterReply(value: unknown, context: string): FilterReply {
  const [tag, inner] = singleVariantTag(value, context)
  if (tag === "ok") return { kind: "ok", outcome: decodeFilterOutcome(inner, `${context}.ok`) }
  if (tag === "err") return { kind: "err", error: decodeFilterError(inner, `${context}.err`) }
  throw new CodecError(`\`${tag}\` is not a recognized filter reply`, context, "reply")
}

export function decodeHeaderScalar(value: unknown, context: string): HeaderScalar {
  const map = expectMap(value, context)
  const kind = field.requiredString(map, "kind", context)
  const inner = map.get("value")
  switch (kind) {
    case "bool":
      if (typeof inner === "boolean") return { kind, value: inner }
      break
    case "int":
    case "uint":
      if (typeof inner === "bigint") return { kind, value: inner }
      if (typeof inner === "number" && Number.isSafeInteger(inner))
        return { kind, value: BigInt(inner) }
      break
    case "float":
      if (typeof inner === "number") return { kind, value: inner }
      if (typeof inner === "bigint") return { kind, value: Number(inner) }
      break
    case "string":
      if (typeof inner === "string") return { kind, value: inner }
      break
    case "raw":
      return { kind, value: bytesOf(inner, `${context}.value`) }
    default:
      throw new CodecError(`\`${kind}\` is not a recognized header kind`, context, "kind")
  }
  throw new CodecError(`header value in ${context} does not match its kind`, context, "value")
}

export function decodeFilterGroupRef(value: unknown, context: string): FilterGroupRef {
  const map = expectMap(value, context)
  return {
    stream: field.requiredString(map, "stream", context),
    topic: field.requiredString(map, "topic", context),
    group: field.requiredString(map, "group", context)
  }
}

export function decodeFilterGroupIdentity(value: unknown, context: string): FilterGroupIdentity {
  const map = expectMap(value, context)
  return {
    streamId: requiredU32(map, "stream_id", context),
    streamCreatedAtMicros: field.requiredU64(map, "stream_created_at_micros", context),
    topicId: requiredU32(map, "topic_id", context),
    topicCreatedAtMicros: field.requiredU64(map, "topic_created_at_micros", context),
    groupId: field.requiredU64(map, "group_id", context)
  }
}

export function decodeFilterMutation(value: unknown, context: string): FilterMutation {
  const [tag, inner] = singleVariantTag(value, context)
  const map = expectMap(inner, `${context}.${tag}`)
  switch (tag) {
    case "register":
      return {
        kind: "register",
        name: field.requiredString(map, "name", context),
        description: field.requiredString(map, "description", context),
        filter: decodeConsumerFilter(map.get("filter"), `${context}.register.filter`)
      }
    case "revise":
      return {
        kind: "revise",
        filterId: requiredU32(map, "filter_id", context),
        expectedRevision: requiredU32(map, "expected_revision", context),
        filter: decodeConsumerFilter(map.get("filter"), `${context}.revise.filter`)
      }
    case "describe":
      return {
        kind: "describe",
        filterId: requiredU32(map, "filter_id", context),
        description: field.requiredString(map, "description", context)
      }
    case "set_revision_enabled":
      return {
        kind: tag,
        filterId: requiredU32(map, "filter_id", context),
        revision: requiredU32(map, "revision", context),
        enabled: field.requiredBoolean(map, "enabled", context)
      }
    case "archive":
    case "drop":
      return { kind: tag, filterId: requiredU32(map, "filter_id", context) }
    case "bind":
      return {
        kind: "bind",
        group: decodeFilterGroupRef(map.get("group"), `${context}.bind.group`),
        filterId: requiredU32(map, "filter_id", context),
        revision: requiredU32(map, "revision", context)
      }
    case "unbind":
      return {
        kind: "unbind",
        group: decodeFilterGroupRef(map.get("group"), `${context}.unbind.group`),
        expectedDigest: requiredBytes(map, "expected_digest", context),
        ...(map.has("expected_identity") && map.get("expected_identity") !== null
          ? {
              expectedIdentity: decodeFilterGroupIdentity(
                map.get("expected_identity"),
                `${context}.expected_identity`
              )
            }
          : {})
      }
    default:
      throw new CodecError(`\`${tag}\` is not a recognized filter mutation`, context, "mutation")
  }
}

export function decodeFilterMutationRequest(
  value: unknown,
  context: string
): FilterMutationRequest {
  const map = expectMap(value, context)
  return {
    v: requiredU32(map, "v", context),
    operationId: operationIdOf(map, context),
    mutation: decodeFilterMutation(map.get("mutation"), `${context}.mutation`)
  }
}

export function decodeFilterCatalogCommand(value: unknown, context: string): FilterCatalogCommand {
  const map = expectMap(value, context)
  return {
    ...(map.has("limits")
      ? { limits: decodeFilterCatalogLimits(map.get("limits"), `${context}.limits`) }
      : {}),
    operationId: operationIdOf(map, context),
    actorUserId: requiredU32(map, "actor_user_id", context),
    mutation: decodeFilterMutation(map.get("mutation"), `${context}.mutation`),
    ...(map.has("identity")
      ? { identity: decodeFilterGroupIdentity(map.get("identity"), `${context}.identity`) }
      : {}),
    ...(map.has("filter_id") ? { filterId: requiredU32(map, "filter_id", context) } : {}),
    ...(map.has("grants")
      ? {
          grants: expectArray(map.get("grants"), `${context}.grants`).map((grant, index) =>
            decodeGrant(expectMap(grant, `${context}.grants[${String(index)}]`), context)
          )
        }
      : {})
  }
}

function decodeFilterCatalogLimits(value: unknown, context: string): FilterCatalogLimits {
  const map = expectMap(value, context)
  return {
    maxDefinitions: requiredU32(map, "max_definitions", context),
    maxTombstones: requiredU32(map, "max_tombstones", context),
    maxRevisions: requiredU32(map, "max_revisions", context),
    maxGroupIdentities: requiredU32(map, "max_group_identities", context),
    maxCatalogBytes:
      field.optionalU64(map, "max_catalog_bytes", context) ?? DEFAULT_MAX_CATALOG_BYTES
  }
}

function decodeRevisionRef(value: unknown, context: string): FilterRevisionRef {
  const map = expectMap(value, context)
  return {
    filterId: requiredU32(map, "filter_id", context),
    revision: requiredU32(map, "revision", context),
    digest: requiredBytes(map, "digest", context)
  }
}

function decodeFilterBinding(value: unknown, context: string): FilterBinding {
  const map = expectMap(value, context)
  return {
    group: decodeFilterGroupRef(map.get("group"), `${context}.group`),
    identity: decodeFilterGroupIdentity(map.get("identity"), `${context}.identity`),
    filterId: requiredU32(map, "filter_id", context),
    revision: requiredU32(map, "revision", context),
    digest: requiredBytes(map, "digest", context),
    boundAtMicros: field.requiredU64(map, "bound_at_micros", context)
  }
}

function decodeMutationResult(value: unknown, context: string): FilterMutationResult {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "revision_state": {
      const map = expectMap(inner, context)
      return {
        kind: tag,
        filterId: requiredU32(map, "filter_id", context),
        revision: requiredU32(map, "revision", context),
        enabled: field.requiredBoolean(map, "enabled", context)
      }
    }
    case "registered":
    case "revised":
      return { kind: tag, revision: decodeRevisionRef(inner, `${context}.${tag}`) }
    case "described":
    case "archived":
    case "dropped":
      return {
        kind: tag,
        filterId: requiredU32(expectMap(inner, `${context}.${tag}`), "filter_id", context)
      }
    case "bound":
    case "unbound":
      return { kind: tag, binding: decodeFilterBinding(inner, `${context}.${tag}`) }
    default:
      throw new CodecError(`\`${tag}\` is not a recognized mutation result`, context, "result")
  }
}

function decodeMutationStatus(value: unknown, context: string): FilterMutationStatus {
  if (value === "pending") return { kind: "pending" }
  const [tag, inner] = singleVariantTag(value, context)
  if (tag === "applied") {
    return { kind: "applied", result: decodeMutationResult(inner, `${context}.applied`) }
  }
  if (tag === "rejected") {
    return { kind: "rejected", error: decodeFilterError(inner, `${context}.rejected`) }
  }
  throw new CodecError(`\`${tag}\` is not a recognized mutation status`, context, "status")
}

export function decodeFilterMutationOutcome(
  value: unknown,
  context: string
): FilterMutationOutcome {
  const map = expectMap(value, context)
  return {
    v: requiredU32(map, "v", context),
    operationId: operationIdOf(map, context),
    status: decodeMutationStatus(map.get("status"), `${context}.status`)
  }
}

function decodeRevisionInfo(value: unknown, context: string): FilterRevisionInfo {
  const map = expectMap(value, context)
  return {
    enabled: map.has("enabled") ? field.requiredBoolean(map, "enabled", context) : true,
    revision: requiredU32(map, "revision", context),
    digest: requiredBytes(map, "digest", context),
    filter: decodeConsumerFilter(map.get("filter"), `${context}.filter`),
    createdAtMicros: field.requiredU64(map, "created_at_micros", context)
  }
}

function decodeSummary(value: unknown, context: string): FilterSummary {
  const map = expectMap(value, context)
  return {
    id: requiredU32(map, "id", context),
    name: field.requiredString(map, "name", context),
    description: field.requiredString(map, "description", context),
    state: oneOf<FilterState>(map.get("state"), FILTER_STATES, `${context}.state`),
    latestRevision: requiredU32(map, "latest_revision", context),
    latestDigest: requiredBytes(map, "latest_digest", context),
    codec: decodeFilterCodec(map.get("codec"), `${context}.codec`),
    bindings: requiredU32(map, "bindings", context),
    createdAtMicros: field.requiredU64(map, "created_at_micros", context),
    updatedAtMicros: field.requiredU64(map, "updated_at_micros", context)
  }
}

function decodeCatalogOutcome(value: unknown, context: string): FilterCatalogOutcome {
  const [tag, inner] = singleVariantTag(value, context)
  const innerContext = `${context}.${tag}`
  const map = expectMap(inner, innerContext)
  const pageOf = <T>(decode: (item: unknown, itemContext: string) => T) => ({
    items: field.requiredArray(map, "items", innerContext, (item, index) =>
      decode(item, `${innerContext}.items[${String(index)}]`)
    ),
    page: requiredU32(map, "page", innerContext),
    pageSize: requiredU32(map, "page_size", innerContext),
    total: requiredU32(map, "total", innerContext)
  })
  switch (tag) {
    case "filter":
      return {
        kind: "filter",
        detail: {
          summary: decodeSummary(map.get("summary"), `${innerContext}.summary`),
          latest: decodeRevisionInfo(map.get("latest"), `${innerContext}.latest`),
          bindings: field.requiredArray(map, "bindings", innerContext, (item, index) =>
            decodeFilterBinding(item, `${innerContext}.bindings[${String(index)}]`)
          )
        }
      }
    case "filters":
      return { kind: "filters", page: pageOf(decodeSummary) }
    case "revisions":
      return {
        kind: "revisions",
        page: {
          filterId: requiredU32(map, "filter_id", innerContext),
          ...pageOf(decodeRevisionInfo)
        }
      }
    case "binding":
      return { kind: "binding", binding: decodeFilterBinding(map, innerContext) }
    case "bindings":
      return { kind: "bindings", page: pageOf(decodeFilterBinding) }
    case "mutation":
      return { kind: "mutation", outcome: decodeFilterMutationOutcome(map, innerContext) }
    case "policy":
      return {
        kind: "policy",
        policy: {
          filterId: requiredU32(map, "filter_id", innerContext),
          revision: requiredU32(map, "revision", innerContext),
          digest: requiredBytes(map, "digest", innerContext),
          state: oneOf<FilterState>(map.get("state"), FILTER_STATES, `${innerContext}.state`),
          filter: decodeConsumerFilter(map.get("filter"), `${innerContext}.filter`)
        }
      }
    default:
      throw new CodecError(`\`${tag}\` is not a recognized catalog outcome`, context, "outcome")
  }
}

export function decodeFilterCatalogReply(value: unknown, context: string): FilterCatalogReply {
  const [tag, inner] = singleVariantTag(value, context)
  if (tag === "ok") return { kind: "ok", outcome: decodeCatalogOutcome(inner, `${context}.ok`) }
  if (tag === "err") return { kind: "err", error: decodeFilterError(inner, `${context}.err`) }
  throw new CodecError(`\`${tag}\` is not a recognized catalog reply`, context, "reply")
}

// Canonical JSON, byte for byte what serde_json writes: struct fields in
// declaration order, strings escaped like `JSON.stringify`, bytes as number
// arrays, and doubles in Rust's shortest round-trip notation.
function writeJson(value: unknown): string {
  if (value instanceof JsonDouble) return rustDouble(value.value)
  if (value instanceof Map) {
    const members = [...(value as Map<string, unknown>).entries()].map(
      ([key, item]) => `${JSON.stringify(key)}:${writeJson(item)}`
    )
    return `{${members.join(",")}}`
  }
  if (value instanceof Uint8Array) return `[${[...value].join(",")}]`
  if (Array.isArray(value)) return `[${value.map(writeJson).join(",")}]`
  if (typeof value === "bigint") return value.toString()
  if (typeof value === "number") return String(value)
  if (typeof value === "string" || typeof value === "boolean" || value === null) {
    return JSON.stringify(value)
  }
  throw new InvalidError("the filter holds a value JSON cannot carry")
}

// Rust (ryu) notation of a finite double: the shortest round-trip digits, with
// `.0` on integral values below 1e16 and a bare exponent (`1e21`, `1e-7`)
// outside the plain-notation window.
export function rustDouble(value: number): string {
  if (!Number.isFinite(value)) throw new InvalidError("a double must be finite")
  if (value === 0) return Object.is(value, -0) ? "-0.0" : "0.0"
  const [mantissa = "", exponentText = "0"] = Math.abs(value).toExponential().split("e")
  const digits = mantissa.replace(".", "")
  const length = digits.length
  const kk = Number(exponentText) + 1
  const k = kk - length
  let text: string
  if (k >= 0 && kk <= 16) {
    text = `${digits}${"0".repeat(k)}.0`
  } else if (kk > 0 && kk <= 16) {
    text = `${digits.slice(0, kk)}.${digits.slice(kk)}`
  } else if (kk > -5 && kk <= 0) {
    text = `0.${"0".repeat(-kk)}${digits}`
  } else if (length === 1) {
    text = `${digits}e${String(kk - 1)}`
  } else {
    text = `${digits.slice(0, 1)}.${digits.slice(1)}e${String(kk - 1)}`
  }
  return value < 0 ? `-${text}` : text
}

/**
 * Parse JSON the way serde_json does, keeping integers exact. Objects become
 * maps with the last duplicate key winning. Throws on anything serde_json
 * rejects. `payload` mode decodes a record payload like the evaluator: only
 * integers that fit i64 or u64 stay bigints, any other number is a double.
 * `document` mode keeps every integer a bigint, for u128 fields.
 */
export function parseCanonicalJson(
  text: string,
  mode: "document" | "payload" = "document"
): unknown {
  const parser = new JsonParser(text, mode)
  const value = parser.value()
  parser.end()
  return value
}

const U64_MAX = (1n << 64n) - 1n

class JsonParser {
  private position = 0

  constructor(
    private readonly text: string,
    private readonly mode: "document" | "payload"
  ) {}

  value(): unknown {
    this.whitespace()
    const character = this.text[this.position]
    switch (character) {
      case "{":
        return this.object()
      case "[":
        return this.array()
      case '"':
        return this.string()
      case "t":
        return this.literal("true", true)
      case "f":
        return this.literal("false", false)
      case "n":
        return this.literal("null", null)
      case undefined:
        return this.fail("unexpected end of input")
      default:
        return this.number()
    }
  }

  end(): void {
    this.whitespace()
    if (this.position !== this.text.length) this.fail("trailing characters")
  }

  private object(): Map<string, unknown> {
    const map = new Map<string, unknown>()
    this.position += 1
    this.whitespace()
    if (this.text[this.position] === "}") {
      this.position += 1
      return map
    }
    for (;;) {
      this.whitespace()
      if (this.text[this.position] !== '"') this.fail("expected a key")
      const key = this.string()
      this.whitespace()
      if (this.text[this.position] !== ":") this.fail("expected `:`")
      this.position += 1
      map.set(key, this.value())
      this.whitespace()
      const next = this.text[this.position]
      this.position += 1
      if (next === "}") return map
      if (next !== ",") this.fail("expected `,` or `}`")
    }
  }

  private array(): unknown[] {
    const items: unknown[] = []
    this.position += 1
    this.whitespace()
    if (this.text[this.position] === "]") {
      this.position += 1
      return items
    }
    for (;;) {
      items.push(this.value())
      this.whitespace()
      const next = this.text[this.position]
      this.position += 1
      if (next === "]") return items
      if (next !== ",") this.fail("expected `,` or `]`")
    }
  }

  // Plain runs are copied as one slice each, so a long key or value is not
  // built one character at a time.
  private string(): string {
    this.position += 1
    let out = ""
    let run = this.position
    for (;;) {
      const code = this.text.charCodeAt(this.position)
      if (Number.isNaN(code)) this.fail("unterminated string")
      if (code === 0x22) {
        out += this.text.slice(run, this.position)
        this.position += 1
        return out
      }
      if (code < 0x20) this.fail("control character in string")
      if (code === 0x5c) {
        out += this.text.slice(run, this.position)
        const escape = this.text[this.position + 1]
        this.position += 2
        switch (escape) {
          case '"':
          case "\\":
          case "/":
            out += escape
            break
          case "b":
            out += "\b"
            break
          case "f":
            out += "\f"
            break
          case "n":
            out += "\n"
            break
          case "r":
            out += "\r"
            break
          case "t":
            out += "\t"
            break
          case "u":
            out += this.unicodeEscape()
            break
          case undefined:
            this.fail("unterminated escape")
            break
          default:
            this.fail("invalid escape")
        }
        run = this.position
        continue
      }
      if (code >= 0xd800 && code <= 0xdfff) {
        const low = this.text.charCodeAt(this.position + 1)
        if (code > 0xdbff || !(low >= 0xdc00 && low <= 0xdfff)) this.fail("lone surrogate")
        this.position += 2
        continue
      }
      this.position += 1
    }
  }

  private unicodeEscape(): string {
    const high = this.hex4()
    if (high >= 0xdc00 && high <= 0xdfff) this.fail("lone trailing surrogate")
    if (high < 0xd800 || high > 0xdbff) return String.fromCharCode(high)
    if (this.text[this.position] !== "\\" || this.text[this.position + 1] !== "u") {
      this.fail("lone leading surrogate")
    }
    this.position += 2
    const low = this.hex4()
    if (low < 0xdc00 || low > 0xdfff) this.fail("invalid surrogate pair")
    return String.fromCharCode(high, low)
  }

  private hex4(): number {
    let value = 0
    for (let index = 0; index < 4; index += 1) {
      const code = this.text.charCodeAt(this.position + index)
      const digit =
        code >= 48 && code <= 57
          ? code - 48
          : code >= 65 && code <= 70
            ? code - 55
            : code >= 97 && code <= 102
              ? code - 87
              : -1
      if (digit < 0) this.fail("invalid unicode escape")
      value = value * 16 + digit
    }
    this.position += 4
    return value
  }

  private literal<T>(word: string, value: T): T {
    if (!this.text.startsWith(word, this.position)) this.fail("invalid literal")
    this.position += word.length
    return value
  }

  // Scanned by character code, with no slice or match array until the token
  // is known. A bare `-0` is the double negative zero, as the server's parser
  // reads it.
  private number(): bigint | number {
    const start = this.position
    const digitAt = (index: number): boolean => {
      const code = this.text.charCodeAt(index)
      return code >= 0x30 && code <= 0x39
    }
    let at = start
    if (this.text.charCodeAt(at) === 0x2d) at += 1
    if (this.text.charCodeAt(at) === 0x30) {
      at += 1
    } else if (digitAt(at)) {
      while (digitAt(at)) at += 1
    } else {
      this.fail("invalid value")
    }
    let integral = true
    if (this.text.charCodeAt(at) === 0x2e) {
      integral = false
      at += 1
      if (!digitAt(at)) this.fail("invalid value")
      while (digitAt(at)) at += 1
    }
    const exponent = this.text.charCodeAt(at)
    if (exponent === 0x65 || exponent === 0x45) {
      integral = false
      at += 1
      const sign = this.text.charCodeAt(at)
      if (sign === 0x2b || sign === 0x2d) at += 1
      if (!digitAt(at)) this.fail("invalid value")
      while (digitAt(at)) at += 1
    }
    const token = this.text.slice(start, at)
    this.position = at
    if (integral) {
      if (token === "-0" && this.mode === "payload") return -0
      const integer = BigInt(token)
      if (this.mode === "document" || (integer >= I64_MIN && integer <= U64_MAX)) return integer
    }
    const float = Number(token)
    if (!Number.isFinite(float)) this.fail("number out of range")
    return float
  }

  private whitespace(): void {
    for (;;) {
      const code = this.text.charCodeAt(this.position)
      if (code !== 0x20 && code !== 0x09 && code !== 0x0a && code !== 0x0d) return
      this.position += 1
    }
  }

  private fail(reason: string): never {
    throw new CodecError(`invalid JSON at ${String(this.position)}: ${reason}`, "json", "value")
  }
}
