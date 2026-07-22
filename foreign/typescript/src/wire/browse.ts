import { CodecError } from "../client/errors.js"
import { type CborMap, expectMap, field, singleVariantTag } from "./cbor.js"
import {
  decodeProjection,
  decodeProjectionBinding,
  decodeSchemaDef,
  decodeSchemaSource,
  encodeProjection,
  encodeProjectionBinding,
  encodeSchemaDef,
  encodeSchemaSource,
  type Projection,
  type ProjectionBinding,
  type SchemaDef,
  type SchemaSource
} from "./control.js"
import { decodeQueryError, encodeQueryError, type QueryError } from "./query.js"

export interface ProjectionInfo {
  readonly projection: Projection
  readonly bindings: readonly ProjectionBinding[]
}

export interface SchemaInfo {
  readonly schema: SchemaDef
  readonly dropped: boolean
}

export interface GetProjection {
  readonly v: number
  readonly id: string
}

export interface ListProjections {
  readonly v: number
  readonly topics: readonly string[]
  readonly nameContains?: string
  readonly idPrefix?: string
  readonly search?: string
}

export interface GetSchema {
  readonly v: number
  readonly id: number
}

export interface ListSchemas {
  readonly v: number
  readonly nameContains?: string
}

export interface RegisterSchema {
  readonly v: number
  readonly source: SchemaSource
  readonly name?: string
  readonly version?: number
}

export interface DecodeRecord {
  readonly v: number
  readonly id: number
  readonly payload: Uint8Array
}

export type BrowseOutcome =
  | { readonly kind: "projections"; readonly projections: readonly ProjectionInfo[] }
  | { readonly kind: "projection"; readonly projection?: ProjectionInfo }
  | { readonly kind: "schemas"; readonly schemas: readonly SchemaInfo[] }
  | { readonly kind: "schema"; readonly schema?: SchemaInfo }
  | { readonly kind: "schemaRegistered"; readonly id: number }
  | { readonly kind: "decoded"; readonly value?: unknown }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

export type BrowseReply =
  | { readonly kind: "ok"; readonly outcome: BrowseOutcome }
  | { readonly kind: "err"; readonly error: QueryError }
  | { readonly kind: "unrecognized"; readonly tag: string; readonly value: unknown }

function encodeVersioned(
  v: number,
  entries: readonly (readonly [string, unknown])[]
): Map<string, unknown> {
  return new Map<string, unknown>([["v", BigInt(v)], ...entries])
}

export function encodeGetProjection(request: GetProjection): Map<string, unknown> {
  return encodeVersioned(request.v, [["id", request.id]])
}

export function decodeGetProjection(map: CborMap, context: string): GetProjection {
  return { v: field.requiredU32(map, "v", context), id: field.requiredString(map, "id", context) }
}

export function encodeListProjections(request: ListProjections): Map<string, unknown> {
  const map = encodeVersioned(request.v, [])
  if (request.topics.length > 0) map.set("topics", [...request.topics])
  if (request.nameContains !== undefined) map.set("name_contains", request.nameContains)
  if (request.idPrefix !== undefined) map.set("id_prefix", request.idPrefix)
  if (request.search !== undefined) map.set("search", request.search)
  return map
}

export function decodeListProjections(map: CborMap, context: string): ListProjections {
  const nameContains = field.optionalString(map, "name_contains", context)
  const idPrefix = field.optionalString(map, "id_prefix", context)
  const search = field.optionalString(map, "search", context)
  return {
    v: field.requiredU32(map, "v", context),
    topics: field.optionalArray(map, "topics", context, (item, index) => {
      if (typeof item !== "string") {
        throw new CodecError(
          `${context}.topics[${String(index)}] must be a string`,
          context,
          "topics"
        )
      }
      return item
    }),
    ...(nameContains !== undefined ? { nameContains } : {}),
    ...(idPrefix !== undefined ? { idPrefix } : {}),
    ...(search !== undefined ? { search } : {})
  }
}

export function encodeGetSchema(request: GetSchema): Map<string, unknown> {
  return encodeVersioned(request.v, [["id", BigInt(request.id)]])
}

export function decodeGetSchema(map: CborMap, context: string): GetSchema {
  return { v: field.requiredU32(map, "v", context), id: field.requiredU32(map, "id", context) }
}

export function encodeListSchemas(request: ListSchemas): Map<string, unknown> {
  const map = encodeVersioned(request.v, [])
  if (request.nameContains !== undefined) map.set("name_contains", request.nameContains)
  return map
}

export function decodeListSchemas(map: CborMap, context: string): ListSchemas {
  const nameContains = field.optionalString(map, "name_contains", context)
  return {
    v: field.requiredU32(map, "v", context),
    ...(nameContains !== undefined ? { nameContains } : {})
  }
}

export function encodeRegisterSchema(request: RegisterSchema): Map<string, unknown> {
  const map = encodeVersioned(request.v, [["source", encodeSchemaSource(request.source)]])
  if (request.name !== undefined) map.set("name", request.name)
  if (request.version !== undefined) map.set("version", BigInt(request.version))
  return map
}

export function decodeRegisterSchema(map: CborMap, context: string): RegisterSchema {
  const name = field.optionalString(map, "name", context)
  const version = field.optionalU32(map, "version", context)
  return {
    v: field.requiredU32(map, "v", context),
    source: decodeSchemaSource(field.requiredMap(map, "source", context), `${context}.source`),
    ...(name !== undefined ? { name } : {}),
    ...(version !== undefined ? { version } : {})
  }
}

export function encodeDecodeRecord(request: DecodeRecord): Map<string, unknown> {
  return encodeVersioned(request.v, [
    ["id", BigInt(request.id)],
    ["payload", request.payload]
  ])
}

export function decodeDecodeRecord(map: CborMap, context: string): DecodeRecord {
  return {
    v: field.requiredU32(map, "v", context),
    id: field.requiredU32(map, "id", context),
    payload: field.requiredBytes(map, "payload", context)
  }
}

export function encodeProjectionInfo(info: ProjectionInfo): Map<string, unknown> {
  const map = new Map<string, unknown>([["projection", encodeProjection(info.projection)]])
  if (info.bindings.length > 0)
    map.set(
      "bindings",
      info.bindings.map((binding) => encodeProjectionBinding(binding))
    )
  return map
}

export function decodeProjectionInfo(map: CborMap, context: string): ProjectionInfo {
  return {
    projection: decodeProjection(
      field.requiredMap(map, "projection", context),
      `${context}.projection`
    ),
    bindings: field.optionalArray(map, "bindings", context, (item, index) =>
      decodeProjectionBinding(
        expectMap(item, `${context}.bindings[${String(index)}]`),
        `${context}.bindings[${String(index)}]`
      )
    )
  }
}

export function encodeSchemaInfo(info: SchemaInfo): Map<string, unknown> {
  return new Map<string, unknown>([
    ["schema", encodeSchemaDef(info.schema)],
    ["dropped", info.dropped]
  ])
}

export function decodeSchemaInfo(map: CborMap, context: string): SchemaInfo {
  return {
    schema: decodeSchemaDef(field.requiredMap(map, "schema", context), `${context}.schema`),
    dropped: field.optionalBoolean(map, "dropped", context) ?? false
  }
}

export function encodeBrowseOutcome(outcome: BrowseOutcome): Map<string, unknown> {
  switch (outcome.kind) {
    case "projections":
      return new Map([
        ["Projections", outcome.projections.map((info) => encodeProjectionInfo(info))]
      ])
    case "projection":
      return new Map([
        [
          "Projection",
          outcome.projection === undefined ? null : encodeProjectionInfo(outcome.projection)
        ]
      ])
    case "schemas":
      return new Map([["Schemas", outcome.schemas.map((info) => encodeSchemaInfo(info))]])
    case "schema":
      return new Map([
        ["Schema", outcome.schema === undefined ? null : encodeSchemaInfo(outcome.schema)]
      ])
    case "schemaRegistered":
      return new Map([["SchemaRegistered", BigInt(outcome.id)]])
    case "decoded":
      return new Map([["Decoded", outcome.value ?? null]])
    case "unrecognized":
      return new Map([[outcome.tag, outcome.value]])
  }
}

export function decodeBrowseOutcome(value: unknown, context: string): BrowseOutcome {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Projections":
      return { kind: "projections", projections: decodeArray(inner, context, decodeProjectionInfo) }
    case "Projection":
      return inner === null
        ? { kind: "projection" }
        : {
            kind: "projection",
            projection: decodeProjectionInfo(expectMap(inner, context), context)
          }
    case "Schemas":
      return { kind: "schemas", schemas: decodeArray(inner, context, decodeSchemaInfo) }
    case "Schema":
      return inner === null
        ? { kind: "schema" }
        : { kind: "schema", schema: decodeSchemaInfo(expectMap(inner, context), context) }
    case "SchemaRegistered":
      return { kind: "schemaRegistered", id: decodeU32(inner, context) }
    case "Decoded":
      return inner === null ? { kind: "decoded" } : { kind: "decoded", value: inner }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

export function encodeBrowseReply(reply: BrowseReply): Map<string, unknown> {
  switch (reply.kind) {
    case "ok":
      return new Map([["Ok", encodeBrowseOutcome(reply.outcome)]])
    case "err":
      return new Map([["Err", encodeQueryError(reply.error)]])
    case "unrecognized":
      return new Map([[reply.tag, reply.value]])
  }
}

export function decodeBrowseReply(value: unknown, context: string): BrowseReply {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "Ok":
      return { kind: "ok", outcome: decodeBrowseOutcome(inner, context) }
    case "Err":
      return { kind: "err", error: decodeQueryError(inner, context) }
    default:
      return { kind: "unrecognized", tag, value: inner }
  }
}

function decodeArray<T>(
  value: unknown,
  context: string,
  decode: (map: CborMap, context: string) => T
): T[] {
  if (!Array.isArray(value)) throw new CodecError(`${context} must be an array`, context, "value")
  return value.map((item, index) =>
    decode(expectMap(item, `${context}[${String(index)}]`), `${context}[${String(index)}]`)
  )
}

function decodeU32(value: unknown, context: string): number {
  if (typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 0xffff_ffff)
    return value
  throw new CodecError(`${context} must fit u32`, context, "value")
}
