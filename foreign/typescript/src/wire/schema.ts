import { sha256 } from "@noble/hashes/sha2.js"
import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, expectMap, field } from "./cbor.js"
import { LogicalSchemaId } from "./ids.js"
import { MAX_VALUE_BYTES } from "./limits.js"

export const MAX_LOGICAL_SCHEMA_DEPTH = 16
export const MAX_LOGICAL_SCHEMA_FIELDS = 4096
export const MAX_LOGICAL_SCHEMA_BYTES = 1_048_576
export const MAX_FIELD_NAME_BYTES = 255
export const MAX_FIELD_DOC_BYTES = 4096
export const MAX_FIXED_BYTES = 1_048_576
export const MAX_DECIMAL_PRECISION = 38
export const MICROS_PER_DAY = 86_400_000_000n
export const PROVENANCE_FIELD_ID_START = 2_000_000_000

export const PROVENANCE_FIELDS: readonly (readonly [number, string])[] = [
  [2_000_000_001, "__laser_cluster_incarnation"],
  [2_000_000_002, "__laser_source_incarnation"],
  [2_000_000_003, "__laser_stream_id"],
  [2_000_000_004, "__laser_topic_id"],
  [2_000_000_005, "__laser_partition_id"],
  [2_000_000_006, "__laser_offset"],
  [2_000_000_007, "__laser_row_ordinal"],
  [2_000_000_008, "__laser_projection_id"],
  [2_000_000_009, "__laser_projection_version"],
  [2_000_000_010, "__laser_destination_id"],
  [2_000_000_011, "__laser_destination_generation"],
  [2_000_000_012, "__laser_original_payload"],
  [2_000_000_013, "__laser_original_content_type"],
  [2_000_000_014, "__laser_original_schema_id"]
]

export type LogicalType =
  | {
      readonly kind:
        | "boolean"
        | "int"
        | "long"
        | "float"
        | "double"
        | "date"
        | "time_micros"
        | "timestamp_micros"
        | "timestamp_tz_micros"
        | "string"
        | "uuid"
        | "binary"
    }
  | { readonly kind: "decimal"; readonly precision: number; readonly scale: number }
  | { readonly kind: "fixed"; readonly length: number }
  | { readonly kind: "struct"; readonly fields: readonly LogicalField[] }
  | {
      readonly kind: "list"
      readonly elementId: number
      readonly elementRequired: boolean
      readonly element: LogicalType
    }
  | {
      readonly kind: "map"
      readonly keyId: number
      readonly key: LogicalType
      readonly valueId: number
      readonly valueRequired: boolean
      readonly value: LogicalType
    }

export type LogicalTypeKind = LogicalType["kind"]

export interface LogicalField {
  readonly id: number
  readonly name: string
  readonly required: boolean
  readonly fieldType: LogicalType
  readonly doc?: string
}

export interface LogicalSchemaRef {
  readonly id: LogicalSchemaId
  readonly version: number
  readonly fingerprint: Uint8Array
}

export interface LogicalSchema {
  readonly schema: LogicalSchemaRef
  readonly fields: readonly LogicalField[]
}

export function encodeLogicalSchema(value: LogicalSchema): Map<string, unknown> {
  return new Map<string, unknown>([
    ["schema", encodeLogicalSchemaRef(value.schema)],
    ["fields", value.fields.map(encodeLogicalField)]
  ])
}

export function decodeLogicalSchema(map: CborMap, context: string): LogicalSchema {
  return {
    schema: decodeLogicalSchemaRef(field.requiredMap(map, "schema", context), `${context}.schema`),
    fields: field.requiredArray(map, "fields", context, (item, index) =>
      decodeLogicalField(
        expectMap(item, `${context}.fields[${String(index)}]`),
        `${context}.fields[${String(index)}]`
      )
    )
  }
}

export interface DecimalValue {
  readonly unscaled: Uint8Array
  readonly precision: number
  readonly scale: number
}

export type TypedValue =
  | { readonly kind: "null" }
  | { readonly kind: "boolean"; readonly value: boolean }
  | { readonly kind: "int"; readonly value: number }
  | {
      readonly kind: "long" | "time_micros" | "timestamp_micros" | "timestamp_tz_micros"
      readonly value: bigint
    }
  | { readonly kind: "date"; readonly value: number }
  | { readonly kind: "float" | "double"; readonly value: number }
  | { readonly kind: "decimal"; readonly value: DecimalValue }
  | { readonly kind: "string"; readonly value: string }
  | { readonly kind: "uuid" | "fixed" | "binary"; readonly value: Uint8Array }
  | { readonly kind: "struct"; readonly value: readonly FieldValue[] }
  | { readonly kind: "list"; readonly value: readonly TypedValue[] }
  | { readonly kind: "map"; readonly value: readonly MapEntry[] }

export interface FieldValue {
  readonly fieldId: number
  readonly value: TypedValue
}
export interface MapEntry {
  readonly key: TypedValue
  readonly value: TypedValue
}

function mapOf(entries: readonly (readonly [string, unknown])[]): Map<string, unknown> {
  return new Map(entries)
}

export function encodeLogicalSchemaRef(value: LogicalSchemaRef): Map<string, unknown> {
  return mapOf([
    ["id", value.id.toBytes()],
    ["version", value.version],
    ["fingerprint", value.fingerprint]
  ])
}

export function decodeLogicalSchemaRef(map: CborMap, context: string): LogicalSchemaRef {
  return {
    id: LogicalSchemaId.fromBytes(field.requiredBytes(map, "id", context)),
    version: field.requiredU32(map, "version", context),
    fingerprint: fixedBytes(
      field.requiredBytes(map, "fingerprint", context),
      32,
      `${context}.fingerprint`
    )
  }
}

export function encodeLogicalField(value: LogicalField): Map<string, unknown> {
  const map = mapOf([
    ["id", value.id],
    ["name", value.name],
    ["required", value.required],
    ["field_type", encodeLogicalType(value.fieldType)]
  ])
  if (value.doc !== undefined) map.set("doc", value.doc)
  return map
}

export function decodeLogicalField(map: CborMap, context: string): LogicalField {
  const doc = field.optionalString(map, "doc", context)
  return {
    id: field.requiredU32(map, "id", context),
    name: field.requiredString(map, "name", context),
    required: field.requiredBoolean(map, "required", context),
    fieldType: decodeLogicalType(
      field.requiredMap(map, "field_type", context),
      `${context}.field_type`
    ),
    ...(doc === undefined ? {} : { doc })
  }
}

export function encodeLogicalType(value: LogicalType): Map<string, unknown> {
  const map = new Map<string, unknown>([["kind", value.kind]])
  switch (value.kind) {
    case "decimal":
      map.set("precision", value.precision)
      map.set("scale", value.scale)
      break
    case "fixed":
      map.set("length", value.length)
      break
    case "struct":
      map.set("fields", value.fields.map(encodeLogicalField))
      break
    case "list":
      map.set("element_id", value.elementId)
      map.set("element_required", value.elementRequired)
      map.set("element", encodeLogicalType(value.element))
      break
    case "map":
      map.set("key_id", value.keyId)
      map.set("key", encodeLogicalType(value.key))
      map.set("value_id", value.valueId)
      map.set("value_required", value.valueRequired)
      map.set("value", encodeLogicalType(value.value))
      break
    case "boolean":
    case "int":
    case "long":
    case "float":
    case "double":
    case "date":
    case "time_micros":
    case "timestamp_micros":
    case "timestamp_tz_micros":
    case "string":
    case "uuid":
    case "binary":
      break
  }
  return map
}

const PRIMITIVE_TYPES = new Set([
  "boolean",
  "int",
  "long",
  "float",
  "double",
  "date",
  "time_micros",
  "timestamp_micros",
  "timestamp_tz_micros",
  "string",
  "uuid",
  "binary"
])

export function decodeLogicalType(map: CborMap, context: string): LogicalType {
  const kind = field.requiredString(map, "kind", context)
  if (PRIMITIVE_TYPES.has(kind)) return { kind } as LogicalType
  switch (kind) {
    case "decimal":
      return {
        kind,
        precision: field.requiredU8(map, "precision", context),
        scale: field.requiredU8(map, "scale", context)
      }
    case "fixed":
      return { kind, length: field.requiredU32(map, "length", context) }
    case "struct":
      return {
        kind,
        fields: field.requiredArray(map, "fields", context, (item, index) =>
          decodeLogicalField(
            expectMap(item, `${context}.fields[${String(index)}]`),
            `${context}.fields[${String(index)}]`
          )
        )
      }
    case "list":
      return {
        kind,
        elementId: field.requiredU32(map, "element_id", context),
        elementRequired: field.requiredBoolean(map, "element_required", context),
        element: decodeLogicalType(field.requiredMap(map, "element", context), `${context}.element`)
      }
    case "map":
      return {
        kind,
        keyId: field.requiredU32(map, "key_id", context),
        key: decodeLogicalType(field.requiredMap(map, "key", context), `${context}.key`),
        valueId: field.requiredU32(map, "value_id", context),
        valueRequired: field.requiredBoolean(map, "value_required", context),
        value: decodeLogicalType(field.requiredMap(map, "value", context), `${context}.value`)
      }
    default:
      throw new CodecError(`unknown logical type \`${kind}\``, context, "kind")
  }
}

export function encodeTypedValue(value: TypedValue): Map<string, unknown> {
  const map = new Map<string, unknown>([["kind", value.kind]])
  if (value.kind === "null") return map
  if (value.kind === "decimal")
    map.set(
      "value",
      mapOf([
        ["unscaled", value.value.unscaled],
        ["precision", value.value.precision],
        ["scale", value.value.scale]
      ])
    )
  else if (value.kind === "struct")
    map.set(
      "value",
      value.value.map((entry) =>
        mapOf([
          ["field_id", entry.fieldId],
          ["value", encodeTypedValue(entry.value)]
        ])
      )
    )
  else if (value.kind === "list") map.set("value", value.value.map(encodeTypedValue))
  else if (value.kind === "map")
    map.set(
      "value",
      value.value.map((entry) =>
        mapOf([
          ["key", encodeTypedValue(entry.key)],
          ["value", encodeTypedValue(entry.value)]
        ])
      )
    )
  else map.set("value", value.value)
  return map
}

export function decodeTypedValue(value: unknown, context: string): TypedValue {
  const map = expectMap(value, context)
  const kind = field.requiredString(map, "kind", context)
  switch (kind) {
    case "null":
      return { kind }
    case "boolean":
      return { kind, value: field.requiredBoolean(map, "value", context) }
    case "int":
      return { kind, value: field.requiredI32(map, "value", context) }
    case "long":
    case "time_micros":
    case "timestamp_micros":
    case "timestamp_tz_micros":
      return { kind, value: field.requiredI64(map, "value", context) }
    case "date":
      return { kind, value: field.requiredI32(map, "value", context) }
    case "float":
    case "double": {
      const number = map.get("value")
      if (typeof number !== "number")
        throw new CodecError("typed float value must be a number", context, "value")
      return { kind, value: number }
    }
    case "string":
      return { kind, value: field.requiredString(map, "value", context) }
    case "uuid":
      return { kind, value: fixedBytes(field.requiredBytes(map, "value", context), 16, context) }
    case "fixed":
    case "binary":
      return { kind, value: field.requiredBytes(map, "value", context) }
    case "decimal": {
      const decimal = field.requiredMap(map, "value", context)
      return {
        kind,
        value: {
          unscaled: field.requiredBytes(decimal, "unscaled", context),
          precision: field.requiredU8(decimal, "precision", context),
          scale: field.requiredU8(decimal, "scale", context)
        }
      }
    }
    case "struct":
      return {
        kind,
        value: field.requiredArray(map, "value", context, (item, index) => {
          const entry = expectMap(item, `${context}.value[${String(index)}]`)
          return {
            fieldId: field.requiredU32(entry, "field_id", context),
            value: decodeTypedValue(entry.get("value"), context)
          }
        })
      }
    case "list":
      return {
        kind,
        value: field.requiredArray(map, "value", context, (item, index) =>
          decodeTypedValue(item, `${context}.value[${String(index)}]`)
        )
      }
    case "map":
      return {
        kind,
        value: field.requiredArray(map, "value", context, (item, index) => {
          const entry = expectMap(item, `${context}.value[${String(index)}]`)
          return {
            key: decodeTypedValue(entry.get("key"), context),
            value: decodeTypedValue(entry.get("value"), context)
          }
        })
      }
    default:
      throw new CodecError(`unknown typed value \`${kind}\``, context, "kind")
  }
}

function hex(value: Uint8Array): string {
  return [...value].map((byte) => byte.toString(16).padStart(2, "0")).join("")
}

export function typedValueDiagnosticText(value: TypedValue): string {
  switch (value.kind) {
    case "null":
      return "null"
    case "boolean":
    case "int":
    case "float":
    case "double":
    case "date":
      return String(value.value)
    case "long":
    case "time_micros":
    case "timestamp_micros":
    case "timestamp_tz_micros":
      return value.value.toString()
    case "string":
      return value.value
    case "uuid": {
      const text = hex(value.value)
      return `${text.slice(0, 8)}-${text.slice(8, 12)}-${text.slice(12, 16)}-${text.slice(16, 20)}-${text.slice(20)}`
    }
    case "decimal":
      return `0x${hex(value.value.unscaled)} scale ${String(value.value.scale)}`
    case "fixed":
    case "binary":
      return `0x${hex(value.value)}`
    case "struct":
    case "list":
    case "map":
      return JSON.stringify(diagnosticJsonValue(encodeTypedValue(value)))
  }
}

function diagnosticJsonValue(value: unknown): unknown {
  if (typeof value === "bigint") return value.toString()
  if (value instanceof Uint8Array) return Array.from(value)
  if (value instanceof Map) {
    const object: Record<string, unknown> = {}
    for (const [key, item] of value as ReadonlyMap<unknown, unknown>)
      object[String(key)] = diagnosticJsonValue(item)
    return object
  }
  if (Array.isArray(value)) return value.map(diagnosticJsonValue)
  return value
}

export function validateTypedValue(value: TypedValue, depth = 1): void {
  if (depth > MAX_LOGICAL_SCHEMA_DEPTH) throw new InvalidError("typed value depth exceeds cap")
  switch (value.kind) {
    case "int":
    case "date":
      if (
        !Number.isInteger(value.value) ||
        value.value < -2_147_483_648 ||
        value.value > 2_147_483_647
      ) {
        throw new InvalidError("32-bit signed value is outside its bound")
      }
      break
    case "long":
    case "timestamp_micros":
    case "timestamp_tz_micros":
      validateI64(value.value)
      break
    case "float":
    case "double":
      if (!Number.isFinite(value.value) || Object.is(value.value, -0))
        throw new InvalidError("floating values must be finite and cannot be negative zero")
      break
    case "time_micros":
      validateI64(value.value)
      if (value.value < 0n || value.value >= MICROS_PER_DAY)
        throw new InvalidError("time value is outside one day in microseconds")
      break
    case "uuid":
      fixedBytes(value.value, 16, "UUID")
      break
    case "decimal":
      validateDecimal(value.value)
      break
    case "string":
      if (new TextEncoder().encode(value.value).length > MAX_VALUE_BYTES) {
        throw new InvalidError("string value exceeds its byte bound")
      }
      break
    case "fixed":
      if (value.value.length > MAX_FIXED_BYTES) {
        throw new InvalidError("fixed value exceeds its byte bound")
      }
      break
    case "binary":
      if (value.value.length > MAX_VALUE_BYTES) {
        throw new InvalidError("binary value exceeds its byte bound")
      }
      break
    case "struct": {
      if (value.value.length > MAX_LOGICAL_SCHEMA_FIELDS) {
        throw new InvalidError("struct value count exceeds cap")
      }
      let previous = 0
      for (const entry of value.value) {
        if (entry.fieldId <= previous)
          throw new InvalidError("struct field values must use increasing positive field ids")
        previous = entry.fieldId
        validateTypedValue(entry.value, depth + 1)
      }
      break
    }
    case "list":
      if (value.value.length > MAX_LOGICAL_SCHEMA_FIELDS) {
        throw new InvalidError("list value count exceeds cap")
      }
      for (const item of value.value) validateTypedValue(item, depth + 1)
      break
    case "map": {
      if (value.value.length > MAX_LOGICAL_SCHEMA_FIELDS) {
        throw new InvalidError("map value count exceeds cap")
      }
      let previous: Uint8Array | undefined
      for (const entry of value.value) {
        validateTypedValue(entry.key, depth + 1)
        validateTypedValue(entry.value, depth + 1)
        const key = canonicalMapKey(entry.key)
        if (previous !== undefined && compareBytes(previous, key) >= 0)
          throw new InvalidError("map entries must be ordered by canonical key")
        previous = key
      }
      break
    }
    case "null":
    case "boolean":
      break
  }
}

export function validateTypedValueAgainst(
  value: TypedValue,
  logicalType: LogicalType,
  required: boolean
): void {
  if (value.kind === "null") {
    if (required) throw new InvalidError("required value is null")
    return
  }
  validateTypedValue(value)
  switch (logicalType.kind) {
    case "boolean":
    case "int":
    case "long":
    case "float":
    case "double":
    case "date":
    case "time_micros":
    case "timestamp_micros":
    case "timestamp_tz_micros":
    case "string":
    case "uuid":
    case "binary":
      if (value.kind !== logicalType.kind)
        throw new InvalidError("typed value does not match logical type")
      return
    case "decimal":
      if (
        value.kind !== "decimal" ||
        value.value.precision !== logicalType.precision ||
        value.value.scale !== logicalType.scale
      )
        throw new InvalidError("decimal value does not match logical type")
      return
    case "fixed":
      if (value.kind !== "fixed" || value.value.length !== logicalType.length)
        throw new InvalidError("fixed value does not match logical type length")
      return
    case "struct":
      if (value.kind !== "struct" || value.value.length !== logicalType.fields.length)
        throw new InvalidError("struct value does not match logical type")
      for (let index = 0; index < logicalType.fields.length; index += 1) {
        const field = logicalType.fields[index]
        const entry = value.value[index]
        if (field === undefined || entry?.fieldId !== field.id)
          throw new InvalidError("struct field ids do not match logical type")
        validateTypedValueAgainst(entry.value, field.fieldType, field.required)
      }
      return
    case "list":
      if (value.kind !== "list") throw new InvalidError("typed value does not match logical type")
      for (const item of value.value)
        validateTypedValueAgainst(item, logicalType.element, logicalType.elementRequired)
      return
    case "map":
      if (value.kind !== "map") throw new InvalidError("typed value does not match logical type")
      for (const entry of value.value) {
        validateTypedValueAgainst(entry.key, logicalType.key, true)
        validateTypedValueAgainst(entry.value, logicalType.value, logicalType.valueRequired)
      }
  }
}

export function createLogicalSchema(
  id: LogicalSchemaId,
  version: number,
  fields: readonly LogicalField[]
): LogicalSchema {
  validateSchemaShape(id, version, fields)
  const withoutFingerprint: LogicalSchema = {
    schema: { id, version, fingerprint: new Uint8Array(32) },
    fields
  }
  return {
    ...withoutFingerprint,
    schema: { id, version, fingerprint: sha256(canonicalSchemaBytes(withoutFingerprint)) }
  }
}

export function validateLogicalSchema(schema: LogicalSchema): void {
  validateSchemaShape(schema.schema.id, schema.schema.version, schema.fields)
  const expected = sha256(canonicalSchemaBytes(schema))
  if (!equalBytes(expected, schema.schema.fingerprint))
    throw new InvalidError("logical schema fingerprint does not match canonical bytes")
}

export function validateResultFields(fields: readonly LogicalField[]): void {
  if (fields.length === 0) throw new InvalidError("query result schema must not be empty")
  validateFieldShape(fields, true)
}

export function canonicalSchemaBytes(schema: LogicalSchema): Uint8Array {
  validateSchemaShape(schema.schema.id, schema.schema.version, schema.fields)
  const encoder = new SchemaEncoder()
  encoder.bytes(new TextEncoder().encode("AGDX-SCHEMA-V1\0"))
  encoder.bytes(schema.schema.id.toBytes())
  encoder.u32(schema.schema.version)
  encodeFingerprintFields(encoder, schema.fields)
  const bytes = encoder.finish()
  if (bytes.length > MAX_LOGICAL_SCHEMA_BYTES) {
    throw new InvalidError("logical schema canonical bytes exceed cap")
  }
  return bytes
}

function validateSchemaShape(
  id: LogicalSchemaId,
  version: number,
  fields: readonly LogicalField[]
): void {
  if (id.asU128() === 0n || version === 0 || fields.length === 0)
    throw new InvalidError("logical schema identity, version, and fields must be nonzero")
  validateFieldShape(fields, false)
}

function validateFieldShape(
  fields: readonly LogicalField[],
  allowTopLevelProvenance: boolean
): void {
  const ids = new Set<number>()
  let count = 0
  let estimatedSize = 0
  const visit = (items: readonly LogicalField[], depth: number, allowProvenance: boolean): void => {
    if (depth > MAX_LOGICAL_SCHEMA_DEPTH) throw new InvalidError("logical schema depth exceeds cap")
    const names = new Set<string>()
    for (const item of items) {
      const provenancePair = PROVENANCE_FIELDS.some(
        ([reservedId, reservedName]) => reservedId === item.id && reservedName === item.name
      )
      if (
        item.id <= 0 ||
        ids.has(item.id) ||
        (item.id >= PROVENANCE_FIELD_ID_START && !(allowProvenance && provenancePair))
      )
        throw new InvalidError(`field id ${String(item.id)} is invalid or duplicated`)
      if (
        item.name.length === 0 ||
        new TextEncoder().encode(item.name).length > MAX_FIELD_NAME_BYTES ||
        item.name.trim() !== item.name ||
        hasControlCharacter(item.name) ||
        names.has(item.name) ||
        (PROVENANCE_FIELDS.some((entry) => entry[1] === item.name) &&
          !(allowProvenance && provenancePair))
      )
        throw new InvalidError(`field name \`${item.name}\` is invalid, duplicated, or reserved`)
      if (item.doc !== undefined && new TextEncoder().encode(item.doc).length > MAX_FIELD_DOC_BYTES)
        throw new InvalidError("field documentation exceeds cap")
      estimatedSize +=
        new TextEncoder().encode(item.name).length +
        (item.doc === undefined ? 0 : new TextEncoder().encode(item.doc).length) +
        16
      if (estimatedSize > MAX_LOGICAL_SCHEMA_BYTES)
        throw new InvalidError("logical schema exceeds its byte cap")
      ids.add(item.id)
      names.add(item.name)
      count += 1
      if (count > MAX_LOGICAL_SCHEMA_FIELDS)
        throw new InvalidError("logical schema field count exceeds cap")
      visitType(item.fieldType, depth)
    }
  }
  const nestedId = (value: number): void => {
    if (value <= 0 || value >= PROVENANCE_FIELD_ID_START || ids.has(value))
      throw new InvalidError("nested field id is invalid or duplicated")
    ids.add(value)
    count += 1
    if (count > MAX_LOGICAL_SCHEMA_FIELDS) {
      throw new InvalidError("logical schema field count exceeds cap")
    }
  }
  const visitType = (type: LogicalType, depth: number): void => {
    if (
      type.kind === "decimal" &&
      (type.precision < 1 || type.precision > MAX_DECIMAL_PRECISION || type.scale > type.precision)
    )
      throw new InvalidError("decimal precision or scale is invalid")
    if (type.kind === "fixed" && (type.length < 1 || type.length > MAX_FIXED_BYTES))
      throw new InvalidError("fixed length is invalid")
    if (type.kind === "struct") visit(type.fields, depth + 1, false)
    if (type.kind === "list") {
      nestedId(type.elementId)
      visitType(type.element, depth + 1)
    }
    if (type.kind === "map") {
      nestedId(type.keyId)
      nestedId(type.valueId)
      if (
        ![
          "boolean",
          "int",
          "long",
          "decimal",
          "date",
          "time_micros",
          "timestamp_micros",
          "timestamp_tz_micros",
          "string",
          "uuid",
          "fixed",
          "binary"
        ].includes(type.key.kind)
      )
        throw new InvalidError("map key type is not supported")
      visitType(type.key, depth + 1)
      visitType(type.value, depth + 1)
    }
  }
  visit(fields, 1, allowTopLevelProvenance)
}

function encodeFingerprintFields(encoder: SchemaEncoder, fields: readonly LogicalField[]): void {
  encoder.u32(fields.length)
  for (const item of fields) {
    encoder.u32(item.id)
    encoder.u8(item.required ? 1 : 0)
    encoder.text(item.name)
    encoder.u8(item.doc === undefined ? 0 : 1)
    if (item.doc !== undefined) encoder.text(item.doc)
    encodeFingerprintType(encoder, item.fieldType)
  }
}

function encodeFingerprintType(encoder: SchemaEncoder, type: LogicalType): void {
  const tags: Record<string, number> = {
    boolean: 0,
    int: 1,
    long: 2,
    float: 3,
    double: 4,
    decimal: 5,
    date: 6,
    time_micros: 7,
    timestamp_micros: 8,
    timestamp_tz_micros: 9,
    string: 10,
    uuid: 11,
    fixed: 12,
    binary: 13,
    struct: 14,
    list: 15,
    map: 16
  }
  encoder.u8(tags[type.kind] ?? 255)
  if (type.kind === "decimal") {
    encoder.u8(type.precision)
    encoder.u8(type.scale)
  }
  if (type.kind === "fixed") encoder.u32(type.length)
  if (type.kind === "struct") encodeFingerprintFields(encoder, type.fields)
  if (type.kind === "list") {
    encoder.u32(type.elementId)
    encoder.u8(type.elementRequired ? 1 : 0)
    encodeFingerprintType(encoder, type.element)
  }
  if (type.kind === "map") {
    encoder.u32(type.keyId)
    encodeFingerprintType(encoder, type.key)
    encoder.u32(type.valueId)
    encoder.u8(type.valueRequired ? 1 : 0)
    encodeFingerprintType(encoder, type.value)
  }
}

class SchemaEncoder {
  private readonly chunks: Uint8Array[] = []
  u8(value: number): void {
    this.chunks.push(Uint8Array.of(value))
  }
  u32(value: number): void {
    const bytes = new Uint8Array(4)
    new DataView(bytes.buffer).setUint32(0, value, false)
    this.chunks.push(bytes)
  }
  bytes(value: Uint8Array): void {
    this.chunks.push(value)
  }
  text(value: string): void {
    const bytes = new TextEncoder().encode(value)
    this.u32(bytes.length)
    this.bytes(bytes)
  }
  finish(): Uint8Array {
    const size = this.chunks.reduce((total, chunk) => total + chunk.length, 0)
    const output = new Uint8Array(size)
    let offset = 0
    for (const chunk of this.chunks) {
      output.set(chunk, offset)
      offset += chunk.length
    }
    return output
  }
}

function fixedBytes(value: Uint8Array, length: number, context: string): Uint8Array {
  if (value.length !== length)
    throw new CodecError(`${context} must contain ${String(length)} bytes`, context, "bytes")
  return value
}
function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index])
}
function compareBytes(left: Uint8Array, right: Uint8Array): number {
  for (let index = 0; index < Math.min(left.length, right.length); index += 1) {
    const delta = (left[index] ?? 0) - (right[index] ?? 0)
    if (delta !== 0) return delta
  }
  return left.length - right.length
}
function validateDecimal(value: DecimalValue): void {
  if (
    value.precision < 1 ||
    value.precision > MAX_DECIMAL_PRECISION ||
    value.scale > value.precision ||
    value.unscaled.length < 1 ||
    value.unscaled.length > 16 ||
    (value.unscaled.length > 1 &&
      ((value.unscaled[0] === 0 && (value.unscaled[1] ?? 0) < 0x80) ||
        (value.unscaled[0] === 0xff && (value.unscaled[1] ?? 0) >= 0x80)))
  )
    throw new InvalidError("decimal value is not canonical")
  let unscaled = 0n
  for (const byte of value.unscaled) unscaled = (unscaled << 8n) | BigInt(byte)
  if ((value.unscaled[0] ?? 0) >= 0x80) {
    unscaled -= 1n << BigInt(value.unscaled.length * 8)
  }
  const digits = (unscaled < 0n ? -unscaled : unscaled).toString().length
  if (digits > value.precision) {
    throw new InvalidError("decimal value exceeds its declared precision")
  }
}

function validateI64(value: bigint): void {
  if (value < -9_223_372_036_854_775_808n || value > 9_223_372_036_854_775_807n) {
    throw new InvalidError("64-bit signed value is outside its bound")
  }
}
function canonicalMapKey(value: TypedValue): Uint8Array {
  validateTypedValue(value)
  switch (value.kind) {
    case "boolean":
      return Uint8Array.of(0, value.value ? 1 : 0)
    case "int":
      return prefixedSigned(1, value.value, 4)
    case "long":
      return prefixedSigned(2, value.value, 8)
    case "decimal":
      return concatBytes(
        Uint8Array.of(3, value.value.precision, value.value.scale),
        value.value.unscaled
      )
    case "date":
      return prefixedSigned(4, value.value, 4)
    case "time_micros":
      return prefixedSigned(5, value.value, 8)
    case "timestamp_micros":
      return prefixedSigned(6, value.value, 8)
    case "timestamp_tz_micros":
      return prefixedSigned(7, value.value, 8)
    case "string":
      return concatBytes(Uint8Array.of(8), new TextEncoder().encode(value.value))
    case "uuid":
      return concatBytes(Uint8Array.of(9), value.value)
    case "fixed":
      return concatBytes(Uint8Array.of(10), value.value)
    case "binary":
      return concatBytes(Uint8Array.of(11), value.value)
    case "float":
    case "double":
    case "struct":
    case "list":
    case "map":
    case "null":
      throw new InvalidError("map key must be a deterministic non-floating primitive")
  }
}

function prefixedSigned(tag: number, value: number | bigint, width: 4 | 8): Uint8Array {
  const bytes = new Uint8Array(width + 1)
  bytes[0] = tag
  const view = new DataView(bytes.buffer)
  if (width === 4) view.setInt32(1, Number(value), false)
  else view.setBigInt64(1, BigInt(value), false)
  return bytes
}

function concatBytes(left: Uint8Array, right: Uint8Array): Uint8Array {
  const bytes = new Uint8Array(left.length + right.length)
  bytes.set(left)
  bytes.set(right, left.length)
  return bytes
}
function hasControlCharacter(value: string): boolean {
  for (const character of value) {
    const code = character.codePointAt(0) ?? 0
    if (code < 32 || code === 127) return true
  }
  return false
}
