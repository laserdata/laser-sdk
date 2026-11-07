import { Ajv2020, type AnySchema, type ValidateFunction } from "ajv/dist/2020.js"
import type avro from "avsc"
import protobuf from "protobufjs"
import "protobufjs/ext/descriptor.js"

import { CodecError, InvalidError } from "./client/errors.js"
import { toNodeBuffer } from "./iggy/apache-iggy.js"
import type { Codec } from "./stream/codecs.js"
import { ContentType } from "./wire/content.js"
import { type SchemaDef, validateSchemaSource } from "./wire/control.js"
import { parseCanonicalJson } from "./wire/filter.js"
import {
  Input,
  PayloadFault,
  compileAvroWriter,
  compileProtobufWriter,
  protobufMapKey
} from "./wire/filter-codecs.js"
import { MAX_FILTER_PARSE_DEPTH, MAX_PROJECTOR_PAYLOAD_BYTES } from "./wire/limits.js"

export type CompiledSchemaKind = "avro" | "protobuf" | "jsonSchema"

type CompiledDefinition =
  | { readonly kind: "avro"; readonly schema: avro.Type }
  | { readonly kind: "protobuf"; readonly message: protobuf.Type }
  | { readonly kind: "jsonSchema"; readonly validate: ValidateFunction }

// The bounds a stored record decodes under, as Rust `RECORD_DECODE_LIMITS`.
const RECORD_DECODE_LIMITS = {
  maxPayloadBytes: MAX_PROJECTOR_PAYLOAD_BYTES,
  maxDepth: MAX_FILTER_PARSE_DEPTH
}
const LONG_FIELD_TYPES = new Set(["int64", "sint64", "uint64", "fixed64", "sfixed64"])
const UNSIGNED_LONG_FIELD_TYPES = new Set(["uint64", "fixed64"])

/**
 * A registered writer schema compiled once. `decode` returns the model every
 * client and the managed backend share: schema field names, numeric enums,
 * `bigint` for 64-bit integer types and `number` for the others, bytes as
 * arrays of numbers, proto3 defaults present and Avro logical types as their
 * physical values.
 */
export class CompiledSchema {
  readonly kind: CompiledSchemaKind

  private constructor(private readonly definition: CompiledDefinition) {
    this.kind = definition.kind
  }

  static compile(schema: SchemaDef): CompiledSchema {
    validateSchemaSource(schema.source)
    switch (schema.source.kind) {
      case "avro":
        try {
          return new CompiledSchema({
            kind: "avro",
            schema: compileAvroWriter(schema.source.schema)
          })
        } catch (cause) {
          throw new InvalidError(
            `schema ${String(schema.id)}: Avro schema does not compile`,
            undefined,
            { cause }
          )
        }
      case "protobuf":
        try {
          return new CompiledSchema({
            kind: "protobuf",
            message: compileProtobufWriter(schema.source.descriptorSet, schema.source.messageType)
          })
        } catch (cause) {
          throw new InvalidError(
            `schema ${String(schema.id)}: descriptor set has no message \`${schema.source.messageType}\``,
            undefined,
            { cause }
          )
        }
      case "jsonSchema": {
        const source = parseJson(
          schema.source.schema,
          `schema ${String(schema.id)}: JSON Schema is not valid JSON`
        )
        try {
          const ajv = new Ajv2020({ strict: false })
          return new CompiledSchema({
            kind: "jsonSchema",
            validate: ajv.compile(source as AnySchema)
          })
        } catch (cause) {
          throw new InvalidError(
            `schema ${String(schema.id)}: JSON Schema does not compile`,
            undefined,
            { cause }
          )
        }
      }
      case "unknown":
        throw new InvalidError(`schema ${String(schema.id)}: unknown schema source kind`)
    }
  }

  validate(payload: Uint8Array): boolean {
    try {
      this.decode(payload)
      return true
    } catch {
      return false
    }
  }

  validateValue(value: unknown): boolean {
    return this.definition.kind === "jsonSchema" && this.definition.validate(value)
  }

  decode(payload: Uint8Array): unknown {
    switch (this.definition.kind) {
      case "avro":
        try {
          const input = prepass(payload)
          input.avro(this.definition.schema, 0)
          input.finish()
          return avroValue(
            this.definition.schema,
            this.definition.schema.fromBuffer(toNodeBuffer(payload)) as unknown
          )
        } catch (cause) {
          throw codecError("payload does not decode as Avro", "decode", cause)
        }
      case "protobuf":
        try {
          prepass(payload).protobuf(this.definition.message, 0)
          return protobufMessage(this.definition.message, this.definition.message.decode(payload))
        } catch (cause) {
          throw codecError("payload does not decode as Protobuf", "decode", cause)
        }
      case "jsonSchema": {
        let text: string
        let value: unknown
        try {
          text = new TextDecoder("utf-8", { fatal: true }).decode(payload)
          value = jsonValue(parseCanonicalJson(text, "payload"))
        } catch (cause) {
          throw codecError("payload does not parse as JSON", "decode", cause)
        }
        if (!this.definition.validate(JSON.parse(text) as unknown)) {
          throw codecError("payload fails its JSON Schema", "validate")
        }
        return value
      }
    }
  }

  /**
   * Encode `body` as a bare Avro datum, exactly the bytes a producer stamps
   * alongside `agdx.sid`. Values are physical: a decimal is its unscaled
   * bytes, a timestamp its long. Avro schemas only, any other kind throws
   * `InvalidError`.
   */
  encodeAvro(body: unknown): Uint8Array {
    if (this.definition.kind !== "avro") {
      throw new InvalidError("encodeAvro requires an Avro schema")
    }
    return encodeAvroDatum(this.definition.schema, body)
  }

  /** @internal */
  codec<T>(decodeValue: (value: unknown) => T): Codec<T> {
    return {
      contentType:
        this.definition.kind === "avro"
          ? ContentType.Avro
          : this.definition.kind === "protobuf"
            ? ContentType.Protobuf
            : ContentType.Json,
      encode: (value) => this.encodeBody(value),
      decode: (payload) => decodeValue(this.decode(payload))
    }
  }

  private encodeBody(value: unknown): Uint8Array {
    switch (this.definition.kind) {
      case "avro":
        return encodeAvroDatum(this.definition.schema, value)
      case "protobuf": {
        const validation = this.definition.message.verify(value as Record<string, unknown>)
        if (validation !== null) {
          throw codecError(`body does not match the Protobuf schema: ${validation}`, "encode")
        }
        try {
          return this.definition.message
            .encode(this.definition.message.fromObject(value as Record<string, unknown>))
            .finish()
        } catch (cause) {
          throw codecError("Protobuf message encode failed", "encode", cause)
        }
      }
      case "jsonSchema":
        if (!this.definition.validate(value)) {
          throw codecError("body fails its JSON Schema", "validate")
        }
        try {
          return new TextEncoder().encode(JSON.stringify(value))
        } catch (cause) {
          throw codecError("body does not encode as JSON", "encode", cause)
        }
    }
  }
}

function codecError(message: string, operation: string, cause?: unknown): CodecError {
  return new CodecError(message, "schema", operation, cause === undefined ? undefined : { cause })
}

function parseJson(text: string, message: string): unknown {
  try {
    return JSON.parse(text) as unknown
  } catch (cause) {
    throw new InvalidError(message, undefined, { cause })
  }
}

function prepass(payload: Uint8Array): Input {
  if (payload.byteLength > RECORD_DECODE_LIMITS.maxPayloadBytes) throw new PayloadFault("too_large")
  return new Input(payload, RECORD_DECODE_LIMITS)
}

// JSON carries no integer width, so an integer stays exact as a `bigint` only
// when a `number` would round it.
function jsonValue(value: unknown): unknown {
  if (typeof value === "bigint") return Number.isSafeInteger(Number(value)) ? Number(value) : value
  if (Array.isArray(value)) return value.map(jsonValue)
  if (value instanceof Map)
    return Object.fromEntries(
      [...(value as Map<string, unknown>)].map(([key, item]) => [key, jsonValue(item)])
    )
  return value
}

function avroValue(schema: avro.Type, value: unknown): unknown {
  switch (schema.typeName) {
    case "long":
    case "abstract:long":
      return BigInt(String(value))
    case "bytes":
    case "fixed":
      return Array.from(value as Uint8Array)
    case "record":
    case "error": {
      const values = value as Record<string, unknown>
      return Object.fromEntries(
        (schema as avro.types.RecordType).fields.map((field) => [
          field.name,
          avroValue(field.type, values[field.name])
        ])
      )
    }
    case "array":
      return (value as readonly unknown[]).map((item) =>
        avroValue((schema as avro.types.ArrayType).itemsType, item)
      )
    case "map":
      return Object.fromEntries(
        Object.entries(value as Record<string, unknown>).map(([key, item]) => [
          key,
          avroValue((schema as avro.types.MapType).valuesType as avro.Type, item)
        ])
      )
    case "union:wrapped": {
      if (value === null) return null
      const entry = Object.entries(value as Record<string, unknown>)[0]
      const branch =
        entry === undefined
          ? undefined
          : (schema as avro.types.WrappedUnionType).types.find(
              (branch) => branch.branchName === entry[0]
            )
      if (entry === undefined || branch === undefined) throw new PayloadFault("malformed")
      return avroValue(branch, entry[1])
    }
    default:
      return value
  }
}

function protobufMessage(schema: protobuf.Type, message: object): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  const values = message as Record<string, unknown>
  for (const field of schema.fieldsArray) {
    if (!Object.hasOwn(values, field.name)) {
      if (field.required) throw new PayloadFault("malformed")
      if (
        field.hasPresence ||
        (!field.map && !field.repeated && field.resolvedType instanceof protobuf.Type)
      )
        continue
    }
    const value = values[field.name]
    const scalar = (item: unknown): unknown => {
      if (field.resolvedType instanceof protobuf.Type)
        return protobufMessage(field.resolvedType, item as object)
      if (LONG_FIELD_TYPES.has(field.type)) return BigInt(String(item))
      if (field.resolvedType instanceof protobuf.Enum) return Number(item)
      if (item instanceof Uint8Array) return Array.from(item)
      return item
    }
    result[field.name] = field.map
      ? Object.fromEntries(
          Object.entries(value as Record<string, unknown>).map(([key, item]) => [
            field instanceof protobuf.MapField && LONG_FIELD_TYPES.has(field.keyType)
              ? protobufMapKey(key, UNSIGNED_LONG_FIELD_TYPES.has(field.keyType))
              : key,
            scalar(item)
          ])
        )
      : field.repeated
        ? (value as readonly unknown[]).map(scalar)
        : scalar(value)
  }
  return result
}

function prepareAvroValue(type: avro.Type, value: unknown): unknown {
  switch (type.typeName) {
    case "abstract:long":
      return typeof value === "number" && Number.isSafeInteger(value) ? BigInt(value) : value
    case "bytes":
    case "fixed":
      if (value instanceof Uint8Array) return toNodeBuffer(value)
      if (
        Array.isArray(value) &&
        value.every(
          (byte: unknown) =>
            typeof byte === "number" && Number.isInteger(byte) && byte >= 0 && byte <= 255
        )
      ) {
        return toNodeBuffer(Uint8Array.from(value as number[]))
      }
      return value
    case "record":
      if (typeof value !== "object" || value === null || Array.isArray(value)) return value
      return Object.fromEntries(
        (type as avro.types.RecordType).fields.map((field) => [
          field.name,
          prepareAvroValue(field.type, (value as Record<string, unknown>)[field.name])
        ])
      )
    case "array":
      return Array.isArray(value)
        ? value.map((item: unknown) =>
            prepareAvroValue((type as avro.types.ArrayType).itemsType, item)
          )
        : value
    case "map":
      return typeof value === "object" && value !== null && !Array.isArray(value)
        ? Object.fromEntries(
            Object.entries(value).map(([key, item]) => [
              key,
              prepareAvroValue((type as avro.types.MapType).valuesType as avro.Type, item)
            ])
          )
        : value
    // The first branch that accepts the plain value takes it, the Rust resolve rule.
    case "union:wrapped":
      for (const branch of (type as avro.types.WrappedUnionType).types) {
        const candidate = prepareAvroValue(branch, value)
        if (accepts(branch, candidate))
          return branch.typeName === "null" ? null : { [branch.branchName ?? ""]: candidate }
      }
      return value
    default:
      return value
  }
}

// avsc's numeric checks throw on a `bigint` instead of refusing it.
function accepts(branch: avro.Type, value: unknown): boolean {
  try {
    return branch.isValid(value)
  } catch {
    return false
  }
}

function encodeAvroDatum(schema: avro.Type, value: unknown): Uint8Array {
  try {
    return Uint8Array.from(schema.toBuffer(prepareAvroValue(schema, value)))
  } catch (cause) {
    throw codecError("body does not match the Avro schema", "encode", cause)
  }
}
