import { MAX_FILTER_SCHEMA_BYTES, MAX_FILTER_SCHEMA_DEPTH } from "./limits.js"
import avro from "avsc"
import protobuf from "protobufjs"
import "protobufjs/ext/descriptor.js"
import { InvalidError } from "../client/errors.js"
import { toNodeBuffer } from "../iggy/apache-iggy.js"
import type { SchemaDef } from "./control.js"
import { parseCanonicalJson, type ConsumerFilter, type FaultReason } from "./filter.js"
import type { DecodeLimits, FilterRecord, JsonValue } from "./filter-eval.js"

const UTF8_STRICT = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true })

export class PayloadFault extends Error {
  constructor(readonly reason: FaultReason) {
    super(`the payload does not decode: ${reason}`)
  }
}

type WriterSchema =
  | { readonly kind: "avro"; readonly schema: avro.Type }
  | { readonly kind: "protobuf"; readonly schema: protobuf.Type }

const exactLong = avro.types.LongType.__with({
  fromBuffer(value: Uint8Array): bigint {
    return new DataView(value.buffer, value.byteOffset, value.byteLength).getBigInt64(0, true)
  },
  toBuffer(value: bigint): Uint8Array {
    const bytes = new Uint8Array(8)
    new DataView(bytes.buffer).setBigInt64(0, value, true)
    return toNodeBuffer(bytes)
  },
  fromJSON(value: string): bigint {
    return BigInt(value)
  },
  toJSON(value: bigint): string {
    return value.toString()
  },
  isValid(value: unknown): boolean {
    return typeof value === "bigint" && value >= -(1n << 63n) && value < 1n << 63n
  },
  compare(left: bigint, right: bigint): number {
    return left < right ? -1 : left > right ? 1 : 0
  }
})

/** Immutable writer schemas prepared before scanning or local evaluation. */
export class PayloadDecoders {
  private readonly schemas = new Map<number, WriterSchema>()

  constructor(
    private readonly filter: ConsumerFilter,
    schemas: readonly SchemaDef[]
  ) {
    for (const id of filter.schemaRefs) {
      const found = schemas.find((schema) => schema.id === id)
      if (found === undefined)
        throw new InvalidError(`writer schema ${String(id)} was not supplied`)
      if (found.source.kind !== filter.codec)
        throw new InvalidError(`writer schema ${String(id)} does not match codec ${filter.codec}`)
      try {
        switch (found.source.kind) {
          case "avro": {
            if (new TextEncoder().encode(found.source.schema).byteLength > MAX_FILTER_SCHEMA_BYTES)
              throw new InvalidError("writer schema exceeds 1 MiB")
            parseCanonicalJson(found.source.schema, "payload")
            const source = JSON.parse(found.source.schema) as unknown
            const pending: [unknown, number][] = [[source, 1]]
            while (pending.length > 0) {
              const entry = pending.pop()
              if (entry === undefined) break
              const [value, depth] = entry
              if (value !== null && typeof value === "object") {
                if (depth > MAX_FILTER_SCHEMA_DEPTH)
                  throw new InvalidError("writer schema exceeds depth 64")
                for (const child of Object.values(value)) pending.push([child, depth + 1])
              }
            }
            const schema = avro.Type.forSchema(source as never, {
              wrapUnions: true,
              typeHook: (schema) =>
                schema === "long" ||
                (typeof schema === "object" &&
                  !Array.isArray(schema) &&
                  "type" in schema &&
                  schema.type === "long")
                  ? exactLong
                  : undefined
            })
            this.schemas.set(id, { kind: "avro", schema })
            break
          }
          case "protobuf": {
            if (found.source.descriptorSet.byteLength > MAX_FILTER_SCHEMA_BYTES)
              throw new InvalidError("writer schema exceeds 1 MiB")
            const root = protobuf.Root.fromDescriptor(found.source.descriptorSet)
            const schema = root.lookupType(found.source.messageType.replace(/^\./, ""))
            schema.resolveAll()
            this.schemas.set(id, { kind: "protobuf", schema })
            break
          }
          case "unknown":
            throw new InvalidError(`writer schema ${String(id)} has no payload decoder`)
        }
      } catch (cause) {
        throw new InvalidError(`writer schema ${String(id)} does not compile`, undefined, { cause })
      }
    }
  }

  decode(record: FilterRecord, limits: DecodeLimits): JsonValue | PayloadFault {
    try {
      if (record.payload.byteLength > limits.maxPayloadBytes) throw new PayloadFault("too_large")
      const input = new Input(record.payload, limits)
      if (this.filter.codec === "cbor") {
        const value = input.cbor(0)
        input.finish()
        return value
      }
      const header = record.headers.findLast((header) => header.key === "agdx.sid")
      if (header?.value.kind !== "uint" || header.value.value > 0xffffffffn)
        throw new PayloadFault("missing_schema")
      const schema = this.schemas.get(Number(header.value.value))
      if (schema === undefined) throw new PayloadFault("schema_not_allowed")
      if (schema.kind !== this.filter.codec) throw new PayloadFault("schema_mismatch")
      if (schema.kind === "avro") {
        input.avro(schema.schema, 0)
        input.finish()
        return avroValue(
          schema.schema,
          schema.schema.fromBuffer(toNodeBuffer(record.payload)) as unknown
        )
      }
      input.protobuf(schema.schema, 0)
      return protoMessage(schema.schema, schema.schema.decode(record.payload))
    } catch (error) {
      return error instanceof PayloadFault ? error : new PayloadFault("malformed")
    }
  }
}

function normalize(value: unknown): JsonValue {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean" ||
    typeof value === "bigint"
  )
    return value
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new PayloadFault("malformed")
    return value
  }
  if (value instanceof Uint8Array) return [...value].map(BigInt)
  if (Array.isArray(value)) return value.map(normalize)
  if (value instanceof Map) {
    const entries: [string, JsonValue][] = []
    for (const [key, item] of value as Map<unknown, unknown>) {
      if (typeof key !== "string") throw new PayloadFault("malformed")
      entries.push([key, normalize(item)])
    }
    return new Map(entries)
  }
  if (typeof value === "object")
    return new Map(Object.entries(value).map(([key, item]) => [key, normalize(item)]))
  throw new PayloadFault("malformed")
}

function avroValue(schema: avro.Type, value: unknown): JsonValue {
  switch (schema.typeName) {
    case "int":
    case "long":
    case "abstract:long":
      return BigInt(String(value))
    case "record":
    case "error": {
      const values = value as Record<string, unknown>
      return new Map(
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
      return new Map(
        Object.entries(value as Record<string, unknown>).map(([key, item]) => [
          key,
          avroValue((schema as avro.types.MapType).valuesType as avro.Type, item)
        ])
      )
    case "union:wrapped":
    case "union:unwrapped": {
      if (value === null) return null
      const entry = Object.entries(value as Record<string, unknown>)[0]
      if (entry === undefined) throw new PayloadFault("malformed")
      const [name, item] = entry
      const branch = (schema as avro.types.WrappedUnionType).types.find(
        (branch) => (branch.name ?? branch.typeName.replace(/^abstract:/, "")) === name
      )
      if (branch === undefined) throw new PayloadFault("malformed")
      return avroValue(branch, item)
    }
    default:
      return normalize(value)
  }
}

function protobufMapKey(hash: string, unsigned: boolean): string {
  if (hash.length !== 8) throw new PayloadFault("malformed")
  let value = 0n
  for (let index = 0; index < hash.length; index += 1) {
    const byte = hash.charCodeAt(index)
    if (byte > 255) throw new PayloadFault("malformed")
    value |= BigInt(byte) << BigInt(index * 8)
  }
  return (unsigned ? value : BigInt.asIntN(64, value)).toString()
}

function protoMessage(schema: protobuf.Type, message: object): JsonValue {
  const result = new Map<string, JsonValue>()
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
    const scalar = (value: unknown): JsonValue => {
      if (field.resolvedType instanceof protobuf.Type)
        return protoMessage(field.resolvedType, value as object)
      if (["int64", "sint64", "uint64", "fixed64", "sfixed64"].includes(field.type))
        return BigInt(String(value))
      if (
        field.resolvedType instanceof protobuf.Enum ||
        ["int32", "sint32", "uint32", "fixed32", "sfixed32"].includes(field.type)
      )
        return BigInt(String(value))
      return normalize(value)
    }
    const lowered = field.map
      ? new Map(
          Object.entries(value as Record<string, unknown>).map(([key, item]) => [
            field instanceof protobuf.MapField &&
            ["int64", "sint64", "uint64", "fixed64", "sfixed64"].includes(field.keyType)
              ? protobufMapKey(key, ["uint64", "fixed64"].includes(field.keyType))
              : key,
            scalar(item)
          ])
        )
      : field.repeated
        ? (value as readonly unknown[]).map(scalar)
        : scalar(value)
    // A field without presence tracking (a proto3 scalar or any repeated
    // field) is always part of the message, at its default when the bytes
    // omit it, as in Rust.
    result.set(field.name, lowered)
  }
  return result
}

class Input {
  private position = 0
  private remaining: number
  constructor(
    private readonly bytes: Uint8Array,
    private readonly limits: DecodeLimits
  ) {
    this.remaining = Math.max(1, limits.maxPayloadBytes)
  }
  finish(): void {
    if (this.position !== this.bytes.length) throw new PayloadFault("malformed")
  }
  private visit(depth: number): void {
    if (depth > this.limits.maxDepth) throw new PayloadFault("too_deep")
    if (--this.remaining < 0) throw new PayloadFault("too_large")
  }
  private take(count: number): Uint8Array {
    if (!Number.isSafeInteger(count) || count < 0 || count > this.bytes.length - this.position)
      throw new PayloadFault("malformed")
    const value = this.bytes.subarray(this.position, this.position + count)
    this.position += count
    return value
  }
  private varint(): bigint {
    let result = 0n
    for (let shift = 0n; shift < 70n; shift += 7n) {
      const byte = this.take(1)[0] ?? 0
      if (shift === 63n && byte > 1) throw new PayloadFault("malformed")
      result |= BigInt(byte & 127) << shift
      if (byte < 128) return result
    }
    throw new PayloadFault("malformed")
  }
  private long(): bigint {
    const value = this.varint()
    return (value >> 1n) ^ -(value & 1n)
  }
  private sized(text = false): void {
    const bytes = this.take(Number(this.long()))
    if (text) UTF8_STRICT.decode(bytes)
  }
  avro(schema: avro.Type, depth: number): void {
    this.visit(depth)
    switch (schema.typeName) {
      case "null":
        return
      case "boolean":
        if ((this.take(1)[0] ?? 0) > 1) throw new PayloadFault("malformed")
        return
      case "int":
      case "long":
      case "abstract:long":
      case "enum":
        this.long()
        return
      case "float":
      case "double": {
        const width = schema.typeName === "float" ? 4 : 8
        const bytes = this.take(width)
        const view = new DataView(bytes.buffer, bytes.byteOffset, width)
        if (!Number.isFinite(width === 4 ? view.getFloat32(0, true) : view.getFloat64(0, true)))
          throw new PayloadFault("malformed")
        return
      }
      case "string":
        this.sized(true)
        return
      case "bytes":
        this.sized()
        return
      case "fixed":
        this.take((schema as avro.types.FixedType).size)
        return
      case "record":
      case "error":
        for (const field of (schema as avro.types.RecordType).fields)
          this.avro(field.type, depth + 1)
        return
      case "union:unwrapped":
      case "union:wrapped": {
        const variant = (schema as avro.types.UnwrappedUnionType).types[Number(this.long())]
        if (variant === undefined) throw new PayloadFault("malformed")
        this.avro(variant, depth + 1)
        return
      }
      case "array":
      case "map": {
        const item =
          schema.typeName === "array"
            ? (schema as avro.types.ArrayType).itemsType
            : ((schema as avro.types.MapType).valuesType as avro.Type)
        for (;;) {
          let count = this.long()
          if (count === 0n) return
          const size = count < 0n ? Number(this.long()) : undefined
          const end = size === undefined ? undefined : this.position + size
          if (count < 0n) count = -count
          if (count > BigInt(this.remaining)) throw new PayloadFault("too_large")
          for (let index = 0n; index < count; index += 1n) {
            if (schema.typeName === "map") this.sized(true)
            this.avro(item, depth + 1)
          }
          if (end !== undefined && this.position !== end) throw new PayloadFault("malformed")
        }
      }
      default:
        throw new PayloadFault("schema_mismatch")
    }
  }
  protobuf(schema: protobuf.Type, depth: number, mapValue?: protobuf.Field): void {
    this.visit(depth)
    while (this.position < this.bytes.length) {
      const key = this.varint()
      const number = Number(key >> 3n)
      if (number <= 0 || number > 0x1fffffff) throw new PayloadFault("malformed")
      const wire = Number(key & 7n)
      const declared = mapValue === undefined ? schema.fieldsById[number] : undefined
      if (declared !== undefined) {
        const expected =
          declared.map ||
          declared.resolvedType instanceof protobuf.Type ||
          ["string", "bytes"].includes(declared.type)
            ? 2
            : ["double", "fixed64", "sfixed64"].includes(declared.type)
              ? 1
              : ["float", "fixed32", "sfixed32"].includes(declared.type)
                ? 5
                : 0
        if (wire !== expected && !(declared.repeated && expected !== 2 && wire === 2))
          throw new PayloadFault("malformed")
      }
      switch (wire) {
        case 0:
          this.varint()
          break
        case 1:
          this.take(8)
          break
        case 5:
          this.take(4)
          break
        case 2: {
          const bytes = this.take(Number(this.varint()))
          const field = declared
          if (
            field?.type === "string" ||
            (mapValue !== undefined &&
              ((number === 1 &&
                mapValue instanceof protobuf.MapField &&
                mapValue.keyType === "string") ||
                (number === 2 && mapValue.type === "string")))
          )
            UTF8_STRICT.decode(bytes)
          const nested =
            mapValue !== undefined
              ? number === 2 && mapValue.resolvedType instanceof protobuf.Type
                ? mapValue.resolvedType
                : undefined
              : field?.map
                ? schema
                : field?.resolvedType instanceof protobuf.Type
                  ? field.resolvedType
                  : undefined
          if (nested !== undefined) {
            const child = new Input(bytes, this.limits)
            child.remaining = this.remaining
            child.protobuf(nested, depth + 1, field?.map === true ? field : undefined)
            this.remaining = child.remaining
          }
          break
        }
        default:
          throw new PayloadFault("malformed")
      }
    }
  }
  cbor(depth: number): JsonValue {
    this.visit(depth)
    const head = this.take(1)[0] ?? 0
    const major = head >> 5
    const info = head & 31
    if (major === 6 || info === 31) throw new PayloadFault("malformed")
    let value = BigInt(info)
    if (info >= 24) {
      if (info > 27) throw new PayloadFault("malformed")
      value = 0n
      for (const byte of this.take(2 ** (info - 24))) value = (value << 8n) | BigInt(byte)
    }
    switch (major) {
      case 0:
        return value
      case 1:
        if (value >= 1n << 63n) throw new PayloadFault("malformed")
        return -1n - value
      case 2:
        return [...this.take(Number(value))].map(BigInt)
      case 3:
        return UTF8_STRICT.decode(this.take(Number(value)))
      case 4: {
        if (value > BigInt(this.bytes.length - this.position)) throw new PayloadFault("malformed")
        const items: JsonValue[] = []
        for (let index = 0n; index < value; index++) items.push(this.cbor(depth + 1))
        return items
      }
      case 5: {
        if (value * 2n > BigInt(this.bytes.length - this.position))
          throw new PayloadFault("malformed")
        const items = new Map<string, JsonValue>()
        for (let index = 0n; index < value; index++) {
          const key = this.cbor(depth + 1)
          if (typeof key !== "string" || items.has(key)) throw new PayloadFault("malformed")
          items.set(key, this.cbor(depth + 1))
        }
        return items
      }
      case 7: {
        if (info === 20 || info === 21) return info === 21
        if (info === 22 || info === 23) return null
        let result: number
        if (info === 25) {
          const bits = Number(value)
          const exponent = (bits >> 10) & 31
          const fraction = bits & 1023
          result =
            (bits & 32768 ? -1 : 1) *
            (exponent === 0
              ? (2 ** -14 * fraction) / 1024
              : exponent === 31
                ? fraction === 0
                  ? Infinity
                  : NaN
                : 2 ** (exponent - 15) * (1 + fraction / 1024))
        } else if (info === 26 || info === 27) {
          const bytes = new Uint8Array(8)
          const view = new DataView(bytes.buffer)
          view.setBigUint64(0, value)
          result = info === 26 ? view.getFloat32(4) : view.getFloat64(0)
        } else throw new PayloadFault("malformed")
        if (!Number.isFinite(result)) throw new PayloadFault("malformed")
        return result
      }
      default:
        throw new PayloadFault("malformed")
    }
  }
}
