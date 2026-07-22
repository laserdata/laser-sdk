import { Ajv2020, type AnySchema, type ValidateFunction } from "ajv/dist/2020.js"
import avro, { type Type as AvroType } from "avsc"
import protobuf from "protobufjs"
import "protobufjs/ext/descriptor.js"

import { CodecError, InvalidError } from "./client/errors.js"
import { toNodeBuffer } from "./iggy/apache-iggy.js"
import type { Codec } from "./stream/codecs.js"
import type { SchemaDef } from "./wire/control.js"

export type CompiledSchemaKind = "avro" | "protobuf" | "jsonSchema"

type CompiledDefinition =
  | { readonly kind: "avro"; readonly schema: AvroType }
  | { readonly kind: "protobuf"; readonly message: protobuf.Type }
  | { readonly kind: "jsonSchema"; readonly validate: ValidateFunction }

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

function lowerDecodedValue(value: unknown): unknown {
  if (value instanceof Uint8Array) return Array.from(value)
  if (Array.isArray(value)) return value.map(lowerDecodedValue)
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, nested]) => [key, lowerDecodedValue(nested)])
    )
  }
  return value
}

export class CompiledSchema {
  readonly kind: CompiledSchemaKind

  private constructor(private readonly definition: CompiledDefinition) {
    this.kind = definition.kind
  }

  static compile(schema: SchemaDef): CompiledSchema {
    switch (schema.source.kind) {
      case "avro": {
        const source = parseJson(
          schema.source.schema,
          `schema ${String(schema.id)}: Avro schema is not valid JSON`
        )
        try {
          return new CompiledSchema({ kind: "avro", schema: avro.Type.forSchema(source as never) })
        } catch (cause) {
          throw new InvalidError(
            `schema ${String(schema.id)}: Avro schema does not compile`,
            undefined,
            { cause }
          )
        }
      }
      case "protobuf": {
        let root: protobuf.Root
        try {
          root = protobuf.Root.fromDescriptor(schema.source.descriptorSet)
        } catch (cause) {
          throw new InvalidError(
            `schema ${String(schema.id)}: Protobuf descriptor set does not decode`,
            undefined,
            { cause }
          )
        }
        const messageType = schema.source.messageType.replace(/^\./, "")
        try {
          return new CompiledSchema({
            kind: "protobuf",
            message: root.lookupType(messageType)
          })
        } catch (cause) {
          throw new InvalidError(
            `schema ${String(schema.id)}: descriptor set has no message \`${schema.source.messageType}\``,
            undefined,
            { cause }
          )
        }
      }
      case "jsonSchema": {
        const source = parseJson(
          schema.source.schema,
          `schema ${String(schema.id)}: JSON Schema is not valid JSON`
        )
        try {
          const ajv = new Ajv2020({ strict: true })
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
          return lowerDecodedValue(this.definition.schema.fromBuffer(toNodeBuffer(payload)))
        } catch (cause) {
          throw codecError("payload does not decode as Avro", "decode", cause)
        }
      case "protobuf":
        try {
          const message = this.definition.message.decode(payload)
          return this.definition.message.toObject(message, {
            longs: String,
            enums: String,
            bytes: Array,
            defaults: false,
            oneofs: true
          })
        } catch (cause) {
          throw codecError("payload does not decode as Protobuf", "decode", cause)
        }
      case "jsonSchema": {
        let value: unknown
        try {
          value = JSON.parse(new TextDecoder().decode(payload)) as unknown
        } catch (cause) {
          throw codecError("payload does not parse as JSON", "decode", cause)
        }
        if (!this.definition.validate(value)) {
          throw codecError("payload fails its JSON Schema", "validate")
        }
        return value
      }
    }
  }

  encode(value: unknown): Uint8Array {
    switch (this.definition.kind) {
      case "avro":
        try {
          return Uint8Array.from(this.definition.schema.toBuffer(value))
        } catch (cause) {
          throw codecError("body does not match the Avro schema", "encode", cause)
        }
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

  codec<T>(decodeValue: (value: unknown) => T): Codec<T> {
    return {
      encode: (value) => this.encode(value),
      decode: (payload) => decodeValue(this.decode(payload))
    }
  }
}
