import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, expectMap, expectString, field, singleVariantTag } from "./cbor.js"
import { ContentType, type ContentType as ContentTypeValue } from "./content.js"

export type FieldType = "text" | "int" | "float" | "bool"

export interface IndexField {
  readonly name: string
  readonly pointer: string
  readonly fieldType?: FieldType
}

export interface IndexSchema {
  readonly fields: readonly IndexField[]
  readonly vectorField?: string
  readonly inlinePayload: boolean
}

export type ProjectionId = string & { readonly __projectionId: unique symbol }

export function parseProjectionId(value: string): ProjectionId {
  if (value.length === 0) throw new InvalidError("projection id must not be empty")
  return value as ProjectionId
}

function projectionIdFromWire(value: string): ProjectionId {
  return value as ProjectionId
}

export type ProjectionKind =
  | { readonly kind: "row" }
  | { readonly kind: "graph" }
  | { readonly kind: "unrecognized"; readonly code: number }

export interface EntitySchema {
  readonly nodes: readonly NodeExtract[]
  readonly edges: readonly EdgeExtract[]
}

export interface NodeExtract {
  readonly label: string
  readonly valuePointer: string
  readonly embeddingPointer?: string
}

export interface EdgeExtract {
  readonly edgeType: string
  readonly fromPointer: string
  readonly toPointer: string
  readonly validFromPointer?: string
  readonly validToPointer?: string
}

export interface Projection {
  readonly id: ProjectionId
  readonly name: string
  readonly version: number
  readonly kind: ProjectionKind
  readonly contentType: ContentTypeValue
  readonly extraction: IndexSchema
  readonly entitySchema?: EntitySchema
  readonly inlinePayloadDefault: boolean
}

export type RetentionPolicy =
  | { readonly kind: "mirrorLog" }
  | { readonly kind: "keep" }
  | { readonly kind: "keepUntilSourceDeleted" }
  | { readonly kind: "timeToLive"; readonly ttlMicros: bigint }
  | { readonly kind: "maxRows"; readonly rows: bigint }
  | { readonly kind: "unknown" }

export interface SourceSelector {
  readonly stream: string
  readonly topic: string
}

export type TargetRole = "readWrite" | "writeOnly"
export type Delivery = "effectivelyOnce" | "atMostOnce"

export interface Target {
  readonly backend: string
  readonly table: string
  readonly role: TargetRole
  readonly delivery: Delivery
  readonly required: boolean
}

export interface ProjectionBinding {
  readonly source: SourceSelector
  readonly allowedProjections: readonly ProjectionId[]
  readonly defaultProjection?: ProjectionId
  readonly targets: readonly Target[]
  readonly notify: boolean
  readonly retention?: RetentionPolicy
}

export type SchemaSource =
  | { readonly kind: "avro"; readonly schema: string }
  | {
      readonly kind: "protobuf"
      readonly descriptorSet: Uint8Array
      readonly messageType: string
    }
  | { readonly kind: "jsonSchema"; readonly schema: string }
  | { readonly kind: "unknown" }

export interface SchemaDef {
  readonly id: number
  readonly source: SchemaSource
  readonly name?: string
  readonly version?: number
}

export type ControlCommand =
  | { readonly kind: "registerProjection"; readonly projection: Projection }
  | { readonly kind: "dropProjection"; readonly id: string }
  | { readonly kind: "applyBinding"; readonly binding: ProjectionBinding }
  | {
      readonly kind: "removeBinding"
      readonly source: SourceSelector
      readonly projectionRef?: string
    }
  | { readonly kind: "registerSchema"; readonly schema: SchemaDef }
  | { readonly kind: "dropSchema"; readonly id: number }
  | { readonly kind: "registerGraph"; readonly projection: Projection }
  | { readonly kind: "dropGraph"; readonly id: string }
  | { readonly kind: "registerRunSource"; readonly source: SourceSelector }
  | { readonly kind: "removeRunSource"; readonly source: SourceSelector }

export interface ControlEnvelope {
  readonly v: number
  readonly timestampMicros: bigint
  readonly command: ControlCommand
}

const FIELD_TYPES: ReadonlySet<string> = new Set(["text", "int", "float", "bool"])
const CONTENT_TYPES: ReadonlySet<string> = new Set(Object.values(ContentType))

function parseFieldType(value: string, context: string): FieldType {
  if (!FIELD_TYPES.has(value)) {
    throw new CodecError(`\`${value}\` is not a recognized field type`, context, "field_type")
  }
  return value as FieldType
}

function parseContentType(value: string, context: string): ContentTypeValue {
  if (!CONTENT_TYPES.has(value)) {
    throw new CodecError(`\`${value}\` is not a recognized content type`, context, "content_type")
  }
  return value as ContentTypeValue
}

export function encodeIndexField(indexField: IndexField): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["name", indexField.name],
    ["pointer", indexField.pointer]
  ])
  if (indexField.fieldType !== undefined) map.set("field_type", indexField.fieldType)
  return map
}

export function decodeIndexField(map: CborMap, context: string): IndexField {
  const fieldType = field.optionalString(map, "field_type", context)
  return {
    name: field.requiredString(map, "name", context),
    pointer: field.requiredString(map, "pointer", context),
    ...(fieldType !== undefined ? { fieldType: parseFieldType(fieldType, context) } : {})
  }
}

export function encodeIndexSchema(schema: IndexSchema): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["fields", schema.fields.map((indexField) => encodeIndexField(indexField))]
  ])
  if (schema.vectorField !== undefined) map.set("vector_field", schema.vectorField)
  map.set("inline_payload", schema.inlinePayload)
  return map
}

export function decodeIndexSchema(map: CborMap, context: string): IndexSchema {
  const vectorField = field.optionalString(map, "vector_field", context)
  return {
    fields: field.requiredArray(map, "fields", context, (item, index) =>
      decodeIndexField(
        expectMap(item, `${context}.fields[${String(index)}]`),
        `${context}.fields[${String(index)}]`
      )
    ),
    ...(vectorField !== undefined ? { vectorField } : {}),
    inlinePayload: field.optionalBoolean(map, "inline_payload", context) ?? false
  }
}

export function projectionKindCode(kind: ProjectionKind): number {
  switch (kind.kind) {
    case "row":
      return 0
    case "graph":
      return 1
    case "unrecognized":
      return kind.code
  }
}

export function projectionKindFromCode(code: number): ProjectionKind {
  if (code === 0) return { kind: "row" }
  if (code === 1) return { kind: "graph" }
  return { kind: "unrecognized", code }
}

export function encodeEntitySchema(schema: EntitySchema): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["nodes", schema.nodes.map((node) => encodeNodeExtract(node))]
  ])
  if (schema.edges.length > 0)
    map.set(
      "edges",
      schema.edges.map((edge) => encodeEdgeExtract(edge))
    )
  return map
}

export function decodeEntitySchema(map: CborMap, context: string): EntitySchema {
  return {
    nodes: field.requiredArray(map, "nodes", context, (item, index) =>
      decodeNodeExtract(
        expectMap(item, `${context}.nodes[${String(index)}]`),
        `${context}.nodes[${String(index)}]`
      )
    ),
    edges: field.optionalArray(map, "edges", context, (item, index) =>
      decodeEdgeExtract(
        expectMap(item, `${context}.edges[${String(index)}]`),
        `${context}.edges[${String(index)}]`
      )
    )
  }
}

function encodeNodeExtract(node: NodeExtract): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["label", node.label],
    ["value_pointer", node.valuePointer]
  ])
  if (node.embeddingPointer !== undefined) map.set("embedding_pointer", node.embeddingPointer)
  return map
}

function decodeNodeExtract(map: CborMap, context: string): NodeExtract {
  const embeddingPointer = field.optionalString(map, "embedding_pointer", context)
  return {
    label: field.requiredString(map, "label", context),
    valuePointer: field.requiredString(map, "value_pointer", context),
    ...(embeddingPointer !== undefined ? { embeddingPointer } : {})
  }
}

function encodeEdgeExtract(edge: EdgeExtract): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["edge_type", edge.edgeType],
    ["from_pointer", edge.fromPointer],
    ["to_pointer", edge.toPointer]
  ])
  if (edge.validFromPointer !== undefined) map.set("valid_from_pointer", edge.validFromPointer)
  if (edge.validToPointer !== undefined) map.set("valid_to_pointer", edge.validToPointer)
  return map
}

function decodeEdgeExtract(map: CborMap, context: string): EdgeExtract {
  const validFromPointer = field.optionalString(map, "valid_from_pointer", context)
  const validToPointer = field.optionalString(map, "valid_to_pointer", context)
  return {
    edgeType: field.requiredString(map, "edge_type", context),
    fromPointer: field.requiredString(map, "from_pointer", context),
    toPointer: field.requiredString(map, "to_pointer", context),
    ...(validFromPointer !== undefined ? { validFromPointer } : {}),
    ...(validToPointer !== undefined ? { validToPointer } : {})
  }
}

export function encodeProjection(projection: Projection): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["id", projection.id],
    ["name", projection.name],
    ["version", BigInt(projection.version)]
  ])
  if (projection.kind.kind !== "row") map.set("kind", BigInt(projectionKindCode(projection.kind)))
  map.set("content_type", projection.contentType)
  map.set("extraction", encodeIndexSchema(projection.extraction))
  if (projection.entitySchema !== undefined)
    map.set("entity_schema", encodeEntitySchema(projection.entitySchema))
  map.set("inline_payload_default", projection.inlinePayloadDefault)
  return map
}

export function decodeProjection(map: CborMap, context: string): Projection {
  const kind = field.optionalU8(map, "kind", context)
  const entitySchema = field.optionalMap(map, "entity_schema", context)
  return {
    id: projectionIdFromWire(field.requiredString(map, "id", context)),
    name: field.requiredString(map, "name", context),
    version: field.requiredU32(map, "version", context),
    kind: projectionKindFromCode(kind ?? 0),
    contentType: parseContentType(field.requiredString(map, "content_type", context), context),
    extraction: decodeIndexSchema(
      field.requiredMap(map, "extraction", context),
      `${context}.extraction`
    ),
    ...(entitySchema !== undefined
      ? { entitySchema: decodeEntitySchema(entitySchema, `${context}.entity_schema`) }
      : {}),
    inlinePayloadDefault: field.optionalBoolean(map, "inline_payload_default", context) ?? false
  }
}

export function encodeRetentionPolicy(policy: RetentionPolicy): Map<string, unknown> {
  switch (policy.kind) {
    case "mirrorLog":
      return new Map([["kind", "mirror_log"]])
    case "keep":
      return new Map([["kind", "keep"]])
    case "keepUntilSourceDeleted":
      return new Map([["kind", "keep_until_source_deleted"]])
    case "timeToLive":
      return new Map<string, unknown>([
        ["kind", "time_to_live"],
        ["ttl_micros", policy.ttlMicros]
      ])
    case "maxRows":
      return new Map<string, unknown>([
        ["kind", "max_rows"],
        ["rows", policy.rows]
      ])
    case "unknown":
      return new Map([["kind", "unknown"]])
  }
}

export function decodeRetentionPolicy(map: CborMap, context: string): RetentionPolicy {
  const kind = field.requiredString(map, "kind", context)
  switch (kind) {
    case "mirror_log":
      return { kind: "mirrorLog" }
    case "keep":
      return { kind: "keep" }
    case "keep_until_source_deleted":
      return { kind: "keepUntilSourceDeleted" }
    case "time_to_live":
      return { kind: "timeToLive", ttlMicros: field.requiredU64(map, "ttl_micros", context) }
    case "max_rows":
      return { kind: "maxRows", rows: field.requiredU64(map, "rows", context) }
    default:
      return { kind: "unknown" }
  }
}

export function encodeSourceSelector(source: SourceSelector): Map<string, unknown> {
  return new Map<string, unknown>([
    ["stream", source.stream],
    ["topic", source.topic]
  ])
}

export function decodeSourceSelector(map: CborMap, context: string): SourceSelector {
  return {
    stream: field.requiredString(map, "stream", context),
    topic: field.requiredString(map, "topic", context)
  }
}

function targetRoleToWord(role: TargetRole): string {
  return role === "readWrite" ? "read_write" : "write_only"
}

function parseTargetRole(word: string, context: string): TargetRole {
  if (word === "read_write") return "readWrite"
  if (word === "write_only") return "writeOnly"
  throw new CodecError(`\`${word}\` is not a recognized target role`, context, "role")
}

function deliveryToWord(delivery: Delivery): string {
  return delivery === "effectivelyOnce" ? "effectively_once" : "at_most_once"
}

function parseDelivery(word: string, context: string): Delivery {
  if (word === "effectively_once") return "effectivelyOnce"
  if (word === "at_most_once") return "atMostOnce"
  throw new CodecError(`\`${word}\` is not a recognized delivery mode`, context, "delivery")
}

export function encodeTarget(target: Target): Map<string, unknown> {
  return new Map<string, unknown>([
    ["backend", target.backend],
    ["table", target.table],
    ["role", targetRoleToWord(target.role)],
    ["delivery", deliveryToWord(target.delivery)],
    ["required", target.required]
  ])
}

export function decodeTarget(map: CborMap, context: string): Target {
  const role = field.optionalString(map, "role", context)
  const delivery = field.optionalString(map, "delivery", context)
  return {
    backend: field.requiredString(map, "backend", context),
    table: field.requiredString(map, "table", context),
    role: role !== undefined ? parseTargetRole(role, context) : "readWrite",
    delivery: delivery !== undefined ? parseDelivery(delivery, context) : "effectivelyOnce",
    required: field.optionalBoolean(map, "required", context) ?? false
  }
}

export function encodeProjectionBinding(binding: ProjectionBinding): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["source", encodeSourceSelector(binding.source)],
    ["allowed_projections", [...binding.allowedProjections]],
    ["default_projection", binding.defaultProjection ?? null],
    ["targets", binding.targets.map((target) => encodeTarget(target))]
  ])
  if (binding.notify) map.set("notify", true)
  if (binding.retention !== undefined)
    map.set("retention", encodeRetentionPolicy(binding.retention))
  return map
}

export function decodeProjectionBinding(map: CborMap, context: string): ProjectionBinding {
  const defaultProjection = decodeOptionalNullableString(map, "default_projection", context)
  const retention = field.optionalMap(map, "retention", context)
  return {
    source: decodeSourceSelector(field.requiredMap(map, "source", context), `${context}.source`),
    allowedProjections: field.optionalArray(map, "allowed_projections", context, (item, index) =>
      projectionIdFromWire(expectString(item, `${context}.allowed_projections[${String(index)}]`))
    ),
    ...(defaultProjection !== undefined
      ? { defaultProjection: projectionIdFromWire(defaultProjection) }
      : {}),
    targets: field.requiredArray(map, "targets", context, (item, index) =>
      decodeTarget(
        expectMap(item, `${context}.targets[${String(index)}]`),
        `${context}.targets[${String(index)}]`
      )
    ),
    notify: field.optionalBoolean(map, "notify", context) ?? false,
    ...(retention !== undefined
      ? { retention: decodeRetentionPolicy(retention, `${context}.retention`) }
      : {})
  }
}

export function encodeSchemaSource(source: SchemaSource): Map<string, unknown> {
  switch (source.kind) {
    case "avro":
      return new Map<string, unknown>([
        ["kind", "avro"],
        ["schema", source.schema]
      ])
    case "protobuf":
      return new Map<string, unknown>([
        ["kind", "protobuf"],
        ["descriptor_set", source.descriptorSet],
        ["message_type", source.messageType]
      ])
    case "jsonSchema":
      return new Map<string, unknown>([
        ["kind", "json_schema"],
        ["schema", source.schema]
      ])
    case "unknown":
      return new Map([["kind", "unknown"]])
  }
}

export function decodeSchemaSource(map: CborMap, context: string): SchemaSource {
  const kind = field.requiredString(map, "kind", context)
  switch (kind) {
    case "avro":
      return { kind: "avro", schema: field.requiredString(map, "schema", context) }
    case "protobuf":
      return {
        kind: "protobuf",
        descriptorSet: field.requiredBytes(map, "descriptor_set", context),
        messageType: field.requiredString(map, "message_type", context)
      }
    case "json_schema":
      return { kind: "jsonSchema", schema: field.requiredString(map, "schema", context) }
    default:
      return { kind: "unknown" }
  }
}

export function encodeSchemaDef(schema: SchemaDef): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["id", BigInt(schema.id)],
    ["source", encodeSchemaSource(schema.source)]
  ])
  if (schema.name !== undefined) map.set("name", schema.name)
  if (schema.version !== undefined) map.set("version", BigInt(schema.version))
  return map
}

export function decodeSchemaDef(map: CborMap, context: string): SchemaDef {
  const name = field.optionalString(map, "name", context)
  const version = field.optionalU32(map, "version", context)
  return {
    id: field.requiredU32(map, "id", context),
    source: decodeSchemaSource(field.requiredMap(map, "source", context), `${context}.source`),
    ...(name !== undefined ? { name } : {}),
    ...(version !== undefined ? { version } : {})
  }
}

export function encodeControlCommand(command: ControlCommand): Map<string, unknown> {
  switch (command.kind) {
    case "registerProjection":
      return new Map([["RegisterProjection", encodeProjection(command.projection)]])
    case "dropProjection":
      return new Map([["DropProjection", command.id]])
    case "applyBinding":
      return new Map([["ApplyBinding", encodeProjectionBinding(command.binding)]])
    case "removeBinding":
      return new Map([
        [
          "RemoveBinding",
          new Map<string, unknown>([
            ["source", encodeSourceSelector(command.source)],
            ["projection_ref", command.projectionRef ?? null]
          ])
        ]
      ])
    case "registerSchema":
      return new Map([["RegisterSchema", encodeSchemaDef(command.schema)]])
    case "dropSchema":
      return new Map([["DropSchema", BigInt(command.id)]])
    case "registerGraph":
      return new Map([["RegisterGraph", encodeProjection(command.projection)]])
    case "dropGraph":
      return new Map([["DropGraph", command.id]])
    case "registerRunSource":
      return new Map([["RegisterRunSource", encodeSourceSelector(command.source)]])
    case "removeRunSource":
      return new Map([["RemoveRunSource", encodeSourceSelector(command.source)]])
  }
}

export function decodeControlCommand(value: unknown, context: string): ControlCommand {
  const [tag, inner] = singleVariantTag(value, context)
  switch (tag) {
    case "RegisterProjection":
      return {
        kind: "registerProjection",
        projection: decodeProjection(expectMap(inner, context), context)
      }
    case "DropProjection":
      return { kind: "dropProjection", id: expectString(inner, context) }
    case "ApplyBinding":
      return {
        kind: "applyBinding",
        binding: decodeProjectionBinding(expectMap(inner, context), context)
      }
    case "RemoveBinding": {
      const map = expectMap(inner, context)
      const projectionRef = decodeOptionalNullableString(map, "projection_ref", context)
      return {
        kind: "removeBinding",
        source: decodeSourceSelector(
          field.requiredMap(map, "source", context),
          `${context}.source`
        ),
        ...(projectionRef !== undefined ? { projectionRef } : {})
      }
    }
    case "RegisterSchema":
      return { kind: "registerSchema", schema: decodeSchemaDef(expectMap(inner, context), context) }
    case "DropSchema":
      return { kind: "dropSchema", id: expectU32Value(inner, context) }
    case "RegisterGraph":
      return {
        kind: "registerGraph",
        projection: decodeProjection(expectMap(inner, context), context)
      }
    case "DropGraph":
      return { kind: "dropGraph", id: expectString(inner, context) }
    case "RegisterRunSource":
      return {
        kind: "registerRunSource",
        source: decodeSourceSelector(expectMap(inner, context), context)
      }
    case "RemoveRunSource":
      return {
        kind: "removeRunSource",
        source: decodeSourceSelector(expectMap(inner, context), context)
      }
    default:
      throw new CodecError(`\`${tag}\` is not a recognized control command`, context, "command")
  }
}

export function encodeControlEnvelope(envelope: ControlEnvelope): Map<string, unknown> {
  return new Map<string, unknown>([
    ["v", BigInt(envelope.v)],
    ["timestamp_micros", envelope.timestampMicros],
    ["command", encodeControlCommand(envelope.command)]
  ])
}

export function decodeControlEnvelope(map: CborMap, context: string): ControlEnvelope {
  return {
    v: field.requiredU32(map, "v", context),
    timestampMicros: field.requiredU64(map, "timestamp_micros", context),
    command: decodeControlCommand(map.get("command"), `${context}.command`)
  }
}

function decodeOptionalNullableString(
  map: CborMap,
  key: string,
  context: string
): string | undefined {
  if (!map.has(key)) return undefined
  const value = map.get(key)
  if (value === null) return undefined
  if (typeof value === "string") return value
  throw new CodecError(`field \`${key}\` in ${context} must be a string or null`, context, key)
}

function expectU32Value(value: unknown, context: string): number {
  if (typeof value === "number" && Number.isInteger(value) && value >= 0 && value <= 0xffff_ffff) {
    return value
  }
  throw new CodecError(`value in ${context} must fit u32`, context, "value")
}
