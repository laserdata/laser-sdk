import {
  type FilterCatalogCommand,
  decodeFilterCatalogCommand,
  encodeFilterCatalogCommand,
  validateFilterMutation
} from "./filter.js"
import { CodecError, InvalidError } from "../client/errors.js"
import { type CborMap, expectMap, expectString, field, singleVariantTag } from "./cbor.js"
import { ContentType, type ContentType as ContentTypeValue } from "./content.js"
import { type BackendBinding, decodeBackendBinding, encodeBackendBinding } from "./destination.js"
import { type Caller, decodeCaller, encodeCaller } from "./mutation.js"
import {
  MAX_FILTER_SCHEMA_BYTES,
  MAX_FILTER_SCHEMA_DEPTH,
  MAX_GRAPH_LABEL_BYTES,
  MAX_INDEX_FIELD_BYTES,
  MAX_PROJECTION_ID_BYTES,
  MAX_PROJECTION_NAME_BYTES,
  MAX_SCHEMA_NAME_BYTES
} from "./limits.js"
import { jsonDepthWithin, validateBoundedText } from "./validate.js"

/** Prefix no projection id may start with: activity keys an index-only binding under it. */
export const RESERVED_PROJECTION_ID_PREFIX = "index:"

/** The one JSON Schema dialect the registry accepts, named or implied. */
export const JSON_SCHEMA_DIALECT = "https://json-schema.org/draft/2020-12/schema"

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

export class IndexSchemaBuilder {
  private readonly fields: IndexField[] = []
  private vectorPointer: string | undefined
  private inline = false

  /** Index the root-level JSON value `name` under the same index key. */
  field(name: string): this {
    this.fields.push({ name, pointer: `/${name}` })
    return this
  }

  /** Index the JSON value at `pointer` (RFC 6901) under the index key `name`. */
  fieldAt(name: string, pointer: string): this {
    this.fields.push({ name, pointer })
    return this
  }

  /** Extract the embedding vector from `pointer` instead of `/embedding`. */
  vectorField(pointer: string): this {
    this.vectorPointer = pointer
    return this
  }

  /** Inline the payload by default for every record projected through this schema. */
  inlinePayload(): this {
    this.inline = true
    return this
  }

  build(): IndexSchema {
    return {
      fields: [...this.fields],
      ...(this.vectorPointer !== undefined ? { vectorField: this.vectorPointer } : {}),
      inlinePayload: this.inline
    }
  }
}

export type ProjectionId = string & { readonly __projectionId: unique symbol }

export function parseProjectionId(value: string): ProjectionId {
  validateProjectionId(value)
  return value as ProjectionId
}

/** 1 to `MAX_PROJECTION_ID_BYTES` bytes without a control character, never under `RESERVED_PROJECTION_ID_PREFIX`. */
export function validateProjectionId(value: string): void {
  validateBoundedText("projection id", value, MAX_PROJECTION_ID_BYTES)
  if (value.startsWith(RESERVED_PROJECTION_ID_PREFIX))
    throw new InvalidError(`projection id must not start with "${RESERVED_PROJECTION_ID_PREFIX}"`)
}

/** Rust `Projection::validate`: the id, an optional name, index fields, and entity labels. */
export function validateProjection(projection: Projection): void {
  if (
    projection.kind.kind === "record" &&
    (projection.extraction.fields.length > 0 ||
      projection.extraction.vectorField !== undefined ||
      projection.entitySchema !== undefined)
  )
    throw new InvalidError(
      "record projections do not declare extraction fields or an entity schema"
    )
  if (projection.history.kind === "keyed") {
    if (projection.kind.kind === "graph")
      throw new InvalidError("graph projections do not support keyed history")
    if (projection.history.deleteHeader !== undefined)
      validateBoundedText("deletion header", projection.history.deleteHeader, MAX_INDEX_FIELD_BYTES)
    const key = projection.history.key
    if (key.kind === "header") validateBoundedText("key header", key.name, MAX_INDEX_FIELD_BYTES)
    else {
      if (projection.kind.kind !== "row")
        throw new InvalidError("payload key pointers require a row projection")
      if (key.pointer.length > 0) {
        validateBoundedText("key pointer", key.pointer, MAX_INDEX_FIELD_BYTES)
        if (!key.pointer.startsWith("/"))
          throw new InvalidError("key pointer must be a JSON pointer")
      }
      if (/~(?:[^01]|$)/u.test(key.pointer))
        throw new InvalidError("key pointer has an invalid escape")
    }
  }
  validateProjectionId(projection.id)
  if (projection.name.length > 0)
    validateBoundedText("projection name", projection.name, MAX_PROJECTION_NAME_BYTES)
  for (const indexField of projection.extraction.fields) {
    validateBoundedText("index field name", indexField.name, MAX_INDEX_FIELD_BYTES)
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(indexField.name))
      throw new InvalidError(
        `index field name \`${indexField.name}\` must start with an ASCII letter or \`_\` and hold only ASCII letters, digits and \`_\``
      )
    validateBoundedText("index field pointer", indexField.pointer, MAX_INDEX_FIELD_BYTES)
  }
  if (projection.extraction.vectorField !== undefined)
    validateBoundedText(
      "index vector field",
      projection.extraction.vectorField,
      MAX_INDEX_FIELD_BYTES
    )
  for (const node of projection.entitySchema?.nodes ?? [])
    validateBoundedText("graph node label", node.label, MAX_GRAPH_LABEL_BYTES)
  for (const edge of projection.entitySchema?.edges ?? [])
    validateBoundedText("graph edge type", edge.edgeType, MAX_GRAPH_LABEL_BYTES)
}

/** Rust `SchemaDef::validate`: a name, when present, is bounded text. */
export function validateSchemaName(name: string | undefined): void {
  if (name !== undefined) validateBoundedText("schema name", name, MAX_SCHEMA_NAME_BYTES)
}

/** Rust `SchemaSource::validate`: the registry's size, nesting and dialect rules. */
export function validateSchemaSource(source: SchemaSource): void {
  switch (source.kind) {
    case "avro":
      validateSchemaText("Avro schema", source.schema)
      return
    case "jsonSchema": {
      validateSchemaText("JSON Schema", source.schema)
      let value: unknown
      try {
        value = JSON.parse(source.schema) as unknown
      } catch (cause) {
        throw new InvalidError("JSON Schema is not valid JSON", undefined, { cause })
      }
      if (value === null || typeof value !== "object" || !("$schema" in value)) return
      const dialect = value.$schema
      if (typeof dialect === "string" && trimTrailingHashes(dialect) === JSON_SCHEMA_DIALECT) return
      throw new InvalidError(`a JSON Schema uses draft 2020-12, not ${JSON.stringify(dialect)}`)
    }
    case "protobuf":
      if (source.descriptorSet.byteLength > MAX_FILTER_SCHEMA_BYTES)
        throw new InvalidError(
          `Protobuf descriptor set exceeds ${String(MAX_FILTER_SCHEMA_BYTES)} bytes`
        )
      validateBoundedText("Protobuf message type", source.messageType, MAX_SCHEMA_NAME_BYTES)
      return
    case "unknown":
      throw new InvalidError("a schema source of an unknown kind cannot be registered")
  }
}

/** Rust `str::trim_end_matches('#')`, a scan rather than a regex so a long run of `#` stays linear. */
function trimTrailingHashes(text: string): string {
  let end = text.length
  while (end > 0 && text[end - 1] === "#") end -= 1
  return text.slice(0, end)
}

function validateSchemaText(label: string, text: string): void {
  if (new TextEncoder().encode(text).byteLength > MAX_FILTER_SCHEMA_BYTES)
    throw new InvalidError(`${label} exceeds ${String(MAX_FILTER_SCHEMA_BYTES)} bytes`)
  if (!jsonDepthWithin(text, MAX_FILTER_SCHEMA_DEPTH))
    throw new InvalidError(`${label} nests deeper than ${String(MAX_FILTER_SCHEMA_DEPTH)}`)
}

/** Rust `ControlCommand::validate`, checked before a command is published. */
export function validateControlCommand(command: ControlCommand): void {
  switch (command.kind) {
    case "registerProjection":
    case "registerGraph":
      validateProjection(command.projection)
      return
    case "dropProjection":
    case "dropGraph":
      validateProjectionId(command.id)
      return
    case "applyBinding":
      if (
        command.binding.invalidRecords === "unknown" ||
        command.binding.oversize === "unknown" ||
        command.binding.retention?.kind === "unknown"
      )
        throw new InvalidError("a binding cannot apply a policy this build does not know")
      if (
        (command.binding.retention?.kind === "timeToLive" &&
          command.binding.retention.ttlMicros === 0n) ||
        (command.binding.retention?.kind === "maxRows" && command.binding.retention.rows === 0n)
      )
        throw new InvalidError(
          "a time to live retention needs a lifetime above zero, and a max rows retention at least one row"
        )
      if (
        command.binding.oversize === "reference" &&
        command.binding.retention !== undefined &&
        command.binding.retention.kind !== "mirrorLog"
      )
        throw new InvalidError("body references require mirror_log retention")
      for (const projection of command.binding.allowedProjections) validateProjectionId(projection)
      if (command.binding.defaultProjection !== undefined)
        validateProjectionId(command.binding.defaultProjection)
      return
    case "removeBinding":
      if (command.projectionRef !== undefined) validateProjectionId(command.projectionRef)
      return
    case "registerSchema":
      if (command.schema.id === 0) throw new InvalidError("writer schema ids start at 1")
      validateSchemaName(command.schema.name)
      validateSchemaSource(command.schema.source)
      return
    case "allocateSchema":
      validateSchemaName(command.allocation.name)
      validateSchemaSource(command.allocation.source)
      return
    case "filterCatalog":
      validateFilterMutation(command.command.mutation)
      return
    case "dropSchema":
    case "registerSessionSource":
    case "removeSessionSource":
    case "acceptGap":
    case "skipRecord":
    case "sourceRetained":
      return
  }
}

function projectionIdFromWire(value: string): ProjectionId {
  return value as ProjectionId
}

export type ProjectionKind =
  | { readonly kind: "row" }
  | { readonly kind: "graph" }
  | { readonly kind: "record" }
  | { readonly kind: "unrecognized"; readonly code: number }

export function projectionKindIsRow(kind: ProjectionKind): boolean {
  return kind.kind === "row"
}

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

export type ProjectionKey =
  | { readonly kind: "header"; readonly name: string }
  | { readonly kind: "pointer"; readonly pointer: string }
export type ProjectionHistory =
  | { readonly kind: "append" }
  | {
      readonly kind: "keyed"
      readonly key: ProjectionKey
      /** The header whose presence deletes the record's key. Without one the view never deletes. */
      readonly deleteHeader?: string
    }
export function projectionHistoryIsAppend(history: ProjectionHistory): boolean {
  return history.kind === "append"
}
export function encodeProjectionHistory(history: ProjectionHistory): Map<string, unknown> {
  if (history.kind === "append") return new Map([["kind", "append"]])
  const key =
    history.key.kind === "header"
      ? new Map([
          ["kind", "header"],
          ["name", history.key.name]
        ])
      : new Map([
          ["kind", "pointer"],
          ["pointer", history.key.pointer]
        ])
  const map = new Map<string, unknown>([
    ["kind", "keyed"],
    ["key", key]
  ])
  if (history.deleteHeader !== undefined) map.set("delete_header", history.deleteHeader)
  return map
}
export function decodeProjectionHistory(map: CborMap, context: string): ProjectionHistory {
  const kind = field.requiredString(map, "kind", context)
  if (kind === "append") return { kind: "append" }
  if (kind !== "keyed") throw new CodecError("unknown projection history", context, "kind")
  const key = field.requiredMap(map, "key", context)
  const keyKind = field.requiredString(key, "kind", context)
  if (keyKind !== "header" && keyKind !== "pointer")
    throw new CodecError("unknown projection key", context, "key")
  const deleteHeader = field.optionalString(map, "delete_header", context)
  return {
    kind: "keyed",
    key:
      keyKind === "header"
        ? { kind: "header", name: field.requiredString(key, "name", context) }
        : { kind: "pointer", pointer: field.requiredString(key, "pointer", context) },
    ...(deleteHeader !== undefined ? { deleteHeader } : {})
  }
}

export interface Projection {
  readonly id: ProjectionId
  readonly name: string
  readonly version: number
  readonly kind: ProjectionKind
  readonly history: ProjectionHistory
  readonly contentType: ContentTypeValue
  readonly extraction: IndexSchema
  readonly entitySchema?: EntitySchema
  readonly inlinePayloadDefault: boolean
}

export class ProjectionBuilder {
  private readonly id: ProjectionId
  private projectionName = ""
  private projectionVersion = 1
  private projectionKind: ProjectionKind = { kind: "row" }
  private history: ProjectionHistory = { kind: "append" }
  private projectionContentType: ContentTypeValue = ContentType.Any
  private fieldList: IndexField[] = []
  private vectorPointer: string | undefined
  private inlineExtraction = true
  private entitySchema: EntitySchema | undefined
  private inlineDefault = true

  /** Starts a row projection that inlines the payload, at version 1, for any content type. */
  constructor(id: string) {
    this.id = parseProjectionId(id)
  }

  name(value: string): this {
    this.projectionName = value
    return this
  }

  version(value: number): this {
    this.projectionVersion = value
    return this
  }

  contentType(value: ContentTypeValue): this {
    this.projectionContentType = value
    return this
  }

  /** Keeps the current row of each key, deleted by a record carrying `deleteHeader` when one is named. */
  keyed(key: ProjectionKey, deleteHeader?: string): this {
    this.history = { kind: "keyed", key, ...(deleteHeader !== undefined ? { deleteHeader } : {}) }
    return this
  }

  record(): this {
    this.projectionKind = { kind: "record" }
    this.fieldList = []
    this.vectorPointer = undefined
    this.entitySchema = undefined
    this.inlineExtraction = false
    this.inlineDefault = false
    return this
  }

  /** Replaces the whole index schema. */
  extraction(value: IndexSchema): this {
    this.fieldList = [...value.fields]
    this.vectorPointer = value.vectorField
    this.inlineExtraction = value.inlinePayload
    return this
  }

  /** Makes this a graph projection with the given node and edge extraction plan. */
  graph(schema: EntitySchema): this {
    this.projectionKind = { kind: "graph" }
    this.entitySchema = schema
    return this
  }

  /** Indexes the root-level JSON value `name` under the same index key. */
  field(name: string): this {
    this.fieldList.push({ name, pointer: `/${name}` })
    return this
  }

  /** Indexes each root-level JSON value in `names` under its own name. */
  fields(names: Iterable<string>): this {
    for (const name of names) this.field(name)
    return this
  }

  /** Indexes the JSON value at `pointer` (RFC 6901) under the index key `name`. */
  fieldAt(name: string, pointer: string): this {
    this.fieldList.push({ name, pointer })
    return this
  }

  /** Like `field`, with a storage type hint for columnar backends. */
  fieldTyped(name: string, fieldType: FieldType): this {
    this.fieldList.push({ name, pointer: `/${name}`, fieldType })
    return this
  }

  /** Like `fieldAt`, with a storage type hint for columnar backends. */
  fieldAtTyped(name: string, pointer: string, fieldType: FieldType): this {
    this.fieldList.push({ name, pointer, fieldType })
    return this
  }

  /** Extracts the embedding vector from `pointer`. */
  vectorField(pointer: string): this {
    this.vectorPointer = pointer
    return this
  }

  /** Inlines the original payload alongside each row. This is already the default. */
  inlinePayload(): this {
    this.inlineDefault = true
    this.inlineExtraction = true
    return this
  }

  /** Keeps only the indexed values and leaves the payload on the log. */
  indexOnly(): this {
    this.inlineDefault = false
    this.inlineExtraction = false
    return this
  }

  build(): Projection {
    return {
      id: this.id,
      name: this.projectionName,
      version: this.projectionVersion,
      kind: this.projectionKind,
      history: this.history,
      contentType: this.projectionContentType,
      extraction: {
        fields: [...this.fieldList],
        ...(this.vectorPointer !== undefined ? { vectorField: this.vectorPointer } : {}),
        inlinePayload: this.inlineExtraction
      },
      ...(this.entitySchema !== undefined ? { entitySchema: this.entitySchema } : {}),
      inlinePayloadDefault: this.inlineDefault
    }
  }
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

export type InvalidRecordPolicy = "block" | "dead_letter" | "unknown"
export type OversizePolicy = "block" | "reference" | "unknown"

export interface ProjectionBinding {
  readonly source: SourceSelector
  readonly allowedProjections: readonly ProjectionId[]
  readonly defaultProjection?: ProjectionId
  readonly backend?: BackendBinding
  readonly index: string
  readonly notify: boolean
  readonly retention?: RetentionPolicy
  readonly invalidRecords: InvalidRecordPolicy
  readonly oversize: OversizePolicy
}

export class ProjectionBindingBuilder {
  private sourceSelector: SourceSelector | undefined
  private readonly allowed: ProjectionId[] = []
  private defaultProjectionId: ProjectionId | undefined
  private backendBinding: BackendBinding | undefined
  private indexName: string | undefined
  private notifyChanges = false
  private retentionPolicy: RetentionPolicy | undefined
  private invalidRecordPolicy: InvalidRecordPolicy = "block"
  private oversizePolicy: OversizePolicy = "block"

  /** Sets the source stream and topic. Both are required. */
  source(stream: string, topic: string): this {
    this.sourceSelector = { stream, topic }
    return this
  }

  selector(source: SourceSelector): this {
    this.sourceSelector = source
    return this
  }

  /** Allows records to route to this projection. */
  allow(projection: string): this {
    this.allowed.push(parseProjectionId(projection))
    return this
  }

  /** The projection used when a record carries no projection reference. */
  defaultProjection(projection: string): this {
    this.defaultProjectionId = parseProjectionId(projection)
    return this
  }

  /** Binds the operational index to an exact backend resource generation. */
  backend(backend: BackendBinding): this {
    this.backendBinding = backend
    return this
  }

  /** How long the materialized rows live, independent of the source topic retention. */
  retention(retention: RetentionPolicy): this {
    this.retentionPolicy = retention
    return this
  }

  /** The operational index name. Defaults to the source topic name. */
  index(index: string): this {
    this.indexName = index
    return this
  }

  /** Publishes one change record per committed projector batch for this binding. */
  notify(): this {
    this.notifyChanges = true
    return this
  }

  invalidRecords(policy: InvalidRecordPolicy): this {
    this.invalidRecordPolicy = policy
    return this
  }

  oversize(policy: OversizePolicy): this {
    this.oversizePolicy = policy
    return this
  }

  /** Throws `InvalidError` when no source was set. */
  build(): ProjectionBinding {
    const binding = this.tryBuild()
    if (binding === undefined) throw new InvalidError("ProjectionBinding requires a source")
    return binding
  }

  /** The binding, or `undefined` when no source was set. */
  tryBuild(): ProjectionBinding | undefined {
    const source = this.sourceSelector
    if (source === undefined) return undefined
    return {
      source,
      allowedProjections: [...this.allowed],
      ...(this.defaultProjectionId !== undefined
        ? { defaultProjection: this.defaultProjectionId }
        : {}),
      ...(this.backendBinding !== undefined ? { backend: this.backendBinding } : {}),
      index: this.indexName ?? source.topic,
      notify: this.notifyChanges,
      ...(this.retentionPolicy !== undefined ? { retention: this.retentionPolicy } : {}),
      invalidRecords: this.invalidRecordPolicy,
      oversize: this.oversizePolicy
    }
  }
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

/** A writer schema whose id the control fold chooses at the record's ordered
 * log position, as Rust `SchemaAllocation`. */
export interface SchemaAllocation {
  readonly source: SchemaSource
  readonly name?: string
  readonly version?: number
}

export function schemaDefContentType(schema: SchemaDef): ContentTypeValue {
  switch (schema.source.kind) {
    case "avro":
      return ContentType.Avro
    case "protobuf":
      return ContentType.Protobuf
    case "jsonSchema":
      return ContentType.Json
    case "unknown":
      return ContentType.Any
  }
}

export type ControlCommand =
  | { readonly kind: "registerProjection"; readonly projection: Projection }
  | { readonly kind: "dropProjection"; readonly id: string }
  | { readonly kind: "applyBinding"; readonly binding: ProjectionBinding }
  | {
      readonly kind: "removeBinding"
      readonly source: SourceSelector
      readonly projectionRef?: string
      /** Remove the binding only when it belongs to this incarnation of its
       * source stream or an earlier one. Absent removes it whatever its
       * incarnation. */
      readonly generation?: ControlStreamGeneration
    }
  | { readonly kind: "registerSchema"; readonly schema: SchemaDef }
  | { readonly kind: "dropSchema"; readonly id: number }
  | { readonly kind: "allocateSchema"; readonly allocation: SchemaAllocation }
  | { readonly kind: "registerGraph"; readonly projection: Projection }
  | { readonly kind: "dropGraph"; readonly id: string }
  | {
      readonly kind: "registerSessionSource"
      readonly stream: string
      readonly topics: SessionTopics
    }
  | { readonly kind: "removeSessionSource"; readonly stream: string }
  | { readonly kind: "filterCatalog"; readonly command: FilterCatalogCommand }
  | { readonly kind: "acceptGap"; readonly acceptance: GapAcceptance }
  | { readonly kind: "skipRecord"; readonly skip: RecordSkip }
  | { readonly kind: "sourceRetained"; readonly retention: SourceRetention }

/** The exact missing range an operator accepted for one source partition. */
export interface GapAcceptance {
  readonly source: SourceSelector
  readonly partitionId: number
  readonly topicCreatedAtMicros: bigint
  readonly fromOffset: bigint
  readonly toOffset: bigint
}

/** The first offset one source partition incarnation still retains. Skips and
 * accepted gaps below it can never be reached again. */
export interface SourceRetention {
  readonly source: SourceSelector
  readonly partitionId: number
  readonly topicCreatedAtMicros: bigint
  readonly firstOffset: bigint
}

/** One record a binding skips, on one topic incarnation. */
export interface RecordSkip {
  readonly source: SourceSelector
  readonly partitionId: number
  readonly topicCreatedAtMicros: bigint
  readonly offset: bigint
  readonly target: string
  readonly reason: string
}

/** Who appended a control record and for which resource generation. The
 * backend sets it, a client never does. */
export interface ControlStamp {
  readonly deploymentId: bigint
  readonly operationId: bigint
  readonly streamGeneration?: ControlStreamGeneration
  readonly keyId: bigint
  /** The client that asked for the record, the only actor it names. Absent on
   * a record the backend or the streaming server issues on its own, so the
   * root user, id zero, is never mistaken for them. */
  readonly caller?: Caller
}

export interface ControlStreamGeneration {
  readonly streamId: number
  readonly createdAtMicros: bigint
}

export function encodeGapAcceptance(acceptance: GapAcceptance): Map<string, unknown> {
  return new Map<string, unknown>([
    ["source", encodeSourceSelector(acceptance.source)],
    ["partition_id", BigInt(acceptance.partitionId)],
    ["topic_created_at_micros", acceptance.topicCreatedAtMicros],
    ["from_offset", acceptance.fromOffset],
    ["to_offset", acceptance.toOffset]
  ])
}

export function decodeGapAcceptance(map: CborMap, context: string): GapAcceptance {
  return {
    source: decodeSourceSelector(field.requiredMap(map, "source", context), `${context}.source`),
    partitionId: field.requiredU32(map, "partition_id", context),
    topicCreatedAtMicros: field.requiredU64(map, "topic_created_at_micros", context),
    fromOffset: field.requiredU64(map, "from_offset", context),
    toOffset: field.requiredU64(map, "to_offset", context)
  }
}

function encodeRecordSkip(skip: RecordSkip): Map<string, unknown> {
  return new Map<string, unknown>([
    ["source", encodeSourceSelector(skip.source)],
    ["partition_id", BigInt(skip.partitionId)],
    ["topic_created_at_micros", skip.topicCreatedAtMicros],
    ["offset", skip.offset],
    ["target", skip.target],
    ["reason", skip.reason]
  ])
}

function decodeRecordSkip(map: CborMap, context: string): RecordSkip {
  return {
    source: decodeSourceSelector(field.requiredMap(map, "source", context), `${context}.source`),
    partitionId: field.requiredU32(map, "partition_id", context),
    topicCreatedAtMicros: field.requiredU64(map, "topic_created_at_micros", context),
    offset: field.requiredU64(map, "offset", context),
    target: field.requiredString(map, "target", context),
    reason: field.requiredString(map, "reason", context)
  }
}

function encodeSourceRetention(retention: SourceRetention): Map<string, unknown> {
  return new Map<string, unknown>([
    ["source", encodeSourceSelector(retention.source)],
    ["partition_id", BigInt(retention.partitionId)],
    ["topic_created_at_micros", retention.topicCreatedAtMicros],
    ["first_offset", retention.firstOffset]
  ])
}

function decodeSourceRetention(map: CborMap, context: string): SourceRetention {
  return {
    source: decodeSourceSelector(field.requiredMap(map, "source", context), `${context}.source`),
    partitionId: field.requiredU32(map, "partition_id", context),
    topicCreatedAtMicros: field.requiredU64(map, "topic_created_at_micros", context),
    firstOffset: field.requiredU64(map, "first_offset", context)
  }
}

export function encodeControlStamp(stamp: ControlStamp): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["deployment_id", stamp.deploymentId],
    ["operation_id", stamp.operationId]
  ])
  if (stamp.streamGeneration !== undefined) {
    map.set("stream_generation", encodeControlStreamGeneration(stamp.streamGeneration))
  }
  map.set("key_id", stamp.keyId)
  if (stamp.caller !== undefined) map.set("caller", encodeCaller(stamp.caller))
  return map
}

export function decodeControlStamp(map: CborMap, context: string): ControlStamp {
  const generation = field.optionalMap(map, "stream_generation", context)
  const callerMap = field.optionalMap(map, "caller", context)
  return {
    deploymentId: field.requiredU64(map, "deployment_id", context),
    operationId: field.requiredU128(map, "operation_id", context),
    ...(generation !== undefined
      ? {
          streamGeneration: decodeControlStreamGeneration(
            generation,
            `${context}.stream_generation`
          )
        }
      : {}),
    keyId: field.requiredU64(map, "key_id", context),
    ...(callerMap !== undefined ? { caller: decodeCaller(callerMap, `${context}.caller`) } : {})
  }
}

function encodeControlStreamGeneration(generation: ControlStreamGeneration): Map<string, unknown> {
  return new Map<string, unknown>([
    ["stream_id", BigInt(generation.streamId)],
    ["created_at_micros", generation.createdAtMicros]
  ])
}

function decodeControlStreamGeneration(map: CborMap, context: string): ControlStreamGeneration {
  return {
    streamId: field.requiredU32(map, "stream_id", context),
    createdAtMicros: field.requiredU64(map, "created_at_micros", context)
  }
}

export type SessionTopics =
  { readonly kind: "all" } | { readonly kind: "named"; readonly names: readonly string[] }

export function encodeSessionTopics(topics: SessionTopics): unknown {
  return topics.kind === "all" ? "All" : new Map([["Named", [...topics.names]]])
}

export function decodeSessionTopics(value: unknown, context: string): SessionTopics {
  if (value === "All") return { kind: "all" }
  const [tag, names] = singleVariantTag(value, context)
  if (tag !== "Named" || !Array.isArray(names))
    throw new CodecError("invalid session topics", context, "topics")
  return {
    kind: "named",
    names: names.map((name, index) => expectString(name, `${context}.topics[${String(index)}]`))
  }
}

export interface ControlEnvelope {
  readonly v: number
  readonly timestampMicros: bigint
  readonly command: ControlCommand
  /** Set by the backend when it appends the record. Absent on a client's own
   * envelope. */
  readonly stamp?: ControlStamp
  /** The stream the command's resource names belong to, set by a client that
   * scopes its resources to a stream. The writer-schema registry keys
   * `registerSchema` and `dropSchema` by stream and id when it is set. */
  readonly stream?: string
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
    case "record":
      return 2
    case "unrecognized":
      return kind.code
  }
}

export function projectionKindFromCode(code: number): ProjectionKind {
  if (code === 0) return { kind: "row" }
  if (code === 1) return { kind: "graph" }
  if (code === 2) return { kind: "record" }
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
  if (projection.history.kind !== "append")
    map.set("history", encodeProjectionHistory(projection.history))
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
    history: map.has("history")
      ? decodeProjectionHistory(field.requiredMap(map, "history", context), context)
      : { kind: "append" },
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

export function encodeProjectionBinding(binding: ProjectionBinding): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["source", encodeSourceSelector(binding.source)],
    ["allowed_projections", [...binding.allowedProjections]],
    ["default_projection", binding.defaultProjection ?? null]
  ])
  if (binding.backend !== undefined) map.set("backend", encodeBackendBinding(binding.backend))
  map.set("index", binding.index)
  if (binding.notify) map.set("notify", true)
  if (binding.retention !== undefined)
    map.set("retention", encodeRetentionPolicy(binding.retention))
  if (binding.invalidRecords !== "block") map.set("invalid_records", binding.invalidRecords)
  if (binding.oversize !== "block") map.set("oversize", binding.oversize)
  return map
}

export function decodeProjectionBinding(map: CborMap, context: string): ProjectionBinding {
  const defaultProjection = decodeOptionalNullableString(map, "default_projection", context)
  const backend = field.optionalMap(map, "backend", context)
  const retention = field.optionalMap(map, "retention", context)
  const invalidRecords = field.optionalString(map, "invalid_records", context) ?? "block"
  const oversize = field.optionalString(map, "oversize", context) ?? "block"
  return {
    source: decodeSourceSelector(field.requiredMap(map, "source", context), `${context}.source`),
    allowedProjections: field.optionalArray(map, "allowed_projections", context, (item, index) =>
      projectionIdFromWire(expectString(item, `${context}.allowed_projections[${String(index)}]`))
    ),
    ...(defaultProjection !== undefined
      ? { defaultProjection: projectionIdFromWire(defaultProjection) }
      : {}),
    ...(backend !== undefined
      ? { backend: decodeBackendBinding(backend, `${context}.backend`) }
      : {}),
    index: field.requiredString(map, "index", context),
    notify: field.optionalBoolean(map, "notify", context) ?? false,
    ...(retention !== undefined
      ? { retention: decodeRetentionPolicy(retention, `${context}.retention`) }
      : {}),
    invalidRecords:
      invalidRecords === "block" || invalidRecords === "dead_letter" ? invalidRecords : "unknown",
    oversize: oversize === "block" || oversize === "reference" ? oversize : "unknown"
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

export function encodeSchemaAllocation(allocation: SchemaAllocation): Map<string, unknown> {
  const map = new Map<string, unknown>([["source", encodeSchemaSource(allocation.source)]])
  if (allocation.name !== undefined) map.set("name", allocation.name)
  if (allocation.version !== undefined) map.set("version", BigInt(allocation.version))
  return map
}

export function decodeSchemaAllocation(map: CborMap, context: string): SchemaAllocation {
  const name = field.optionalString(map, "name", context)
  const version = field.optionalU32(map, "version", context)
  return {
    source: decodeSchemaSource(field.requiredMap(map, "source", context), `${context}.source`),
    ...(name !== undefined ? { name } : {}),
    ...(version !== undefined ? { version } : {})
  }
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
    case "removeBinding": {
      const removal = new Map<string, unknown>([
        ["source", encodeSourceSelector(command.source)],
        ["projection_ref", command.projectionRef ?? null]
      ])
      if (command.generation !== undefined) {
        removal.set("generation", encodeControlStreamGeneration(command.generation))
      }
      return new Map([["RemoveBinding", removal]])
    }
    case "registerSchema":
      return new Map([["RegisterSchema", encodeSchemaDef(command.schema)]])
    case "dropSchema":
      return new Map([["DropSchema", BigInt(command.id)]])
    case "allocateSchema":
      return new Map([["AllocateSchema", encodeSchemaAllocation(command.allocation)]])
    case "registerGraph":
      return new Map([["RegisterGraph", encodeProjection(command.projection)]])
    case "dropGraph":
      return new Map([["DropGraph", command.id]])
    case "registerSessionSource":
      return new Map([
        [
          "RegisterSessionSource",
          new Map<string, unknown>([
            ["stream", command.stream],
            ["topics", encodeSessionTopics(command.topics)]
          ])
        ]
      ])
    case "removeSessionSource":
      return new Map([
        ["RemoveSessionSource", new Map<string, unknown>([["stream", command.stream]])]
      ])
    case "filterCatalog":
      return new Map([["FilterCatalog", encodeFilterCatalogCommand(command.command)]])
    case "acceptGap":
      return new Map([["AcceptGap", encodeGapAcceptance(command.acceptance)]])
    case "skipRecord":
      return new Map([["SkipRecord", encodeRecordSkip(command.skip)]])
    case "sourceRetained":
      return new Map([["SourceRetained", encodeSourceRetention(command.retention)]])
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
      const generation = field.optionalMap(map, "generation", context)
      return {
        kind: "removeBinding",
        source: decodeSourceSelector(
          field.requiredMap(map, "source", context),
          `${context}.source`
        ),
        ...(projectionRef !== undefined ? { projectionRef } : {}),
        ...(generation !== undefined
          ? { generation: decodeControlStreamGeneration(generation, `${context}.generation`) }
          : {})
      }
    }
    case "RegisterSchema":
      return { kind: "registerSchema", schema: decodeSchemaDef(expectMap(inner, context), context) }
    case "DropSchema":
      return { kind: "dropSchema", id: expectU32Value(inner, context) }
    case "AllocateSchema":
      return {
        kind: "allocateSchema",
        allocation: decodeSchemaAllocation(expectMap(inner, context), context)
      }
    case "RegisterGraph":
      return {
        kind: "registerGraph",
        projection: decodeProjection(expectMap(inner, context), context)
      }
    case "DropGraph":
      return { kind: "dropGraph", id: expectString(inner, context) }
    case "RegisterSessionSource": {
      const fields = expectMap(inner, context)
      return {
        kind: "registerSessionSource",
        stream: field.requiredString(fields, "stream", context),
        topics: decodeSessionTopics(fields.get("topics"), context)
      }
    }
    case "RemoveSessionSource":
      return {
        kind: "removeSessionSource",
        stream: field.requiredString(expectMap(inner, context), "stream", context)
      }
    case "FilterCatalog":
      return { kind: "filterCatalog", command: decodeFilterCatalogCommand(inner, context) }
    case "AcceptGap":
      return {
        kind: "acceptGap",
        acceptance: decodeGapAcceptance(expectMap(inner, context), context)
      }
    case "SkipRecord":
      return { kind: "skipRecord", skip: decodeRecordSkip(expectMap(inner, context), context) }
    case "SourceRetained":
      return {
        kind: "sourceRetained",
        retention: decodeSourceRetention(expectMap(inner, context), context)
      }
    default:
      throw new CodecError(`\`${tag}\` is not a recognized control command`, context, "command")
  }
}

export function encodeControlEnvelope(envelope: ControlEnvelope): Map<string, unknown> {
  const map = new Map<string, unknown>([
    ["v", BigInt(envelope.v)],
    ["timestamp_micros", envelope.timestampMicros],
    ["command", encodeControlCommand(envelope.command)]
  ])
  if (envelope.stream !== undefined) map.set("stream", envelope.stream)
  if (envelope.stamp !== undefined) map.set("stamp", encodeControlStamp(envelope.stamp))
  return map
}

export function decodeControlEnvelope(map: CborMap, context: string): ControlEnvelope {
  return {
    v: field.requiredU32(map, "v", context),
    timestampMicros: field.requiredU64(map, "timestamp_micros", context),
    command: decodeControlCommand(map.get("command"), `${context}.command`),
    ...optionalStream(map, context),
    ...optionalStamp(map, context)
  }
}

function optionalStamp(map: CborMap, context: string): { readonly stamp?: ControlStamp } {
  const stamp = field.optionalMap(map, "stamp", context)
  return stamp === undefined ? {} : { stamp: decodeControlStamp(stamp, `${context}.stamp`) }
}

function optionalStream(map: CborMap, context: string): { readonly stream?: string } {
  const stream = field.optionalString(map, "stream", context)
  return stream === undefined ? {} : { stream }
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

export function invalidRecordPolicyIsBlock(policy: InvalidRecordPolicy): boolean {
  return policy === "block"
}
export function oversizePolicyIsBlock(policy: OversizePolicy): boolean {
  return policy === "block"
}
export function gapAcceptanceCovers(
  acceptance: GapAcceptance,
  generation: bigint,
  cursor: bigint,
  firstAvailable: bigint
): boolean {
  return (
    acceptance.topicCreatedAtMicros === generation &&
    acceptance.fromOffset <= cursor &&
    acceptance.toOffset >= firstAvailable
  )
}
