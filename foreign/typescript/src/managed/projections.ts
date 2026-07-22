import type { Capabilities } from "../client/capabilities.js"
import {
  InvalidError,
  ProtocolError,
  QueryExecutionError,
  UnsupportedError
} from "../client/errors.js"
import { executeManaged, type ManagedTransport } from "../client/managed.js"
import {
  type BrowseOutcome,
  type BrowseReply,
  type ProjectionInfo,
  type SchemaInfo
} from "../wire/browse.js"
import {
  GetProjectionCommand,
  GetSchemaCommand,
  ListProjectionsCommand,
  ListSchemasCommand,
  type ManagedCommand,
  RegisterSchemaCommand
} from "../wire/commands.js"
import { QUERY_OP_VERSION } from "../wire/codes.js"
import type {
  ControlCommand,
  Projection,
  ProjectionBinding,
  SchemaSource,
  SourceSelector
} from "../wire/control.js"

export type BrowseBackend = ManagedTransport
export type PublishControl = (command: ControlCommand) => Promise<void>

async function executeBrowse<Request>(
  backend: BrowseBackend,
  capabilities: Capabilities,
  command: ManagedCommand<Request, BrowseReply>,
  request: Request
): Promise<BrowseOutcome> {
  const reply = await executeManaged(backend, capabilities, command, request)
  if (reply.kind === "ok") return reply.outcome
  if (reply.kind === "err") {
    if (reply.error.kind === "unsupported") throw new UnsupportedError(reply.error.message)
    throw new QueryExecutionError(`browse failed: ${reply.error.kind}`, reply.error)
  }
  throw new ProtocolError(`browse: unrecognized reply variant \`${reply.tag}\``, {
    commandCode: command.code
  })
}

function unexpected(op: string, outcome: BrowseOutcome): ProtocolError {
  return new ProtocolError(`${op}: unexpected browse outcome \`${outcome.kind}\``)
}

// Handle to the projection registry: `.register(projection)`, `.drop(id)`,
// `.get(id)`, and the filterable `.list()` browse. Cheap to create, it
// borrows the connection. Writes (`register`/`drop`) publish control
// commands applied asynchronously (202-accepted semantics: poll `.get(id)`
// to observe the apply). Reads (`get`/`list`) are managed-command browses.
export class Projections {
  constructor(
    private readonly backend: BrowseBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly publishControl: PublishControl
  ) {}

  // Register a `Projection` by publishing a `RegisterProjection` control
  // command. Rejects a graph projection with `InvalidError`: a graph
  // materializes nodes and edges, not a queryable row table, so it
  // registers through `.registerGraph` instead.
  async register(projection: Projection): Promise<void> {
    if (projection.kind.kind === "graph") {
      throw new InvalidError(
        `projection \`${projection.id}\` is a graph projection. Register it with registerGraph`
      )
    }
    await this.publishControl({ kind: "registerProjection", projection })
  }

  // Drop a projection by publishing a `DropProjection` control command.
  // Existing materialized rows are left untouched.
  async drop(id: string): Promise<void> {
    await this.publishControl({ kind: "dropProjection", id })
  }

  // Register a graph projection: a `Projection` with `kind.kind ===
  // "graph"` and an entity schema. Records the named knowledge graph so it
  // is discoverable, and declares the node/edge extraction plan. Rejects a
  // non-graph projection with `InvalidError`.
  async registerGraph(projection: Projection): Promise<void> {
    if (projection.kind.kind !== "graph" || projection.entitySchema === undefined) {
      throw new InvalidError(
        `projection \`${projection.id}\` is not a graph projection. Build it with kind "graph" and an entitySchema, or register it with register`
      )
    }
    await this.publishControl({ kind: "registerGraph", projection })
  }

  // Drop the graph projection registered under `id` by publishing a
  // `DropGraph` control command. Materialized nodes and edges are left
  // untouched, the same as `.drop` for a row projection.
  async dropGraph(id: string): Promise<void> {
    await this.publishControl({ kind: "dropGraph", id })
  }

  // Read one projection's details by `id`, or `undefined` when no
  // projection has that id.
  async get(id: string): Promise<ProjectionInfo | undefined> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, GetProjectionCommand, {
      v: QUERY_OP_VERSION,
      id
    })
    if (outcome.kind === "projection") return outcome.projection
    throw unexpected("get", outcome)
  }

  // Browse the registry. Narrow with `.forTopic`/`.forTopics`/
  // `.nameContains`/`.idPrefix`/`.search`, then `.fetch()`. No filter lists
  // every projection.
  list(): ProjectionsRequest {
    return new ProjectionsRequest(this.backend, this.getCapabilities)
  }
}

// Fluent builder for `Projections.list`, finished with `.fetch()`.
export class ProjectionsRequest {
  private topicNames: string[] = []
  private nameContainsFilter: string | undefined
  private idPrefixFilter: string | undefined
  private searchFilter: string | undefined

  constructor(
    private readonly backend: BrowseBackend,
    private readonly getCapabilities: () => Promise<Capabilities>
  ) {}

  // Keep only projections bound to `topic`. Repeatable.
  forTopic(topic: string): this {
    this.topicNames.push(topic)
    return this
  }

  // Keep only projections bound to any of `topics`.
  forTopics(topics: readonly string[]): this {
    this.topicNames.push(...topics)
    return this
  }

  // Keep only projections whose name contains `substring`.
  nameContains(substring: string): this {
    this.nameContainsFilter = substring
    return this
  }

  // Keep only projections whose id starts with `prefix`.
  idPrefix(prefix: string): this {
    this.idPrefixFilter = prefix
    return this
  }

  // Keep only projections whose name or id matches `substring`, the
  // single-box convenience filter. Composes with the narrower filters.
  search(substring: string): this {
    this.searchFilter = substring
    return this
  }

  // Run the browse and return the matching projections.
  async fetch(): Promise<readonly ProjectionInfo[]> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, ListProjectionsCommand, {
      v: QUERY_OP_VERSION,
      topics: this.topicNames,
      ...(this.nameContainsFilter !== undefined ? { nameContains: this.nameContainsFilter } : {}),
      ...(this.idPrefixFilter !== undefined ? { idPrefix: this.idPrefixFilter } : {}),
      ...(this.searchFilter !== undefined ? { search: this.searchFilter } : {})
    })
    if (outcome.kind === "projections") return outcome.projections
    throw unexpected("list", outcome)
  }
}

// Handle to the binding surface: `.apply(binding)` routes a `(stream,
// topic)` source into registered projections, `.remove(source, ..)` stops
// it. Cheap to create. Bindings are browsed through the projections they
// route to: `laser.projections().get(id)` returns each projection with its
// bindings.
export class Bindings {
  constructor(private readonly publishControl: PublishControl) {}

  // Apply a `ProjectionBinding` by publishing an `ApplyBinding` control
  // command. Register the referenced projection first, or the apply is
  // rejected.
  async apply(binding: ProjectionBinding): Promise<void> {
    await this.publishControl({ kind: "applyBinding", binding })
  }

  // Remove a binding by publishing a `RemoveBinding` control command. New
  // records on that `(stream, topic)` no longer materialize, rows already
  // written stay. `projectionRef` scopes the removal to a single allowed
  // projection when set, else removes the whole binding.
  async remove(source: SourceSelector, projectionRef?: string): Promise<void> {
    await this.publishControl({
      kind: "removeBinding",
      source,
      ...(projectionRef !== undefined ? { projectionRef } : {})
    })
  }
}

// Handle to the writer-schema registry (Avro, Protobuf): `.register(def)`,
// `.drop(id)`, `.get(id)`, `.list()`. Cheap to create. Writes are applied
// asynchronously (202-accepted, poll `.get(id)` until defined to observe
// the apply). Reads are managed-command browses.
export class Schemas {
  constructor(
    private readonly backend: BrowseBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly publishControl: PublishControl
  ) {}

  // Register a writer schema. Synchronous: the deployment validates that
  // the definition compiles, allocates the next free id, durably appends
  // the control event, and `.send()` returns the id.
  register(source: SchemaSource): RegisterSchemaRequest {
    return new RegisterSchemaRequest(this.backend, this.getCapabilities, source)
  }

  // Drop the writer schema registered under `id` by publishing a
  // `DropSchema` control command. Drop tombstones, it does not delete: the
  // id leaves the active set but records stamped with it keep decoding, and
  // the id stays reserved against re-registration with a different
  // definition. Dropping an unknown id is a no-op.
  async drop(id: number): Promise<void> {
    await this.publishControl({ kind: "dropSchema", id })
  }

  // Read the writer schema occupying `id` (active or tombstoned, see
  // `SchemaInfo.dropped`), or `undefined` when the id is free.
  async get(id: number): Promise<SchemaInfo | undefined> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, GetSchemaCommand, {
      v: QUERY_OP_VERSION,
      id
    })
    if (outcome.kind === "schema") return outcome.schema
    throw unexpected("schema", outcome)
  }

  // List every known writer schema, active and tombstoned.
  async list(): Promise<readonly SchemaInfo[]> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, ListSchemasCommand, {
      v: QUERY_OP_VERSION
    })
    if (outcome.kind === "schemas") return outcome.schemas
    throw unexpected("schemas", outcome)
  }
}

// Fluent builder for `Schemas.register`, finished with `.send()`.
export class RegisterSchemaRequest {
  private schemaName: string | undefined
  private schemaVersion: number | undefined

  constructor(
    private readonly backend: BrowseBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly source: SchemaSource
  ) {}

  // Optional human label, pure metadata: stored and returned, never
  // dispatched on and not unique.
  name(name: string): this {
    this.schemaName = name
    return this
  }

  // Optional caller-tracked schema version, pure metadata: stored and
  // returned, never dispatched on.
  version(version: number): this {
    this.schemaVersion = version
    return this
  }

  // Execute the register and return the allocated id. An uncompilable
  // definition answers `UnsupportedError` (nothing allocated).
  async send(): Promise<number> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, RegisterSchemaCommand, {
      v: QUERY_OP_VERSION,
      source: this.source,
      ...(this.schemaName !== undefined ? { name: this.schemaName } : {}),
      ...(this.schemaVersion !== undefined ? { version: this.schemaVersion } : {})
    })
    if (outcome.kind === "schemaRegistered") return outcome.id
    throw unexpected("register schema", outcome)
  }
}
