import type { Capabilities } from "../client/capabilities.js"
import { scopedResource } from "../wire/authz.js"
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
  ProjectionId,
  SchemaSource,
  SourceSelector
} from "../wire/control.js"
import { BARE_SCOPE, type ResourceScope } from "../client/resource-scope.js"

/** Publish one control command, its resource names belonging to `stream`
 * when set. */
export type PublishControl = (command: ControlCommand, stream?: string) => Promise<void>

function scopedId(scope: ResourceScope, id: ProjectionId): ProjectionId {
  return scope.name(id) as ProjectionId
}

function localId(scope: ResourceScope, id: ProjectionId): ProjectionId {
  return (scope.local(id) ?? id) as ProjectionId
}

function scopedBinding(scope: ResourceScope, binding: ProjectionBinding): ProjectionBinding {
  return {
    ...binding,
    allowedProjections: binding.allowedProjections.map((id) => scopedId(scope, id)),
    ...(binding.defaultProjection !== undefined
      ? { defaultProjection: scopedId(scope, binding.defaultProjection) }
      : {}),
    index: scope.name(binding.index)
  }
}

function localBinding(scope: ResourceScope, binding: ProjectionBinding): ProjectionBinding {
  return {
    ...binding,
    allowedProjections: binding.allowedProjections.map((id) => localId(scope, id)),
    ...(binding.defaultProjection !== undefined
      ? { defaultProjection: localId(scope, binding.defaultProjection) }
      : {}),
    index: scope.local(binding.index) ?? binding.index
  }
}

// Names under this handle's stream come back as the caller wrote them.
function localInfo(scope: ResourceScope, info: ProjectionInfo): ProjectionInfo {
  return {
    projection: { ...info.projection, id: localId(scope, info.projection.id) },
    bindings: info.bindings.map((binding) => localBinding(scope, binding))
  }
}

async function executeBrowse<Request>(
  backend: ManagedTransport,
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

export class Projections {
  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly publishControl: PublishControl,
    private readonly scope: ResourceScope
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    publishControl: PublishControl,
    scope: ResourceScope = BARE_SCOPE
  ): Projections {
    return new Projections(backend, getCapabilities, publishControl, scope)
  }

  // A control command whose resource names belong to this handle's stream.
  private publishScoped(command: ControlCommand): Promise<void> {
    return this.publishControl(command, this.scope.stream)
  }

  async register(projection: Projection): Promise<void> {
    if (projection.kind.kind === "graph") {
      throw new InvalidError(
        `projection \`${projection.id}\` is a graph projection. Register it with registerGraph`
      )
    }
    await this.publishScoped({
      kind: "registerProjection",
      projection: { ...projection, id: scopedId(this.scope, projection.id) }
    })
  }

  async drop(id: string): Promise<void> {
    await this.publishScoped({ kind: "dropProjection", id: this.scope.name(id) })
  }

  async registerGraph(projection: Projection): Promise<void> {
    if (projection.kind.kind !== "graph" || projection.entitySchema === undefined) {
      throw new InvalidError(
        `projection \`${projection.id}\` is not a graph projection. Build it with kind "graph" and an entitySchema, or register it with register`
      )
    }
    await this.publishScoped({
      kind: "registerGraph",
      projection: { ...projection, id: scopedId(this.scope, projection.id) }
    })
  }

  async dropGraph(id: string): Promise<void> {
    await this.publishScoped({ kind: "dropGraph", id: this.scope.name(id) })
  }

  async get(id: string): Promise<ProjectionInfo | undefined> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, GetProjectionCommand, {
      v: QUERY_OP_VERSION,
      id: this.scope.name(id)
    })
    if (outcome.kind === "projection") {
      return outcome.projection === undefined
        ? undefined
        : localInfo(this.scope, outcome.projection)
    }
    throw unexpected("get", outcome)
  }

  list(): ProjectionsRequest {
    return ProjectionsRequest.create(this.backend, this.getCapabilities, this.scope)
  }
}

export class ProjectionsRequest {
  private topicNames: string[] = []
  private nameContainsFilter: string | undefined
  private idPrefixFilter: string | undefined
  private searchFilter: string | undefined

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly scope: ResourceScope
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    scope: ResourceScope = BARE_SCOPE
  ): ProjectionsRequest {
    return new ProjectionsRequest(backend, getCapabilities, scope)
  }

  forTopic(topic: string): this {
    this.topicNames.push(topic)
    return this
  }

  forTopics(topics: readonly string[]): this {
    this.topicNames.push(...topics)
    return this
  }

  nameContains(substring: string): this {
    this.nameContainsFilter = substring
    return this
  }

  idPrefix(prefix: string): this {
    this.idPrefixFilter = prefix
    return this
  }

  search(substring: string): this {
    this.searchFilter = substring
    return this
  }

  /** Run the browse. A handle that scopes its resources to a stream lists
   * only that stream's projections, under the ids the caller gave them. */
  async fetch(): Promise<readonly ProjectionInfo[]> {
    const capabilities = await this.getCapabilities()
    const idPrefix =
      this.scope.stream !== undefined
        ? scopedResource(this.scope.stream, this.idPrefixFilter ?? "")
        : this.idPrefixFilter
    const outcome = await executeBrowse(this.backend, capabilities, ListProjectionsCommand, {
      v: QUERY_OP_VERSION,
      topics: this.topicNames,
      ...(this.nameContainsFilter !== undefined ? { nameContains: this.nameContainsFilter } : {}),
      ...(idPrefix !== undefined ? { idPrefix } : {}),
      ...(this.searchFilter !== undefined ? { search: this.searchFilter } : {})
    })
    if (outcome.kind === "projections") {
      return outcome.projections
        .filter((info) => this.scope.local(info.projection.id) !== undefined)
        .map((info) => localInfo(this.scope, info))
    }
    throw unexpected("list", outcome)
  }
}

export class Bindings {
  private constructor(
    private readonly publishControl: PublishControl,
    private readonly scope: ResourceScope
  ) {}

  /** @internal */
  static create(publishControl: PublishControl, scope: ResourceScope = BARE_SCOPE): Bindings {
    return new Bindings(publishControl, scope)
  }

  async apply(binding: ProjectionBinding): Promise<void> {
    await this.publishControl(
      { kind: "applyBinding", binding: scopedBinding(this.scope, binding) },
      this.scope.stream
    )
  }

  async remove(source: SourceSelector, projectionRef?: string): Promise<void> {
    await this.publishControl(
      {
        kind: "removeBinding",
        source,
        ...(projectionRef !== undefined ? { projectionRef: this.scope.name(projectionRef) } : {})
      },
      this.scope.stream
    )
  }
}

export class Schemas {
  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly publishControl: PublishControl,
    private readonly scope: ResourceScope
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    publishControl: PublishControl,
    scope: ResourceScope = BARE_SCOPE
  ): Schemas {
    return new Schemas(backend, getCapabilities, publishControl, scope)
  }

  register(source: SchemaSource): RegisterSchemaRequest {
    return RegisterSchemaRequest.create(this.backend, this.getCapabilities, source, this.scope)
  }

  async drop(id: number): Promise<void> {
    await this.publishControl({ kind: "dropSchema", id }, this.scope.stream)
  }

  async get(id: number): Promise<SchemaInfo | undefined> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, GetSchemaCommand, {
      v: QUERY_OP_VERSION,
      id,
      ...streamField(this.scope)
    })
    if (outcome.kind === "schema") return outcome.schema
    throw unexpected("schema", outcome)
  }

  async list(): Promise<readonly SchemaInfo[]> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, ListSchemasCommand, {
      v: QUERY_OP_VERSION,
      ...streamField(this.scope)
    })
    if (outcome.kind === "schemas") return outcome.schemas
    throw unexpected("schemas", outcome)
  }
}

export class RegisterSchemaRequest {
  private schemaName: string | undefined
  private schemaVersion: number | undefined

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly source: SchemaSource,
    private readonly scope: ResourceScope
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    source: SchemaSource,
    scope: ResourceScope = BARE_SCOPE
  ): RegisterSchemaRequest {
    return new RegisterSchemaRequest(backend, getCapabilities, source, scope)
  }

  name(name: string): this {
    this.schemaName = name
    return this
  }

  version(version: number): this {
    this.schemaVersion = version
    return this
  }

  async send(): Promise<number> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, RegisterSchemaCommand, {
      v: QUERY_OP_VERSION,
      source: this.source,
      ...(this.schemaName !== undefined ? { name: this.schemaName } : {}),
      ...(this.schemaVersion !== undefined ? { version: this.schemaVersion } : {}),
      ...streamField(this.scope)
    })
    if (outcome.kind === "schemaRegistered") return outcome.id
    throw unexpected("register schema", outcome)
  }
}

// The schema registry of this handle's stream, the deployment-wide one when
// names stay bare.
function streamField(scope: ResourceScope): { readonly stream?: string } {
  return scope.stream === undefined ? {} : { stream: scope.stream }
}
