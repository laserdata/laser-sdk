import { type CallOptions, cancellable } from "../client/call-options.js"
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
  type BindingStatus,
  type BrowseOutcome,
  type BrowseReply,
  type ProjectionInfo,
  type ProjectionPage,
  type SchemaInfo,
  type SchemaPage
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
import { describeQueryError } from "../wire/query.js"
import {
  type ControlCommand,
  type Projection,
  type ProjectionBinding,
  type ProjectionId,
  type SchemaSource,
  type SourceSelector,
  validateSchemaName,
  validateSchemaSource
} from "../wire/control.js"
import { BARE_SCOPE, type ResourceScope } from "../client/resource-scope.js"

/** Publish one control command, its resource names belonging to `stream`
 * when set. */
export type PublishControl = (
  command: ControlCommand,
  stream?: string,
  options?: CallOptions
) => Promise<void>

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
    bindings: info.bindings.map((binding) => localBinding(scope, binding)),
    status: info.status.map((status) => ({
      ...status,
      index: scope.local(status.index) ?? status.index
    }))
  }
}

async function executeBrowse<Request>(
  backend: ManagedTransport,
  capabilities: Capabilities,
  command: ManagedCommand<Request, BrowseReply>,
  request: Request,
  options: CallOptions
): Promise<BrowseOutcome> {
  const reply = await executeManaged(backend, capabilities, command, request, options)
  if (reply.kind === "ok") return reply.outcome
  if (reply.kind === "err") {
    if (reply.error.kind === "unsupported") throw new UnsupportedError(reply.error.message)
    throw new QueryExecutionError(describeQueryError(reply.error), reply.error)
  }
  throw new ProtocolError(`browse: unrecognized reply variant \`${reply.tag}\``, {
    commandCode: command.code
  })
}

function unexpected(op: string, outcome: BrowseOutcome): ProtocolError {
  return new ProtocolError(`${op}: unexpected browse outcome \`${outcome.kind}\``)
}

function checkedU32(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0 || value > 0xffff_ffff)
    throw new InvalidError(`${name} must be a whole number from 0 to 4294967295`)
  return value
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
  private publishScoped(command: ControlCommand, options: CallOptions): Promise<void> {
    return cancellable(options.signal, "projection control", () =>
      this.publishControl(command, this.scope.stream, options)
    )
  }

  async register(projection: Projection, options: CallOptions = {}): Promise<void> {
    if (projection.kind.kind === "graph") {
      throw new InvalidError(
        `projection \`${projection.id}\` is a graph projection. Register it with registerGraph`
      )
    }
    await this.publishScoped(
      {
        kind: "registerProjection",
        projection: { ...projection, id: scopedId(this.scope, projection.id) }
      },
      options
    )
  }

  async drop(id: string, options: CallOptions = {}): Promise<void> {
    await this.publishScoped({ kind: "dropProjection", id: this.scope.name(id) }, options)
  }

  async registerGraph(projection: Projection, options: CallOptions = {}): Promise<void> {
    if (projection.kind.kind !== "graph" || projection.entitySchema === undefined) {
      throw new InvalidError(
        `projection \`${projection.id}\` is not a graph projection. Build it with kind "graph" and an entitySchema, or register it with register`
      )
    }
    await this.publishScoped(
      {
        kind: "registerGraph",
        projection: { ...projection, id: scopedId(this.scope, projection.id) }
      },
      options
    )
  }

  async dropGraph(id: string, options: CallOptions = {}): Promise<void> {
    await this.publishScoped({ kind: "dropGraph", id: this.scope.name(id) }, options)
  }

  /** Each binding's materialization status for projection `id`, or
   * `undefined` when no projection has that id. A binding whose index the
   * caller may not read is left out. */
  async status(
    id: string,
    options: CallOptions = {}
  ): Promise<readonly BindingStatus[] | undefined> {
    return (await this.statusInfo(id, options))?.status
  }

  /** The projection with its bindings and their status, so a wait tells a
   * removed binding from one that has not materialized yet.
   * @internal */
  async statusInfo(id: string, options: CallOptions = {}): Promise<ProjectionInfo | undefined> {
    const capabilities = await cancellable(
      options.signal,
      "projection status",
      this.getCapabilities
    )
    if (!capabilities.managed || !capabilities.materializations) {
      throw new UnsupportedError("materialization status is not served by this deployment", {
        surface: "materializations"
      })
    }
    const outcome = await executeBrowse(
      this.backend,
      capabilities,
      GetProjectionCommand,
      { v: QUERY_OP_VERSION, id: this.scope.name(id), status: true },
      options
    )
    if (outcome.kind !== "projection") throw unexpected("status", outcome)
    return outcome.projection === undefined ? undefined : localInfo(this.scope, outcome.projection)
  }

  async get(id: string, options: CallOptions = {}): Promise<ProjectionInfo | undefined> {
    const capabilities = await cancellable(options.signal, "projection get", this.getCapabilities)
    const outcome = await executeBrowse(
      this.backend,
      capabilities,
      GetProjectionCommand,
      { v: QUERY_OP_VERSION, id: this.scope.name(id) },
      options
    )
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
  private pageValue = 0
  private resultsValue = 0

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

  /** The 1-based page to read, in id order. Zero reads the first page. */
  page(page: number): this {
    this.pageValue = checkedU32(page, "page")
    return this
  }

  /** The projections per page. Zero leaves it to the server, which serves
   * 10 by default and at most 100. */
  results(results: number): this {
    this.resultsValue = checkedU32(results, "results")
    return this
  }

  /** Run the browse and return one page. A handle that scopes its resources
   * to a stream lists only that stream's projections, under the ids the
   * caller gave them. */
  async fetch(options: CallOptions = {}): Promise<ProjectionPage> {
    const capabilities = await cancellable(options.signal, "projection list", this.getCapabilities)
    const idPrefix =
      this.scope.stream !== undefined
        ? scopedResource(this.scope.stream, this.idPrefixFilter ?? "")
        : this.idPrefixFilter
    const outcome = await executeBrowse(
      this.backend,
      capabilities,
      ListProjectionsCommand,
      {
        v: QUERY_OP_VERSION,
        topics: this.topicNames,
        ...(this.nameContainsFilter !== undefined ? { nameContains: this.nameContainsFilter } : {}),
        ...(idPrefix !== undefined ? { idPrefix } : {}),
        ...(this.searchFilter !== undefined ? { search: this.searchFilter } : {}),
        ...streamField(this.scope),
        page: this.pageValue,
        results: this.resultsValue
      },
      options
    )
    if (outcome.kind === "projections") {
      return {
        ...outcome.page,
        items: outcome.page.items
          .filter((info) => this.scope.local(info.projection.id) !== undefined)
          .map((info) => localInfo(this.scope, info))
      }
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

  async apply(binding: ProjectionBinding, options: CallOptions = {}): Promise<void> {
    await cancellable(options.signal, "binding apply", () =>
      this.publishControl(
        { kind: "applyBinding", binding: scopedBinding(this.scope, binding) },
        this.scope.stream,
        options
      )
    )
  }

  async remove(
    source: SourceSelector,
    projectionRef?: string,
    options: CallOptions = {}
  ): Promise<void> {
    await cancellable(options.signal, "binding remove", () =>
      this.publishControl(
        {
          kind: "removeBinding",
          source,
          ...(projectionRef !== undefined ? { projectionRef: this.scope.name(projectionRef) } : {})
        },
        this.scope.stream,
        options
      )
    )
  }
}

/** The writer schema registry of the default stream, or the deployment-wide
 * registry without one. Ids are unique within one registry, and a registry
 * holds at most 1,024 schemas, tombstones included. */
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

  async drop(id: number, options: CallOptions = {}): Promise<void> {
    await cancellable(options.signal, "schema drop", () =>
      this.publishControl({ kind: "dropSchema", id }, this.scope.stream, options)
    )
  }

  async get(id: number, options: CallOptions = {}): Promise<SchemaInfo | undefined> {
    const capabilities = await cancellable(options.signal, "schema get", this.getCapabilities)
    const outcome = await executeBrowse(
      this.backend,
      capabilities,
      GetSchemaCommand,
      { v: QUERY_OP_VERSION, id, ...streamField(this.scope) },
      options
    )
    if (outcome.kind === "schema") return outcome.schema
    throw unexpected("schema", outcome)
  }

  /** One page of writer schemas, active and tombstoned, in id order. */
  list(): SchemasRequest {
    return SchemasRequest.create(this.backend, this.getCapabilities, this.scope)
  }
}

export class SchemasRequest {
  private nameContainsFilter: string | undefined
  private pageValue = 0
  private resultsValue = 0

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
  ): SchemasRequest {
    return new SchemasRequest(backend, getCapabilities, scope)
  }

  /** Keep only schemas whose optional name contains `substring`. */
  nameContains(substring: string): this {
    this.nameContainsFilter = substring
    return this
  }

  /** The 1-based page to read, in id order. Zero reads the first page. */
  page(page: number): this {
    this.pageValue = checkedU32(page, "page")
    return this
  }

  /** The schemas per page. Zero leaves it to the server, which serves 10 by
   * default and at most 100. */
  results(results: number): this {
    this.resultsValue = checkedU32(results, "results")
    return this
  }

  /** Run the browse and return one page of the matching schemas. */
  async fetch(options: CallOptions = {}): Promise<SchemaPage> {
    const capabilities = await cancellable(options.signal, "schema list", this.getCapabilities)
    const outcome = await executeBrowse(
      this.backend,
      capabilities,
      ListSchemasCommand,
      {
        v: QUERY_OP_VERSION,
        ...(this.nameContainsFilter !== undefined ? { nameContains: this.nameContainsFilter } : {}),
        ...streamField(this.scope),
        page: this.pageValue,
        results: this.resultsValue
      },
      options
    )
    if (outcome.kind === "schemas") return outcome.page
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

  async send(options: CallOptions = {}): Promise<number> {
    validateSchemaName(this.schemaName)
    validateSchemaSource(this.source)
    const capabilities = await cancellable(options.signal, "schema register", this.getCapabilities)
    const outcome = await executeBrowse(
      this.backend,
      capabilities,
      RegisterSchemaCommand,
      {
        v: QUERY_OP_VERSION,
        source: this.source,
        ...(this.schemaName !== undefined ? { name: this.schemaName } : {}),
        ...(this.schemaVersion !== undefined ? { version: this.schemaVersion } : {}),
        ...streamField(this.scope)
      },
      options
    )
    if (outcome.kind === "schemaRegistered") return outcome.id
    throw unexpected("register schema", outcome)
  }
}

// The schema registry of this handle's stream, the deployment-wide one when
// names stay bare.
function streamField(scope: ResourceScope): { readonly stream?: string } {
  return scope.stream === undefined ? {} : { stream: scope.stream }
}
