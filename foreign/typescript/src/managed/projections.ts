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

export class Projections {
  constructor(
    private readonly backend: BrowseBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly publishControl: PublishControl
  ) {}

  async register(projection: Projection): Promise<void> {
    if (projection.kind.kind === "graph") {
      throw new InvalidError(
        `projection \`${projection.id}\` is a graph projection. Register it with registerGraph`
      )
    }
    await this.publishControl({ kind: "registerProjection", projection })
  }

  async drop(id: string): Promise<void> {
    await this.publishControl({ kind: "dropProjection", id })
  }

  async registerGraph(projection: Projection): Promise<void> {
    if (projection.kind.kind !== "graph" || projection.entitySchema === undefined) {
      throw new InvalidError(
        `projection \`${projection.id}\` is not a graph projection. Build it with kind "graph" and an entitySchema, or register it with register`
      )
    }
    await this.publishControl({ kind: "registerGraph", projection })
  }

  async dropGraph(id: string): Promise<void> {
    await this.publishControl({ kind: "dropGraph", id })
  }

  async get(id: string): Promise<ProjectionInfo | undefined> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, GetProjectionCommand, {
      v: QUERY_OP_VERSION,
      id
    })
    if (outcome.kind === "projection") return outcome.projection
    throw unexpected("get", outcome)
  }

  list(): ProjectionsRequest {
    return new ProjectionsRequest(this.backend, this.getCapabilities)
  }
}

export class ProjectionsRequest {
  private topicNames: string[] = []
  private nameContainsFilter: string | undefined
  private idPrefixFilter: string | undefined
  private searchFilter: string | undefined

  constructor(
    private readonly backend: BrowseBackend,
    private readonly getCapabilities: () => Promise<Capabilities>
  ) {}

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

export class Bindings {
  constructor(private readonly publishControl: PublishControl) {}

  async apply(binding: ProjectionBinding): Promise<void> {
    await this.publishControl({ kind: "applyBinding", binding })
  }

  async remove(source: SourceSelector, projectionRef?: string): Promise<void> {
    await this.publishControl({
      kind: "removeBinding",
      source,
      ...(projectionRef !== undefined ? { projectionRef } : {})
    })
  }
}

export class Schemas {
  constructor(
    private readonly backend: BrowseBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly publishControl: PublishControl
  ) {}

  register(source: SchemaSource): RegisterSchemaRequest {
    return new RegisterSchemaRequest(this.backend, this.getCapabilities, source)
  }

  async drop(id: number): Promise<void> {
    await this.publishControl({ kind: "dropSchema", id })
  }

  async get(id: number): Promise<SchemaInfo | undefined> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, GetSchemaCommand, {
      v: QUERY_OP_VERSION,
      id
    })
    if (outcome.kind === "schema") return outcome.schema
    throw unexpected("schema", outcome)
  }

  async list(): Promise<readonly SchemaInfo[]> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeBrowse(this.backend, capabilities, ListSchemasCommand, {
      v: QUERY_OP_VERSION
    })
    if (outcome.kind === "schemas") return outcome.schemas
    throw unexpected("schemas", outcome)
  }
}

export class RegisterSchemaRequest {
  private schemaName: string | undefined
  private schemaVersion: number | undefined

  constructor(
    private readonly backend: BrowseBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly source: SchemaSource
  ) {}

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
      ...(this.schemaVersion !== undefined ? { version: this.schemaVersion } : {})
    })
    if (outcome.kind === "schemaRegistered") return outcome.id
    throw unexpected("register schema", outcome)
  }
}
