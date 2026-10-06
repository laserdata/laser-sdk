import type { Capabilities } from "../client/capabilities.js"
import { ForkExecutionError, InvalidError, ProtocolError } from "../client/errors.js"
import { executeManaged, type ManagedTransport } from "../client/managed.js"
import {
  ForkCreateCommand,
  ForkDeleteCommand,
  ForkListCommand,
  ForkPromoteCommand,
  ForkPutCommand,
  type ManagedCommand
} from "../wire/commands.js"
import {
  type ForkInfo,
  type ForkKind,
  type ForkOutcome,
  type ForkReply,
  validateForkId
} from "../wire/fork.js"

async function executeFork<Request>(
  backend: ManagedTransport,
  capabilities: Capabilities,
  command: ManagedCommand<Request, ForkReply>,
  request: Request
): Promise<ForkOutcome> {
  const reply = await executeManaged(backend, capabilities, command, request)
  if (reply.kind === "ok") return reply.outcome
  if (reply.kind === "err")
    throw new ForkExecutionError(`fork command failed: ${reply.error.kind}`, reply.error)
  throw new ProtocolError(`fork: unrecognized reply variant \`${reply.tag}\``, {
    commandCode: command.code
  })
}

function unexpected(op: string, outcome: ForkOutcome): ProtocolError {
  return new ProtocolError(`fork ${op}: unexpected reply outcome \`${outcome.kind}\``)
}

async function fetchForks(
  backend: ManagedTransport,
  capabilities: Capabilities
): Promise<readonly ForkInfo[]> {
  const outcome = await executeFork(backend, capabilities, ForkListCommand, undefined)
  if (outcome.kind === "list") return outcome.forks
  throw unexpected("list", outcome)
}

/** Operates on one copy-on-write fork. */
export class ForkHandle {
  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    readonly id: string
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    id: string
  ): ForkHandle {
    return new ForkHandle(backend, getCapabilities, id)
  }

  /** @internal */
  static async forks(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>
  ): Promise<readonly ForkInfo[]> {
    const capabilities = await getCapabilities()
    return fetchForks(backend, capabilities)
  }

  /** Starts a fork creation request. */
  create(): ForkCreateRequest {
    return ForkCreateRequest.create(this.backend, this.getCapabilities, this.id)
  }

  /** Applies speculative rows to the trunk and returns the applied row count. */
  async promote(): Promise<number> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeFork(this.backend, capabilities, ForkPromoteCommand, {
      forkId: this.id
    })
    if (outcome.kind === "promoted") return outcome.rows
    throw unexpected("promote", outcome)
  }

  /** Discards speculative rows and reports whether the fork existed. */
  async squash(): Promise<boolean> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeFork(this.backend, capabilities, ForkDeleteCommand, {
      forkId: this.id
    })
    if (outcome.kind === "deleted") return outcome.removed
    throw unexpected("squash", outcome)
  }

  /** Starts a speculative row write at an exact log position. */
  putRow(table: string, partitionId: number, offset: bigint): ForkPutRequest {
    return ForkPutRequest.create(
      this.backend,
      this.getCapabilities,
      this.id,
      table,
      partitionId,
      offset
    )
  }
}

/** Builds a fork creation request. */
export class ForkCreateRequest {
  private forkParent: string | undefined
  private forkKind: ForkKind = "continuous"
  private forkTables: readonly string[] = []

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly forkId: string
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    forkId: string
  ): ForkCreateRequest {
    return new ForkCreateRequest(backend, getCapabilities, forkId)
  }

  /** Creates a frozen snapshot at current trunk offsets. */
  severed(): this {
    this.forkKind = "severed"
    return this
  }

  /** Creates a live branch that follows trunk appends. */
  continuous(): this {
    this.forkKind = "continuous"
    return this
  }

  /** Records an audit parent without changing the trunk base. */
  parent(parent: string): this {
    this.forkParent = parent
    return this
  }

  /** Restricts a severed snapshot to the supplied tables. */
  tables(tables: readonly string[]): this {
    this.forkTables = tables
    return this
  }

  /** Opens the fork and returns its metadata. */
  async send(): Promise<ForkInfo> {
    validateForkId(this.forkId)
    const capabilities = await this.getCapabilities()
    const outcome = await executeFork(this.backend, capabilities, ForkCreateCommand, {
      forkId: this.forkId,
      kind: this.forkKind,
      tables: this.forkTables,
      ...(this.forkParent !== undefined ? { parent: this.forkParent } : {})
    })
    if (outcome.kind === "created") return outcome.info
    throw unexpected("create", outcome)
  }
}

/** Builds one speculative row write. */
export class ForkPutRequest {
  private forkProjectionId = ""
  private forkProjectionVersion = 0
  private forkFields = new Map<string, string>()
  private forkMetadata = new Map<string, string>()
  private forkPayload: Uint8Array | undefined
  private forkEmbedding: string | undefined
  private forkTombstone = false

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly forkId: string,
    private readonly table: string,
    private readonly partitionId: number,
    private readonly offset: bigint
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    forkId: string,
    table: string,
    partitionId: number,
    offset: bigint
  ): ForkPutRequest {
    return new ForkPutRequest(backend, getCapabilities, forkId, table, partitionId, offset)
  }

  /** Sets the row projection identity. */
  projection(id: string, version: number): this {
    this.forkProjectionId = id
    this.forkProjectionVersion = version
    return this
  }

  /** Adds an indexed field. */
  field(name: string, value: string): this {
    this.forkFields.set(name, value)
    return this
  }

  /** Adds non-indexed metadata. */
  metadata(name: string, value: string): this {
    this.forkMetadata.set(name, value)
    return this
  }

  /** Attaches an opaque payload. */
  payload(payload: Uint8Array): this {
    this.forkPayload = payload
    return this
  }

  /** Attaches an embedding vector. It travels as a JSON array literal, and a
   * non-finite component is rejected. */
  embedding(embedding: Iterable<number>): this {
    const components = [...embedding]
    if (!components.every((component) => Number.isFinite(component))) {
      throw new InvalidError("embedding components must be finite numbers")
    }
    this.forkEmbedding = JSON.stringify(components)
    return this
  }

  /** Hides the trunk row at this coordinate. */
  tombstone(): this {
    this.forkTombstone = true
    return this
  }

  /** Writes the speculative row. */
  async send(): Promise<void> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeFork(this.backend, capabilities, ForkPutCommand, {
      forkId: this.forkId,
      table: this.table,
      partitionId: this.partitionId,
      offset: this.offset,
      projectionId: this.forkProjectionId,
      projectionVersion: this.forkProjectionVersion,
      fields: this.forkFields,
      metadata: this.forkMetadata,
      tombstone: this.forkTombstone,
      ...(this.forkPayload !== undefined ? { payload: this.forkPayload } : {}),
      ...(this.forkEmbedding !== undefined ? { embedding: this.forkEmbedding } : {})
    })
    if (outcome.kind === "written") return
    throw unexpected("put_row", outcome)
  }
}
