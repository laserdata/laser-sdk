import type { Capabilities } from "../client/capabilities.js"
import { ForkExecutionError } from "../client/errors.js"
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

export type ForkBackend = ManagedTransport

async function executeFork<Request>(
  backend: ForkBackend,
  capabilities: Capabilities,
  command: ManagedCommand<Request, ForkReply>,
  request: Request
): Promise<ForkOutcome> {
  const reply = await executeManaged(backend, capabilities, command, request)
  if (reply.kind === "ok") return reply.outcome
  if (reply.kind === "err")
    throw new ForkExecutionError(`fork command failed: ${reply.error.kind}`, reply.error)
  return { kind: "unrecognized", tag: reply.tag, value: undefined }
}

function unexpected(op: string, outcome: ForkOutcome): ForkExecutionError {
  return new ForkExecutionError(`fork ${op}: unexpected reply outcome \`${outcome.kind}\``, outcome)
}

async function fetchForks(
  backend: ForkBackend,
  capabilities: Capabilities
): Promise<readonly ForkInfo[]> {
  const outcome = await executeFork(backend, capabilities, ForkListCommand, undefined)
  if (outcome.kind === "list") return outcome.forks
  throw unexpected("list", outcome)
}

/** Operates on one copy-on-write fork. */
export class Fork {
  constructor(
    private readonly backend: ForkBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    readonly forkId: string
  ) {}

  static async forks(
    backend: ForkBackend,
    getCapabilities: () => Promise<Capabilities>
  ): Promise<readonly ForkInfo[]> {
    const capabilities = await getCapabilities()
    return fetchForks(backend, capabilities)
  }

  /** Starts a fork creation request. */
  create(): ForkCreateRequest {
    return new ForkCreateRequest(this.backend, this.getCapabilities, this.forkId)
  }

  /** Applies speculative rows to the trunk and returns the applied row count. */
  async promote(): Promise<number> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeFork(this.backend, capabilities, ForkPromoteCommand, {
      forkId: this.forkId
    })
    if (outcome.kind === "promoted") return outcome.rows
    throw unexpected("promote", outcome)
  }

  /** Discards speculative rows and reports whether the fork existed. */
  async squash(): Promise<boolean> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeFork(this.backend, capabilities, ForkDeleteCommand, {
      forkId: this.forkId
    })
    if (outcome.kind === "deleted") return outcome.removed
    throw unexpected("squash", outcome)
  }

  /** Starts a speculative row write at an exact log position. */
  putRow(table: string, partitionId: number, offset: bigint): ForkPutRequest {
    return new ForkPutRequest(
      this.backend,
      this.getCapabilities,
      this.forkId,
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

  constructor(
    private readonly backend: ForkBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly forkId: string
  ) {}

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

  constructor(
    private readonly backend: ForkBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly forkId: string,
    private readonly table: string,
    private readonly partitionId: number,
    private readonly offset: bigint
  ) {}

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

  /** Attaches an embedding encoded as a JSON array. */
  embedding(embedding: string): this {
    this.forkEmbedding = embedding
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
