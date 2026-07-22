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

// A handle to one fork by id. Cheap to create, it borrows the connection.
// Open the fork with `.create()`, write rows with `.putRow(...)`, query it
// (`laser.query(...).fork(id)`), then `.promote()` or `.squash()` it.
export class Fork {
  constructor(
    private readonly backend: ForkBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    readonly forkId: string
  ) {}

  static forks(
    backend: ForkBackend,
    getCapabilities: () => Promise<Capabilities>
  ): Promise<readonly ForkInfo[]> {
    return getCapabilities().then((capabilities) => fetchForks(backend, capabilities))
  }

  // Start opening this fork. Choose `.severed()` or `.continuous()` (default
  // continuous), optionally narrow the snapshot with `.tables([...])`, then
  // `.send()` for the fork's metadata.
  create(): ForkCreateRequest {
    return new ForkCreateRequest(this.backend, this.getCapabilities, this.forkId)
  }

  // Promote this fork: splice its speculative rows onto the trunk (and apply
  // its tombstones), then squash it. Returns the number of rows applied.
  async promote(): Promise<number> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeFork(this.backend, capabilities, ForkPromoteCommand, {
      forkId: this.forkId
    })
    if (outcome.kind === "promoted") return outcome.rows
    throw unexpected("promote", outcome)
  }

  // Squash this fork: discard its speculative rows. Returns `true` when an
  // open fork was removed, `false` when none existed.
  async squash(): Promise<boolean> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeFork(this.backend, capabilities, ForkDeleteCommand, {
      forkId: this.forkId
    })
    if (outcome.kind === "deleted") return outcome.removed
    throw unexpected("squash", outcome)
  }

  // Write one speculative row into this fork at `(table, partitionId,
  // offset)`. Add indexed fields with `.field`, an opaque body with
  // `.payload`, an embedding with `.embedding`, or mark `.tombstone()` to
  // hide the trunk row at that coordinate from the fork's view. Finish with
  // `.send()`.
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

// Fluent builder for `Fork.create`, finished with `.send()`.
export class ForkCreateRequest {
  private forkParent: string | undefined
  private forkKind: ForkKind = "continuous"
  private forkTables: readonly string[] = []

  constructor(
    private readonly backend: ForkBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly forkId: string
  ) {}

  // Make a frozen snapshot at the trunk's current offsets (later appends
  // hidden).
  severed(): this {
    this.forkKind = "severed"
    return this
  }

  // Make a live branch that keeps seeing new trunk appends (the default).
  continuous(): this {
    this.forkKind = "continuous"
    return this
  }

  // Record this fork's parent (audit only, the fork still branches off the
  // trunk).
  parent(parent: string): this {
    this.forkParent = parent
    return this
  }

  // Narrow a severed snapshot to these tables. Empty captures every table.
  tables(tables: readonly string[]): this {
    this.forkTables = tables
    return this
  }

  // Open the fork. Returns its metadata.
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

// Fluent builder for `Fork.putRow`, finished with `.send()`.
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

  // Set the projection id/version this speculative row belongs to.
  projection(id: string, version: number): this {
    this.forkProjectionId = id
    this.forkProjectionVersion = version
    return this
  }

  // Add one indexed field (the columns queries filter and order on).
  field(name: string, value: string): this {
    this.forkFields.set(name, value)
    return this
  }

  // Add one metadata header (non-indexed).
  metadata(name: string, value: string): this {
    this.forkMetadata.set(name, value)
    return this
  }

  // Attach an opaque payload body.
  payload(payload: Uint8Array): this {
    this.forkPayload = payload
    return this
  }

  // Attach an embedding as a JSON array literal (e.g. `[0.1,0.2,...]`).
  embedding(embedding: string): this {
    this.forkEmbedding = embedding
    return this
  }

  // Mark this as a tombstone: hide the trunk row at this coordinate from
  // the fork's view instead of writing a value.
  tombstone(): this {
    this.forkTombstone = true
    return this
  }

  // Write the speculative row.
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
