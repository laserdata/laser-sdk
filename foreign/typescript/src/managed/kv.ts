import { type Capabilities, requireCapability } from "../client/capabilities.js"
import {
  InvalidError,
  KvExecutionError,
  ProtocolError,
  UnsupportedError
} from "../client/errors.js"
import { decodeManagedReply, executeManaged, type ManagedTransport } from "../client/managed.js"
import { executeBatch } from "./batch.js"
import { LeaseCoordinator } from "./coordination.js"
import { saturatingAdd } from "../runtime/clock.js"
import type { Codec } from "../stream/codecs.js"
import { Json, Msgpack } from "../stream/codecs.js"
import type { BatchItem } from "../wire/batch.js"
import {
  KvCasCommand,
  KvCasFencedCommand,
  KvCopyCommand,
  KvDeleteCommand,
  KvDeleteManyCommand,
  KvExistsCommand,
  KvExpireCommand,
  KvGetCommand,
  KvLeaseRenewCommand,
  KvMoveCommand,
  KvNamespacesCommand,
  KvPatchCommand,
  KvReleaseCommand,
  KvScanCommand,
  KvSetCommand,
  type ManagedCommand
} from "../wire/commands.js"
import {
  type CasExpect,
  type KvEntry,
  type KvMetadata,
  type KvNamespaceInfo,
  type KvOutcome,
  type KvPage,
  type KvReply,
  validateNamespace
} from "../wire/kv.js"
import {
  DEFAULT_SCAN_LIMIT,
  MAX_KEY_BYTES,
  MAX_SCAN_LIMIT,
  MAX_VALUE_BYTES
} from "../wire/limits.js"
import type { MutationPosition } from "../wire/mutation.js"

/** A granted revocable lease: the fencing token, the TTL the store granted,
 * and the mutation position at which the answering fold applied the grant or
 * renewal (the barrier a takeover read passes to `getEntryAtLeast`). */
export interface Lease {
  readonly token: bigint
  readonly grantedTtlMicros: bigint
  readonly position: MutationPosition
}

// A store built without the client's coordinator has no connection to acquire
// a lease on, as for an injected client.
const NO_LEASE_COORDINATOR = new LeaseCoordinator()

async function executeKv<Request>(
  backend: ManagedTransport,
  capabilities: Capabilities,
  namespace: string | undefined,
  command: ManagedCommand<Request, KvReply>,
  request: Request,
  options?: { readonly retryAfterReconnect?: boolean }
): Promise<KvOutcome> {
  if (namespace !== undefined) validateNamespace(namespace)
  const reply = await executeManaged(backend, capabilities, command, request, options)
  if (reply.kind === "ok") return reply.outcome
  if (reply.kind === "err") {
    throw new KvExecutionError(`kv command failed: ${reply.error.kind}`, reply.error)
  }
  throw new ProtocolError(`kv: unrecognized reply variant \`${reply.tag}\``, {
    commandCode: command.code
  })
}

function unexpected(op: string, outcome: KvOutcome): ProtocolError {
  return new ProtocolError(`kv ${op}: unexpected reply outcome \`${outcome.kind}\``)
}

function validatedKey(key: Uint8Array): Uint8Array {
  if (key.byteLength === 0) throw new InvalidError("key must not be empty")
  if (key.byteLength > MAX_KEY_BYTES) {
    throw new InvalidError(
      `key is ${String(key.byteLength)}B, exceeds cap ${String(MAX_KEY_BYTES)}B`
    )
  }
  return key
}

function validatedValue(value: Uint8Array): void {
  if (value.byteLength > MAX_VALUE_BYTES) {
    throw new InvalidError(
      `value is ${String(value.byteLength)}B, exceeds cap ${String(MAX_VALUE_BYTES)}B`
    )
  }
}

async function fetchNamespaces(
  backend: ManagedTransport,
  capabilities: Capabilities
): Promise<readonly KvNamespaceInfo[]> {
  const outcome = await executeKv(backend, capabilities, undefined, KvNamespacesCommand, undefined)
  if (outcome.kind === "namespaces") return outcome.namespaces
  throw unexpected("namespaces", outcome)
}

/** A namespace-scoped managed key-value view. */
export class Kv {
  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    readonly namespace: string,
    private readonly leases?: LeaseCoordinator
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string,
    leases?: LeaseCoordinator
  ): Kv {
    return new Kv(backend, getCapabilities, namespace, leases)
  }

  /** @internal */
  static async namespaces(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>
  ): Promise<readonly KvNamespaceInfo[]> {
    const capabilities = await getCapabilities()
    return fetchNamespaces(backend, capabilities)
  }

  async get(key: Uint8Array): Promise<Uint8Array | undefined> {
    const entry = await this.getEntry(key)
    return entry?.value
  }

  async getEntry(key: Uint8Array): Promise<KvEntry | undefined> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvGetCommand, {
      namespace: this.namespace,
      key: validatedKey(key)
    })
    if (outcome.kind === "value") return outcome.entry
    throw unexpected("get", outcome)
  }

  /** Barriered read: resolves only once the answering fold has applied at
   * least `minPosition` (the position a `Lease` grant or renewal returned). A
   * fold that does not catch up fails with the typed `stale` error, never an
   * absent value. Needs the `kvFencedLeases` capability: a pre-fencing server
   * would silently ignore the barrier. */
  async getEntryAtLeast(
    key: Uint8Array,
    minPosition: MutationPosition
  ): Promise<KvEntry | undefined> {
    const capabilities = await this.getCapabilities()
    if (!capabilities.kv.fencedLeases) {
      throw new UnsupportedError("the fenced-lease contract is not advertised by this deployment")
    }
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvGetCommand, {
      namespace: this.namespace,
      key: validatedKey(key),
      minPosition
    })
    if (outcome.kind === "value") return outcome.entry
    throw unexpected("get", outcome)
  }

  /** Decodes a value with the supplied codec, or returns undefined. */
  async getAs<T>(key: Uint8Array, codec: Codec<T>): Promise<T | undefined> {
    const value = await this.get(key)
    return value === undefined ? undefined : codec.decode(value)
  }

  async getTyped<T>(key: Uint8Array, decodeValue: (value: unknown) => T): Promise<T | undefined> {
    return this.getAs(key, new Json(decodeValue))
  }

  /** Starts a value write or compare-and-swap request. */
  set(key: Uint8Array): KvSetRequest {
    return KvSetRequest.create(
      this.backend,
      this.getCapabilities,
      this.namespace,
      validatedKey(key)
    )
  }

  /** Starts a compare-and-swap that requires a live lease at
   * (`fenceNamespace`, `fenceKey`) and a fence sequence still equal to
   * `fenceToken`. */
  casFenced(
    key: Uint8Array,
    fenceNamespace: string,
    fenceKey: Uint8Array,
    fenceToken: bigint
  ): KvCasFencedRequest {
    validateNamespace(fenceNamespace)
    return KvCasFencedRequest.create(
      this.backend,
      this.getCapabilities,
      this.namespace,
      validatedKey(key),
      fenceNamespace,
      validatedKey(fenceKey),
      fenceToken
    )
  }

  async delete(key: Uint8Array): Promise<boolean> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvDeleteCommand, {
      namespace: this.namespace,
      key: validatedKey(key)
    })
    if (outcome.kind === "deleted") return outcome.removed
    throw unexpected("delete", outcome)
  }

  async exists(key: Uint8Array): Promise<KvMetadata | undefined> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvExistsCommand, {
      namespace: this.namespace,
      key: validatedKey(key)
    })
    if (outcome.kind === "metadata") return outcome.metadata
    throw unexpected("exists", outcome)
  }

  /** Expires an entry `ttlMicros` from now without rewriting its value, or
   * clears its expiry when `ttlMicros` is omitted. Returns the new version.
   * Pass `nowMicros` for deterministic tests. */
  async expire(
    key: Uint8Array,
    ttlMicros?: bigint,
    nowMicros: bigint = BigInt(Date.now()) * 1000n
  ): Promise<bigint> {
    if (ttlMicros !== undefined && ttlMicros < 0n) {
      throw new InvalidError("expire ttlMicros must be non-negative")
    }
    return this.expireAt(
      key,
      ttlMicros === undefined ? undefined : saturatingAdd(nowMicros, ttlMicros)
    )
  }

  /** Sets an entry's absolute expiry (epoch microseconds) without rewriting
   * its value, or clears it when `expiresAtMicros` is omitted. Returns the new
   * version. */
  async expireAt(key: Uint8Array, expiresAtMicros?: bigint): Promise<bigint> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvExpireCommand, {
      namespace: this.namespace,
      key: validatedKey(key),
      ...(expiresAtMicros !== undefined ? { expiresAtMicros } : {})
    })
    if (outcome.kind === "versioned") return outcome.version
    throw unexpected("expire", outcome)
  }

  /** Applies a codec-specific merge patch and returns the new version. */
  async patch(key: Uint8Array, patch: Uint8Array): Promise<bigint> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvPatchCommand, {
      namespace: this.namespace,
      key: validatedKey(key),
      patch
    })
    if (outcome.kind === "versioned") return outcome.version
    throw unexpected("patch", outcome)
  }

  /** Acquires a revocable lease as `holderId` (a stable node or worker id). A
   * live lease always conflicts: extend with `renewLease`, never by
   * re-acquiring. `leaseTtlMicros` is a requested maximum within
   * `MIN_LEASE_TTL_MICROS`..=`MAX_LEASE_TTL_MICROS`. The store may grant less and
   * never more, so anything outside that range throws `InvalidError` before the
   * round trip, and a holder needing longer renews. Needs the `kvFencedLeases`
   * capability. Acquisitions are serialized per client over a dedicated
   * connection, each attempt bounded by `DEFAULT_ATTEMPT_TIMEOUT_MS`. An
   * ambiguous outcome waits through the requested TTL, then throws
   * `AmbiguousMutationError`. */
  async lease(key: Uint8Array, holderId: string, leaseTtlMicros: bigint): Promise<Lease> {
    const request = { namespace: this.namespace, key: validatedKey(key), leaseTtlMicros, holderId }
    requireCapability(await this.getCapabilities(), "kvFencedLeases")
    return (this.leases ?? NO_LEASE_COORDINATOR).acquire(request)
  }

  /** Extends a held lease without changing its token. `leaseTtlMicros` obeys the
   * same range as `lease`. An expired, released, or re-acquired lease fails with
   * the typed lease-lost error: stop the protected work, a new epoch needs a
   * fresh `lease`. */
  async renewLease(
    key: Uint8Array,
    holderId: string,
    token: bigint,
    leaseTtlMicros: bigint
  ): Promise<Lease> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.namespace,
      KvLeaseRenewCommand,
      {
        namespace: this.namespace,
        key: validatedKey(key),
        holderId,
        leaseToken: token,
        leaseTtlMicros
      }
    )
    if (outcome.kind === "renewed") {
      return {
        token: outcome.leaseToken,
        grantedTtlMicros: outcome.grantedTtlMicros,
        position: outcome.position
      }
    }
    throw unexpected("renewLease", outcome)
  }

  /** Releases a held lease early, presenting the same `holderId` and its
   * token. Resolves `true` when a held lease was released. */
  async release(key: Uint8Array, holderId: string, token: bigint): Promise<boolean> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvReleaseCommand, {
      namespace: this.namespace,
      key: validatedKey(key),
      leaseToken: token,
      holderId
    })
    if (outcome.kind === "released") return outcome.wasHeld
    throw unexpected("release", outcome)
  }

  /** Starts an atomic value copy that preserves remaining expiry. */
  copyTo(key: Uint8Array, toKey: Uint8Array): KvCopyRequest {
    return KvCopyRequest.create(
      this.backend,
      this.getCapabilities,
      this.namespace,
      validatedKey(key),
      validatedKey(toKey),
      false
    )
  }

  moveTo(key: Uint8Array, toKey: Uint8Array): KvCopyRequest {
    return KvCopyRequest.create(
      this.backend,
      this.getCapabilities,
      this.namespace,
      validatedKey(key),
      validatedKey(toKey),
      true
    )
  }

  /** Reads several keys in one round trip while preserving key order. */
  async getMany(keys: readonly Uint8Array[]): Promise<readonly (Uint8Array | undefined)[]> {
    const capabilities = await this.getCapabilities()
    const ops: BatchItem[] = keys.map((key) => ({
      code: KvGetCommand.code,
      payload: KvGetCommand.encode({ namespace: this.namespace, key: validatedKey(key) })
    }))
    const results = await executeBatch(this.backend, capabilities, ops)
    return results.map((slot) => {
      const reply = decodeManagedReply((bytes) => KvGetCommand.decode(bytes), slot)
      if (reply.kind === "ok" && reply.outcome.kind === "value") return reply.outcome.entry?.value
      if (reply.kind === "ok") throw unexpected("get", reply.outcome)
      if (reply.kind === "err") {
        throw new KvExecutionError(`kv command failed: ${reply.error.kind}`, reply.error)
      }
      throw new ProtocolError(`kv: unrecognized reply variant in a batch slot \`${reply.tag}\``)
    })
  }

  /** Starts a filtered bulk delete. */
  deleteMany(): KvDeleteManyRequest {
    return KvDeleteManyRequest.create(this.backend, this.getCapabilities, this.namespace)
  }

  scan(): KvScanRequest {
    return KvScanRequest.create(this.backend, this.getCapabilities, this.namespace)
  }
}

/** Builds a value write or compare-and-swap. */
export class KvSetRequest {
  private value: Uint8Array = new Uint8Array()
  private expiresAtMicros: bigint | undefined
  private expect: CasExpect | undefined

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string,
    private readonly key: Uint8Array
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string,
    key: Uint8Array
  ): KvSetRequest {
    return new KvSetRequest(backend, getCapabilities, namespace, key)
  }

  bytes(payload: Uint8Array): this {
    this.value = payload
    return this
  }

  encodeWith<T>(codec: Codec<T>, value: T): this {
    this.value = codec.encode(value)
    return this
  }

  json(value: unknown): this {
    return this.encodeWith(new Json(), value)
  }

  msgpack(value: unknown): this {
    return this.encodeWith(new Msgpack(), value)
  }

  expiresAt(epochMicros: bigint): this {
    this.expiresAtMicros = epochMicros
    return this
  }

  /** Expires the entry after `ttlMicros`. Pass `nowMicros` for deterministic tests. */
  ttl(ttlMicros: bigint, nowMicros: bigint = BigInt(Date.now()) * 1000n): this {
    return this.expiresAt(saturatingAdd(nowMicros, ttlMicros))
  }

  expectVersion(version: bigint): this {
    this.expect = { kind: "match", version }
    return this
  }

  expectAbsent(): this {
    this.expect = { kind: "absent" }
    return this
  }

  /** Writes the value unconditionally. A precondition belongs to `commit()`,
   * so a request with `expectVersion(..)` or `expectAbsent()` set is refused
   * rather than silently written without it. */
  async send(): Promise<void> {
    if (this.expect !== undefined) {
      throw new InvalidError("a precondition was set: call commit() instead of send()")
    }
    validatedValue(this.value)
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvSetCommand, {
      namespace: this.namespace,
      key: this.key,
      value: this.value,
      ...(this.expiresAtMicros !== undefined ? { expiresAtMicros: this.expiresAtMicros } : {})
    })
    if (outcome.kind === "written") return
    throw unexpected("set", outcome)
  }

  /** Commits the configured compare-and-swap and returns its new version. */
  async commit(): Promise<bigint> {
    if (this.expect === undefined) {
      throw new InvalidError(
        "commit() needs a precondition: call expectVersion(..) or expectAbsent()"
      )
    }
    const capabilities = await this.getCapabilities()
    if (!capabilities.kv.cas) {
      throw new UnsupportedError("compare-and-swap is not advertised by this deployment")
    }
    validatedValue(this.value)
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvCasCommand, {
      namespace: this.namespace,
      key: this.key,
      value: this.value,
      expect: this.expect,
      ...(this.expiresAtMicros !== undefined ? { expiresAtMicros: this.expiresAtMicros } : {})
    })
    if (outcome.kind === "committed") return outcome.version
    throw unexpected("cas", outcome)
  }
}

/** Builds a compare-and-swap protected by a lease fencing token. */
export class KvCasFencedRequest {
  private value: Uint8Array = new Uint8Array()
  private expiresAtMicros: bigint | undefined
  private expect: CasExpect | undefined

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string,
    private readonly key: Uint8Array,
    private readonly fenceNamespace: string,
    private readonly fenceKey: Uint8Array,
    private readonly fenceToken: bigint
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string,
    key: Uint8Array,
    fenceNamespace: string,
    fenceKey: Uint8Array,
    fenceToken: bigint
  ): KvCasFencedRequest {
    return new KvCasFencedRequest(
      backend,
      getCapabilities,
      namespace,
      key,
      fenceNamespace,
      fenceKey,
      fenceToken
    )
  }

  bytes(payload: Uint8Array): this {
    this.value = payload
    return this
  }

  encodeWith<T>(codec: Codec<T>, value: T): this {
    this.value = codec.encode(value)
    return this
  }

  json(value: unknown): this {
    return this.encodeWith(new Json(), value)
  }

  msgpack(value: unknown): this {
    return this.encodeWith(new Msgpack(), value)
  }

  expiresAt(epochMicros: bigint): this {
    this.expiresAtMicros = epochMicros
    return this
  }

  /** Expires the entry after `ttlMicros`. Pass `nowMicros` for deterministic tests. */
  ttl(ttlMicros: bigint, nowMicros: bigint = BigInt(Date.now()) * 1000n): this {
    return this.expiresAt(saturatingAdd(nowMicros, ttlMicros))
  }

  expectVersion(version: bigint): this {
    this.expect = { kind: "match", version }
    return this
  }

  expectAbsent(): this {
    this.expect = { kind: "absent" }
    return this
  }

  /** Commits the fenced compare-and-swap and returns its new version. */
  async commit(): Promise<bigint> {
    if (this.expect === undefined) {
      throw new InvalidError(
        "commit() needs a precondition: call expectVersion(..) or expectAbsent()"
      )
    }
    const capabilities = await this.getCapabilities()
    if (!capabilities.kv.fencedLeases) {
      throw new UnsupportedError("the fenced-lease contract is not advertised by this deployment")
    }
    validatedValue(this.value)
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.namespace,
      KvCasFencedCommand,
      {
        namespace: this.namespace,
        key: this.key,
        value: this.value,
        expect: this.expect,
        fenceNamespace: this.fenceNamespace,
        fenceKey: this.fenceKey,
        fenceToken: this.fenceToken,
        ...(this.expiresAtMicros !== undefined ? { expiresAtMicros: this.expiresAtMicros } : {})
      }
    )
    if (outcome.kind === "committed") return outcome.version
    throw unexpected("cas_fenced", outcome)
  }
}

/** Builds a paged namespace scan. */
export class KvScanRequest {
  private boundPrefix: Uint8Array | undefined
  private boundStart: Uint8Array | undefined
  private boundEnd: Uint8Array | undefined
  private boundKeyContains: string | undefined
  private boundConversation: string | undefined
  private pageLimit = DEFAULT_SCAN_LIMIT
  private pageCursor: Uint8Array | undefined

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string
  ): KvScanRequest {
    return new KvScanRequest(backend, getCapabilities, namespace)
  }

  /** Restricts a memory namespace scan to one conversation. */
  conversation(conversationId: string): this {
    this.boundConversation = conversationId
    return this
  }

  prefix(prefix: Uint8Array): this {
    this.boundPrefix = prefix
    return this
  }

  range(start: Uint8Array, end: Uint8Array): this {
    this.boundStart = start
    this.boundEnd = end
    return this
  }

  keyContains(substring: string): this {
    this.boundKeyContains = substring
    return this
  }

  /** Caps the page at `n` entries, clamped to `MAX_SCAN_LIMIT`. */
  limit(n: number): this {
    if (!Number.isSafeInteger(n) || n < 0) {
      throw new InvalidError("scan limit must be a non-negative integer")
    }
    this.pageLimit = Math.min(n, MAX_SCAN_LIMIT)
    return this
  }

  cursor(cursor: Uint8Array): this {
    this.pageCursor = cursor
    return this
  }

  async fetch(): Promise<KvPage> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.namespace,
      KvScanCommand,
      this.request()
    )
    if (outcome.kind === "page") return outcome.page
    throw unexpected("scan", outcome)
  }

  /** Collects every matching entry across pages. */
  async entries(): Promise<readonly KvEntry[]> {
    const capabilities = await this.getCapabilities()
    const out: KvEntry[] = []
    let request = this.request()
    for (;;) {
      const outcome = await executeKv(
        this.backend,
        capabilities,
        this.namespace,
        KvScanCommand,
        request
      )
      if (outcome.kind !== "page") throw unexpected("scan", outcome)
      out.push(...outcome.page.entries)
      if (outcome.page.cursor === undefined) return out
      request = { ...request, cursor: outcome.page.cursor }
    }
  }

  private request(): {
    readonly namespace: string
    readonly prefix?: Uint8Array
    readonly start?: Uint8Array
    readonly end?: Uint8Array
    readonly keyContains?: string
    readonly conversation?: string
    readonly limit: number
    readonly cursor?: Uint8Array
  } {
    return {
      namespace: this.namespace,
      ...(this.boundPrefix !== undefined ? { prefix: this.boundPrefix } : {}),
      ...(this.boundStart !== undefined ? { start: this.boundStart } : {}),
      ...(this.boundEnd !== undefined ? { end: this.boundEnd } : {}),
      ...(this.boundKeyContains !== undefined ? { keyContains: this.boundKeyContains } : {}),
      ...(this.boundConversation !== undefined ? { conversation: this.boundConversation } : {}),
      limit: this.pageLimit,
      ...(this.pageCursor !== undefined ? { cursor: this.pageCursor } : {})
    }
  }
}

/** Builds a filtered bulk delete. */
export class KvDeleteManyRequest {
  private boundPrefix: Uint8Array | undefined
  private boundStart: Uint8Array | undefined
  private boundEnd: Uint8Array | undefined
  private boundKeyContains: string | undefined
  private boundConversation: string | undefined

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string
  ): KvDeleteManyRequest {
    return new KvDeleteManyRequest(backend, getCapabilities, namespace)
  }

  conversation(conversationId: string): this {
    this.boundConversation = conversationId
    return this
  }

  prefix(prefix: Uint8Array): this {
    this.boundPrefix = prefix
    return this
  }

  range(start: Uint8Array, end: Uint8Array): this {
    this.boundStart = start
    this.boundEnd = end
    return this
  }

  keyContains(substring: string): this {
    this.boundKeyContains = substring
    return this
  }

  async send(): Promise<number> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.namespace,
      KvDeleteManyCommand,
      {
        namespace: this.namespace,
        ...(this.boundPrefix !== undefined ? { prefix: this.boundPrefix } : {}),
        ...(this.boundStart !== undefined ? { start: this.boundStart } : {}),
        ...(this.boundEnd !== undefined ? { end: this.boundEnd } : {}),
        ...(this.boundKeyContains !== undefined ? { keyContains: this.boundKeyContains } : {}),
        ...(this.boundConversation !== undefined ? { conversation: this.boundConversation } : {})
      }
    )
    if (outcome.kind === "deletedMany") return outcome.count
    throw unexpected("delete_many", outcome)
  }
}

/** Builds an atomic value copy or move. */
export class KvCopyRequest {
  private toNamespaceOverride: string | undefined

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string,
    private readonly key: Uint8Array,
    private readonly toKey: Uint8Array,
    private readonly deleteSource: boolean
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string,
    key: Uint8Array,
    toKey: Uint8Array,
    deleteSource: boolean
  ): KvCopyRequest {
    return new KvCopyRequest(backend, getCapabilities, namespace, key, toKey, deleteSource)
  }

  intoNamespace(namespace: string): this {
    this.toNamespaceOverride = namespace
    return this
  }

  async send(): Promise<bigint> {
    if (this.toNamespaceOverride !== undefined) validateNamespace(this.toNamespaceOverride)
    const capabilities = await this.getCapabilities()
    const command = this.deleteSource ? KvMoveCommand : KvCopyCommand
    const outcome = await executeKv(this.backend, capabilities, this.namespace, command, {
      namespace: this.namespace,
      key: this.key,
      toKey: this.toKey,
      ...(this.toNamespaceOverride !== undefined ? { toNamespace: this.toNamespaceOverride } : {})
    })
    if (outcome.kind === "committed") return outcome.version
    throw unexpected(this.deleteSource ? "move" : "copy", outcome)
  }
}
