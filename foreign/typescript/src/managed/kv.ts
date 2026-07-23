import type { Capabilities } from "../client/capabilities.js"
import {
  InvalidError,
  KvExecutionError,
  ProtocolError,
  UnsupportedError
} from "../client/errors.js"
import { executeManaged, type ManagedTransport } from "../client/managed.js"
import { executeBatch } from "./batch.js"
import type { Codec } from "../stream/codecs.js"
import { jsonCodec } from "../stream/codecs.js"
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
  KvLeaseCommand,
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
import { DEFAULT_SCAN_LIMIT, MAX_KEY_BYTES, MAX_VALUE_BYTES } from "../wire/limits.js"

/** A granted advisory lease with its fencing token and effective TTL. */
export interface Lease {
  readonly token: bigint
  readonly grantedTtlMicros: bigint
}

export type KvBackend = ManagedTransport

async function executeKv<Request>(
  backend: KvBackend,
  capabilities: Capabilities,
  namespace: string | undefined,
  command: ManagedCommand<Request, KvReply>,
  request: Request
): Promise<KvOutcome> {
  if (namespace !== undefined) validateNamespace(namespace)
  const reply = await executeManaged(backend, capabilities, command, request)
  if (reply.kind === "ok") return reply.outcome
  if (reply.kind === "err") {
    if (reply.error.kind === "unsupported") throw new UnsupportedError(reply.error.message)
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
  backend: KvBackend,
  capabilities: Capabilities
): Promise<readonly KvNamespaceInfo[]> {
  const outcome = await executeKv(backend, capabilities, undefined, KvNamespacesCommand, undefined)
  if (outcome.kind === "namespaces") return outcome.namespaces
  throw unexpected("namespaces", outcome)
}

/** A namespace-scoped managed key-value view. */
export class Kv {
  constructor(
    private readonly backend: KvBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    readonly namespace: string
  ) {}

  static async namespaces(
    backend: KvBackend,
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

  /** Decodes a value with the supplied codec, or returns undefined. */
  async getAs<T>(key: Uint8Array, codec: Codec<T>): Promise<T | undefined> {
    const value = await this.get(key)
    return value === undefined ? undefined : codec.decode(value)
  }

  async getTyped<T>(key: Uint8Array, decodeValue: (value: unknown) => T): Promise<T | undefined> {
    return this.getAs(key, jsonCodec(decodeValue))
  }

  /** Starts a value write or compare-and-swap request. */
  set(key: Uint8Array): KvSetRequest {
    return new KvSetRequest(this.backend, this.getCapabilities, this.namespace, validatedKey(key))
  }

  /** Starts a compare-and-swap protected by a lease fencing token. */
  casFenced(key: Uint8Array, fenceKey: Uint8Array, fenceToken: bigint): KvCasFencedRequest {
    return new KvCasFencedRequest(
      this.backend,
      this.getCapabilities,
      this.namespace,
      validatedKey(key),
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

  /** Changes an entry expiry without rewriting its value. */
  async expire(key: Uint8Array, expiresAtMicros?: bigint): Promise<bigint> {
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

  async lease(key: Uint8Array, leaseTtlMicros: bigint): Promise<Lease> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvLeaseCommand, {
      namespace: this.namespace,
      key: validatedKey(key),
      leaseTtlMicros
    })
    if (outcome.kind === "leased") {
      return { token: outcome.leaseToken, grantedTtlMicros: outcome.grantedTtlMicros }
    }
    throw unexpected("lease", outcome)
  }

  async release(key: Uint8Array, token: bigint): Promise<boolean> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvReleaseCommand, {
      namespace: this.namespace,
      key: validatedKey(key),
      leaseToken: token
    })
    if (outcome.kind === "released") return outcome.wasHeld
    throw unexpected("release", outcome)
  }

  /** Starts an atomic value copy that preserves remaining expiry. */
  copyTo(key: Uint8Array, toKey: Uint8Array): KvCopyRequest {
    return new KvCopyRequest(
      this.backend,
      this.getCapabilities,
      this.namespace,
      validatedKey(key),
      validatedKey(toKey),
      false
    )
  }

  moveTo(key: Uint8Array, toKey: Uint8Array): KvCopyRequest {
    return new KvCopyRequest(
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
      const reply = KvGetCommand.decode(slot)
      if (reply.kind === "ok" && reply.outcome.kind === "value") return reply.outcome.entry?.value
      if (reply.kind === "ok") throw unexpected("get", reply.outcome)
      if (reply.kind === "err") {
        if (reply.error.kind === "unsupported") throw new UnsupportedError(reply.error.message)
        throw new KvExecutionError(`kv command failed: ${reply.error.kind}`, reply.error)
      }
      throw new ProtocolError(`kv: unrecognized reply variant in a batch slot \`${reply.tag}\``)
    })
  }

  /** Starts a filtered bulk delete. */
  deleteMany(): KvDeleteManyRequest {
    return new KvDeleteManyRequest(this.backend, this.getCapabilities, this.namespace)
  }

  scan(): KvScanRequest {
    return new KvScanRequest(this.backend, this.getCapabilities, this.namespace)
  }
}

/** Builds a value write or compare-and-swap. */
export class KvSetRequest {
  private value: Uint8Array = new Uint8Array()
  private expiresAtMicros: bigint | undefined
  private expect: CasExpect | undefined

  constructor(
    private readonly backend: KvBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string,
    private readonly key: Uint8Array
  ) {}

  bytes(payload: Uint8Array): this {
    this.value = payload
    return this
  }

  encodeWith<T>(codec: Codec<T>, value: T): this {
    this.value = codec.encode(value)
    return this
  }

  json(value: unknown): this {
    return this.encodeWith(
      jsonCodec((decoded) => decoded),
      value
    )
  }

  expiresAt(epochMicros: bigint): this {
    this.expiresAtMicros = epochMicros
    return this
  }

  /** Expires the entry after `ttlMicros`. Pass `nowMicros` for deterministic tests. */
  ttl(ttlMicros: bigint, nowMicros: bigint = BigInt(Date.now()) * 1000n): this {
    return this.expiresAt(nowMicros + ttlMicros)
  }

  expectVersion(version: bigint): this {
    this.expect = { kind: "match", version }
    return this
  }

  expectAbsent(): this {
    this.expect = { kind: "absent" }
    return this
  }

  async send(): Promise<void> {
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

  constructor(
    private readonly backend: KvBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string,
    private readonly key: Uint8Array,
    private readonly fenceKey: Uint8Array,
    private readonly fenceToken: bigint
  ) {}

  bytes(payload: Uint8Array): this {
    this.value = payload
    return this
  }

  encodeWith<T>(codec: Codec<T>, value: T): this {
    this.value = codec.encode(value)
    return this
  }

  json(value: unknown): this {
    return this.encodeWith(
      jsonCodec((decoded) => decoded),
      value
    )
  }

  expiresAt(epochMicros: bigint): this {
    this.expiresAtMicros = epochMicros
    return this
  }

  /** Expires the entry after `ttlMicros`. Pass `nowMicros` for deterministic tests. */
  ttl(ttlMicros: bigint, nowMicros: bigint = BigInt(Date.now()) * 1000n): this {
    return this.expiresAt(nowMicros + ttlMicros)
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
    if (!capabilities.kv.casFenced) {
      throw new UnsupportedError("fenced compare-and-swap is not advertised by this deployment")
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

  constructor(
    private readonly backend: KvBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string
  ) {}

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

  limit(n: number): this {
    this.pageLimit = n
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

  constructor(
    private readonly backend: KvBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string
  ) {}

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

  constructor(
    private readonly backend: KvBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string,
    private readonly key: Uint8Array,
    private readonly toKey: Uint8Array,
    private readonly deleteSource: boolean
  ) {}

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
