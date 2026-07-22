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

// A granted advisory lease: the fencing token to present on protected
// mutations, and the TTL the store actually granted (which may be shorter
// than requested).
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

// Reject an empty or over-cap key before the round trip, returning it
// unchanged. Keys are arbitrary bytes.
function validatedKey(key: Uint8Array): Uint8Array {
  if (key.byteLength === 0) throw new InvalidError("key must not be empty")
  if (key.byteLength > MAX_KEY_BYTES) {
    throw new InvalidError(
      `key is ${String(key.byteLength)}B, exceeds cap ${String(MAX_KEY_BYTES)}B`
    )
  }
  return key
}

// Reject an over-cap value before the round trip. Shared by `set` and the
// compare-and-swap `commit` so the cap and message cannot drift between them.
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

// A namespace-scoped view of the managed key-value store. Build it with
// `Laser.kv(namespace)`. Keys are unique within a namespace, scans are
// scoped to it.
export class Kv {
  constructor(
    private readonly backend: KvBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    readonly namespace: string
  ) {}

  static namespaces(
    backend: KvBackend,
    getCapabilities: () => Promise<Capabilities>
  ): Promise<readonly KvNamespaceInfo[]> {
    return getCapabilities().then((capabilities) => fetchNamespaces(backend, capabilities))
  }

  // Fetch the raw value bytes stored at `key`, or `undefined` if absent or
  // expired.
  async get(key: Uint8Array): Promise<Uint8Array | undefined> {
    const entry = await this.getEntry(key)
    return entry?.value
  }

  // Fetch the full `KvEntry` at `key` (key, value, expiry, version), or
  // `undefined`.
  async getEntry(key: Uint8Array): Promise<KvEntry | undefined> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvGetCommand, {
      namespace: this.namespace,
      key: validatedKey(key)
    })
    if (outcome.kind === "value") return outcome.entry
    throw unexpected("get", outcome)
  }

  // Fetch and decode the value at `key` with any `Codec`, or `undefined` if
  // absent. The store keeps values as opaque bytes, so the codec is the
  // caller's choice, reads are no more JSON-locked than writes.
  async getAs<T>(key: Uint8Array, codec: Codec<T>): Promise<T | undefined> {
    const value = await this.get(key)
    return value === undefined ? undefined : codec.decode(value)
  }

  // Fetch and JSON-decode the value at `key` into `T`, or `undefined`.
  async getTyped<T>(key: Uint8Array, decodeValue: (value: unknown) => T): Promise<T | undefined> {
    return this.getAs(key, jsonCodec(decodeValue))
  }

  // Start a `set`. Finish with `.send()` after supplying a value (`.bytes` /
  // `.json` / `.encodeWith`) and an optional `.ttl` / `.expiresAt`, or make it
  // a compare-and-swap with `.expectVersion` / `.expectAbsent` then `.commit()`.
  set(key: Uint8Array): KvSetRequest {
    return new KvSetRequest(this.backend, this.getCapabilities, this.namespace, validatedKey(key))
  }

  // Start a fenced compare-and-swap on `key`: the write applies only while
  // the caller's fence sequence still equals `fenceToken` (the value a
  // `lease` returned). Supply a value and a precondition, then `.commit()`.
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

  // Delete `key`. Returns `true` when a live entry was removed, `false` when
  // none existed.
  async delete(key: Uint8Array): Promise<boolean> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvDeleteCommand, {
      namespace: this.namespace,
      key: validatedKey(key)
    })
    if (outcome.kind === "deleted") return outcome.removed
    throw unexpected("delete", outcome)
  }

  // Test presence and read metadata (version, expiry, value size) without
  // transferring the value.
  async exists(key: Uint8Array): Promise<KvMetadata | undefined> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeKv(this.backend, capabilities, this.namespace, KvExistsCommand, {
      namespace: this.namespace,
      key: validatedKey(key)
    })
    if (outcome.kind === "metadata") return outcome.metadata
    throw unexpected("exists", outcome)
  }

  // Set, refresh, or clear an entry's expiry in place without rewriting its
  // value. `expiresAtMicros` absent clears the expiry. Returns the entry's
  // (unchanged) version.
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

  // Apply a merge `patch` to a structured value without transferring the
  // whole object, returning the new version. The patch bytes are
  // codec-specific (a JSON merge patch over a JSON value, for instance).
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

  // Acquire an advisory lease (a bounded-TTL distributed lock) on `key`.
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

  // Release a held lease early, presenting the `token` the grant returned.
  // Returns `true` when a held lease was released.
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

  // Copy the value at `key` to `toKey` in one backend transaction: chain
  // `.intoNamespace()` for a cross-namespace copy, then `.send()` for the
  // destination's new version. The destination is overwritten, and the
  // value moves with its remaining expiry.
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

  // Move the value at `key` to `toKey`: `copyTo` plus the source delete, one
  // backend transaction.
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

  // Point-read several keys in ONE round trip, riding the mixed-operation
  // batch (`executeBatch`): one value-or-`undefined` per key, in key order.
  // The common multi-get amortization for agent hot loops.
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

  // Start a filtered bulk delete over this namespace. Narrow with
  // `.prefix` / `.range` / `.keyContains`, then `.send()` for the count
  // removed. With no bounds it clears the whole namespace.
  deleteMany(): KvDeleteManyRequest {
    return new KvDeleteManyRequest(this.backend, this.getCapabilities, this.namespace)
  }

  // Start a `scan` over this namespace. Finish with `.fetch()` for one page,
  // or `.entries()` to walk every match across pages.
  scan(): KvScanRequest {
    return new KvScanRequest(this.backend, this.getCapabilities, this.namespace)
  }
}

// Fluent builder for `Kv.set`. Supply a value with one of `.bytes` / `.json`
// / `.encodeWith`, optionally `.ttl` / `.expiresAt`, then `.send()`. Make it
// a compare-and-swap with `.expectVersion` / `.expectAbsent`, then `.commit()`.
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

  // Store raw `bytes` as the value. Use this for an already-encoded body.
  bytes(payload: Uint8Array): this {
    this.value = payload
    return this
  }

  // Encode `value` with any `Codec` and store the bytes.
  encodeWith<T>(codec: Codec<T>, value: T): this {
    this.value = codec.encode(value)
    return this
  }

  // JSON-encode `value` and store the bytes.
  json(value: unknown): this {
    return this.encodeWith(
      jsonCodec((decoded) => decoded),
      value
    )
  }

  // Expire the entry at an absolute epoch-microseconds timestamp.
  expiresAt(epochMicros: bigint): this {
    this.expiresAtMicros = epochMicros
    return this
  }

  // Expire the entry `ttlMicros` from now.
  ttl(ttlMicros: bigint): this {
    return this.expiresAt(BigInt(Date.now()) * 1000n + ttlMicros)
  }

  // Make this a compare-and-swap that applies only if the key currently
  // holds `version`. Finish with `.commit()`.
  expectVersion(version: bigint): this {
    this.expect = { kind: "match", version }
    return this
  }

  // Make this a compare-and-swap that applies only if the key does not yet
  // exist (a create-if-absent). Finish with `.commit()`.
  expectAbsent(): this {
    this.expect = { kind: "absent" }
    return this
  }

  // Apply the write unconditionally.
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

  // Apply a compare-and-swap set up by `.expectVersion` / `.expectAbsent`,
  // returning the entry's new version. Throws `InvalidError` if no
  // precondition was set (use `.send()` for an unconditional write), and
  // `UnsupportedError` when the deployment does not advertise `kv.cas`.
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

// Fluent builder for `Kv.casFenced`. Supply a value, a precondition, an
// optional expiry, then `.commit()`. The write lands only while the caller's
// fence sequence still equals the held token.
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

  ttl(ttlMicros: bigint): this {
    return this.expiresAt(BigInt(Date.now()) * 1000n + ttlMicros)
  }

  expectVersion(version: bigint): this {
    this.expect = { kind: "match", version }
    return this
  }

  expectAbsent(): this {
    this.expect = { kind: "absent" }
    return this
  }

  // Apply the fenced compare-and-swap, returning the entry's new version.
  // Throws `InvalidError` if no precondition was set, and `UnsupportedError`
  // when the deployment does not advertise `kv.casFenced`.
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

// Fluent builder for `Kv.scan`. Narrow with `.prefix` / `.range` /
// `.keyContains` / `.conversation`, cap with `.limit`, then `.fetch()` for
// one page or `.entries()` for everything.
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

  // The memory read view stamps this on every record it materializes, so a
  // scan of a memory namespace narrows to one conversation's memory. Generic
  // key-value entries carry no conversation, so this filters them out.
  conversation(conversationId: string): this {
    this.boundConversation = conversationId
    return this
  }

  // Only keys starting with `prefix` (byte order).
  prefix(prefix: Uint8Array): this {
    this.boundPrefix = prefix
    return this
  }

  // Only keys in `[start, end)` (inclusive start, exclusive end, byte order).
  range(start: Uint8Array, end: Uint8Array): this {
    this.boundStart = start
    this.boundEnd = end
    return this
  }

  // Keep only keys that are valid UTF-8 and contain `substring`. Binary keys
  // are skipped. Composes with `.prefix` / `.range`.
  keyContains(substring: string): this {
    this.boundKeyContains = substring
    return this
  }

  // Cap the page at `n` entries (clamped server-side to `MAX_SCAN_LIMIT`).
  limit(n: number): this {
    this.pageLimit = n
    return this
  }

  // Resume after a previous page's cursor.
  cursor(cursor: Uint8Array): this {
    this.pageCursor = cursor
    return this
  }

  // Fetch one page (entries plus the cursor to continue).
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

  // Walk every matching entry across pages, following the cursor until the
  // scan is exhausted. Convenient when the working set fits in memory.
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

// Fluent builder for `Kv.deleteMany`. Narrow with `.prefix` / `.range` /
// `.keyContains` / `.conversation`, then `.send()` for the count removed.
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

  // The conversation lens over a bulk delete: clear only the entries the
  // given conversation wrote.
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

  // Apply the bulk delete. Returns the number of entries removed.
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

// One copy or move, built by `Kv.copyTo` / `Kv.moveTo`, finished with
// `.send()`. An absent or expired source is the typed `KvError.notFound`.
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

  // Send the copy into another namespace instead of the source's.
  intoNamespace(namespace: string): this {
    this.toNamespaceOverride = namespace
    return this
  }

  // Apply the copy (or move) and return the destination's new version.
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
