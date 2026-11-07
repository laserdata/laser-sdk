import { microsToMillis, millis, millisToMicros } from "../client/duration.js"
import { BARE_SCOPE, type ResourceScope } from "../client/resource-scope.js"
import { type Capabilities, requireCapability } from "../client/capabilities.js"
import { type CallOptions, cancellable } from "../client/call-options.js"
import {
  InvalidError,
  KvExecutionError,
  ProtocolError,
  UnsupportedError
} from "../client/errors.js"
import { isLeaseLost } from "../client/error-classify.js"
import { decodeManagedReply, executeManaged, type ManagedTransport } from "../client/managed.js"
import type { LaserTransport } from "../iggy/apache-iggy.js"
import { executeBatch } from "./batch.js"
import { LeaseCoordinator } from "./coordination.js"
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
import type { SessionRef } from "../wire/ids.js"

/** A granted revocable lease: the fencing token, the TTL the store granted
 * in milliseconds, the mutation position at which the answering fold applied the grant or
 * renewal (the barrier a takeover read passes to `getEntryAtLeast`), and the
 * `performance.now()` instant the lease stops being valid for this process. */
export interface Lease {
  readonly token: bigint
  readonly grantedTtlMs: number
  readonly position: MutationPosition
  /** The first send of the grant or renewal plus the granted TTL, on the
   * `performance.now()` clock. The store starts the TTL no earlier than it
   * received the request, so work that stops before this instant never
   * outlives the lease, however late the reply arrived. */
  readonly validUntilMs: number
}

/** The answer to a conditional read: the live version still equals the one
 * the caller holds, so no value was sent, or the entry as it stands now,
 * `undefined` when absent or expired. */
export type KvRevalidation =
  { readonly kind: "notModified" } | { readonly kind: "changed"; readonly entry?: KvEntry }

/** A lease that releases itself: `await using guard = await kv.leaseGuard(..)`
 * frees the key when the scope ends, however it ends, instead of after its
 * TTL. Renew it with `renew` before `lease.validUntilMs`, and end it with
 * `release` to learn whether it was still held. A guard whose renewal reported
 * the lease lost releases nothing, so it never ends a successor's lease. */
export class LeaseGuard implements AsyncDisposable {
  private held = true

  private constructor(
    private readonly store: Pick<Kv, "renewLease" | "release">,
    private readonly key: Uint8Array,
    private readonly holderId: string,
    private current: Lease
  ) {}

  /** @internal */
  static create(
    store: Pick<Kv, "renewLease" | "release">,
    key: Uint8Array,
    holderId: string,
    current: Lease
  ): LeaseGuard {
    return new LeaseGuard(store, key, holderId, current)
  }

  /** The current grant: the fencing token to present and its local deadline. */
  get lease(): Lease {
    return this.current
  }

  /** Renews the lease for `ttlMs`, keeping its token. */
  async renew(ttlMs: number, options: CallOptions = {}): Promise<Lease> {
    try {
      this.current = await this.store.renewLease(
        this.key,
        this.holderId,
        this.current.token,
        ttlMs,
        options
      )
      return this.current
    } catch (error) {
      if (isLeaseLost(error)) this.held = false
      throw error
    }
  }

  /** Releases the lease now. Resolves `true` when it was still held. */
  async release(): Promise<boolean> {
    this.held = false
    return this.store.release(this.key, this.holderId, this.current.token)
  }

  async [Symbol.asyncDispose](): Promise<void> {
    if (this.held) await this.release().catch(() => undefined)
  }
}

// The shared connection a store reads and writes through. A lease granted over
// the dedicated coordination connection raises its frontier, and a batch read
// carries it.
type KvTransport = ManagedTransport & Pick<LaserTransport, "observePositions" | "seenPositions">

// A store built without the client's coordinator has no connection to acquire
// a lease on, as for an injected client.
const NO_LEASE_COORDINATOR = new LeaseCoordinator()

/** The lease calls a handle makes over its client's coordination connection.
 * @internal */
export type LeaseCalls = Pick<LeaseCoordinator, "acquire" | "renew">

async function executeKv<Request>(
  backend: ManagedTransport,
  capabilities: Capabilities,
  namespace: string | undefined,
  command: ManagedCommand<Request, KvReply>,
  request: Request,
  options?: { readonly retryAfterReconnect?: boolean } & CallOptions
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

// The namespaces a scoped handle lists: only its own stream's, under the names
// the caller gave them.
function localNamespaces(
  namespaces: readonly KvNamespaceInfo[],
  scope: ResourceScope
): readonly KvNamespaceInfo[] {
  return namespaces.flatMap((info) => {
    const local = scope.local(info.namespace)
    return local === undefined ? [] : [{ ...info, namespace: local }]
  })
}

async function fetchNamespaces(
  backend: ManagedTransport,
  capabilities: Capabilities,
  scope: ResourceScope,
  options: CallOptions
): Promise<readonly KvNamespaceInfo[]> {
  const outcome = await executeKv(
    backend,
    capabilities,
    undefined,
    KvNamespacesCommand,
    scope.stream !== undefined ? { stream: scope.stream } : {},
    options
  )
  if (outcome.kind === "namespaces") return outcome.namespaces
  throw unexpected("namespaces", outcome)
}

/** A namespace-scoped managed key-value view. */
export class Kv {
  private linkedSession: SessionRef | undefined

  private constructor(
    private readonly backend: KvTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    /** The namespace this handle sends, scoped by the connection's
     * `ResourceNaming`. */
    readonly resourceNamespace: string,
    private readonly leases?: LeaseCalls,
    private readonly scope: ResourceScope = BARE_SCOPE
  ) {}

  /** The namespace this handle is bound to, as the caller named it. */
  get namespace(): string {
    return this.scope.local(this.resourceNamespace) ?? this.resourceNamespace
  }

  /** This handle with every set, compare-and-swap, delete, and patch linked
   * to `session`, so the deployment records which session wrote each key. */
  inSession(session: SessionRef): Kv {
    const linked = new Kv(
      this.backend,
      this.getCapabilities,
      this.resourceNamespace,
      this.leases,
      this.scope
    )
    linked.linkedSession = session
    return linked
  }

  /** The session this handle links its writes to. */
  session(): SessionRef | undefined {
    return this.linkedSession
  }

  /** @internal */
  static create(
    backend: KvTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string,
    leases?: LeaseCalls,
    scope: ResourceScope = BARE_SCOPE
  ): Kv {
    return new Kv(backend, getCapabilities, scope.name(namespace), leases, scope)
  }

  /** @internal */
  static async namespaces(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    scope: ResourceScope = BARE_SCOPE,
    options: CallOptions = {}
  ): Promise<readonly KvNamespaceInfo[]> {
    const capabilities = await cancellable(options.signal, "kv namespaces", getCapabilities)
    return localNamespaces(await fetchNamespaces(backend, capabilities, scope, options), scope)
  }

  async get(key: Uint8Array, options: CallOptions = {}): Promise<Uint8Array | undefined> {
    const entry = await this.getEntry(key, options)
    return entry?.value
  }

  async getEntry(key: Uint8Array, options: CallOptions = {}): Promise<KvEntry | undefined> {
    const capabilities = await cancellable(options.signal, "kv get", this.getCapabilities)
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.resourceNamespace,
      KvGetCommand,
      {
        namespace: this.resourceNamespace,
        key: validatedKey(key)
      },
      options
    )
    if (outcome.kind === "value") return outcome.entry
    throw unexpected("get", outcome)
  }

  /** Conditional read: answers `notModified` without the value while `key`
   * still holds `version` (the entry version of a prior read), else the entry
   * as it stands now. An up-to-date cache skips the body transfer. */
  async getEntryIfChanged(
    key: Uint8Array,
    version: bigint,
    options: CallOptions = {}
  ): Promise<KvRevalidation> {
    const capabilities = await cancellable(options.signal, "kv get", this.getCapabilities)
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.resourceNamespace,
      KvGetCommand,
      {
        namespace: this.resourceNamespace,
        key: validatedKey(key),
        ifNoneMatch: version
      },
      options
    )
    if (outcome.kind === "notModified") return { kind: "notModified" }
    if (outcome.kind === "value")
      return outcome.entry === undefined
        ? { kind: "changed" }
        : { kind: "changed", entry: outcome.entry }
    throw unexpected("get", outcome)
  }

  /** Barriered read: resolves only once the answering fold has applied at
   * least `minPosition` (the position a `Lease` grant or renewal returned). A
   * fold that does not catch up fails with the typed `stale` error, never an
   * absent value. Needs the `kvFencedLeases` capability: a pre-fencing server
   * would silently ignore the barrier. */
  async getEntryAtLeast(
    key: Uint8Array,
    minPosition: MutationPosition,
    options: CallOptions = {}
  ): Promise<KvEntry | undefined> {
    const capabilities = await cancellable(options.signal, "kv get", this.getCapabilities)
    if (!capabilities.kv.fencedLeases) {
      throw new UnsupportedError("the fenced-lease contract is not advertised by this deployment")
    }
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.resourceNamespace,
      KvGetCommand,
      {
        namespace: this.resourceNamespace,
        key: validatedKey(key),
        minPosition
      },
      options
    )
    if (outcome.kind === "value") return outcome.entry
    throw unexpected("get", outcome)
  }

  /** Decodes a value with the supplied codec, or returns undefined. */
  async getAs<T>(
    key: Uint8Array,
    codec: Codec<T>,
    options: CallOptions = {}
  ): Promise<T | undefined> {
    const value = await this.get(key, options)
    return value === undefined ? undefined : codec.decode(value)
  }

  async getTyped<T>(
    key: Uint8Array,
    decodeValue: (value: unknown) => T,
    options: CallOptions = {}
  ): Promise<T | undefined> {
    return this.getAs(key, new Json(decodeValue), options)
  }

  /** Starts a value write or compare-and-swap request. */
  set(key: Uint8Array): KvSetRequest {
    return KvSetRequest.create(
      this.backend,
      this.getCapabilities,
      this.resourceNamespace,
      validatedKey(key),
      this.linkedSession
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
      this.resourceNamespace,
      validatedKey(key),
      this.scope.name(fenceNamespace),
      validatedKey(fenceKey),
      fenceToken,
      this.linkedSession
    )
  }

  async delete(key: Uint8Array, options: CallOptions = {}): Promise<boolean> {
    return this.deleteMatching(key, undefined, options)
  }

  /** Deletes `key` only while it holds `version` (the entry version of a prior
   * read), a conditional delete. A live entry at another version throws the
   * typed version-conflict error carrying the current version. */
  async deleteIfVersion(
    key: Uint8Array,
    version: bigint,
    options: CallOptions = {}
  ): Promise<boolean> {
    return this.deleteMatching(key, version, options)
  }

  private async deleteMatching(
    key: Uint8Array,
    ifMatch: bigint | undefined,
    options: CallOptions
  ): Promise<boolean> {
    const capabilities = await cancellable(options.signal, "kv delete", this.getCapabilities)
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.resourceNamespace,
      KvDeleteCommand,
      {
        namespace: this.resourceNamespace,
        ...this.sessionField(),
        key: validatedKey(key),
        ...(ifMatch === undefined ? {} : { ifMatch })
      },
      options
    )
    if (outcome.kind === "deleted") return outcome.removed
    throw unexpected("delete", outcome)
  }

  async exists(key: Uint8Array, options: CallOptions = {}): Promise<KvMetadata | undefined> {
    const capabilities = await cancellable(options.signal, "kv exists", this.getCapabilities)
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.resourceNamespace,
      KvExistsCommand,
      {
        namespace: this.resourceNamespace,
        key: validatedKey(key)
      },
      options
    )
    if (outcome.kind === "metadata") return outcome.metadata
    throw unexpected("exists", outcome)
  }

  /** Expires an entry `ttlMs` milliseconds after the server's time of the
   * write, without rewriting its value, or clears its expiry when `ttlMs` is
   * omitted. Returns the new version. */
  async expire(key: Uint8Array, ttlMs?: number, options: CallOptions = {}): Promise<bigint> {
    if (ttlMs === undefined) return this.expireAt(key, undefined, options)
    return this.expireWith(key, { ttlMicros: durationMicros(ttlMs, "expire ttlMs") }, options)
  }

  /** Sets an entry's absolute expiry (epoch microseconds) without rewriting
   * its value, or clears it when `expiresAtMicros` is omitted. Returns the new
   * version. */
  async expireAt(
    key: Uint8Array,
    expiresAtMicros?: bigint,
    options: CallOptions = {}
  ): Promise<bigint> {
    return this.expireWith(key, expiresAtMicros === undefined ? {} : { expiresAtMicros }, options)
  }

  private async expireWith(
    key: Uint8Array,
    expiry: { readonly expiresAtMicros?: bigint; readonly ttlMicros?: bigint },
    options: CallOptions
  ): Promise<bigint> {
    const capabilities = await cancellable(options.signal, "kv expire", this.getCapabilities)
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.resourceNamespace,
      KvExpireCommand,
      {
        namespace: this.resourceNamespace,
        key: validatedKey(key),
        ...expiry
      },
      options
    )
    if (outcome.kind === "versioned") return outcome.version
    throw unexpected("expire", outcome)
  }

  /** Applies a codec-specific merge patch and returns the new version. */
  async patch(key: Uint8Array, patch: Uint8Array, options: CallOptions = {}): Promise<bigint> {
    return this.patchMatching(key, patch, undefined, options)
  }

  /** `patch` applied only while `key` holds `version`. A live entry at another
   * version throws the typed version-conflict error. */
  async patchIfVersion(
    key: Uint8Array,
    patch: Uint8Array,
    version: bigint,
    options: CallOptions = {}
  ): Promise<bigint> {
    return this.patchMatching(key, patch, version, options)
  }

  private async patchMatching(
    key: Uint8Array,
    patch: Uint8Array,
    ifMatch: bigint | undefined,
    options: CallOptions
  ): Promise<bigint> {
    const capabilities = await cancellable(options.signal, "kv patch", this.getCapabilities)
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.resourceNamespace,
      KvPatchCommand,
      {
        namespace: this.resourceNamespace,
        ...this.sessionField(),
        key: validatedKey(key),
        patch,
        ...(ifMatch === undefined ? {} : { ifMatch })
      },
      options
    )
    if (outcome.kind === "versioned") return outcome.version
    throw unexpected("patch", outcome)
  }

  private sessionField(): { readonly session?: SessionRef } {
    return this.linkedSession === undefined ? {} : { session: this.linkedSession }
  }

  /** Acquires a revocable lease as `holderId` (a stable node or worker id). A
   * live lease always conflicts with the typed lease-held error, which names
   * the holder's expiry, and `tryLease` resolves `undefined` instead: extend
   * with `renewLease`, never by re-acquiring. `ttlMs` (milliseconds) is a requested maximum within
   * `MIN_LEASE_TTL_MICROS`..=`MAX_LEASE_TTL_MICROS`. The store may grant less and
   * never more, so anything outside that range throws `InvalidError` before the
   * round trip, and a holder needing longer renews. Needs the `kvFencedLeases`
   * capability. Acquisitions ride a dedicated connection, each attempt bounded
   * by `DEFAULT_ATTEMPT_TIMEOUT_MS`. An ambiguous outcome repeats the same
   * operation identity until the requested TTL since the first send has passed,
   * then throws `AmbiguousMutationError`, or until the signal aborts. A grant
   * that arrives after its `validUntilMs` throws the typed lease-lost error.
   * The grant position joins this connection's read-your-writes frontier. */
  async lease(
    key: Uint8Array,
    holderId: string,
    ttlMs: number,
    options: CallOptions = {}
  ): Promise<Lease> {
    const request = {
      namespace: this.resourceNamespace,
      key: validatedKey(key),
      leaseTtlMicros: durationMicros(ttlMs, "lease ttlMs"),
      holderId
    }
    requireCapability(
      await cancellable(options.signal, "kv lease", this.getCapabilities),
      "kvFencedLeases"
    )
    const granted = await (this.leases ?? NO_LEASE_COORDINATOR).acquire(request, options)
    this.backend.observePositions?.({ kv: granted.position })
    return granted
  }

  /** `lease` held by a `LeaseGuard`, which releases it when the scope that
   * holds it with `await using` ends. */
  async leaseGuard(
    key: Uint8Array,
    holderId: string,
    ttlMs: number,
    options: CallOptions = {}
  ): Promise<LeaseGuard> {
    return LeaseGuard.create(this, key, holderId, await this.lease(key, holderId, ttlMs, options))
  }

  /** `lease` that resolves `undefined` instead of throwing when a live lease
   * holds `key`, the non-blocking acquire. Every other failure, an ambiguous
   * outcome included, throws as `lease` throws it. */
  async tryLease(
    key: Uint8Array,
    holderId: string,
    ttlMs: number,
    options: CallOptions = {}
  ): Promise<Lease | undefined> {
    try {
      return await this.lease(key, holderId, ttlMs, options)
    } catch (error) {
      if (
        error instanceof KvExecutionError &&
        (error.detail as { readonly kind?: string } | undefined)?.kind === "leaseHeld"
      )
        return undefined
      throw error
    }
  }

  /** Extends a held lease without changing its token. `ttlMs` obeys the same
   * range as `lease`. An expired, released, or re-acquired lease fails with
   * the typed lease-lost error: stop the protected work, a new epoch needs a
   * fresh `lease`. */
  async renewLease(
    key: Uint8Array,
    holderId: string,
    token: bigint,
    ttlMs: number,
    options: CallOptions = {}
  ): Promise<Lease> {
    const leaseTtlMicros = durationMicros(ttlMs, "lease ttlMs")
    const capabilities = await cancellable(options.signal, "kv renew", this.getCapabilities)
    const sentMs = performance.now()
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.resourceNamespace,
      KvLeaseRenewCommand,
      {
        namespace: this.resourceNamespace,
        key: validatedKey(key),
        holderId,
        leaseToken: token,
        leaseTtlMicros
      },
      options
    )
    if (outcome.kind === "renewed") {
      const grantedTtlMs = microsToMillis(outcome.grantedTtlMicros)
      return {
        token: outcome.leaseToken,
        grantedTtlMs,
        position: outcome.position,
        validUntilMs: sentMs + grantedTtlMs
      }
    }
    throw unexpected("renewLease", outcome)
  }

  /** @internal `renewLease` over the coordination connection, repeating an
   * ambiguous attempt under its identity until `deadlineMs` on the
   * `performance.now()` clock, then throwing the typed lease-lost error. */
  async renewLeaseUntil(
    key: Uint8Array,
    holderId: string,
    token: bigint,
    ttlMs: number,
    deadlineMs: number,
    options: CallOptions = {}
  ): Promise<Lease> {
    const request = {
      namespace: this.resourceNamespace,
      key: validatedKey(key),
      holderId,
      leaseToken: token,
      leaseTtlMicros: durationMicros(ttlMs, "lease ttlMs")
    }
    requireCapability(
      await cancellable(options.signal, "kv renew", this.getCapabilities),
      "kvFencedLeases"
    )
    const renewed = await (this.leases ?? NO_LEASE_COORDINATOR).renew(request, deadlineMs, options)
    this.backend.observePositions?.({ kv: renewed.position })
    return renewed
  }

  /** Releases a held lease early, presenting the same `holderId` and its
   * token. Resolves `true` when a held lease was released. */
  async release(
    key: Uint8Array,
    holderId: string,
    token: bigint,
    options: CallOptions = {}
  ): Promise<boolean> {
    const capabilities = await cancellable(options.signal, "kv release", this.getCapabilities)
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.resourceNamespace,
      KvReleaseCommand,
      {
        namespace: this.resourceNamespace,
        key: validatedKey(key),
        leaseToken: token,
        holderId
      },
      options
    )
    if (outcome.kind === "released") return outcome.wasHeld
    throw unexpected("release", outcome)
  }

  /** Starts an atomic value copy that preserves remaining expiry. */
  copyTo(key: Uint8Array, toKey: Uint8Array): KvCopyRequest {
    return KvCopyRequest.create(
      this.backend,
      this.getCapabilities,
      this.resourceNamespace,
      validatedKey(key),
      validatedKey(toKey),
      false,
      this.scope
    )
  }

  moveTo(key: Uint8Array, toKey: Uint8Array): KvCopyRequest {
    return KvCopyRequest.create(
      this.backend,
      this.getCapabilities,
      this.resourceNamespace,
      validatedKey(key),
      validatedKey(toKey),
      true,
      this.scope
    )
  }

  /** Reads several keys in one round trip while preserving key order. */
  async getMany(
    keys: readonly Uint8Array[],
    options: CallOptions = {}
  ): Promise<readonly (Uint8Array | undefined)[]> {
    const capabilities = await cancellable(options.signal, "kv get many", this.getCapabilities)
    // Batch items travel without the read envelope a single read carries, so
    // each read names the frontier itself. Only a server that honors the
    // barrier gets it, as an older one would ignore it.
    const frontier = capabilities.kv.fencedLeases ? this.backend.seenPositions?.().kv : undefined
    const ops: BatchItem[] = keys.map((key) => ({
      code: KvGetCommand.code,
      payload: KvGetCommand.encode({
        namespace: this.resourceNamespace,
        key: validatedKey(key),
        ...(frontier === undefined ? {} : { minPosition: frontier })
      })
    }))
    const results = await executeBatch(this.backend, capabilities, ops, options)
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
    return KvDeleteManyRequest.create(
      this.backend,
      this.getCapabilities,
      this.resourceNamespace,
      this.scope
    )
  }

  scan(): KvScanRequest {
    return KvScanRequest.create(
      this.backend,
      this.getCapabilities,
      this.resourceNamespace,
      this.scope
    )
  }
}

/** Builds a value write. A precondition turns it into a `KvCasRequest`. */
export class KvSetRequest {
  private value: Uint8Array = new Uint8Array()
  private expiresAtMicros: bigint | undefined
  private ttlMicros: bigint | undefined

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string,
    private readonly key: Uint8Array,
    private readonly session?: SessionRef
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string,
    key: Uint8Array,
    session?: SessionRef
  ): KvSetRequest {
    return new KvSetRequest(backend, getCapabilities, namespace, key, session)
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

  /** Expires the entry at an absolute epoch-microseconds time. Overrides any
   * prior `ttl`. */
  expiresAt(epochMicros: bigint): this {
    this.expiresAtMicros = epochMicros
    this.ttlMicros = undefined
    return this
  }

  /** Expires the entry `ttlMs` milliseconds after the server's time of the
   * write, so a client clock cannot skew it. Overrides any prior `expiresAt`. */
  ttl(ttlMs: number): this {
    this.ttlMicros = durationMicros(ttlMs, "ttl ttlMs")
    this.expiresAtMicros = undefined
    return this
  }

  /** A compare-and-swap that applies only if the key holds `version`. */
  expectVersion(version: bigint): KvCasRequest {
    return KvCasRequest.create(this, { kind: "match", version })
  }

  /** A compare-and-swap that applies only if the key does not exist yet. */
  expectAbsent(): KvCasRequest {
    return KvCasRequest.create(this, { kind: "absent" })
  }

  /** Writes the value unconditionally. */
  async send(options: CallOptions = {}): Promise<void> {
    validatedValue(this.value)
    const capabilities = await cancellable(options.signal, "kv set", this.getCapabilities)
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.namespace,
      KvSetCommand,
      {
        namespace: this.namespace,
        ...(this.session !== undefined ? { session: this.session } : {}),
        key: this.key,
        value: this.value,
        ...(this.expiresAtMicros !== undefined ? { expiresAtMicros: this.expiresAtMicros } : {}),
        ...(this.ttlMicros !== undefined ? { ttlMicros: this.ttlMicros } : {})
      },
      options
    )
    if (outcome.kind === "written") return
    throw unexpected("set", outcome)
  }

  /** @internal */
  async commitWith(expect: CasExpect, options: CallOptions = {}): Promise<bigint> {
    const capabilities = await cancellable(options.signal, "kv cas", this.getCapabilities)
    if (!capabilities.kv.cas) {
      throw new UnsupportedError("compare-and-swap is not advertised by this deployment", {
        surface: "kv",
        feature: "cas"
      })
    }
    validatedValue(this.value)
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.namespace,
      KvCasCommand,
      {
        namespace: this.namespace,
        ...(this.session !== undefined ? { session: this.session } : {}),
        key: this.key,
        value: this.value,
        expect,
        ...(this.expiresAtMicros !== undefined ? { expiresAtMicros: this.expiresAtMicros } : {}),
        ...(this.ttlMicros !== undefined ? { ttlMicros: this.ttlMicros } : {})
      },
      options
    )
    if (outcome.kind === "committed") return outcome.version
    throw unexpected("cas", outcome)
  }
}

/** A compare-and-swap write, built by `KvSetRequest.expectVersion` or
 * `expectAbsent`. It only commits, so a precondition is never dropped by an
 * unconditional send. */
export class KvCasRequest {
  private constructor(
    private readonly write: KvSetRequest,
    private readonly expect: CasExpect
  ) {}

  /** @internal */
  static create(write: KvSetRequest, expect: CasExpect): KvCasRequest {
    return new KvCasRequest(write, expect)
  }

  expiresAt(epochMicros: bigint): this {
    this.write.expiresAt(epochMicros)
    return this
  }

  /** Expires the entry `ttlMs` milliseconds after the server's time of the
   * write. Overrides any prior `expiresAt`. */
  ttl(ttlMs: number): this {
    this.write.ttl(ttlMs)
    return this
  }

  /** Commits the compare-and-swap and returns the entry's new version. */
  commit(options: CallOptions = {}): Promise<bigint> {
    return this.write.commitWith(this.expect, options)
  }
}

/** Builds a compare-and-swap protected by a lease fencing token. */
export class KvCasFencedRequest {
  private value: Uint8Array = new Uint8Array()
  private expiresAtMicros: bigint | undefined
  private ttlMicros: bigint | undefined
  private expect: CasExpect | undefined

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string,
    private readonly key: Uint8Array,
    private readonly fenceNamespace: string,
    private readonly fenceKey: Uint8Array,
    private readonly fenceToken: bigint,
    private readonly session?: SessionRef
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string,
    key: Uint8Array,
    fenceNamespace: string,
    fenceKey: Uint8Array,
    fenceToken: bigint,
    session?: SessionRef
  ): KvCasFencedRequest {
    return new KvCasFencedRequest(
      backend,
      getCapabilities,
      namespace,
      key,
      fenceNamespace,
      fenceKey,
      fenceToken,
      session
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

  /** Expires the entry at an absolute epoch-microseconds time. Overrides any
   * prior `ttl`. */
  expiresAt(epochMicros: bigint): this {
    this.expiresAtMicros = epochMicros
    this.ttlMicros = undefined
    return this
  }

  /** Expires the entry `ttlMs` milliseconds after the server's time of the
   * write, so a client clock cannot skew it. Overrides any prior `expiresAt`. */
  ttl(ttlMs: number): this {
    this.ttlMicros = durationMicros(ttlMs, "ttl ttlMs")
    this.expiresAtMicros = undefined
    return this
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
  async commit(options: CallOptions = {}): Promise<bigint> {
    if (this.expect === undefined) {
      throw new InvalidError(
        "commit() needs a precondition: call expectVersion(..) or expectAbsent()"
      )
    }
    const capabilities = await cancellable(options.signal, "kv cas fenced", this.getCapabilities)
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
        ...(this.session !== undefined ? { session: this.session } : {}),
        key: this.key,
        value: this.value,
        expect: this.expect,
        fenceNamespace: this.fenceNamespace,
        fenceKey: this.fenceKey,
        fenceToken: this.fenceToken,
        ...(this.expiresAtMicros !== undefined ? { expiresAtMicros: this.expiresAtMicros } : {}),
        ...(this.ttlMicros !== undefined ? { ttlMicros: this.ttlMicros } : {})
      },
      options
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
  private boundStream: string | undefined
  private boundUser: string | undefined
  private boundAgent: string | undefined
  private boundApp: string | undefined
  private pageNumber = 0
  private pageResults = DEFAULT_SCAN_LIMIT

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string,
    private readonly scope: ResourceScope
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string,
    scope: ResourceScope = BARE_SCOPE
  ): KvScanRequest {
    return new KvScanRequest(backend, getCapabilities, namespace, scope)
  }

  /** Restricts a memory namespace scan to one conversation. */
  conversation(conversationId: string): this {
    this.boundConversation = conversationId
    this.boundStream = this.scope.stream
    return this
  }

  /** The stream the conversation lens belongs to when it is not the default
   * stream, as for memory riding an explicitly named stream.
   * @internal */
  lensStream(stream: string | undefined): this {
    this.boundStream = stream
    return this
  }

  /** The memory scope lenses, so a scoped recall reads only the rows of its
   * user, agent, and app instead of the whole namespace.
   * @internal */
  memoryLens(user: string | undefined, agent: string | undefined, app: string | undefined): this {
    this.boundUser = user
    this.boundAgent = agent
    this.boundApp = app
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

  /** The 1-based page to read. Zero reads the first page. */
  page(page: number): this {
    if (!Number.isSafeInteger(page) || page < 0 || page > 0xffff_ffff) {
      throw new InvalidError("scan page must be a non-negative u32 integer")
    }
    this.pageNumber = page
    return this
  }

  /** The entries per page, clamped to `MAX_SCAN_LIMIT`. Zero reads the
   * server's default page size. */
  results(results: number): this {
    if (!Number.isSafeInteger(results) || results < 0) {
      throw new InvalidError("scan page size must be a non-negative integer")
    }
    this.pageResults = Math.min(results, MAX_SCAN_LIMIT)
    return this
  }

  /** Fetches the chosen page with the totals of the whole scan. */
  async fetch(options: CallOptions = {}): Promise<KvPage> {
    const capabilities = await cancellable(options.signal, "kv scan", this.getCapabilities)
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.namespace,
      KvScanCommand,
      this.request(),
      options
    )
    if (outcome.kind === "page") return outcome.page
    throw unexpected("scan", outcome)
  }

  /** Collects every matching entry from the first page to the last the
   * server reports. Pages are read by offset, so entries written or removed
   * during the walk can shift the page bounds. */
  async entries(options: CallOptions = {}): Promise<readonly KvEntry[]> {
    const capabilities = await cancellable(options.signal, "kv scan", this.getCapabilities)
    const out: KvEntry[] = []
    let request = { ...this.request(), page: 1 }
    for (;;) {
      const outcome = await executeKv(
        this.backend,
        capabilities,
        this.namespace,
        KvScanCommand,
        request,
        options
      )
      if (outcome.kind !== "page") throw unexpected("scan", outcome)
      out.push(...outcome.page.items)
      if (outcome.page.items.length === 0 || outcome.page.page >= outcome.page.totalPages)
        return out
      request = { ...request, page: outcome.page.page + 1 }
    }
  }

  private request(): {
    readonly namespace: string
    readonly prefix?: Uint8Array
    readonly start?: Uint8Array
    readonly end?: Uint8Array
    readonly keyContains?: string
    readonly conversation?: string
    readonly stream?: string
    readonly user?: string
    readonly agent?: string
    readonly app?: string
    readonly page: number
    readonly results: number
  } {
    return {
      namespace: this.namespace,
      ...(this.boundPrefix !== undefined ? { prefix: this.boundPrefix } : {}),
      ...(this.boundStart !== undefined ? { start: this.boundStart } : {}),
      ...(this.boundEnd !== undefined ? { end: this.boundEnd } : {}),
      ...(this.boundKeyContains !== undefined ? { keyContains: this.boundKeyContains } : {}),
      ...(this.boundConversation !== undefined ? { conversation: this.boundConversation } : {}),
      ...(this.boundStream !== undefined ? { stream: this.boundStream } : {}),
      ...(this.boundUser !== undefined ? { user: this.boundUser } : {}),
      ...(this.boundAgent !== undefined ? { agent: this.boundAgent } : {}),
      ...(this.boundApp !== undefined ? { app: this.boundApp } : {}),
      page: this.pageNumber,
      results: this.pageResults
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
  private boundStream: string | undefined

  private constructor(
    private readonly backend: ManagedTransport,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly namespace: string,
    private readonly scope: ResourceScope
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string,
    scope: ResourceScope = BARE_SCOPE
  ): KvDeleteManyRequest {
    return new KvDeleteManyRequest(backend, getCapabilities, namespace, scope)
  }

  conversation(conversationId: string): this {
    this.boundConversation = conversationId
    this.boundStream = this.scope.stream
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

  async send(options: CallOptions = {}): Promise<number> {
    const capabilities = await cancellable(options.signal, "kv delete many", this.getCapabilities)
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
        ...(this.boundConversation !== undefined ? { conversation: this.boundConversation } : {}),
        ...(this.boundStream !== undefined ? { stream: this.boundStream } : {})
      },
      options
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
    private readonly deleteSource: boolean,
    private readonly scope: ResourceScope
  ) {}

  /** @internal */
  static create(
    backend: ManagedTransport,
    getCapabilities: () => Promise<Capabilities>,
    namespace: string,
    key: Uint8Array,
    toKey: Uint8Array,
    deleteSource: boolean,
    scope: ResourceScope = BARE_SCOPE
  ): KvCopyRequest {
    return new KvCopyRequest(backend, getCapabilities, namespace, key, toKey, deleteSource, scope)
  }

  intoNamespace(namespace: string): this {
    this.toNamespaceOverride = this.scope.name(namespace)
    return this
  }

  async send(options: CallOptions = {}): Promise<bigint> {
    if (this.toNamespaceOverride !== undefined) validateNamespace(this.toNamespaceOverride)
    const what = this.deleteSource ? "kv move" : "kv copy"
    const capabilities = await cancellable(options.signal, what, this.getCapabilities)
    const command = this.deleteSource ? KvMoveCommand : KvCopyCommand
    const outcome = await executeKv(
      this.backend,
      capabilities,
      this.namespace,
      command,
      {
        namespace: this.namespace,
        key: this.key,
        toKey: this.toKey,
        ...(this.toNamespaceOverride !== undefined ? { toNamespace: this.toNamespaceOverride } : {})
      },
      options
    )
    if (outcome.kind === "committed") return outcome.version
    throw unexpected(this.deleteSource ? "move" : "copy", outcome)
  }
}

// A relative duration in milliseconds, the SDK-wide unit, as the wire's
// microseconds. Fractions of a millisecond are kept to the microsecond.
function durationMicros(ms: number, name: string): bigint {
  return millisToMicros(millis(ms, name))
}
