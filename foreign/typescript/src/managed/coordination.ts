import { microsToMillis } from "../client/duration.js"
import { type CallOptions, cancellable, delay, throwIfAborted } from "../client/call-options.js"
import { managedCapabilitiesFrom } from "../client/capabilities.js"
import { decodeManagedReply } from "../client/managed.js"
import { isPermissionDenied, isRetryable } from "../client/error-classify.js"
import {
  AmbiguousMutationError,
  CancelledError,
  ConfigError,
  InvalidError,
  KvExecutionError,
  LaserError,
  ProtocolError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "../client/errors.js"
import {
  ApacheIggyTransport,
  type FrameWritten,
  frameNotWritten,
  frameWritten,
  notWrittenError,
  pendingReplyMessage
} from "../iggy/apache-iggy.js"
import { mintUlidValue } from "../runtime/ulid.js"
import { decodeOne, encodeNamed } from "../wire/cbor.js"
import {
  AGDX_HELLO_CODE,
  AGDX_KV_CAS_FENCED_CODE,
  AGDX_KV_GET_CODE,
  AGDX_KV_LEASE_CODE,
  AGDX_KV_LEASE_RENEW_CODE,
  AGDX_KV_RELEASE_CODE
} from "../wire/codes.js"
import { decodeBackendAnnounce } from "../wire/hello.js"
import {
  decodeKvReply,
  encodeKvCasFenced,
  encodeKvGet,
  encodeKvLease,
  encodeKvLeaseRenew,
  encodeKvRelease,
  type KvCasFenced,
  type KvEntry,
  type KvGet,
  type KvLease,
  type KvLeaseRenew,
  type KvOutcome,
  type KvRelease
} from "../wire/kv.js"
import { encodeManagedRequestEnvelope, MANAGED_REQUEST_VERSION } from "../wire/mutation.js"
import type { Lease } from "./kv.js"

/** Above the managed backend's 10 second fold wait, so a mutation the fold has
 * not applied yet comes back as a pending reply rather than a timeout. */
export const DEFAULT_ATTEMPT_TIMEOUT_MS = 15_000

const FIRST_REPEAT_DELAY_MS = 100
const MAX_REPEAT_DELAY_MS = 2_000
const CAPABILITY_REPROBE_INTERVAL_MS = 1_000

/** Reset must stop all in-flight work before returning. */
export interface ManagedKvTransport {
  ready?(): Promise<void>
  send(code: number, frame: Uint8Array): Promise<Uint8Array>
  reset(): Promise<void>
  /** The connection `sendOn` would use now, read when a call starts. A
   * transport without a replaceable connection leaves it out. */
  generation?(): number
  /** Sends on the connection `generation` names. A transport that has
   * replaced it refuses with a retryable error before sending. */
  sendOn?(generation: number, code: number, frame: Uint8Array): Promise<Uint8Array>
  /** `reset` only while the connection is still `generation`, so a late
   * failure on a retired connection never ends the one that replaced it. */
  resetGeneration?(generation: number): Promise<void>
  close?(): Promise<void>
}

export type AmbiguousMutationRecovery =
  | { readonly kind: "waitForLeaseExpiry"; readonly ttlMs: number }
  | { readonly kind: "repeatPrepared" }
  | { readonly kind: "reconcileTargetPrecondition" }

export interface PreparedMutation {
  readonly operationId: bigint
  readonly ambiguousRecovery: AmbiguousMutationRecovery
}

interface PreparedOperation {
  readonly clientId: bigint
  readonly code: number
  readonly frame: Uint8Array
  // A grant or renewal applies no earlier than the first send, so a validity
  // measured from it never outlasts the backend's own expiry.
  firstSentMs?: number
}

const preparedOperations = new WeakMap<PreparedMutation, PreparedOperation>()

function within<T>(pending: Promise<T>, milliseconds: number, what: string): Promise<T> {
  return new Promise((resolve, reject) => {
    let settled = false
    let timer: ReturnType<typeof setTimeout> | undefined
    const deadline = Date.now() + milliseconds
    const complete = (finish: () => void): void => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      finish()
    }
    const tick = (): void => {
      const remaining = deadline - Date.now()
      if (remaining <= 0)
        complete(() => {
          reject(new TimeoutError(what))
        })
      else timer = setTimeout(tick, Math.min(remaining, 2_147_483_647))
    }
    tick()
    pending.then(
      (value) => {
        complete(() => {
          resolve(value)
        })
      },
      (error: unknown) => {
        complete(() => {
          reject(error instanceof Error ? error : new TransportError(what, true, { cause: error }))
        })
      }
    )
  })
}

function outcome(bytes: Uint8Array): KvOutcome {
  const reply = decodeManagedReply(
    (value) => decodeKvReply(decodeOne(value, "KvReply"), "KvReply"),
    bytes
  )
  if (reply.kind === "ok") return reply.outcome
  if (reply.kind === "err") {
    throw new KvExecutionError(`kv coordination failed: ${reply.error.kind}`, reply.error)
  }
  throw new ProtocolError("kv coordination: unknown reply variant")
}

function lease(value: KvOutcome, expected: "leased" | "renewed", firstSentMs: number): Lease {
  if (value.kind !== expected)
    throw new ProtocolError(`kv coordination ${expected}: unexpected outcome ${value.kind}`)
  const grantedTtlMs = microsToMillis(value.grantedTtlMicros)
  return {
    token: value.leaseToken,
    grantedTtlMs,
    position: value.position,
    validUntilMs: firstSentMs + grantedTtlMs
  }
}

// Whether a failed send may have written its frame: the driver says it did or
// did not, and a failure it cannot place counts as written when it is a
// retryable mid-request one.
function mayHaveWritten(error: unknown): boolean {
  if (frameWritten(error)) return true
  if (frameNotWritten(error)) return false
  return error instanceof LaserError && isRetryable(error) && !isPermissionDenied(error)
}

function firstSentMs(operation: PreparedMutation): number {
  return preparedOperations.get(operation)?.firstSentMs ?? performance.now()
}

export class DedicatedKvTransport implements ManagedKvTransport, AsyncDisposable {
  private transport: ApacheIggyTransport | undefined
  private opening: Promise<ApacheIggyTransport> | undefined
  private resetting: Promise<void> | undefined
  private currentGeneration = 0
  private supported = false
  private probedAtMs: number | undefined
  private closed = false

  constructor(private readonly connectionString: string) {}

  async ready(): Promise<void> {
    await this.resetting
    if (this.closed) throw new TransportError("coordination transport is closed", false)
    const generation = this.currentGeneration
    if (this.transport === undefined) {
      this.opening ??= ApacheIggyTransport.connect(this.connectionString)
      const opening = this.opening
      try {
        const transport = await opening
        if (this.retired(generation)) {
          await transport.close().catch(() => undefined)
          throw new TimeoutError("kv coordination connect")
        }
        this.transport = transport
      } finally {
        if (this.opening === opening) this.opening = undefined
      }
    }
    const transport = this.transport
    // A connection that lacks the capability answers from its last probe until
    // a second has passed, so a refused call costs no round trip.
    const probeDue =
      this.probedAtMs === undefined ||
      performance.now() - this.probedAtMs >= CAPABILITY_REPROBE_INTERVAL_MS
    if (!this.supported && probeDue) {
      this.probedAtMs = performance.now()
      let supported = false
      try {
        const reply = await transport.sendManaged(AGDX_HELLO_CODE, new Uint8Array())
        supported = managedCapabilitiesFrom(decodeBackendAnnounce(reply)).kv.fencedLeases
      } catch {
        // A failed probe does not advertise fenced leases.
      }
      if (this.retired(generation, transport)) throw new TimeoutError("kv coordination connect")
      this.supported = supported
    }
    if (!this.supported)
      throw new UnsupportedError("the fenced-lease contract is not advertised by this deployment")
  }

  private retired(generation: number, transport?: ApacheIggyTransport): boolean {
    return (
      this.closed ||
      generation !== this.currentGeneration ||
      (transport !== undefined && transport !== this.transport)
    )
  }

  async send(code: number, frame: Uint8Array): Promise<Uint8Array> {
    if (this.closed) throw new TransportError("coordination transport is closed", false)
    const transport = this.transport
    if (transport === undefined) throw new InvalidError("coordination transport is not ready")
    const written: FrameWritten = { written: false }
    try {
      return await transport.sendManagedPreframed(code, frame, {
        retryAfterReconnect: false,
        written
      })
    } catch (error) {
      // A frame an earlier attempt wrote keeps its unknown outcome, whatever a
      // later attempt answered.
      if (written.written && !frameWritten(error)) {
        throw new AmbiguousMutationError("the frame was written and its outcome is unknown", {
          cause: error
        })
      }
      throw error
    }
  }

  generation(): number {
    return this.currentGeneration
  }

  async sendOn(generation: number, code: number, frame: Uint8Array): Promise<Uint8Array> {
    // The connection this call readied was retired, so nothing is sent on its replacement.
    if (generation !== this.currentGeneration)
      throw notWrittenError("the coordination connection was retired before the send")
    return this.send(code, frame)
  }

  async resetGeneration(generation: number): Promise<void> {
    if (generation !== this.currentGeneration) {
      await this.resetting
      return
    }
    await this.reset()
  }

  async reset(): Promise<void> {
    if (this.resetting !== undefined) return this.resetting
    this.currentGeneration += 1
    const retired = this.transport
    const opening = this.opening
    this.transport = undefined
    this.opening = undefined
    this.supported = false
    this.probedAtMs = undefined
    const resetting = (async () => {
      if (retired !== undefined) await retired.close().catch(() => undefined)
      if (opening !== undefined) {
        try {
          await (await opening).close()
        } catch {
          // The failed dial retained no connection.
        }
      }
    })()
    this.resetting = resetting
    try {
      await resetting
    } finally {
      if (this.resetting === resetting) this.resetting = undefined
    }
  }

  async close(): Promise<void> {
    this.closed = true
    await this.reset()
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close()
  }
}

/** Prepare once, then repeat the same operation only when its recovery permits it. */
export class FencedLeaseClient implements AsyncDisposable {
  private readonly clientId = mintUlidValue()
  private attemptTimeoutMs = DEFAULT_ATTEMPT_TIMEOUT_MS
  private active = 0
  private closed = false
  private closing: Promise<void> | undefined

  constructor(private readonly transport: ManagedKvTransport) {}

  static connectDedicated(connectionString: string): FencedLeaseClient {
    return new FencedLeaseClient(new DedicatedKvTransport(connectionString))
  }

  withAttemptTimeout(timeoutMs: number): this {
    if (!Number.isFinite(timeoutMs) || timeoutMs < 0)
      throw new InvalidError("coordination attempt timeout must be a non-negative finite number")
    if (this.active !== 0) throw new InvalidError("cannot change the timeout during an attempt")
    this.attemptTimeoutMs = timeoutMs
    return this
  }

  prepareAcquire(request: KvLease): PreparedMutation {
    return this.prepare(AGDX_KV_LEASE_CODE, encodeKvLease(request), {
      kind: "waitForLeaseExpiry",
      ttlMs: Number(request.leaseTtlMicros) / 1_000
    })
  }

  prepareRenew(request: KvLeaseRenew): PreparedMutation {
    return this.prepare(AGDX_KV_LEASE_RENEW_CODE, encodeKvLeaseRenew(request), {
      kind: "repeatPrepared"
    })
  }

  prepareRelease(request: KvRelease): PreparedMutation {
    return this.prepare(AGDX_KV_RELEASE_CODE, encodeKvRelease(request), { kind: "repeatPrepared" })
  }

  prepareCasFenced(request: KvCasFenced): PreparedMutation {
    return this.prepare(AGDX_KV_CAS_FENCED_CODE, encodeKvCasFenced(request), {
      kind: "reconcileTargetPrecondition"
    })
  }

  async acquire(operation: PreparedMutation, options: CallOptions = {}): Promise<Lease> {
    const value = await this.execute(operation, AGDX_KV_LEASE_CODE, "acquire", options)
    return lease(value, "leased", firstSentMs(operation))
  }

  async renew(operation: PreparedMutation, options: CallOptions = {}): Promise<Lease> {
    return this.renewWithin(operation, this.attemptTimeoutMs, options)
  }

  /** @internal `renew` with its attempt bounded by `timeoutMs` as well. */
  async renewWithin(
    operation: PreparedMutation,
    timeoutMs: number,
    options: CallOptions = {}
  ): Promise<Lease> {
    const value = await this.execute(
      operation,
      AGDX_KV_LEASE_RENEW_CODE,
      "renew",
      options,
      Math.min(timeoutMs, this.attemptTimeoutMs)
    )
    return lease(value, "renewed", firstSentMs(operation))
  }

  async release(operation: PreparedMutation, options: CallOptions = {}): Promise<boolean> {
    const value = await this.execute(operation, AGDX_KV_RELEASE_CODE, "release", options)
    if (value.kind !== "released")
      throw new ProtocolError(`kv coordination release: unexpected outcome ${value.kind}`)
    return value.wasHeld
  }

  async casFenced(operation: PreparedMutation, options: CallOptions = {}): Promise<bigint> {
    const value = await this.execute(operation, AGDX_KV_CAS_FENCED_CODE, "cas_fenced", options)
    if (value.kind !== "committed")
      throw new ProtocolError(`kv coordination cas_fenced: unexpected outcome ${value.kind}`)
    return value.version
  }

  async get(request: KvGet, options: CallOptions = {}): Promise<KvEntry | undefined> {
    throwIfAborted(options.signal, "kv coordination get")
    this.ensureTimeout()
    this.active += 1
    try {
      await this.ready(options.signal, this.attemptTimeoutMs)
      const generation = this.transport.generation?.()
      let bytes: Uint8Array
      try {
        bytes = await within(
          cancellable(options.signal, "kv coordination get", () =>
            this.sendOn(generation, AGDX_KV_GET_CODE, encodeNamed(encodeKvGet(request)))
          ),
          this.attemptTimeoutMs,
          "kv coordination get"
        )
      } catch (error) {
        // An abandoned request still answers whole, so the connection stays in step.
        if (!(error instanceof CancelledError)) await this.retire(generation)
        throw error
      }
      const value = outcome(bytes)
      if (value.kind !== "value")
        throw new ProtocolError(`kv coordination get: unexpected outcome ${value.kind}`)
      return value.entry
    } finally {
      this.active -= 1
    }
  }

  async close(): Promise<void> {
    this.closed = true
    this.closing ??= this.transport.close?.() ?? this.transport.reset()
    return this.closing
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close()
  }

  private ensureTimeout(): void {
    if (this.closed) throw new TransportError("fenced-lease client is closed", false)
    if (this.attemptTimeoutMs === 0)
      throw new InvalidError("coordination attempt timeout must be greater than zero")
  }

  private prepare(
    code: number,
    request: ReadonlyMap<string, unknown>,
    ambiguousRecovery: AmbiguousMutationRecovery
  ): PreparedMutation {
    const operationId = mintUlidValue()
    const operation: PreparedMutation = Object.freeze({
      operationId,
      ambiguousRecovery: Object.freeze(ambiguousRecovery)
    })
    preparedOperations.set(operation, {
      clientId: this.clientId,
      code,
      frame: encodeNamed(
        encodeManagedRequestEnvelope({
          v: MANAGED_REQUEST_VERSION,
          operationId,
          payload: encodeNamed(request)
        })
      )
    })
    return operation
  }

  private async ready(signal: AbortSignal | undefined, timeoutMs: number): Promise<void> {
    const generation = this.transport.generation?.()
    try {
      await within(
        cancellable(
          signal,
          "kv coordination connect",
          () => this.transport.ready?.() ?? Promise.resolve()
        ),
        timeoutMs,
        "kv coordination connect"
      )
    } catch (error) {
      if (!(error instanceof UnsupportedError) && !(error instanceof CancelledError))
        await this.retire(generation)
      throw error
    }
  }

  private sendOn(generation: number | undefined, code: number, frame: Uint8Array) {
    return generation === undefined || this.transport.sendOn === undefined
      ? this.transport.send(code, frame)
      : this.transport.sendOn(generation, code, frame)
  }

  // A failure retires only the connection the call used, never one a later
  // call has opened since.
  private retire(generation: number | undefined): Promise<void> {
    return generation === undefined || this.transport.resetGeneration === undefined
      ? this.transport.reset()
      : this.transport.resetGeneration(generation)
  }

  private async execute(
    operation: PreparedMutation,
    code: number,
    what: string,
    options: CallOptions,
    timeoutMs = this.attemptTimeoutMs
  ): Promise<KvOutcome> {
    throwIfAborted(options.signal, `kv coordination ${what}`)
    this.ensureTimeout()
    const prepared = preparedOperations.get(operation)
    if (prepared?.clientId !== this.clientId)
      throw new InvalidError("prepared mutation belongs to a different fenced-lease client")
    if (prepared.code !== code)
      throw new InvalidError(
        `prepared mutation carries code ${String(prepared.code)} but was executed as ${what}`
      )
    this.active += 1
    try {
      await this.ready(options.signal, timeoutMs)
      const generation = this.transport.generation?.()
      prepared.firstSentMs ??= performance.now()
      let bytes: Uint8Array
      try {
        bytes = await within(
          cancellable(options.signal, `kv coordination ${what}`, () =>
            this.sendOn(generation, code, prepared.frame.slice())
          ),
          timeoutMs,
          `kv coordination ${what}`
        )
      } catch (error) {
        // An abandoned request still answers whole, so the connection stays in step.
        if (error instanceof CancelledError) throw error
        await this.retire(generation)
        if (!mayHaveWritten(error)) throw error
        throw new AmbiguousMutationError(
          `${what} failed mid-request and requires operation-specific recovery`,
          { cause: error, operationId: operation.operationId }
        )
      }
      // The backend accepted the frame but names no outcome yet, or the node
      // could not reach its backend: the mutation may still apply. The reply
      // arrived whole, so the connection stays in step for other requests.
      const unknown = pendingReplyMessage(bytes)
      if (unknown !== undefined) {
        const identity = operation.operationId.toString(16).padStart(32, "0")
        throw new AmbiguousMutationError(
          `${what} operation ${identity} outcome is unknown (${unknown}) and requires operation-specific recovery`,
          { operationId: operation.operationId }
        )
      }
      return outcome(bytes)
    } finally {
      this.active -= 1
    }
  }
}

/** Runs one client's lease acquisitions over a dedicated coordination
 * connection. An ambiguous acquisition repeats its prepared identity, which the
 * store answers with its recorded outcome, until the requested TTL since the
 * first send has passed, and only then throws `AmbiguousMutationError`. A grant
 * that arrives after its `validUntilMs` throws the typed lease-lost error. An
 * aborted signal stops the recovery and leaves the acquisition's outcome unknown. */
export class LeaseCoordinator implements AsyncDisposable {
  /** Without a client every acquisition fails with `ConfigError`, as for an
   * injected Iggy client that has no connection string to dial. */
  constructor(private readonly client?: FencedLeaseClient) {}

  static forConnection(connectionString: string | undefined): LeaseCoordinator {
    return new LeaseCoordinator(
      connectionString === undefined
        ? undefined
        : FencedLeaseClient.connectDedicated(connectionString)
    )
  }

  acquire(request: KvLease, options: CallOptions = {}): Promise<Lease> {
    return this.acquireNow(request, options)
  }

  /** Renews under one prepared identity, repeating an ambiguous attempt with
   * the acquisition's backoff until `deadlineMs` on the `performance.now()`
   * clock, each attempt bounded by the time left, then throws the typed
   * lease-lost error. */
  async renew(
    request: KvLeaseRenew,
    deadlineMs: number,
    options: CallOptions = {}
  ): Promise<Lease> {
    const client = this.dedicated()
    const operation = client.prepareRenew(request)
    let delayMs = FIRST_REPEAT_DELAY_MS
    for (;;) {
      const remainingMs = deadlineMs - performance.now()
      if (remainingMs <= 0) {
        throw new KvExecutionError("kv coordination failed: leaseLost", { kind: "leaseLost" })
      }
      try {
        return await client.renewWithin(operation, remainingMs, options)
      } catch (error) {
        if (!(error instanceof AmbiguousMutationError)) throw error
        const leftMs = Math.max(0, deadlineMs - performance.now())
        await delay(Math.min(delayMs, leftMs), options.signal, "kv lease renewal")
        delayMs = Math.min(delayMs * 2, MAX_REPEAT_DELAY_MS)
      }
    }
  }

  async close(): Promise<void> {
    await this.client?.close()
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close()
  }

  private dedicated(): FencedLeaseClient {
    if (this.client === undefined) {
      throw new ConfigError(
        "a coordinated lease needs a connection-backed Laser, use FencedLeaseClient with a dedicated transport for an injected client"
      )
    }
    return this.client
  }

  private async acquireNow(request: KvLease, options: CallOptions): Promise<Lease> {
    const client = this.dedicated()
    const operation = client.prepareAcquire(request)
    const windowMs = microsToMillis(request.leaseTtlMicros)
    let delayMs = FIRST_REPEAT_DELAY_MS
    for (;;) {
      try {
        const granted = await client.acquire(operation, options)
        if (granted.validUntilMs <= performance.now()) {
          throw new KvExecutionError("kv coordination failed: leaseLost", { kind: "leaseLost" })
        }
        return granted
      } catch (error) {
        const sentMs = preparedOperations.get(operation)?.firstSentMs
        if (!(error instanceof AmbiguousMutationError) || sentMs === undefined) throw error
        const remainingMs = sentMs + windowMs - performance.now()
        if (remainingMs <= 0) throw error
        await delay(Math.min(delayMs, remainingMs), options.signal, "kv lease")
        delayMs = Math.min(delayMs * 2, MAX_REPEAT_DELAY_MS)
      }
    }
  }
}
