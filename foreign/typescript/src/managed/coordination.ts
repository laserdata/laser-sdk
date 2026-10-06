import { managedCapabilitiesFrom } from "../client/capabilities.js"
import { decodeManagedReply } from "../client/managed.js"
import { isRetryable } from "../agent/reliable-consumer.js"
import { isPermissionDenied } from "../client/error-classify.js"
import {
  AmbiguousMutationError,
  ConfigError,
  InvalidError,
  KvExecutionError,
  LaserError,
  ProtocolError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "../client/errors.js"
import { ApacheIggyTransport } from "../iggy/apache-iggy.js"
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

export const DEFAULT_ATTEMPT_TIMEOUT_MS = 10_000

const MAX_TIMER_MILLIS = 2_147_483_647n

/** Reset must stop all in-flight work before returning. */
export interface ManagedKvTransport {
  ready?(): Promise<void>
  send(code: number, frame: Uint8Array): Promise<Uint8Array>
  reset(): Promise<void>
  close?(): Promise<void>
}

export type AmbiguousMutationRecovery =
  | { readonly kind: "waitForLeaseExpiry"; readonly ttlMicros: bigint }
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

function lease(value: KvOutcome, expected: "leased" | "renewed"): Lease {
  if (value.kind !== expected)
    throw new ProtocolError(`kv coordination ${expected}: unexpected outcome ${value.kind}`)
  return {
    token: value.leaseToken,
    grantedTtlMicros: value.grantedTtlMicros,
    position: value.position
  }
}

export class DedicatedKvTransport implements ManagedKvTransport, AsyncDisposable {
  private transport: ApacheIggyTransport | undefined
  private opening: Promise<ApacheIggyTransport> | undefined
  private resetting: Promise<void> | undefined
  private generation = 0
  private supported = false
  private closed = false

  constructor(private readonly connectionString: string) {}

  async ready(): Promise<void> {
    await this.resetting
    if (this.closed) throw new TransportError("coordination transport is closed", false)
    const generation = this.generation
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
    if (!this.supported) {
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
      generation !== this.generation ||
      (transport !== undefined && transport !== this.transport)
    )
  }

  async send(code: number, frame: Uint8Array): Promise<Uint8Array> {
    if (this.closed) throw new TransportError("coordination transport is closed", false)
    const transport = this.transport
    if (transport === undefined) throw new InvalidError("coordination transport is not ready")
    return transport.sendManagedPreframed(code, frame, { retryAfterReconnect: false })
  }

  async reset(): Promise<void> {
    if (this.resetting !== undefined) return this.resetting
    this.generation += 1
    const retired = this.transport
    const opening = this.opening
    this.transport = undefined
    this.opening = undefined
    this.supported = false
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

  withAttemptTimeout(milliseconds: number): this {
    if (!Number.isFinite(milliseconds) || milliseconds < 0)
      throw new InvalidError("coordination attempt timeout must be a non-negative finite number")
    if (this.active !== 0) throw new InvalidError("cannot change the timeout during an attempt")
    this.attemptTimeoutMs = milliseconds
    return this
  }

  prepareAcquire(request: KvLease): PreparedMutation {
    return this.prepare(AGDX_KV_LEASE_CODE, encodeKvLease(request), {
      kind: "waitForLeaseExpiry",
      ttlMicros: request.leaseTtlMicros
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

  async acquire(operation: PreparedMutation): Promise<Lease> {
    return lease(await this.execute(operation, AGDX_KV_LEASE_CODE, "acquire"), "leased")
  }

  async renew(operation: PreparedMutation): Promise<Lease> {
    return lease(await this.execute(operation, AGDX_KV_LEASE_RENEW_CODE, "renew"), "renewed")
  }

  async release(operation: PreparedMutation): Promise<boolean> {
    const value = await this.execute(operation, AGDX_KV_RELEASE_CODE, "release")
    if (value.kind !== "released")
      throw new ProtocolError(`kv coordination release: unexpected outcome ${value.kind}`)
    return value.wasHeld
  }

  async casFenced(operation: PreparedMutation): Promise<bigint> {
    const value = await this.execute(operation, AGDX_KV_CAS_FENCED_CODE, "cas_fenced")
    if (value.kind !== "committed")
      throw new ProtocolError(`kv coordination cas_fenced: unexpected outcome ${value.kind}`)
    return value.version
  }

  async get(request: KvGet): Promise<KvEntry | undefined> {
    this.ensureTimeout()
    this.active += 1
    try {
      await this.ready()
      let bytes: Uint8Array
      try {
        bytes = await within(
          this.transport.send(AGDX_KV_GET_CODE, encodeNamed(encodeKvGet(request))),
          this.attemptTimeoutMs,
          "kv coordination get"
        )
      } catch (error) {
        await this.transport.reset()
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

  private async ready(): Promise<void> {
    try {
      await within(
        this.transport.ready?.() ?? Promise.resolve(),
        this.attemptTimeoutMs,
        "kv coordination connect"
      )
    } catch (error) {
      if (!(error instanceof UnsupportedError)) await this.transport.reset()
      throw error
    }
  }

  private async execute(
    operation: PreparedMutation,
    code: number,
    what: string
  ): Promise<KvOutcome> {
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
      await this.ready()
      let bytes: Uint8Array
      try {
        bytes = await within(
          this.transport.send(code, prepared.frame.slice()),
          this.attemptTimeoutMs,
          `kv coordination ${what}`
        )
      } catch (error) {
        await this.transport.reset()
        if (!(error instanceof LaserError) || !isRetryable(error) || isPermissionDenied(error))
          throw error
        throw new AmbiguousMutationError(
          `${what} failed mid-request and requires operation-specific recovery`,
          { cause: error }
        )
      }
      return outcome(bytes)
    } finally {
      this.active -= 1
    }
  }
}

/** Serializes one client's lease acquisitions over a dedicated coordination
 * connection. An ambiguous acquisition waits out the requested TTL before it
 * reports, so a caller cannot race a grant it never saw. The recovery keeps
 * running when the caller stops waiting. */
export class LeaseCoordinator implements AsyncDisposable {
  private queue: Promise<unknown> = Promise.resolve()

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

  acquire(request: KvLease): Promise<Lease> {
    const acquired = this.queue.then(() => this.acquireNow(request))
    this.queue = acquired.catch(() => undefined)
    return acquired
  }

  async close(): Promise<void> {
    await this.client?.close()
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.close()
  }

  private async acquireNow(request: KvLease): Promise<Lease> {
    const client = this.client
    if (client === undefined) {
      throw new ConfigError(
        "lease acquisition needs a connection-backed Laser, use FencedLeaseClient with a dedicated transport for an injected client"
      )
    }
    try {
      return await client.acquire(client.prepareAcquire(request))
    } catch (error) {
      if (error instanceof AmbiguousMutationError) await waitForLeaseExpiry(request.leaseTtlMicros)
      throw error
    }
  }
}

async function waitForLeaseExpiry(leaseTtlMicros: bigint): Promise<void> {
  let millis = (leaseTtlMicros + 999n) / 1_000n
  while (millis > 0n) {
    const chunk = millis > MAX_TIMER_MILLIS ? MAX_TIMER_MILLIS : millis
    await new Promise((resolve) => setTimeout(resolve, Number(chunk)))
    millis -= chunk
  }
}
