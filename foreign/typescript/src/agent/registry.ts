import { type LaserTransport } from "../iggy/apache-iggy.js"
import { type CallOptions, abortable, throwIfAborted } from "../client/call-options.js"
import { InvalidError, ProtocolError } from "../client/errors.js"
import type { Cursor } from "../stream/cursor.js"
import { AgentId, PrincipalId } from "../types/ids.js"
import {
  OPERATION_CARD,
  OPERATION_QUARANTINE,
  OPERATION_UNQUARANTINE,
  decodeAgentCard,
  decodeAgentPresence,
  encodeAgentPresence,
  validateAgentCard,
  validateAgentPresence,
  type AgentCard,
  type AgentEnvelope,
  type AgentPresence,
  type SignatureContext
} from "../wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../wire/cbor.js"
import {
  decodeClientMetadataList,
  encodeClientMetadataQuery,
  type ClientMetadata,
  type ClientMetadataQuery
} from "../wire/clients.js"
import { AGDX_GET_CLIENTS_METADATA_CODE, CLIENT_METADATA_OP_VERSION } from "../wire/codes.js"
import { MAX_PAGE_SIZE, MAX_REGISTRY_FACT_TTL_MICROS } from "../wire/limits.js"
import { KeyKind, type KeyRegistry } from "../signing.js"
import { decodeAgentMessage } from "./reliable-consumer.js"
import { compareCodePoints } from "../runtime/compare.js"

const PRESENCE_TTL_MICROS = 2_000_000n
const APPLIED_FACT_CAPACITY = 16_384
// Cards are keyed by the self-asserted envelope source and a card without a
// ttl never ages out, so the map is capped as Rust caps it.
const MAX_REGISTERED_CARDS = 4_096

function u32(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value < 0 || value > 0xffff_ffff) {
    throw new InvalidError(`${field} must be an unsigned 32-bit integer`, { field, value })
  }
  return value
}

export interface RegistryCache {
  cards: Map<string, RegisteredCard>
  quarantined: Set<string>
  offsets: Map<number, bigint>
  presence: Map<string, PresenceEntry>
  presenceReadAtMicros?: bigint
  appliedFacts: Map<string, bigint>
}

export function newRegistryCache(): RegistryCache {
  return {
    cards: new Map(),
    quarantined: new Set(),
    offsets: new Map(),
    presence: new Map(),
    appliedFacts: new Map()
  }
}

function tryAgentId(value: string): AgentId | undefined {
  try {
    return AgentId.new(value)
  } catch {
    return undefined
  }
}

function tryUtf8(bytes: Uint8Array): string | undefined {
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes)
  } catch {
    return undefined
  }
}

export interface RegisteredCard {
  readonly agent: AgentId
  readonly card: AgentCard
  readonly observedAtMicros: bigint
}

export function cardIsFresh(card: RegisteredCard, nowMicros: bigint): boolean {
  const ttl = card.card.ttlMicros
  return ttl === undefined || card.observedAtMicros + ttl >= nowMicros
}

export function cardServes(card: RegisteredCard, skillId: string): boolean {
  return card.card.capabilities.some((capability) => capability.skillId === skillId)
}

export function cardAvailableFor(card: RegisteredCard, skillId: string): boolean {
  const capability = card.card.capabilities.find((entry) => entry.skillId === skillId)
  if (capability === undefined) return false
  const health = capability.health
  return !(health?.kind === "known" && health.name === "Unavailable")
}

export interface PresenceEntry {
  readonly presence: AgentPresence
  readonly principal?: PrincipalId
}

export function applyCard(
  cards: Map<string, RegisteredCard>,
  envelope: AgentEnvelope,
  observedAtMicros: bigint
): boolean {
  if (envelope.operation !== OPERATION_CARD) return false
  const agent = tryAgentId(envelope.source)
  if (agent === undefined) return false
  let card: AgentCard
  try {
    card = decodeAgentCard(
      expectMap(decodeOne(envelope.body, "agent card"), "agent card"),
      "agent card"
    )
    // The card comes off the registry topic, so its caps are enforced on the
    // read side too.
    validateAgentCard(card)
  } catch {
    return false
  }
  if (cards.size >= MAX_REGISTERED_CARDS && !cards.has(agent.asStr())) {
    let oldest: RegisteredCard | undefined
    for (const registered of cards.values()) {
      if (oldest === undefined || registered.observedAtMicros < oldest.observedAtMicros) {
        oldest = registered
      }
    }
    if (oldest !== undefined) cards.delete(oldest.agent.asStr())
  }
  cards.set(agent.asStr(), { agent, card, observedAtMicros })
  return true
}

export function applyQuarantine(quarantined: Set<string>, envelope: AgentEnvelope): boolean {
  const text = tryUtf8(envelope.body)
  const agent = text !== undefined ? tryAgentId(text) : undefined
  if (agent === undefined || quarantined.has(agent.asStr())) return false
  quarantined.add(agent.asStr())
  return true
}

export function liftQuarantine(quarantined: Set<string>, envelope: AgentEnvelope): boolean {
  const text = tryUtf8(envelope.body)
  const agent = text !== undefined ? tryAgentId(text) : undefined
  return agent !== undefined && quarantined.delete(agent.asStr())
}

/** Whether a registry fact was appended before its signed deadline, with no more validity left than the applied-fact set remembers its id for. */
export function factLive(envelope: AgentEnvelope, observedAtMicros: bigint | undefined): boolean {
  const deadline = envelope.deadlineMicros
  return (
    deadline !== undefined &&
    observedAtMicros !== undefined &&
    observedAtMicros < deadline &&
    deadline - observedAtMicros <= BigInt(MAX_REGISTRY_FACT_TTL_MICROS)
  )
}

export function applyPresence(
  presence: Map<string, PresenceEntry>,
  metadata: Uint8Array,
  userId: number | undefined
): boolean {
  let body: AgentPresence
  try {
    body = decodeAgentPresence(
      expectMap(decodeOne(metadata, "agent presence"), "agent presence"),
      "agent presence"
    )
  } catch {
    return false
  }
  const agent = tryAgentId(body.agent)
  if (agent === undefined) return false
  presence.set(agent.asStr(), {
    presence: body,
    ...(userId !== undefined ? { principal: PrincipalId.new(userId) } : {})
  })
  return true
}

export function encodePresence(presence: AgentPresence): Uint8Array {
  validateAgentPresence(presence)
  return encodeNamed(encodeAgentPresence(presence))
}

export interface ClientMetadataPage {
  readonly clients: readonly ClientMetadata[]
  readonly nextCursor?: number
}

export class ClientMetadataRequest {
  private query: ClientMetadataQuery = {
    v: CLIENT_METADATA_OP_VERSION,
    withMetadataOnly: false,
    limit: MAX_PAGE_SIZE
  }

  private constructor(
    private readonly transport: Pick<LaserTransport, "sendManaged" | "sendManagedTo" | "replicaIds">
  ) {}

  /** @internal */
  static create(
    transport: Pick<LaserTransport, "sendManaged" | "sendManagedTo" | "replicaIds">
  ): ClientMetadataRequest {
    return new ClientMetadataRequest(transport)
  }

  withMetadataOnly(value: boolean): this {
    this.query = { ...this.query, withMetadataOnly: value }
    return this
  }

  principal(principal: PrincipalId): this {
    this.query = { ...this.query, userId: principal.get() }
    return this
  }

  limit(limit: number): this {
    this.query = { ...this.query, limit: u32(limit, "limit") }
    return this
  }

  after(clientId: number): this {
    this.query = { ...this.query, afterClientId: u32(clientId, "clientId") }
    return this
  }

  async page(options: CallOptions = {}): Promise<ClientMetadataPage> {
    throwIfAborted(options.signal, "client metadata read")
    const payload = encodeNamed(encodeClientMetadataQuery(this.query))
    const reply = await abortable(
      this.transport.sendManaged(AGDX_GET_CLIENTS_METADATA_CODE, payload),
      options.signal,
      "client metadata read"
    )
    const context = "client metadata reply"
    return decodeClientMetadataList(expectMap(decodeOne(reply, context), context), context)
  }

  async all(options: CallOptions = {}): Promise<readonly ClientMetadata[]> {
    throwIfAborted(options.signal, "client metadata read")
    const clients: ClientMetadata[] = []
    const replicas =
      this.transport.replicaIds === undefined
        ? [undefined]
        : await abortable(this.transport.replicaIds(), options.signal, "client metadata read")
    for (const replica of replicas) {
      let cursor = this.query.afterClientId
      do {
        throwIfAborted(options.signal, "client metadata read")
        const query = { ...this.query, ...(cursor === undefined ? {} : { afterClientId: cursor }) }
        const payload = encodeNamed(encodeClientMetadataQuery(query))
        const reply =
          replica === undefined || this.transport.sendManagedTo === undefined
            ? await abortable(
                this.transport.sendManaged(AGDX_GET_CLIENTS_METADATA_CODE, payload),
                options.signal,
                "client metadata read"
              )
            : (
                await abortable(
                  this.transport.sendManagedTo(AGDX_GET_CLIENTS_METADATA_CODE, payload, {
                    kind: "replica",
                    replica
                  }),
                  options.signal,
                  "client metadata read"
                )
              ).reply
        const context = "client metadata reply"
        const page = decodeClientMetadataList(
          expectMap(decodeOne(reply, context), context),
          context
        )
        clients.push(...page.clients)
        if (page.nextCursor !== undefined && cursor !== undefined && page.nextCursor <= cursor) {
          throw new ProtocolError("the client metadata page did not advance")
        }
        cursor = page.nextCursor
      } while (cursor !== undefined)
    }
    return clients
  }
}

/** Whether the server serves live presence: `yes` when the hello answered,
 * `no` on Apache Iggy, `unknown` when no probe established it.
 * @internal */
export type PresenceServed = "yes" | "no" | "unknown"

export class AgentRegistry {
  private cards: Map<string, RegisteredCard>
  private presence: Map<string, PresenceEntry>
  private quarantined: Set<string>
  private appliedFacts: Map<string, bigint>
  private presenceReadAtMicros: bigint | undefined

  private constructor(
    private readonly cursor: Cursor,
    private readonly cache: RegistryCache,
    private readonly clientMetadata: () => ClientMetadataRequest,
    private readonly nowMicros: () => bigint = () => BigInt(Date.now()) * 1000n,
    private readonly verifier?: KeyRegistry,
    private readonly presenceServed: () => Promise<PresenceServed> = () => Promise.resolve("yes")
  ) {
    this.cards = new Map(cache.cards)
    this.presence = new Map(cache.presence)
    this.quarantined = new Set(cache.quarantined)
    this.appliedFacts = new Map(cache.appliedFacts)
    this.presenceReadAtMicros = cache.presenceReadAtMicros
    this.cursor.fromOffsets(cache.offsets)
  }

  /** @internal */
  static create(
    cursor: Cursor,
    cache: RegistryCache,
    clientMetadata: () => ClientMetadataRequest,
    nowMicros?: () => bigint,
    verifier?: KeyRegistry,
    presenceServed?: () => Promise<PresenceServed>
  ): AgentRegistry {
    return new AgentRegistry(cursor, cache, clientMetadata, nowMicros, verifier, presenceServed)
  }

  async refresh(nowMicros: bigint = this.nowMicros(), options: CallOptions = {}): Promise<number> {
    const signal = options.signal
    const messages = await this.cursor.pollRecords(signal === undefined ? {} : { signal })
    let folded = 0
    for (const message of messages) {
      const decoded = decodeAgentMessage(message)
      if (decoded.kind === "error" || decoded.message.envelope === undefined) continue
      const envelope = decoded.message.envelope
      const observedAtMicros = decoded.observedAtMicros ?? nowMicros
      const operation = envelope.operation
      let applied: boolean
      if (operation === OPERATION_QUARANTINE || operation === OPERATION_UNQUARANTINE) {
        const record = envelope.record?.toString()
        if (record !== undefined && this.appliedFacts.has(record)) continue
        if (!factLive(envelope, decoded.observedAtMicros)) continue
        if (!this.factAuthorized(envelope, decoded.signatureContext, decoded.observedAtMicros)) {
          continue
        }
        if (record !== undefined) this.appliedFacts.set(record, observedAtMicros)
        applied =
          operation === OPERATION_QUARANTINE
            ? applyQuarantine(this.quarantined, envelope)
            : liftQuarantine(this.quarantined, envelope)
      } else {
        applied = applyCard(this.cards, envelope, observedAtMicros)
      }
      if (applied) folded += 1
    }
    for (const [agent, card] of this.cards) {
      if (!cardIsFresh(card, nowMicros)) this.cards.delete(agent)
    }
    const cutoff = nowMicros - BigInt(MAX_REGISTRY_FACT_TTL_MICROS)
    for (const [record, observed] of this.appliedFacts) {
      if (observed < cutoff) this.appliedFacts.delete(record)
    }
    while (this.appliedFacts.size > APPLIED_FACT_CAPACITY) {
      const oldest = this.appliedFacts.keys().next().value
      if (oldest === undefined) break
      this.appliedFacts.delete(oldest)
    }
    this.persist()
    return folded
  }

  private factAuthorized(
    envelope: AgentEnvelope,
    context: SignatureContext | undefined,
    observedAtMicros: bigint | undefined
  ): boolean {
    if (this.verifier === undefined) return true
    if (context === undefined || observedAtMicros === undefined) return false
    try {
      return (
        this.verifier.verifyObservedAt(envelope, context, observedAtMicros).kind ===
        KeyKind.Operator
      )
    } catch {
      return false
    }
  }

  /** Every registered card, ordered by agent id in byte order like Rust, so
   * `RoutePolicy` `any` and ranking ties pick the same agent in every SDK. */
  agents(): readonly RegisteredCard[] {
    return [...this.cards.values()].sort(byAgentId)
  }

  lookup(agent: AgentId): RegisteredCard | undefined {
    return this.cards.get(agent.asStr())
  }

  resolve(skillId: string, nowMicros: bigint = this.nowMicros()): readonly RegisteredCard[] {
    return this.agents().filter(
      (card) =>
        cardAvailableFor(card, skillId) &&
        cardIsFresh(card, nowMicros) &&
        !this.quarantined.has(card.agent.asStr())
    )
  }

  isQuarantined(agent: AgentId): boolean {
    return this.quarantined.has(agent.asStr())
  }

  /** The inbox `agent` advertises in its live presence, else the inbox its
   * card names. The card inbox serves a server without live presence. */
  inboxFor(agent: AgentId): string | undefined {
    return (
      this.presence.get(agent.asStr())?.presence.inbox ?? this.cards.get(agent.asStr())?.card.inbox
    )
  }

  inboxForPrincipal(agent: AgentId, principal: PrincipalId): string | undefined {
    const entry = this.presence.get(agent.asStr())
    return entry?.principal?.get() === principal.get() ? entry.presence.inbox : undefined
  }

  principalFor(agent: AgentId): PrincipalId | undefined {
    return this.presence.get(agent.asStr())?.principal
  }

  async refreshPresence(options: CallOptions = {}): Promise<number> {
    throwIfAborted(options.signal, "presence read")
    const now = this.nowMicros()
    if (
      this.presenceReadAtMicros !== undefined &&
      now - this.presenceReadAtMicros < PRESENCE_TTL_MICROS
    ) {
      return this.presence.size
    }
    // Apache Iggy keeps no connection metadata, so there is no live presence
    // to read and routes resolve to the card inbox. A server not known to
    // serve it that refuses the read is read the same way.
    const served = await abortable(this.presenceServed(), options.signal, "presence read")
    if (served === "no") return 0
    let connections: readonly ClientMetadata[]
    try {
      connections = await this.clientMetadata().withMetadataOnly(true).all(options)
    } catch (error) {
      // A cancelled read proves nothing about whether the server serves presence.
      if (served === "yes" || options.signal?.aborted === true) throw error
      this.presenceReadAtMicros = now
      return 0
    }
    const fresh = new Map<string, PresenceEntry>()
    for (const connection of connections) {
      if (connection.metadata !== undefined) {
        applyPresence(fresh, connection.metadata, connection.userId)
      }
    }
    this.presence = fresh
    this.presenceReadAtMicros = now
    this.persist()
    return fresh.size
  }

  private persist(): void {
    this.cache.cards = new Map(this.cards)
    this.cache.presence = new Map(this.presence)
    this.cache.quarantined = new Set(this.quarantined)
    this.cache.appliedFacts = new Map(this.appliedFacts)
    this.cache.offsets = new Map(this.cursor.offsets)
    if (this.presenceReadAtMicros !== undefined) {
      this.cache.presenceReadAtMicros = this.presenceReadAtMicros
    }
  }
}

function byAgentId(left: RegisteredCard, right: RegisteredCard): number {
  return compareCodePoints(left.agent.asStr(), right.agent.asStr())
}
