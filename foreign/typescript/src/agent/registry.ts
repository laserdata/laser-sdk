import { type LaserTransport } from "../iggy/apache-iggy.js"
import { InvalidError } from "../client/errors.js"
import type { Cursor } from "../stream/cursor.js"
import { AgentId, PrincipalId } from "../types/ids.js"
import {
  OPERATION_CARD,
  OPERATION_QUARANTINE,
  OPERATION_UNQUARANTINE,
  decodeAgentCard,
  decodeAgentPresence,
  decodeAgentEnvelope,
  encodeAgentPresence,
  parseAgentId,
  validateAgentPresence,
  type AgentCard,
  type AgentEnvelope,
  type AgentPresence
} from "../wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../wire/cbor.js"
import {
  decodeClientMetadataList,
  encodeClientMetadataQuery,
  type ClientMetadata,
  type ClientMetadataQuery
} from "../wire/clients.js"
import { AGDX_GET_CLIENTS_METADATA_CODE, CLIENT_METADATA_OP_VERSION } from "../wire/codes.js"
import { MAX_PAGE_SIZE } from "../wire/limits.js"
import { KeyKind, type KeyRegistry } from "../signing.js"

const PRESENCE_TTL_MICROS = 2_000_000n

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
  appliedFacts: Set<string>
}

export function newRegistryCache(): RegistryCache {
  return {
    cards: new Map(),
    quarantined: new Set(),
    offsets: new Map(),
    presence: new Map(),
    appliedFacts: new Set()
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

// One agent's latest card, with the time the registry folded it in. A
// card older than its `AgentCard.ttlMicros` is treated as a dead agent.
export interface RegisteredCard {
  readonly agent: AgentId
  readonly card: AgentCard
  readonly observedAtMicros: bigint
}

// Whether `card` is still fresh at `nowMicros`: a card with no ttl never
// expires, otherwise it is fresh while `observedAtMicros + ttl >= now`.
export function cardIsFresh(card: RegisteredCard, nowMicros: bigint): boolean {
  const ttl = card.card.ttlMicros
  return ttl === undefined || card.observedAtMicros + ttl >= nowMicros
}

// Whether `card` advertises `skillId`.
export function cardServes(card: RegisteredCard, skillId: string): boolean {
  return card.card.capabilities.some((capability) => capability.skillId === skillId)
}

// Whether the agent's advertised health for `skillId` permits routing: an
// `Unavailable` skill is skipped, everything else (healthy, degraded, or
// unspecified) stays routable, and a skill the card does not advertise is
// not available. The health-aware routing gate, so a self-declared
// unavailable agent is not handed work.
export function cardAvailableFor(card: RegisteredCard, skillId: string): boolean {
  const capability = card.card.capabilities.find((entry) => entry.skillId === skillId)
  if (capability === undefined) return false
  const health = capability.health
  return !(health?.kind === "known" && health.name === "Unavailable")
}

// A folded presence and the authenticated principal of the connection
// that advertised it. The principal is the identity the substrate
// authenticated (the trustworthy half). The agent id and inbox inside
// the presence are the connection's own claim, so an inbox is only safe
// to trust as that principal's.
export interface PresenceEntry {
  readonly presence: AgentPresence
  readonly principal?: PrincipalId
}

// Agent-keyed maps/sets here are keyed by `AgentId.asString()`, not the
// `AgentId` object itself: Rust's `HashMap<AgentId, _>` hashes by value,
// but a plain JS `Map`/`Set` compares object keys by identity, and
// `AgentId` mints a fresh instance on every parse, so keying by the
// object would never find a prior entry for the same agent. The
// `Map<string, FenceEntry>`/`Set<string>` idiom `consumer.ts` already
// uses for the same reason.

// Apply one envelope to the card map: when it is a status/card record
// from a resolvable agent carrying a decodable `AgentCard` body, store
// it as that agent's latest card. Returns whether a card was applied.
// Pure (the clock is passed in), so the fold is unit-testable without a
// live log.
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
  } catch {
    return false
  }
  cards.set(agent.asString(), { agent, card, observedAtMicros })
  return true
}

// Apply one envelope to the quarantine set: a status/quarantine record
// whose body is a valid agent id marks that agent quarantined. Returns
// whether one was applied. Pure, so the fold is unit-testable without a
// live log.
export function applyQuarantine(quarantined: Set<string>, envelope: AgentEnvelope): boolean {
  const text = tryUtf8(envelope.body)
  const agent = text !== undefined ? tryAgentId(text) : undefined
  if (agent === undefined || quarantined.has(agent.asString())) return false
  quarantined.add(agent.asString())
  return true
}

// Apply one envelope to the quarantine set as a lift: an unquarantine
// record whose body is a valid agent id returns that agent to routing.
// Returns whether a quarantine was actually lifted. Pure, so the fold is
// unit-testable without a live log.
export function liftQuarantine(quarantined: Set<string>, envelope: AgentEnvelope): boolean {
  const text = tryUtf8(envelope.body)
  const agent = text !== undefined ? tryAgentId(text) : undefined
  return agent !== undefined && quarantined.delete(agent.asString())
}

// Apply one connection's advertised metadata to the presence map: when
// it decodes as an `AgentPresence` claiming a valid agent id, store it
// keyed by that agent (last write wins). Returns whether a presence was
// applied. A blob that is not a presence body is skipped (the metadata
// channel is opaque). Pure, so the decode-and-key fold is unit-testable
// without a live connection.
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
  presence.set(agent.asString(), {
    presence: body,
    ...(userId !== undefined ? { principal: PrincipalId.new(userId) } : {})
  })
  return true
}

export interface AgentPresenceInput {
  readonly agent: AgentId
  readonly inbox?: string
}

export function encodePresenceInput(input: AgentPresenceInput): Uint8Array {
  const presence: AgentPresence = {
    v: 1,
    agent: parseAgentId(input.agent.asString()),
    ...(input.inbox !== undefined ? { inbox: input.inbox } : {})
  }
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

  constructor(private readonly transport: Pick<LaserTransport, "sendManaged">) {}

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

  async page(): Promise<ClientMetadataPage> {
    const payload = encodeNamed(encodeClientMetadataQuery(this.query))
    const reply = await this.transport.sendManaged(AGDX_GET_CLIENTS_METADATA_CODE, payload)
    const context = "client metadata reply"
    return decodeClientMetadataList(expectMap(decodeOne(reply, context), context), context)
  }

  async all(): Promise<readonly ClientMetadata[]> {
    const clients: ClientMetadata[] = []
    let cursor = this.query.afterClientId
    do {
      const request = new ClientMetadataRequest(this.transport)
      request.query = {
        ...this.query,
        ...(cursor !== undefined ? { afterClientId: cursor } : {})
      }
      const page = await request.page()
      clients.push(...page.clients)
      cursor = page.nextCursor
    } while (cursor !== undefined)
    return clients
  }
}

export class AgentRegistry {
  private cards: Map<string, RegisteredCard>
  private presence: Map<string, PresenceEntry>
  private quarantined: Set<string>
  private appliedFacts: Set<string>
  private presenceReadAtMicros: bigint | undefined

  constructor(
    private readonly cursor: Cursor,
    private readonly cache: RegistryCache,
    private readonly clientMetadata: () => ClientMetadataRequest,
    private readonly nowMicros: () => bigint = () => BigInt(Date.now()) * 1000n,
    private readonly verifier?: KeyRegistry
  ) {
    this.cards = new Map(cache.cards)
    this.presence = new Map(cache.presence)
    this.quarantined = new Set(cache.quarantined)
    this.appliedFacts = new Set(cache.appliedFacts)
    this.presenceReadAtMicros = cache.presenceReadAtMicros
    this.cursor.fromOffsets(cache.offsets)
  }

  async refresh(nowMicros: bigint = this.nowMicros()): Promise<number> {
    const messages = await this.cursor.poll()
    let folded = 0
    for (const message of messages) {
      let envelope: AgentEnvelope
      try {
        const context = "agent registry record"
        envelope = decodeAgentEnvelope(
          expectMap(decodeOne(message.payload, context), context),
          context
        )
      } catch {
        continue
      }
      const operation = envelope.operation
      let applied: boolean
      if (operation === OPERATION_QUARANTINE || operation === OPERATION_UNQUARANTINE) {
        const record = envelope.record?.toString()
        if (record !== undefined && this.appliedFacts.has(record)) continue
        if (!this.factAuthorized(envelope, nowMicros)) continue
        if (record !== undefined) this.appliedFacts.add(record)
        applied =
          operation === OPERATION_QUARANTINE
            ? applyQuarantine(this.quarantined, envelope)
            : liftQuarantine(this.quarantined, envelope)
      } else {
        applied = applyCard(this.cards, envelope, nowMicros)
      }
      if (applied) folded += 1
    }
    for (const [agent, card] of this.cards) {
      if (!cardIsFresh(card, nowMicros)) this.cards.delete(agent)
    }
    this.persist()
    return folded
  }

  private factAuthorized(envelope: AgentEnvelope, nowMicros: bigint): boolean {
    if (this.verifier === undefined) return true
    try {
      return this.verifier.verifyAt(envelope, nowMicros).kind === KeyKind.Operator
    } catch {
      return false
    }
  }

  agents(): readonly RegisteredCard[] {
    return [...this.cards.values()]
  }

  lookup(agent: AgentId): RegisteredCard | undefined {
    return this.cards.get(agent.asString())
  }

  resolve(skillId: string, nowMicros: bigint = this.nowMicros()): readonly RegisteredCard[] {
    return [...this.cards.values()].filter(
      (card) =>
        cardAvailableFor(card, skillId) &&
        cardIsFresh(card, nowMicros) &&
        !this.quarantined.has(card.agent.asString())
    )
  }

  isQuarantined(agent: AgentId): boolean {
    return this.quarantined.has(agent.asString())
  }

  inboxFor(agent: AgentId): string | undefined {
    return this.presence.get(agent.asString())?.presence.inbox
  }

  inboxForPrincipal(agent: AgentId, principal: PrincipalId): string | undefined {
    const entry = this.presence.get(agent.asString())
    return entry?.principal?.get() === principal.get() ? entry.presence.inbox : undefined
  }

  principalFor(agent: AgentId): PrincipalId | undefined {
    return this.presence.get(agent.asString())?.principal
  }

  async refreshPresence(): Promise<number> {
    const now = this.nowMicros()
    if (
      this.presenceReadAtMicros !== undefined &&
      now - this.presenceReadAtMicros < PRESENCE_TTL_MICROS
    ) {
      return this.presence.size
    }
    const connections = await this.clientMetadata().withMetadataOnly(true).all()
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
    this.cache.appliedFacts = new Set(this.appliedFacts)
    this.cache.offsets = new Map(this.cursor.offsets)
    if (this.presenceReadAtMicros !== undefined) {
      this.cache.presenceReadAtMicros = this.presenceReadAtMicros
    }
  }
}
