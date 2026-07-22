import assert from "node:assert/strict"
import { test } from "node:test"
import { InvalidError } from "../../src/client/errors.js"
import {
  AgentRegistry,
  ClientMetadataRequest,
  applyCard,
  applyPresence,
  applyQuarantine,
  cardAvailableFor,
  cardIsFresh,
  cardServes,
  liftQuarantine,
  newRegistryCache,
  type PresenceEntry,
  type RegisteredCard
} from "../../src/agent/registry.js"
import type { LaserTransport, PolledMessage } from "../../src/iggy/apache-iggy.js"
import { Cursor } from "../../src/stream/cursor.js"
import { AgentId as SdkAgentId, PrincipalId } from "../../src/types/ids.js"
import {
  OPERATION_CARD,
  OPERATION_QUARANTINE,
  OPERATION_UNQUARANTINE,
  encodeAgentCard,
  encodeAgentEnvelope,
  encodeAgentPresence,
  eventEnvelope,
  newAgentPresence,
  parseAgentId,
  statusEnvelope,
  type AgentCard,
  type AgentEnvelope,
  type CapabilityDescriptor
} from "../../src/wire/agent.js"
import { decodeOne, encodeNamed, expectMap } from "../../src/wire/cbor.js"
import {
  decodeClientMetadataQuery,
  encodeClientMetadataList,
  type ClientMetadataQuery
} from "../../src/wire/clients.js"
import { ConversationId, RecordId } from "../../src/wire/ids.js"

class RegistryTransport implements LaserTransport {
  readonly kind = "apache-iggy" as const
  readonly managedQueries: ClientMetadataQuery[] = []
  readonly pollPages: (readonly PolledMessage[])[] = []
  metadataPages: ((query: ClientMetadataQuery) => Uint8Array) | undefined

  get iggyClient(): never {
    throw new Error("unused")
  }

  sendManaged(_code: number, payload: Uint8Array): Promise<Uint8Array> {
    const context = "metadata query"
    const query = decodeClientMetadataQuery(
      expectMap(decodeOne(payload, context), context),
      context
    )
    this.managedQueries.push(query)
    return Promise.resolve(
      this.metadataPages?.(query) ?? encodeNamed(encodeClientMetadataList({ clients: [] }))
    )
  }

  ensureStream(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  ensureTopic(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  findTopicPartitionCount(): Promise<number | undefined> {
    return Promise.resolve(1)
  }

  getTopicPartitionCount(): Promise<number> {
    return Promise.resolve(1)
  }

  sendMessages(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  sendMessageWithHeaders(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  sendMessagesWithHeaders(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  pollMessages(): Promise<readonly PolledMessage[]> {
    return Promise.resolve(this.pollPages.shift() ?? [])
  }

  storeOffset(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  joinConsumerGroup(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  leaveConsumerGroup(): Promise<void> {
    return Promise.reject(new Error("unused"))
  }

  close(): Promise<void> {
    return Promise.resolve()
  }
}

function polled(envelope: AgentEnvelope, offset: bigint): PolledMessage {
  return {
    payload: encodeNamed(encodeAgentEnvelope(envelope)),
    partitionId: 0,
    offset,
    headers: new Map()
  }
}

function descriptor(
  skillId: string,
  health?: CapabilityDescriptor["health"]
): CapabilityDescriptor {
  return { skillId, ...(health !== undefined ? { health } : {}) }
}

function cardEnvelope(
  source: string,
  skills: readonly string[],
  ttlMicros?: bigint
): AgentEnvelope {
  const card: AgentCard = {
    capabilities: skills.map((skill) => descriptor(skill)),
    ...(ttlMicros !== undefined ? { ttlMicros } : {})
  }
  const envelope = statusEnvelope(
    RecordId.fromU128(1n),
    ConversationId.fromU128(2n),
    parseAgentId(source),
    OPERATION_CARD
  )
  return { ...envelope, body: encodeNamed(encodeAgentCard(card)) }
}

void test("given_cards_when_folded_then_should_keep_the_latest_per_agent_and_resolve_by_capability", () => {
  const cards = new Map<string, RegisteredCard>()

  assert.equal(applyCard(cards, cardEnvelope("planner", ["plan"]), 100n), true)
  assert.equal(applyCard(cards, cardEnvelope("worker", ["diagnose"], 50n), 100n), true)
  assert.equal(cards.size, 2)

  // A re-publish replaces the agent's card (latest wins).
  assert.equal(applyCard(cards, cardEnvelope("planner", ["plan", "summarize"]), 200n), true)
  assert.equal(cards.size, 2)
  const planner = cards.get("planner")
  assert.ok(planner !== undefined)
  assert.equal(planner.observedAtMicros, 200n)
  assert.equal(cardServes(planner, "summarize"), true)

  // A non-card envelope is skipped.
  const notACard = eventEnvelope(
    RecordId.fromU128(9n),
    ConversationId.fromU128(9n),
    parseAgentId("x"),
    new TextEncoder().encode("{}")
  )
  assert.equal(applyCard(cards, notACard, 300n), false)

  // The worker's card (ttl 50, observed at 100) is stale past 150.
  const worker = cards.get("worker")
  assert.ok(worker !== undefined)
  assert.equal(cardIsFresh(worker, 150n), true)
  assert.equal(cardIsFresh(worker, 151n), false)
})

void test("given_a_quarantine_record_when_folded_then_should_mark_the_named_agent", () => {
  const quarantined = new Set<string>()

  const envelope = {
    ...statusEnvelope(
      RecordId.fromU128(1n),
      ConversationId.fromU128(2n),
      parseAgentId("operator"),
      OPERATION_QUARANTINE
    ),
    body: new TextEncoder().encode("worker")
  }
  assert.equal(applyQuarantine(quarantined, envelope), true)
  assert.ok(quarantined.has("worker"))

  // A second identical record is a no-op (already quarantined).
  assert.equal(applyQuarantine(quarantined, envelope), false)

  // A body that is not a valid agent id is skipped, not poisoned in.
  const bad = { ...envelope, body: new Uint8Array([0xff, 0xfe]) }
  assert.equal(applyQuarantine(quarantined, bad), false)

  // An un-quarantine record lifts it, returning the agent to routing.
  const lift = {
    ...statusEnvelope(
      RecordId.fromU128(3n),
      ConversationId.fromU128(4n),
      parseAgentId("operator"),
      OPERATION_UNQUARANTINE
    ),
    body: new TextEncoder().encode("worker")
  }
  assert.equal(liftQuarantine(quarantined, lift), true)
  assert.ok(!quarantined.has("worker"))
  // Lifting an agent that is not quarantined is a no-op.
  assert.equal(liftQuarantine(quarantined, lift), false)
})

void test("given_advertised_health_when_checking_availability_then_should_skip_only_unavailable", () => {
  const withHealth = (health?: CapabilityDescriptor["health"]): RegisteredCard => ({
    agent: SdkAgentId.new("a"),
    card: { capabilities: [descriptor("diagnose", health)] },
    observedAtMicros: 0n
  })
  assert.equal(cardAvailableFor(withHealth({ kind: "known", name: "Healthy" }), "diagnose"), true)
  assert.equal(cardAvailableFor(withHealth({ kind: "known", name: "Degraded" }), "diagnose"), true)
  assert.equal(cardAvailableFor(withHealth(undefined), "diagnose"), true)
  assert.equal(
    cardAvailableFor(withHealth({ kind: "known", name: "Unavailable" }), "diagnose"),
    false
  )
  // A skill the card never advertises is not available.
  assert.equal(cardAvailableFor(withHealth({ kind: "known", name: "Healthy" }), "other"), false)
})

void test("given_connection_metadata_when_folded_as_presence_then_should_key_by_agent_and_skip_non_presence", () => {
  const presence = new Map<string, PresenceEntry>()

  // A presence body declaring an inbox is folded, keyed by its agent id,
  // carrying the connection's authenticated user_id.
  const body = encodeNamed(
    encodeAgentPresence(newAgentPresence(parseAgentId("worker"), "worker.work"))
  )
  assert.equal(applyPresence(presence, body, 7), true)
  assert.equal(presence.get("worker")?.presence.inbox, "worker.work")
  assert.equal(presence.get("worker")?.principal?.get(), 7)

  // Re-advertising moves the inbox (last write wins), the per-workflow case.
  const moved = encodeNamed(
    encodeAgentPresence(newAgentPresence(parseAgentId("worker"), "worker.work.v2"))
  )
  assert.equal(applyPresence(presence, moved, 7), true)
  assert.equal(presence.get("worker")?.presence.inbox, "worker.work.v2")

  // An opaque non-presence blob (a regular app's metadata) is skipped, not
  // poisoned into the map.
  assert.equal(
    applyPresence(presence, new TextEncoder().encode("not a presence body"), undefined),
    false
  )
  assert.equal(presence.size, 1)
})

void test("given_paged_client_metadata_when_all_is_called_then_should_follow_the_server_cursor", async () => {
  const transport = new RegistryTransport()
  transport.metadataPages = (query) =>
    encodeNamed(
      encodeClientMetadataList(
        query.afterClientId === undefined
          ? {
              clients: [
                {
                  clientId: 5,
                  userId: 7,
                  transport: 1,
                  address: "one",
                  consumerGroupsCount: 0
                }
              ],
              nextCursor: 5
            }
          : {
              clients: [
                {
                  clientId: 9,
                  userId: 8,
                  transport: 1,
                  address: "two",
                  consumerGroupsCount: 1
                }
              ]
            }
      )
    )

  const clients = await new ClientMetadataRequest(transport)
    .withMetadataOnly(true)
    .principal(PrincipalId.new(7))
    .limit(1)
    .all()

  assert.deepEqual(
    clients.map((client) => client.clientId),
    [5, 9]
  )
  assert.equal(transport.managedQueries.length, 2)
  const [firstQuery, secondQuery] = transport.managedQueries
  assert.ok(firstQuery !== undefined)
  assert.ok(secondQuery !== undefined)
  assert.equal(firstQuery.withMetadataOnly, true)
  assert.equal(firstQuery.userId, 7)
  assert.equal(secondQuery.afterClientId, 5)
})

void test("given_invalid_client_metadata_page_numbers_when_set_then_should_reject_locally", () => {
  const request = new ClientMetadataRequest(new RegistryTransport())
  assert.throws(() => request.limit(-1), InvalidError)
  assert.throws(() => request.limit(1.5), InvalidError)
  assert.throws(() => request.after(0x1_0000_0000), InvalidError)
})

void test("given_registry_records_when_refreshed_then_should_cache_offsets_and_exclude_quarantine", async () => {
  const transport = new RegistryTransport()
  const quarantine = {
    ...statusEnvelope(
      RecordId.fromU128(3n),
      ConversationId.fromU128(4n),
      parseAgentId("operator"),
      OPERATION_QUARANTINE
    ),
    body: new TextEncoder().encode("planner")
  }
  transport.pollPages.push([polled(cardEnvelope("planner", ["plan"]), 0n), polled(quarantine, 1n)])
  const cache = newRegistryCache()
  const registry = new AgentRegistry(
    new Cursor(transport, "stream", "agent.registry", [0]),
    cache,
    () => new ClientMetadataRequest(transport),
    () => 100n
  )

  assert.equal(await registry.refresh(), 2)
  assert.equal(registry.lookup(SdkAgentId.new("planner"))?.agent.asString(), "planner")
  assert.equal(registry.resolve("plan").length, 0)
  assert.equal(registry.isQuarantined(SdkAgentId.new("planner")), true)
  assert.equal(cache.offsets.get(0), 2n)

  const resumed = new AgentRegistry(
    new Cursor(new RegistryTransport(), "stream", "agent.registry", [0]),
    cache,
    () => new ClientMetadataRequest(transport),
    () => 100n
  )
  assert.equal(resumed.isQuarantined(SdkAgentId.new("planner")), true)
  assert.equal(resumed.lookup(SdkAgentId.new("planner"))?.agent.asString(), "planner")
})

void test("given_live_presence_when_refreshed_within_ttl_then_should_reuse_the_cached_page", async () => {
  const transport = new RegistryTransport()
  const metadata = encodeNamed(
    encodeAgentPresence(newAgentPresence(parseAgentId("worker"), "worker.inbox"))
  )
  transport.metadataPages = () =>
    encodeNamed(
      encodeClientMetadataList({
        clients: [
          {
            clientId: 1,
            userId: 7,
            transport: 1,
            address: "worker",
            consumerGroupsCount: 1,
            metadata
          }
        ]
      })
    )
  let now = 10_000_000n
  const registry = new AgentRegistry(
    new Cursor(transport, "stream", "agent.registry", [0]),
    newRegistryCache(),
    () => new ClientMetadataRequest(transport),
    () => now
  )

  assert.equal(await registry.refreshPresence(), 1)
  assert.equal(registry.inboxFor(SdkAgentId.new("worker")), "worker.inbox")
  assert.equal(
    registry.inboxForPrincipal(SdkAgentId.new("worker"), PrincipalId.new(7)),
    "worker.inbox"
  )
  now += 1_000_000n
  assert.equal(await registry.refreshPresence(), 1)
  assert.equal(transport.managedQueries.length, 1)
})
