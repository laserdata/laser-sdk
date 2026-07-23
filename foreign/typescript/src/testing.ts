import { AgentContext } from "./agent/context.js"
import type { AgentMessage } from "./agent/reliable-consumer.js"
import { ADVERTISED_INBOX_ROUTE, type InboxRoute } from "./agent/router.js"
import type { BytesLike } from "./client/bytes.js"
import { ownedBytes } from "./client/bytes.js"
import type { Laser } from "./client/laser.js"
import type { Provenance } from "./provenance/provenance.js"
import type { AgentId } from "./types/ids.js"

export { TestClock } from "./runtime/clock.js"
export { InMemoryStore } from "./state-store.js"

export function agentMessage(payload: BytesLike, provenance: Provenance): AgentMessage {
  return {
    provenance,
    payload: ownedBytes(payload),
    id: { partitionId: 0, offset: 0n }
  }
}

export function agentContext(
  laser: Laser,
  message: AgentMessage,
  options: {
    readonly agent?: AgentId
    readonly respondOn?: string
    readonly inboxRoute?: InboxRoute
  } = {}
): AgentContext {
  return new AgentContext(laser, message, {
    ...options,
    inboxRoute: options.inboxRoute ?? ADVERTISED_INBOX_ROUTE
  })
}
