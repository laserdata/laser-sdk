import type { BytesLike } from "../client/bytes.js"
import { PresenceConflictError } from "../client/errors.js"
import type { Laser } from "../client/laser.js"
import type { Provenance } from "../provenance/provenance.js"
import type { AgentId } from "../types/ids.js"
import type { AgentCard, CapabilityDescriptor } from "../wire/agent.js"
import type { AgentMessage } from "./reliable-consumer.js"

export class AgentScope {
  constructor(
    private readonly laser: Laser,
    readonly id: AgentId
  ) {}

  send(topic: string, payload: BytesLike, provenance: Provenance): Promise<void> {
    return this.laser.sendAgent(topic, payload, { ...provenance, agent: this.id })
  }

  ask(
    requestTopic: string,
    replyTopic: string,
    payload: BytesLike,
    provenance: Provenance,
    timeoutMs: number,
    signal?: AbortSignal
  ): Promise<AgentMessage> {
    return this.laser.request(
      requestTopic,
      replyTopic,
      payload,
      { ...provenance, agent: this.id },
      timeoutMs,
      signal
    )
  }

  publishCard(card: AgentCard): Promise<void> {
    return this.laser.publishCard(this.id, card)
  }

  async advertise(listenOn: string, capabilities: readonly CapabilityDescriptor[]): Promise<void> {
    try {
      await this.publishCard({ capabilities })
    } catch {
      // Presence remains useful when card publication is unavailable.
    }
    try {
      await this.laser.advertisePresence({ agent: this.id, inbox: listenOn })
    } catch (error) {
      if (error instanceof PresenceConflictError) throw error
    }
  }
}
