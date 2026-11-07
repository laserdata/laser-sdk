import type { BytesLike } from "../client/bytes.js"
import { type CallOptions, cancellable, throwIfAborted } from "../client/call-options.js"
import { PresenceConflictError } from "../client/errors.js"
import type { Laser } from "../client/laser.js"
import type { Provenance } from "../provenance/provenance.js"
import type { AgentId } from "../types/ids.js"
import { newAgentPresence, type AgentCard, type CapabilityDescriptor } from "../wire/agent.js"
import type { ContractBuilder } from "./contract.js"
import type { AgentMessage } from "./reliable-consumer.js"
import type { Router } from "./router.js"

export class AgentScope {
  private constructor(
    private readonly laser: Laser,
    readonly id: AgentId
  ) {}

  /** @internal */
  static create(laser: Laser, id: AgentId): AgentScope {
    return new AgentScope(laser, id)
  }

  send(
    topic: string,
    payload: BytesLike,
    provenance: Provenance,
    options: CallOptions = {}
  ): Promise<void> {
    return cancellable(options.signal, "agent send", () =>
      this.laser.sendAgent(topic, payload, { ...provenance, agent: this.id }, options)
    )
  }

  ask(
    requestTopic: string,
    replyTopic: string,
    payload: BytesLike,
    provenance: Provenance,
    timeoutMs: number,
    options: CallOptions = {}
  ): Promise<AgentMessage> {
    return this.laser.request(
      requestTopic,
      replyTopic,
      payload,
      { ...provenance, agent: this.id },
      timeoutMs,
      options
    )
  }

  /** A deadline-bound contract sent as this agent: `laser.contract(router)`
   * with `.from(id)` already applied. */
  contract(router: Router): ContractBuilder {
    return this.laser.contract(router).from(this.id)
  }

  publishCard(card: AgentCard, options: CallOptions = {}): Promise<void> {
    return cancellable(options.signal, "card publish", () => this.laser.publishCard(this.id, card))
  }

  /** Advertise `capabilities`: a durable card naming `listenOn` as this
   * agent's inbox, with no ttl, plus the same inbox as live presence where the
   * server serves it. A spawned agent refreshes a card with a ttl instead. */
  async advertise(
    listenOn: string,
    capabilities: readonly CapabilityDescriptor[],
    options: CallOptions = {}
  ): Promise<void> {
    await advertiseAgent(this.laser, this.id, listenOn, capabilities, undefined, options)
  }
}

/** Publish a card naming `listenOn` as the inbox and fresh for `ttlMicros`
 * (none never expires), then advertise the inbox as live presence. Both are
 * best effort, except that a presence conflict is thrown. Returns the card as
 * published, so a spawned agent can refresh and withdraw it.
 * @internal */
export function advertiseAgent(
  laser: Laser,
  id: AgentId,
  listenOn: string,
  capabilities: readonly CapabilityDescriptor[],
  ttlMicros?: bigint,
  options: CallOptions = {}
): Promise<AgentCard> {
  return cancellable(options.signal, "advertise", async () => {
    const card: AgentCard = {
      capabilities,
      inbox: listenOn,
      ...(ttlMicros !== undefined ? { ttlMicros } : {})
    }
    try {
      await laser.publishCard(id, card)
    } catch {
      // Presence remains useful when card publication is unavailable.
    }
    // A caller that gave up during the card publish advertises no presence.
    throwIfAborted(options.signal, "advertise")
    try {
      await laser.advertisePresence(newAgentPresence(id.wireId(), listenOn))
    } catch (error) {
      if (error instanceof PresenceConflictError) throw error
    }
    return card
  })
}
