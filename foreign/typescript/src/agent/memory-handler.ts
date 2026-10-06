import type { MemoryHandle } from "../memory/handle.js"
import type { MemoryKind } from "../memory/types.js"
import type { AgentCtx } from "./context.js"
import type { AgentHandler, AgentMessage } from "./reliable-consumer.js"

/**
 * Wraps an agent handler so each handled message becomes queryable memory:
 * after the inner handler succeeds, the message is remembered under its
 * conversation. A handler reads the memory back through recall.
 *
 * A memory write that fails does not fail the turn. The message was already
 * handled, and running it again would repeat the handler's effects.
 */
export class MemoryHandler implements AgentHandler {
  private rememberKind: MemoryKind | undefined

  /** Wraps `handler`, writing to `memory`. Remembering is off until
   * `autoRemember` selects a kind. */
  constructor(
    private readonly handler: AgentHandler,
    private readonly memory: MemoryHandle
  ) {}

  /** Remembers each successfully handled message under its conversation,
   * stored as `kind` (`MemoryKind.Message` for a chat turn). */
  autoRemember(kind: MemoryKind): this {
    this.rememberKind = kind
    return this
  }

  async handle(message: AgentMessage, context: AgentCtx): Promise<void> {
    await this.handler.handle(message, context)
    if (this.rememberKind === undefined) return
    const remember = this.memory
      .remember(message.payload.slice())
      .scope(message.provenance.conversationId)
      .kind(this.rememberKind)
    if (message.provenance.agent !== undefined) remember.agent(message.provenance.agent)
    try {
      await remember.send()
    } catch {
      // A handled turn stays handled when its memory copy fails to land.
    }
  }
}
