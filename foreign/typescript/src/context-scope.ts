import type { Laser } from "./client/laser.js"
import { ContextAssembler, LastN, type ContextMessage, type ContextPolicy } from "./context.js"
import { ConversationState, type ReplayBound } from "./conversation-state.js"
import type { SnapshotStore } from "./snapshot.js"
import type { BytesLike } from "./client/bytes.js"
import type { ConversationId } from "./types/ids.js"
import type { MemoryHandle } from "./memory/handle.js"
import type { Feedback, MemoryId } from "./memory/types.js"

export class ContextScope {
  constructor(
    private readonly laser: Laser,
    readonly conversation: ConversationId
  ) {}

  append(topic: string, payload: BytesLike): Promise<void> {
    return this.laser.sendAgent(topic, payload, { conversationId: this.conversation })
  }

  fetch(topics: readonly string[], count: number): Promise<readonly ContextMessage[]> {
    return this.fetchWith(topics, new LastN(count))
  }

  fetchWith(topics: readonly string[], policy: ContextPolicy): Promise<readonly ContextMessage[]> {
    return ContextAssembler.builder(this.conversation)
      .topics(topics)
      .policy(policy)
      .build()
      .assemble(this.laser)
  }

  async block(topics: readonly string[], count: number): Promise<string> {
    const messages = await this.fetch(topics, count)
    return messages.map((message) => new TextDecoder().decode(message.payload)).join("\n")
  }

  memory(namespace: string): ScopedMemory {
    return new ScopedMemory(this.laser.memory(namespace), this.conversation)
  }

  state<State>(
    topics: readonly string[],
    bound: ReplayBound,
    initial: State,
    fold: (state: State, message: ContextMessage) => State
  ): Promise<State> {
    return ConversationState.load(this.laser, this.conversation, topics, bound, initial, fold)
  }

  stateWith<State>(
    store: SnapshotStore,
    topics: readonly string[],
    initial: State,
    decodeState: (bytes: Uint8Array) => State,
    fold: (state: State, message: ContextMessage) => State
  ): Promise<State> {
    return ConversationState.loadWith(
      this.laser,
      store,
      this.conversation,
      topics,
      initial,
      decodeState,
      fold
    )
  }
}

export class ScopedMemory {
  constructor(
    private readonly handle: MemoryHandle,
    private readonly conversation: ConversationId
  ) {}

  remember(payload: Uint8Array) {
    return this.handle.remember(payload).conversation(this.conversation)
  }

  recall() {
    return this.handle.recall().conversation(this.conversation)
  }

  context(tokenBudget?: number): Promise<string> {
    return this.handle.context(
      { conversation: this.conversation },
      tokenBudget === undefined ? {} : { tokenBudget }
    )
  }

  forget(id: MemoryId): Promise<void> {
    return this.handle.forget({ conversation: this.conversation }, id)
  }

  improve(feedback: Feedback): Promise<MemoryId> {
    return this.handle.improve({ conversation: this.conversation }, feedback)
  }
}
