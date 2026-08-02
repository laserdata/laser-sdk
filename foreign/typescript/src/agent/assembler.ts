import { AgentKind, type AgentEnvelope, type TokenUsage } from "../wire/agent.js"

export const FINISH_REASON_ABANDONED = "abandoned"
export const FINISH_REASON_GAP = "gap"

export type StreamEvent =
  | { readonly kind: "body"; readonly sequence: bigint; readonly payload: Uint8Array }
  | {
      readonly kind: "finished"
      readonly finishReason?: string
      readonly usage?: TokenUsage
      readonly synthetic: boolean
    }
  | { readonly kind: "failed"; readonly body: Uint8Array }

export class ChunkAssembler {
  private nextSequence = 0n
  private finished = false
  private duplicates = 0n
  private late = 0n

  feed(envelope: AgentEnvelope): readonly StreamEvent[] {
    if (envelope.kind === AgentKind.Chunk) return this.feedChunk(envelope)
    if (envelope.kind !== AgentKind.Error) return []
    if (this.finished) {
      this.late += 1n
      return []
    }
    this.finished = true
    return [{ kind: "failed", body: new Uint8Array(envelope.body) }]
  }

  abandon(): StreamEvent | undefined {
    if (this.finished) return undefined
    this.finished = true
    return { kind: "finished", finishReason: FINISH_REASON_ABANDONED, synthetic: true }
  }

  get isFinished(): boolean {
    return this.finished
  }

  get duplicatesDropped(): bigint {
    return this.duplicates
  }

  get lateDropped(): bigint {
    return this.late
  }

  private feedChunk(envelope: AgentEnvelope): readonly StreamEvent[] {
    const sequence = envelope.sequence
    if (sequence === undefined) return []
    if (this.finished) {
      this.late += 1n
      return []
    }
    if (sequence < this.nextSequence) {
      this.duplicates += 1n
      return []
    }
    if (sequence > this.nextSequence) {
      this.finished = true
      return [{ kind: "finished", finishReason: FINISH_REASON_GAP, synthetic: true }]
    }
    this.nextSequence += 1n
    const events: StreamEvent[] = []
    if (envelope.body.byteLength > 0) {
      events.push({ kind: "body", sequence, payload: new Uint8Array(envelope.body) })
    }
    if (envelope.last) {
      this.finished = true
      events.push({
        kind: "finished",
        ...(envelope.finishReason !== undefined ? { finishReason: envelope.finishReason } : {}),
        ...(envelope.usage !== undefined ? { usage: envelope.usage } : {}),
        synthetic: false
      })
    }
    return events
  }
}
