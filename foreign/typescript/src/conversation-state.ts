import type { Laser } from "./client/laser.js"
import { ContextAssembler, LastN, type ContextMessage } from "./context.js"
import type { SnapshotStore } from "./snapshot.js"
import type { ConversationId } from "./types/ids.js"
import { foldSnapshotResumeOffset, type FoldSnapshot } from "./wire/snapshot.js"

export type ReplayBound =
  | { readonly kind: "from-offsets"; readonly offsets: ReadonlyMap<number, bigint> }
  | { readonly kind: "last"; readonly count: number }
  | { readonly kind: "full" }

export const FULL_REPLAY: ReplayBound = { kind: "full" }

export function resumeOffsets(snapshot: FoldSnapshot): ReadonlyMap<number, bigint> {
  return new Map(
    [...snapshot.asOf.keys()].map((partition) => [
      partition,
      foldSnapshotResumeOffset(snapshot, partition)
    ])
  )
}

async function load<State>(
  laser: Laser,
  conversation: ConversationId,
  topics: readonly string[],
  bound: ReplayBound,
  initial: State,
  fold: (state: State, message: ContextMessage) => State
): Promise<State> {
  const builder = ContextAssembler.builder(conversation).topics(topics)
  if (bound.kind === "last") builder.policy(new LastN(bound.count))
  if (bound.kind === "full") builder.policy(new LastN(Number.MAX_SAFE_INTEGER))
  if (bound.kind === "from-offsets") {
    builder.policy(new LastN(Number.MAX_SAFE_INTEGER)).fromOffsets(bound.offsets)
  }
  const history = await builder.build().assemble(laser)
  return history.reduce(fold, initial)
}

async function loadWith<State>(
  laser: Laser,
  store: SnapshotStore,
  conversation: ConversationId,
  topics: readonly string[],
  initial: State,
  decodeState: (bytes: Uint8Array) => State,
  fold: (state: State, message: ContextMessage) => State
): Promise<State> {
  const snapshot = await store.latest(conversation)
  return load(
    laser,
    conversation,
    topics,
    snapshot === undefined
      ? FULL_REPLAY
      : { kind: "from-offsets", offsets: resumeOffsets(snapshot) },
    snapshot === undefined ? initial : decodeState(snapshot.state),
    fold
  )
}

export const ConversationState = { load, loadWith } as const
