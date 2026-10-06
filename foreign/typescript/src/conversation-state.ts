import { CodecError } from "./client/errors.js"
import type { Laser } from "./client/laser.js"
import {
  ContextAssembler,
  LastN,
  replayContext,
  type Checkpoint,
  type ContextMessage
} from "./context.js"
import type { SnapshotStore } from "./snapshot.js"
import type { ConversationId } from "./types/ids.js"
import { foldSnapshotResumeOffset, type FoldSnapshot } from "./wire/snapshot.js"

export type ReplayBound =
  | { readonly kind: "from-offsets"; readonly offsets: ReadonlyMap<number, bigint> }
  | { readonly kind: "from-checkpoint"; readonly checkpoint: Checkpoint }
  | { readonly kind: "at"; readonly checkpoint: Checkpoint }
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

// `last` reads the newest context window like a context read. Every other
// bound folds its whole range, read in chunks up to the tail seen when the
// replay starts or up to the checkpoint.
async function load<State>(
  laser: Laser,
  conversation: ConversationId,
  topics: readonly string[],
  bound: ReplayBound,
  initial: State,
  fold: (state: State, message: ContextMessage) => State
): Promise<State> {
  if (bound.kind === "last") {
    const history = await ContextAssembler.builder()
      .conversationId(conversation)
      .topics(topics)
      .policy(new LastN(bound.count))
      .build()
      .assemble(laser)
    return history.reduce(fold, initial)
  }
  const history = await replayContext(
    {
      conversation,
      acrossSubconversations: false,
      topics,
      policy: new LastN(Number.MAX_SAFE_INTEGER),
      fromOffsets: bound.kind === "from-offsets" ? bound.offsets : new Map(),
      ...(bound.kind === "from-checkpoint" ? { fromCheckpoint: bound.checkpoint } : {}),
      ...(bound.kind === "at" ? { toCheckpoint: bound.checkpoint } : {})
    },
    laser
  )
  return history.reduce(fold, initial)
}

// The newest snapshot's state (JSON by default, like the Rust SDK) plus a
// replay of only the tail past it.
async function loadWith<State>(
  laser: Laser,
  store: SnapshotStore,
  conversation: ConversationId,
  topics: readonly string[],
  initial: State,
  fold: (state: State, message: ContextMessage) => State,
  decodeState: (bytes: Uint8Array) => State = (bytes) => decodeJsonState(bytes) as State
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

function decodeJsonState(bytes: Uint8Array): unknown {
  try {
    return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as unknown
  } catch (error) {
    throw new CodecError(`decode snapshot state: ${String(error)}`, "json", "decode", {
      cause: error
    })
  }
}

export const ConversationState = { load, loadWith } as const
