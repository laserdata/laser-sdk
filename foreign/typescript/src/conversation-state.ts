import { CodecError, InvalidError, UnsupportedError } from "./client/errors.js"
import { INTERNAL_TRANSPORT } from "./client/internals.js"
import type { Laser } from "./client/laser.js"
import {
  ContextAssembler,
  Checkpoint,
  LastN,
  replayContext,
  type ContextMessage
} from "./context.js"
import type { SnapshotStore } from "./snapshot.js"
import type { ConversationId } from "./types/ids.js"
import { ConversationId as WireConversationId } from "./wire/ids.js"
import {
  foldSnapshotResumeOffset,
  type FoldSnapshot,
  type SnapshotOffset,
  validateFoldSnapshot
} from "./wire/snapshot.js"

export type ReplayBound =
  | {
      readonly kind: "from-offsets"
      readonly offsets: ReadonlyMap<string, ReadonlyMap<number, bigint>>
    }
  | { readonly kind: "from-checkpoint"; readonly checkpoint: Checkpoint }
  | { readonly kind: "at"; readonly checkpoint: Checkpoint }
  | { readonly kind: "last"; readonly count: number }
  | { readonly kind: "full" }

export const FULL_REPLAY: ReplayBound = { kind: "full" }

export function resumeOffsets(snapshot: FoldSnapshot): readonly SnapshotOffset[] {
  return snapshot.asOf.map((entry) => ({
    ...entry,
    offset: entry.offset === (1n << 64n) - 1n ? entry.offset : entry.offset + 1n
  }))
}

/** A snapshot of `state` folded up to `checkpoint` for `conversation` under the
 * fold named `fold`. It records the stream and every checkpointed topic by id
 * and creation time, so a resume refuses a recreated source. Empty partitions
 * are left out. */
export async function snapshotFromCheckpoint(
  laser: Laser,
  conversation: ConversationId,
  fold: string,
  checkpoint: Checkpoint,
  state: Uint8Array
): Promise<FoldSnapshot> {
  const streamName = laser.defaultStream
  if (streamName === undefined) throw new InvalidError("a snapshot requires a default stream")
  const transport = laser[INTERNAL_TRANSPORT]()
  if (transport.findSnapshotStream === undefined || transport.findSnapshotTopic === undefined)
    throw new UnsupportedError("exact snapshot source metadata is unavailable")
  const stream = await transport.findSnapshotStream(streamName)
  if (stream === undefined) throw new InvalidError(`stream \`${streamName}\` does not exist`)
  const asOf: SnapshotOffset[] = []
  for (const [topic, partitions] of checkpoint.topics()) {
    const details = await transport.findSnapshotTopic(streamName, topic)
    if (details === undefined) throw new InvalidError(`topic \`${topic}\` does not exist`)
    for (const [partitionId, next] of partitions) {
      if (next > 0n) {
        asOf.push({
          topicId: details.id,
          topicCreatedAtMicros: details.createdAtMicros,
          partitionId,
          offset: next - 1n
        })
      }
    }
  }
  asOf.sort(
    (left, right) =>
      left.topicId - right.topicId ||
      (left.topicCreatedAtMicros < right.topicCreatedAtMicros
        ? -1
        : left.topicCreatedAtMicros > right.topicCreatedAtMicros
          ? 1
          : 0) ||
      left.partitionId - right.partitionId
  )
  const snapshot: FoldSnapshot = {
    stream: streamName,
    streamId: stream.id,
    streamCreatedAtMicros: stream.createdAtMicros,
    conversation: WireConversationId.parse(conversation.toString()),
    fold,
    asOf,
    state
  }
  validateFoldSnapshot(snapshot)
  return snapshot
}

export async function checkpointFromSnapshot(
  laser: Laser,
  snapshot: FoldSnapshot,
  topics: readonly string[]
): Promise<Checkpoint> {
  validateFoldSnapshot(snapshot)
  if (laser.defaultStream !== snapshot.stream)
    throw new InvalidError("snapshot stream does not match the client")
  const transport = laser[INTERNAL_TRANSPORT]()
  if (transport.findSnapshotStream === undefined || transport.findSnapshotTopic === undefined)
    throw new UnsupportedError("exact snapshot source metadata is unavailable")
  const stream = await transport.findSnapshotStream(snapshot.stream)
  if (stream?.id !== snapshot.streamId || stream.createdAtMicros !== snapshot.streamCreatedAtMicros)
    throw new InvalidError("snapshot stream generation changed")
  const perTopic = new Map<string, ReadonlyMap<number, bigint>>()
  const currentSources = new Set<string>()
  for (const topic of topics) {
    const details = await transport.findSnapshotTopic(snapshot.stream, topic)
    if (details === undefined) throw new InvalidError(`snapshot source topic ${topic} is missing`)
    currentSources.add(`${String(details.id)}:${details.createdAtMicros.toString()}`)
    const offsets = new Map<number, bigint>()
    for (let partition = 0; partition < details.partitions; partition += 1) {
      offsets.set(
        partition,
        foldSnapshotResumeOffset(snapshot, details.id, details.createdAtMicros, partition)
      )
    }
    perTopic.set(topic, offsets)
  }
  if (
    snapshot.asOf.some(
      (entry) =>
        !currentSources.has(`${String(entry.topicId)}:${entry.topicCreatedAtMicros.toString()}`)
    )
  )
    throw new InvalidError("snapshot source topic generation changed")
  return Checkpoint.fromTopicOffsets(perTopic)
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
      fromOffsets: new Map(),
      ...(bound.kind === "from-offsets"
        ? { fromCheckpoint: Checkpoint.fromTopicOffsets(bound.offsets) }
        : {}),
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
  if (snapshot !== undefined && snapshot.conversation.toString() !== conversation.toString())
    throw new InvalidError("snapshot conversation does not match the fold")
  return load(
    laser,
    conversation,
    topics,
    snapshot === undefined
      ? FULL_REPLAY
      : {
          kind: "from-checkpoint",
          checkpoint: await checkpointFromSnapshot(laser, snapshot, topics)
        },
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
