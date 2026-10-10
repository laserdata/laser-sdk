import { decodeOne, encodeNamed, expectMap } from "./wire/cbor.js"
import { decodeFoldSnapshot, encodeFoldSnapshot, type FoldSnapshot } from "./wire/snapshot.js"
import type { ConversationId as SdkConversationId } from "./types/ids.js"
import type { ConversationId } from "./wire/ids.js"
import { NoStreamError, ProtocolError } from "./client/errors.js"
import { INTERNAL_TRANSPORT } from "./client/internals.js"
import type { Laser } from "./client/laser.js"

export const DEFAULT_SNAPSHOT_NAMESPACE = "agent.snapshots"
export const DEFAULT_SNAPSHOT_TOPIC = "agent.snapshots"

const SCAN_BATCH = 256n

export interface SnapshotStore {
  latest(conversation: SdkConversationId): Promise<FoldSnapshot | undefined>
  save(snapshot: FoldSnapshot): Promise<void>
}

/** Encode a fold snapshot to the named-field CBOR bytes used by Rust. */
export function encodeSnapshot(snapshot: FoldSnapshot): Uint8Array {
  return encodeNamed(encodeFoldSnapshot(snapshot))
}

/** Decode a fold snapshot from its stored bytes. */
export function decodeSnapshot(payload: Uint8Array): FoldSnapshot {
  const context = "fold snapshot"
  return decodeFoldSnapshot(expectMap(decodeOne(payload, context), context), context)
}

export class TopicSnapshotStore implements SnapshotStore {
  constructor(
    private readonly laser: Laser,
    private readonly fold: string,
    private readonly topic = DEFAULT_SNAPSHOT_TOPIC
  ) {}

  /** Walks each partition backward from its tail and stops at the newest
   * record for the conversation, so a hit costs the distance to the last
   * checkpoint. A conversation with no snapshot walks the whole topic, so keep
   * the snapshots topic on retention. */
  async latest(conversation: SdkConversationId): Promise<FoldSnapshot | undefined> {
    const stream = this.laser.defaultStream
    if (stream === undefined) {
      throw new NoStreamError(
        "a snapshot lookup requires a default stream, use connectWithStream() or withDefaultStream()"
      )
    }
    const transport = this.laser[INTERNAL_TRANSPORT]()
    const partitions = await transport.findTopicPartitionCount(stream, this.topic)
    if (partitions === undefined) return undefined
    let newest:
      | {
          readonly timestamp: bigint
          readonly partition: number
          readonly offset: bigint
          readonly snapshot: FoldSnapshot
        }
      | undefined
    for (let partitionId = 0; partitionId < partitions; partitionId += 1) {
      const target = { kind: "single", partitionId } as const
      const tail = await transport.pollMessages(
        stream,
        this.topic,
        target,
        { kind: "last" },
        1,
        false
      )
      const last = tail[tail.length - 1]
      if (last === undefined) continue
      let end = last.offset + 1n
      // Newest window first, and within a window the highest matching offset
      // wins, so the walk stops at the conversation's latest checkpoint.
      while (end > 0n) {
        const start = end > SCAN_BATCH ? end - SCAN_BATCH : 0n
        const window = await transport.pollMessages(
          stream,
          this.topic,
          target,
          { kind: "offset", value: start },
          Number(SCAN_BATCH),
          false
        )
        const found = newestMatch(window, end, conversation, stream, this.fold, partitionId)
        if (found !== undefined) {
          if (
            newest === undefined ||
            found.timestamp > newest.timestamp ||
            (found.timestamp === newest.timestamp &&
              (found.partition > newest.partition ||
                (found.partition === newest.partition && found.offset > newest.offset)))
          )
            newest = found
          break
        }
        end = start
      }
    }
    return newest?.snapshot
  }

  async save(snapshot: FoldSnapshot): Promise<void> {
    validateStoreIdentity(this.laser.defaultStream, this.fold, snapshot)
    await this.laser.topic(this.topic).send(encodeNamed(encodeFoldSnapshot(snapshot)), {
      key: new TextEncoder().encode(
        `${snapshot.conversation.toString()}:${String(new TextEncoder().encode(snapshot.fold).length)}:${snapshot.fold}`
      )
    })
  }
}

export class KvSnapshotStore implements SnapshotStore {
  constructor(
    private readonly laser: Laser,
    private readonly fold: string,
    private readonly namespace = DEFAULT_SNAPSHOT_NAMESPACE
  ) {}

  async latest(conversation: SdkConversationId): Promise<FoldSnapshot | undefined> {
    const stream = this.laser.defaultStream
    if (stream === undefined) throw new NoStreamError("a snapshot lookup requires a default stream")
    const payload = await this.laser
      .kv(this.namespace)
      .get(new TextEncoder().encode(snapshotKey(stream, conversation.toString(), this.fold)))
    if (payload === undefined) return undefined
    const context = "fold snapshot"
    const snapshot = decodeFoldSnapshot(expectMap(decodeOne(payload, context), context), context)
    if (snapshot.conversation.toString() !== conversation.toString() || snapshot.fold !== this.fold)
      throw new ProtocolError("snapshot identity does not match the store")
    return snapshot
  }

  async save(snapshot: FoldSnapshot): Promise<void> {
    validateStoreIdentity(this.laser.defaultStream, this.fold, snapshot)
    await this.laser
      .kv(this.namespace)
      .set(
        new TextEncoder().encode(
          snapshotKey(snapshot.stream, snapshot.conversation.toString(), snapshot.fold)
        )
      )
      .bytes(encodeNamed(encodeFoldSnapshot(snapshot)))
      .send()
  }
}

function sameConversation(left: ConversationId, right: SdkConversationId): boolean {
  return left.toString() === right.toString()
}

function snapshotOf(payload: Uint8Array): FoldSnapshot | undefined {
  try {
    return decodeSnapshot(payload)
  } catch {
    return undefined
  }
}

function newestMatch(
  window: readonly {
    readonly offset: bigint
    readonly timestampMicros?: bigint
    readonly payload: Uint8Array
  }[],
  end: bigint,
  conversation: SdkConversationId,
  stream: string,
  fold: string,
  partition: number
):
  | {
      readonly timestamp: bigint
      readonly partition: number
      readonly offset: bigint
      readonly snapshot: FoldSnapshot
    }
  | undefined {
  for (let index = window.length - 1; index >= 0; index -= 1) {
    const message = window[index]
    if (message === undefined || message.offset >= end) continue
    const snapshot = snapshotOf(message.payload)
    if (
      snapshot !== undefined &&
      sameConversation(snapshot.conversation, conversation) &&
      snapshot.stream === stream &&
      snapshot.fold === fold
    ) {
      if (message.timestampMicros === undefined)
        throw new ProtocolError("snapshot record has no broker timestamp")
      return { timestamp: message.timestampMicros, partition, offset: message.offset, snapshot }
    }
  }
  return undefined
}

function snapshotKey(stream: string, conversation: string, fold: string): string {
  return `${String(new TextEncoder().encode(stream).length)}:${stream}:${conversation}:${fold}`
}

function validateStoreIdentity(
  stream: string | undefined,
  fold: string,
  snapshot: FoldSnapshot
): void {
  if (stream === undefined) throw new NoStreamError("a snapshot write requires a default stream")
  if (snapshot.stream !== stream || snapshot.fold !== fold)
    throw new ProtocolError("snapshot identity does not match the store")
}
