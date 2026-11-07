import { decodeOne, encodeNamed, expectMap } from "./wire/cbor.js"
import { decodeFoldSnapshot, encodeFoldSnapshot, type FoldSnapshot } from "./wire/snapshot.js"
import type { ConversationId as SdkConversationId } from "./types/ids.js"
import type { ConversationId } from "./wire/ids.js"
import { InvalidError, NoStreamError } from "./client/errors.js"
import { INTERNAL_TRANSPORT } from "./client/internals.js"
import type { Laser } from "./client/laser.js"
import { xxHash32 } from "./iggy/apache-iggy.js"
import { readThrough } from "./stream/cursor.js"

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

  /** Walks the one partition the conversation's snapshots route to backward
   * from its tail and stops at the newest record for the conversation, so a hit
   * costs the distance to the last checkpoint. A conversation with no snapshot
   * walks that partition fully, so keep the snapshots topic on retention. */
  async latest(conversation: SdkConversationId): Promise<FoldSnapshot | undefined> {
    const stream = this.laser.defaultStream
    if (stream === undefined) {
      throw new NoStreamError(
        "a snapshot lookup requires a default stream, use connectWithStream() or withDefaultStream()"
      )
    }
    const transport = this.laser[INTERNAL_TRANSPORT]()
    const partitions = await transport.findTopicPartitionCount(stream, this.topic)
    if (partitions === undefined || partitions === 0) return undefined
    // The server routes a message key to xxHash32(key) % partitions, so every
    // snapshot `save` wrote for this conversation and fold sits on this one.
    const partitionId = xxHash32(partitionKey(conversation.toString(), this.fold)) % partitions
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
    if (last === undefined) return undefined
    let end = last.offset + 1n
    // Newest window first, and within a window the highest matching offset
    // wins, so the walk stops at the conversation's latest checkpoint.
    while (end > 0n) {
      const start = end > SCAN_BATCH ? end - SCAN_BATCH : 0n
      const window = await readThrough(transport, stream, this.topic, target, start, end - 1n)
      const found = newestMatch(window, end, conversation, stream, this.fold)
      if (found !== undefined) return found
      end = start
    }
    return undefined
  }

  async save(snapshot: FoldSnapshot): Promise<void> {
    validateStoreIdentity(this.laser.defaultStream, this.fold, snapshot)
    await this.laser.topic(this.topic).send(encodeNamed(encodeFoldSnapshot(snapshot)), {
      key: partitionKey(snapshot.conversation.toString(), snapshot.fold)
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
      throw new InvalidError("snapshot identity does not match the store")
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
  fold: string
): FoldSnapshot | undefined {
  for (let index = window.length - 1; index >= 0; index -= 1) {
    const message = window[index]
    if (message === undefined || message.offset >= end) continue
    const snapshot = snapshotOf(message.payload)
    if (
      snapshot !== undefined &&
      sameConversation(snapshot.conversation, conversation) &&
      snapshot.stream === stream &&
      snapshot.fold === fold
    )
      return snapshot
  }
  return undefined
}

// The message key `save` routes a snapshot by, which pins one conversation and
// fold to one partition.
function partitionKey(conversation: string, fold: string): Uint8Array {
  return new TextEncoder().encode(
    `${conversation}:${String(new TextEncoder().encode(fold).length)}:${fold}`
  )
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
    throw new InvalidError("snapshot identity does not match the store")
}
