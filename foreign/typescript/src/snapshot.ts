import { decodeOne, encodeNamed, expectMap } from "./wire/cbor.js"
import { decodeFoldSnapshot, encodeFoldSnapshot, type FoldSnapshot } from "./wire/snapshot.js"
import type { ConversationId as SdkConversationId } from "./types/ids.js"
import type { ConversationId } from "./wire/ids.js"
import { INTERNAL_TRANSPORT } from "./client/internals.js"
import type { Laser } from "./client/laser.js"

export const DEFAULT_SNAPSHOT_NAMESPACE = "agent.snapshots"
export const DEFAULT_SNAPSHOT_TOPIC = "agent.snapshots"

export interface SnapshotStore {
  latest(conversation: SdkConversationId): Promise<FoldSnapshot | undefined>
  save(snapshot: FoldSnapshot): Promise<void>
}

function sameConversation(left: ConversationId, right: SdkConversationId): boolean {
  return left.toString() === right.toString()
}

export class TopicSnapshotStore implements SnapshotStore {
  constructor(
    private readonly laser: Laser,
    readonly topic = DEFAULT_SNAPSHOT_TOPIC
  ) {}

  async latest(conversation: SdkConversationId): Promise<FoldSnapshot | undefined> {
    const stream = this.laser.defaultStream
    if (stream === undefined) return undefined
    const partitions = await this.laser[INTERNAL_TRANSPORT]().findTopicPartitionCount(
      stream,
      this.topic
    )
    if (partitions === undefined) return undefined
    const cursor = await this.laser.topic(this.topic).replay({ batchSize: 256 })
    let latest:
      | { readonly snapshot: FoldSnapshot; readonly timestamp: bigint; readonly offset: bigint }
      | undefined
    for (;;) {
      const records = await cursor.poll()
      if (records.length === 0) break
      for (const record of records) {
        let snapshot: FoldSnapshot
        try {
          const context = "fold snapshot"
          snapshot = decodeFoldSnapshot(
            expectMap(decodeOne(record.payload, context), context),
            context
          )
        } catch {
          continue
        }
        if (!sameConversation(snapshot.conversation, conversation)) continue
        const timestamp = record.timestampMicros ?? 0n
        if (
          latest === undefined ||
          timestamp > latest.timestamp ||
          (timestamp === latest.timestamp && record.offset > latest.offset)
        ) {
          latest = { snapshot, timestamp, offset: record.offset }
        }
      }
    }
    return latest?.snapshot
  }

  save(snapshot: FoldSnapshot): Promise<void> {
    return this.laser.topic(this.topic).send(encodeNamed(encodeFoldSnapshot(snapshot)), {
      key: new TextEncoder().encode(snapshot.conversation.toString())
    })
  }
}

export class KvSnapshotStore implements SnapshotStore {
  constructor(
    private readonly laser: Laser,
    readonly namespace = DEFAULT_SNAPSHOT_NAMESPACE
  ) {}

  async latest(conversation: SdkConversationId): Promise<FoldSnapshot | undefined> {
    const payload = await this.laser
      .kv(this.namespace)
      .get(new TextEncoder().encode(conversation.toString()))
    if (payload === undefined) return undefined
    const context = "fold snapshot"
    return decodeFoldSnapshot(expectMap(decodeOne(payload, context), context), context)
  }

  async save(snapshot: FoldSnapshot): Promise<void> {
    await this.laser
      .kv(this.namespace)
      .set(new TextEncoder().encode(snapshot.conversation.toString()))
      .bytes(encodeNamed(encodeFoldSnapshot(snapshot)))
      .send()
  }
}
