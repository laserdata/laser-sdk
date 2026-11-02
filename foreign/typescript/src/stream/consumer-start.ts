import { UnsupportedError } from "../client/errors.js"
import { type LaserTransport, serverErrorCode } from "../iggy/apache-iggy.js"

/** Polling position for a consumer. `next`, the default, resumes after the
 * server-stored consumer offset. `timestampMicros` begins at a microsecond
 * Unix timestamp. */
export type ConsumerStart =
  | { readonly kind: "first" }
  | { readonly kind: "last" }
  | { readonly kind: "next" }
  | { readonly kind: "offset"; readonly value: bigint }
  | { readonly kind: "timestampMicros"; readonly value: bigint }

/**
 * The offset to store on each partition the group never stored, taken when a
 * group consumer joins so its start names the records of that moment. The
 * consumer stores one when it first owns the partition and the group still
 * stored nothing there.
 */
export async function joinSeeds(
  transport: Pick<LaserTransport, "findTopicPartitionCount" | "getConsumerOffset" | "pollMessages">,
  streamName: string,
  topicName: string,
  group: string,
  start: ConsumerStart
): Promise<Map<number, bigint>> {
  const seeds = new Map<number, bigint>()
  // Reading `next` where nothing is stored begins at the first record.
  if (start.kind === "next" || start.kind === "first") return seeds
  const read = transport.getConsumerOffset?.bind(transport)
  if (read === undefined)
    throw new UnsupportedError("this transport cannot read a group's stored offsets", {
      surface: "stream"
    })
  const partitions = (await transport.findTopicPartitionCount(streamName, topicName)) ?? 0
  for (let partitionId = 0; partitionId < partitions; partitionId += 1) {
    if (
      (await read(streamName, topicName, { kind: "group", name: group }, partitionId)) !== undefined
    )
      continue
    const seed = await startSeed(transport, streamName, topicName, start, partitionId)
    if (seed !== undefined) seeds.set(partitionId, seed)
  }
  return seeds
}

const GROUP_PARTITION_NOT_OWNED = 5009
// A node answers a request for a partition not routable on it yet this way.
const TRANSIENT_NOT_ACCEPTED = 58

/**
 * Store the seed of each of `partitions` the group still stored nothing on,
 * then forget it. Whether every seed was stored: a partition the assignment
 * names before the server installed it for this member refuses the store as
 * not owned or as transient, and keeps its seed for a later sync.
 */
export async function storeSeeds(
  seeds: Map<number, bigint>,
  partitions: Iterable<number>,
  stored: (partitionId: number) => Promise<unknown>,
  store: (partitionId: number, offset: bigint) => Promise<void>
): Promise<boolean> {
  let complete = true
  for (const partitionId of partitions) {
    const seed = seeds.get(partitionId)
    if (seed === undefined) continue
    try {
      if ((await stored(partitionId)) === undefined) await store(partitionId, seed)
    } catch (error) {
      const code = serverErrorCode(error)
      if (code !== GROUP_PARTITION_NOT_OWNED && code !== TRANSIENT_NOT_ACCEPTED) throw error
      complete = false
      continue
    }
    seeds.delete(partitionId)
  }
  return complete
}

// The offset that makes the next read of a partition begin at `start`,
// `undefined` when reading `next` already begins there.
async function startSeed(
  transport: Pick<LaserTransport, "pollMessages">,
  streamName: string,
  topicName: string,
  start: ConsumerStart,
  partitionId: number
): Promise<bigint | undefined> {
  const lookup = async (strategy: ConsumerStart) =>
    (
      await transport.pollMessages(
        streamName,
        topicName,
        { kind: "single", partitionId },
        strategy,
        1,
        false
      )
    )[0]?.offset
  let first: bigint | undefined
  switch (start.kind) {
    case "first":
    case "next":
      return undefined
    case "offset":
      first = start.value
      break
    case "timestampMicros": {
      first = await lookup(start)
      // A timestamp past every record begins after the last one.
      if (first === undefined) {
        const last = await lookup({ kind: "last" })
        if (last !== undefined) first = last + 1n
      }
      break
    }
    case "last":
      first = await lookup({ kind: "last" })
  }
  return first === undefined || first === 0n ? undefined : first - 1n
}
