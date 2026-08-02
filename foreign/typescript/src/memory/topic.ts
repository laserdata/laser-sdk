import { InvalidError, NoStreamError } from "../client/errors.js"
import type { Laser } from "../client/laser.js"
import { MemoryHandle } from "./handle.js"

const DEFAULT_MEMORY_TTL_MS = 30 * 24 * 60 * 60 * 1_000

/** Configures and opens a durable memory topic. */
export class MemoryTopicBuilder {
  private streamValue: string | undefined
  private partitionsValue = 1
  private ttlMs: number | undefined = DEFAULT_MEMORY_TTL_MS

  constructor(
    private readonly laser: Laser,
    private readonly topic: string
  ) {}

  /** Selects a stream instead of the client's default stream. */
  stream(name: string): this {
    this.streamValue = name
    return this
  }

  /** Sets the partition count used to spread independent memory scopes. */
  partitions(count: number): this {
    if (!Number.isSafeInteger(count) || count < 1) {
      throw new InvalidError("memory topic partitions must be a positive safe integer")
    }
    this.partitionsValue = count
    return this
  }

  /** Sets message expiry in milliseconds. */
  ttl(milliseconds: number): this {
    if (!Number.isSafeInteger(milliseconds) || milliseconds < 1) {
      throw new InvalidError("memory topic TTL must be a positive whole number of milliseconds")
    }
    this.ttlMs = milliseconds
    return this
  }

  /** Keeps memory records until ordinary topic retention removes them. */
  noExpiry(): this {
    this.ttlMs = undefined
    return this
  }

  /** Ensures the topic and returns memory bound to it. */
  async build(): Promise<MemoryHandle> {
    const stream = this.streamValue ?? this.laser.defaultStream
    if (stream === undefined) throw new NoStreamError("memory topic requires a stream")
    await this.laser.stream(stream).ensure()
    await this.laser
      .stream(stream)
      .topic(this.topic)
      .ensure(this.partitionsValue, {
        messageExpiryMicros: this.ttlMs === undefined ? 0n : BigInt(this.ttlMs) * 1_000n
      })
    return MemoryHandle.logTopic(this.laser, this.topic, stream)
  }
}
