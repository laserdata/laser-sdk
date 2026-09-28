import type { LaserTransport } from "../iggy/apache-iggy.js"
import { Topic } from "./topic.js"
import type { GovernPublish, ObserveEffect, ResolveSchema } from "./topic.js"

export class Stream {
  constructor(
    private readonly transport: LaserTransport,
    readonly name: string,
    private readonly govern?: GovernPublish,
    private readonly resolveSchema?: ResolveSchema,
    private readonly observe?: ObserveEffect,
    private readonly onDelete?: () => void
  ) {}

  topic(name: string): Topic {
    return new Topic(this.transport, this.name, name, this.govern, this.resolveSchema, this.observe)
  }

  async ensure(): Promise<void> {
    if (this.observe === undefined) {
      await this.transport.ensureStream(this.name)
      return
    }
    await this.observe("laser.stream.ensure", { operation: "ensure", stream: this.name }, () =>
      this.transport.ensureStream(this.name)
    )
  }

  /** Deletes this stream with every topic and message in it. Resolves `false` when the stream did not exist. */
  async delete(): Promise<boolean> {
    const deleted =
      this.observe === undefined
        ? await this.transport.deleteStream(this.name)
        : await this.observe(
            "laser.stream.delete",
            { operation: "delete", stream: this.name },
            () => this.transport.deleteStream(this.name)
          )
    this.onDelete?.()
    return deleted
  }
}
