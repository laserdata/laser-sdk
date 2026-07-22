import type { LaserTransport } from "../iggy/apache-iggy.js"
import { Topic } from "./topic.js"
import type { GovernPublish, ObserveEffect, ResolveSchema } from "./topic.js"

export class Stream {
  constructor(
    private readonly transport: LaserTransport,
    readonly name: string,
    private readonly govern?: GovernPublish,
    private readonly resolveSchema?: ResolveSchema,
    private readonly observe?: ObserveEffect
  ) {}

  topic(name: string): Topic {
    return new Topic(this.transport, this.name, name, this.govern, this.resolveSchema, this.observe)
  }

  // Idempotent create-if-missing, matching `Topic.ensure`.
  async ensure(): Promise<void> {
    if (this.observe === undefined) {
      await this.transport.ensureStream(this.name)
      return
    }
    await this.observe("laser.stream.ensure", { operation: "ensure", stream: this.name }, () =>
      this.transport.ensureStream(this.name)
    )
  }
}
