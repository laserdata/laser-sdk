import { type CallOptions, abortable, cancellable, throwIfAborted } from "../client/call-options.js"
import type { MaterializeRequest } from "../managed/materialize.js"
import type { LaserTransport } from "../iggy/apache-iggy.js"
import { Topic } from "./topic.js"
import type { GroupContext } from "./consumer-group.js"
import type { GovernPublish, ObserveEffect, ResolveSchema } from "./topic.js"

export class Stream {
  private constructor(
    private readonly transport: LaserTransport,
    readonly name: string,
    private readonly govern?: GovernPublish,
    private readonly resolveSchema?: ResolveSchema,
    private readonly observe?: ObserveEffect,
    private readonly onDelete?: () => void,
    private readonly groups?: GroupContext,
    private readonly materializeRequest?: (topic: string) => MaterializeRequest
  ) {}

  /** @internal */
  static create(
    transport: LaserTransport,
    name: string,
    govern?: GovernPublish,
    resolveSchema?: ResolveSchema,
    observe?: ObserveEffect,
    onDelete?: () => void,
    groups?: GroupContext,
    materializeRequest?: (topic: string) => MaterializeRequest
  ): Stream {
    return new Stream(
      transport,
      name,
      govern,
      resolveSchema,
      observe,
      onDelete,
      groups,
      materializeRequest
    )
  }

  topic(name: string): Topic {
    const materializeRequest = this.materializeRequest
    return Topic.create(
      this.transport,
      this.name,
      name,
      this.govern,
      this.resolveSchema,
      this.observe,
      this.groups,
      materializeRequest === undefined ? undefined : () => materializeRequest(name)
    )
  }

  async ensure(options: CallOptions = {}): Promise<void> {
    throwIfAborted(options.signal, "stream ensure")
    if (this.observe === undefined) {
      await abortable(this.transport.ensureStream(this.name), options.signal, "stream ensure")
      return
    }
    await this.observe("laser.stream.ensure", { operation: "ensure", stream: this.name }, () =>
      abortable(this.transport.ensureStream(this.name), options.signal, "stream ensure")
    )
  }

  /** Deletes this stream with every topic and message in it. Resolves `false` when the stream did not exist. */
  delete(options: CallOptions = {}): Promise<boolean> {
    // An abandoned delete that lands still clears what the client cached for the stream.
    return cancellable(options.signal, "stream delete", async () => {
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
    })
  }
}
