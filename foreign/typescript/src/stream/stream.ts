import { type CallOptions, abortable, cancellable, throwIfAborted } from "../client/call-options.js"
import type { MaterializeRequest } from "../managed/materialize.js"
import type { LaserTransport } from "../iggy/apache-iggy.js"
import { type StreamInfo, type TopicInfo, adminOperation } from "./admin.js"
import { Topic } from "./topic.js"
import { administer } from "../client/server.js"
import { renameStream } from "../iggy/server-admin.js"
import type { GroupContext } from "./consumer-group.js"
import type { GovernPublish, ObserveEffect, ResolveSchema } from "./topic.js"

export class Stream {
  private constructor(
    private readonly transport: LaserTransport,
    readonly name: string,
    private readonly govern?: GovernPublish,
    private readonly resolveSchema?: ResolveSchema,
    private readonly observe?: ObserveEffect,
    private readonly forget?: (topic?: string) => void,
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
    forget?: (topic?: string) => void,
    groups?: GroupContext,
    materializeRequest?: (topic: string) => MaterializeRequest
  ): Stream {
    return new Stream(
      transport,
      name,
      govern,
      resolveSchema,
      observe,
      forget,
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
      materializeRequest === undefined ? undefined : () => materializeRequest(name),
      () => this.forget?.(name)
    )
  }

  /** This stream as the server reports it. Resolves `undefined` when it does not exist. */
  async get(options: CallOptions = {}): Promise<StreamInfo | undefined> {
    const find = adminOperation(this.transport.findStream?.bind(this.transport), "stream lookup")
    throwIfAborted(options.signal, "stream lookup")
    return abortable(find(this.name), options.signal, "stream lookup")
  }

  /** Whether this stream exists. */
  async exists(options: CallOptions = {}): Promise<boolean> {
    return (await this.get(options)) !== undefined
  }

  /** Every topic of this stream. A missing stream fails with an error `isStreamOrTopicNotFound` recognizes. */
  async topics(options: CallOptions = {}): Promise<readonly TopicInfo[]> {
    const list = adminOperation(this.transport.listTopics?.bind(this.transport), "topic listing")
    throwIfAborted(options.signal, "topic listing")
    return abortable(list(this.name), options.signal, "topic listing")
  }

  /** Renames this stream. After the rename, address the stream by its new name. */
  async rename(name: string, options: CallOptions = {}): Promise<void> {
    // An abandoned rename that lands still clears what the client cached for the stream.
    return administer(this.transport, "stream rename", false, options, async (client) => {
      await renameStream(client, this.name, name)
      this.transport.forgetStream?.(this.name)
      this.forget?.()
    })
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
  async delete(options: CallOptions = {}): Promise<boolean> {
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
      this.forget?.()
      return deleted
    })
  }
}
