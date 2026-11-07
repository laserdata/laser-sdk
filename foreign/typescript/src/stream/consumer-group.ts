import type { ResourceScope } from "../client/resource-scope.js"
import {
  type CallOptions,
  abortable,
  cancellable,
  delay,
  throwIfAborted
} from "../client/call-options.js"
import type { Capabilities } from "../client/capabilities.js"
import { isStreamOrTopicNotFound, isUnavailable } from "../client/error-classify.js"
import {
  CancelledError,
  ConsumerGroupSetupError,
  FilterExecutionError,
  InvalidError,
  TimeoutError,
  UnsupportedError
} from "../client/errors.js"
import { INTERNAL_NATIVE_CONSUMER } from "../client/internals.js"
import type { LaserTransport } from "../iggy/apache-iggy.js"
import {
  FilteredReaderBuilder,
  Filters,
  nativeGroup,
  policyAware,
  sameGroupIdentity,
  type FilterPreviewOptions,
  type FilterTransport,
  type NativeGroup
} from "../managed/filters.js"
import {
  type CatalogPosition,
  type ConsumerFilter,
  type FilterBinding,
  type FilterConsumer,
  type FilterGroupIdentity,
  type FilterGroupRef,
  type FilterHeader,
  type FilterPreview,
  type FilterRef,
  type FilterRevisionPage,
  type FilterSummary,
  type FilterTestResult,
  type GroupFilterSpec,
  validateFilterMutation
} from "../wire/filter.js"
import {
  Consumer,
  resolveConsumerOptions,
  type ConsumerOptions,
  type PollFailures
} from "./consumer.js"
import { MAX_FILTERED_PAGE_BYTES } from "../wire/limits.js"
import { joinSeeds, storeSeeds } from "./consumer-start.js"
import type { Topic } from "./topic.js"

/** What a consumer group needs beyond the stream transport to reach its filter policy. */
export interface GroupContext {
  readonly transport: FilterTransport
  readonly capabilities: () => Promise<Capabilities>
  /** Probes the server again, for a consumer built before any probe answered. */
  readonly refreshCapabilities?: () => Promise<Capabilities>
  /** How the client names the managed resources it sends.
   * @internal */
  readonly scope?: ResourceScope
}

/** A consumer group by name or by its native numeric id. */
export type GroupTarget =
  { readonly kind: "name"; readonly name: string } | { readonly kind: "id"; readonly id: bigint }

/** A consumer group as the server knows it after `create()` or `info()`. */
export interface ConsumerGroupInfo {
  /** The native numeric group id. */
  readonly id: number
  readonly name: string
  /**
   * The exact group incarnation inside its stream and topic incarnations. A
   * recreated group is another identity and inherits no policy.
   */
  readonly identity: FilterGroupIdentity
  /** The active policy. Absent for an unbound group, whose readers receive every record. */
  readonly filter?: FilterBinding
}

export interface CreateConsumerGroupOptions {
  /** Configure the group with this definition, saved as the group's own filter under `filterName`. */
  readonly filter?: ConsumerFilter
  /**
   * The name `filter` is saved under, required with it. It follows the
   * registered filter name rule and need not be unique.
   */
  readonly filterName?: string
  /** Configure the group with this definition once it exists. */
  readonly policy?: GroupFilterSpec
  /**
   * The idempotency key of the policy configuration. Record it first to
   * resume the same configuration after a crash and read its first outcome.
   */
  readonly operationId?: bigint
}

/**
 * One consumer group of a topic. The group owns its filter policy: consumers
 * and readers built from this handle run whatever the group is configured
 * with, a filter or none, and never name a filter themselves. Build it with
 * `Topic.consumerGroup` or `Topic.consumerGroupId`.
 */
export class ConsumerGroup {
  // The control-log position of the last configuration this handle performed.
  // Every read built from the handle carries it, so the application never
  // observes its own configuration as absent through another node.
  private configuredAt: CatalogPosition | undefined

  private constructor(
    private readonly transport: LaserTransport,
    /** The topic this group consumes. */
    readonly topic: Topic,
    private readonly target: GroupTarget,
    private readonly context?: GroupContext
  ) {
    if (target.kind === "id" && (target.id < 0n || target.id > 0xffff_ffffn)) {
      throw new InvalidError("consumer group id exceeds 32 bits")
    }
  }

  /** @internal */
  static create(
    transport: LaserTransport,
    topic: Topic,
    target: GroupTarget,
    context?: GroupContext
  ): ConsumerGroup {
    return new ConsumerGroup(transport, topic, target, context)
  }

  /** @internal */
  get streamName(): string {
    return this.topic.streamName
  }

  /** @internal */
  get topicName(): string {
    return this.topic.name
  }

  /** The group name, `undefined` for a handle addressed by numeric id. */
  get name(): string | undefined {
    return this.target.kind === "name" ? this.target.name : undefined
  }

  /** The native numeric id, `undefined` for a handle addressed by name. */
  get id(): bigint | undefined {
    return this.target.kind === "id" ? this.target.id : undefined
  }

  /**
   * Create the group, with an optional filter policy configured in the same
   * call. Idempotent: an existing group is kept, and the same policy keeps its
   * binding. A policy is preflighted against the server's capabilities before
   * the native group is created. A group that runs another policy throws
   * `ConsumerGroupSetupError` whose cause has filter reason `conflict`, and
   * the group is left as it is.
   */
  async create(options: CreateConsumerGroupOptions & CallOptions = {}): Promise<ConsumerGroupInfo> {
    const signal = options.signal
    throwIfAborted(signal, "consumer group create")
    if (this.target.kind !== "name") {
      throw new InvalidError(
        "a consumer group is created by name, address it with consumerGroup(name)"
      )
    }
    if (options.filter !== undefined && options.policy !== undefined) {
      throw new InvalidError("pass either `filter` or `policy`")
    }
    if ((options.filter === undefined) !== (options.filterName === undefined)) {
      throw new InvalidError("pass `filterName` with `filter`")
    }
    const policy: GroupFilterSpec | undefined =
      options.policy ??
      (options.filter !== undefined && options.filterName !== undefined
        ? { kind: "definition", name: options.filterName, filter: options.filter }
        : undefined)
    if (policy !== undefined) {
      validateFilterMutation({
        kind: "configure_group",
        group: { stream: this.streamName, topic: this.topicName, group: this.target.name },
        policy
      })
      await abortable(this.filters().requireCatalog(), signal, "consumer group create")
    }
    await abortable(
      this.transport.ensureConsumerGroup(this.streamName, this.topicName, this.target.name),
      signal,
      "consumer group create"
    )
    const native = await abortable(this.native(), signal, "consumer group create")
    if (policy === undefined) return this.described(native, options)
    try {
      const configured = await cancellable(signal, "consumer group create", () =>
        this.filters().configureGroup(
          { stream: this.streamName, topic: this.topicName, group: native.name },
          policy,
          options.operationId,
          native.identity,
          signal === undefined ? {} : { signal }
        )
      )
      requireSameGroup(native.identity, configured.binding.identity)
      this.remember(configured.catalogPosition)
      return {
        id: native.id,
        name: native.name,
        identity: native.identity,
        filter: configured.binding
      }
    } catch (cause) {
      if (cause instanceof CancelledError) throw cause
      throw new ConsumerGroupSetupError(native.id, native.name, native.identity, cause)
    }
  }

  /** The group as the server knows it: its id, exact identity and active policy. */
  async info(options: CallOptions = {}): Promise<ConsumerGroupInfo> {
    throwIfAborted(options.signal, "consumer group info")
    return this.described(
      await abortable(this.native(), options.signal, "consumer group info"),
      options
    )
  }

  /**
   * The group's filter policy: configure it, inspect it, pause and resume
   * it, release it, preview and sample-test it.
   */
  filter(): GroupFilter {
    return GroupFilter.create({
      streamName: this.streamName,
      topicName: this.topicName,
      filters: () => this.filters(),
      groupRef: () => this.groupRef(),
      native: () => this.native(),
      remember: (position) => {
        this.remember(position)
      }
    })
  }

  /**
   * Build a live, load-balanced consumer of this group. On a server that
   * resolves group policies the consumer runs the group's filter, or none, and
   * commits through the group's fenced acknowledgments. On Apache Iggy it is
   * the native group consumer. A server that serves filters without group
   * reads is refused with `UnsupportedError`. When no probe answered yet, the
   * server is probed again, and a probe that still established nothing is a
   * retryable `TimeoutError`, never a native read.
   */
  async consumer(options: ConsumerOptions & CallOptions = {}): Promise<Consumer> {
    const signal = options.signal
    throwIfAborted(signal, "group consumer")
    const context = this.context
    if (context === undefined) {
      throw new UnsupportedError(
        "consumer group policies require capability negotiation, build the topic from a Laser client",
        { surface: "filters" }
      )
    }
    let capabilities = await abortable(context.capabilities(), signal, "group consumer")
    if (capabilities.hello === "unknown" && context.refreshCapabilities !== undefined) {
      capabilities = await abortable(context.refreshCapabilities(), signal, "group consumer")
    }
    if (!policyAware(capabilities)) {
      return this[INTERNAL_NATIVE_CONSUMER](options)
    }
    const resolved = resolveConsumerOptions(options, false)
    if (this.target.kind === "name" && resolved.createGroup) {
      await abortable(
        this.transport.ensureConsumerGroup(this.streamName, this.topicName, this.target.name),
        signal,
        "group consumer"
      )
    }
    const name =
      this.target.kind === "name"
        ? this.target.name
        : (await abortable(this.native(), signal, "group consumer")).name
    const seeds = await abortable(
      joinSeeds(this.transport, this.streamName, this.topicName, name, resolved.startAt),
      signal,
      "group consumer"
    )
    const builder = this.readerWith({ kind: "group" })
      .seedStart(name, seeds)
      .count(resolved.batchLength)
      .maxExamined(resolved.batchLength)
      .maxReplyBytes(Math.min(resolved.maxBytes, MAX_FILTERED_PAGE_BYTES))
      .maxWait(resolved.maxWaitMs)
    const reader = await abortableAcquire(
      withInitRetries(
        resolved.initRetries,
        (error) => isUnavailable(error) || error instanceof TimeoutError,
        () => builder.build(options),
        signal
      ),
      signal,
      (built) => built.close()
    )
    try {
      await abortable(reader.storeOwnedSeeds(), signal, "group consumer")
    } catch (error) {
      await reader.close().catch(() => undefined)
      throw error
    }
    return Consumer.create(
      this.transport,
      this.streamName,
      this.topicName,
      { kind: "group", name },
      options,
      reader
    )
  }

  /**
   * Pages selected by the group's policy with original offsets, an independent
   * scan budget, and explicit acknowledgments. An unbound group returns every
   * record without evaluating its payload.
   */
  reader(): FilteredReaderBuilder {
    return this.readerWith({ kind: "group" })
  }

  /**
   * The native Apache Iggy group consumer, for a runtime that owns its own
   * delivery contract.
   *
   * @internal
   */
  async [INTERNAL_NATIVE_CONSUMER](
    options: ConsumerOptions & CallOptions = {},
    pollFailures: PollFailures = "retry"
  ): Promise<Consumer> {
    const signal = options.signal
    throwIfAborted(signal, "group consumer")
    const resolved = resolveConsumerOptions(options, false)
    const name =
      this.target.kind === "name"
        ? this.target.name
        : (await abortable(this.native(), signal, "group consumer")).name
    await abortableAcquire(
      withInitRetries(
        resolved.initRetries,
        isStreamOrTopicNotFound,
        async () => {
          if (resolved.createGroup) {
            await this.transport.joinConsumerGroup(this.streamName, this.topicName, name)
            return
          }
          if (this.transport.joinExistingConsumerGroup === undefined) {
            throw new UnsupportedError("this transport cannot join a group without creating it", {
              surface: "stream"
            })
          }
          await this.transport.joinExistingConsumerGroup(this.streamName, this.topicName, name)
        },
        signal
      ),
      signal,
      () => this.transport.leaveConsumerGroup(this.streamName, this.topicName, name)
    )
    let seeds: Map<number, bigint>
    try {
      seeds = await abortable(
        joinSeeds(this.transport, this.streamName, this.topicName, name, resolved.startAt),
        signal,
        "group consumer"
      )
      await abortable(this.storeHeldSeeds(name, seeds), signal, "group consumer")
    } catch (error) {
      await this.transport
        .leaveConsumerGroup(this.streamName, this.topicName, name)
        .catch(() => undefined)
      throw error
    }
    return Consumer.create(
      this.transport,
      this.streamName,
      this.topicName,
      { kind: "group", name },
      options,
      undefined,
      pollFailures,
      seeds
    )
  }

  // Store the seeds of the partitions the native member holds after its join,
  // so a refused seed fails the start.
  private async storeHeldSeeds(group: string, seeds: Map<number, bigint>): Promise<void> {
    const read = this.transport.getConsumerOffset?.bind(this.transport)
    if (seeds.size === 0 || read === undefined || this.transport.syncConsumerGroup === undefined)
      return
    const assignment = await this.transport.syncConsumerGroup(
      this.streamName,
      this.topicName,
      group
    )
    const target = { kind: "group" as const, name: group }
    await storeSeeds(
      seeds,
      assignment?.partitions ?? [],
      (partitionId) => read(this.streamName, this.topicName, target, partitionId),
      (partitionId, offset) =>
        this.transport.storeOffset(this.streamName, this.topicName, target, partitionId, offset)
    )
  }

  /** @internal */
  private filters(): Filters {
    const context = this.context
    if (context === undefined) {
      throw new UnsupportedError("consumer group filters need a topic built from a Laser client", {
        surface: "filters"
      })
    }
    return new Filters(context.transport, context.capabilities, context.scope)
  }

  /** @internal */
  private remember(position: CatalogPosition | undefined): void {
    if (position === undefined) return
    const known = this.configuredAt
    if (known === undefined || supersedesPosition(known, position)) {
      this.configuredAt = position
    }
  }

  // The names the catalog addresses the group by. A numeric handle reads them
  // from the server.
  /** @internal */
  private async groupRef(): Promise<FilterGroupRef> {
    return {
      stream: this.streamName,
      topic: this.topicName,
      group: this.target.kind === "name" ? this.target.name : (await this.native()).name
    }
  }

  private readerWith(filter: Extract<FilterRef, { readonly kind: "bound" | "group" }>) {
    const context = this.context
    if (context === undefined) {
      throw new UnsupportedError("a group reader needs a topic built from a Laser client", {
        surface: "filters"
      })
    }
    return FilteredReaderBuilder.create(
      context.transport,
      context.capabilities,
      this.filters(),
      { stream: this.streamName, topic: this.topicName },
      this.selector(),
      filter,
      this.configuredAt
    )
  }

  private selector(): Exclude<FilterConsumer, { readonly kind: "consumer" }> {
    return this.target.kind === "name"
      ? { kind: "group", name: this.target.name }
      : { kind: "group_id", id: this.target.id }
  }

  private native(): Promise<NativeGroup> {
    return nativeGroup(
      this.transport,
      { stream: this.streamName, topic: this.topicName },
      this.target.kind === "name" ? this.target.name : Number(this.target.id)
    )
  }

  private async described(native: NativeGroup, options: CallOptions): Promise<ConsumerGroupInfo> {
    const capabilities =
      this.context === undefined
        ? undefined
        : await abortable(this.context.capabilities(), options.signal, "consumer group info")
    const filter =
      capabilities?.filters.catalog === true ? await this.filter().get(options) : undefined
    if (filter !== undefined) requireSameGroup(native.identity, filter.identity)
    return {
      id: native.id,
      name: native.name,
      identity: native.identity,
      ...(filter !== undefined ? { filter } : {})
    }
  }
}

interface GroupFilterContext {
  readonly streamName: string
  readonly topicName: string
  filters(): Filters
  groupRef(): Promise<FilterGroupRef>
  native(): Promise<NativeGroup>
  remember(position: CatalogPosition | undefined): void
}

/**
 * A consumer group's filter policy. Build it with `ConsumerGroup.filter()`.
 * Every verb needs a managed backend that serves the filter catalog.
 */
export class GroupFilter {
  private constructor(private readonly group: GroupFilterContext) {}

  /** @internal */
  static create(group: GroupFilterContext): GroupFilter {
    return new GroupFilter(group)
  }

  /**
   * Give the group `filter` as its policy. The definition is saved as the
   * group's own filter under `name` and the group is bound to it in one
   * catalog transaction. The name follows the registered filter name rule and
   * need not be unique. The same digest again keeps the binding and renames
   * the own filter when `name` differs. Another digest on a group that runs a
   * policy throws with reason `conflict`: create a new group for another
   * policy.
   */
  configure(
    name: string,
    filter: ConsumerFilter,
    options: CallOptions = {}
  ): Promise<FilterBinding> {
    return this.configureAs(undefined, { kind: "definition", name, filter }, options)
  }

  /** `configure` with a policy spec. */
  configureWith(policy: GroupFilterSpec, options: CallOptions = {}): Promise<FilterBinding> {
    return this.configureAs(undefined, policy, options)
  }

  /**
   * `configureWith` under a caller-chosen operation id, so a caller that
   * records the id first resumes the same configuration after a crash and
   * reads its first outcome.
   */
  async configureAs(
    operationId: bigint | undefined,
    policy: GroupFilterSpec,
    options: CallOptions = {}
  ): Promise<FilterBinding> {
    const signal = options.signal
    throwIfAborted(signal, "group filter configure")
    const group = await abortable(this.group.groupRef(), signal, "group filter configure")
    validateFilterMutation({ kind: "configure_group", group, policy })
    const native = await abortable(this.group.native(), signal, "group filter configure")
    const configured = await cancellable(signal, "group filter configure", () =>
      this.group
        .filters()
        .configureGroup(
          { stream: this.group.streamName, topic: this.group.topicName, group: native.name },
          policy,
          operationId,
          native.identity,
          options
        )
    )
    requireSameGroup(native.identity, configured.binding.identity)
    this.group.remember(configured.catalogPosition)
    return configured.binding
  }

  /** The active policy, `undefined` for an unbound group. */
  async get(options: CallOptions = {}): Promise<FilterBinding | undefined> {
    const signal = options.signal
    throwIfAborted(signal, "group filter get")
    // The catalog gate comes first, like Rust: without a managed backend the
    // answer is `unsupported`, whatever the native group looks like.
    await abortable(this.group.filters().requireCatalog(), signal, "group filter get")
    const native = await abortable(this.group.native(), signal, "group filter get")
    try {
      const binding = await abortable(
        this.group.filters().binding({
          stream: this.group.streamName,
          topic: this.group.topicName,
          group: native.name
        }),
        signal,
        "group filter get"
      )
      requireSameGroup(native.identity, binding.identity)
      return binding
    } catch (error) {
      if (error instanceof FilterExecutionError && error.detail?.reason === "not_found")
        return undefined
      throw error
    }
  }

  /** One page of the group's own filter revisions, newest first. `page`
   * counts from 1 and `results` is the page length. */
  async revisions(
    page?: number,
    results?: number,
    options: CallOptions = {}
  ): Promise<FilterRevisionPage> {
    const active = await this.active(options)
    return abortable(
      this.group.filters().revisions(active.filterId, {
        ...(page !== undefined ? { page } : {}),
        ...(results !== undefined ? { results } : {})
      }),
      options.signal,
      "group filter revisions"
    )
  }

  /**
   * Pause or resume a revision of the group's own filter. A paused active
   * revision refuses new reads, while records already delivered can still be
   * acknowledged.
   */
  async setRevisionEnabled(
    revision: number,
    enabled: boolean,
    options: CallOptions = {}
  ): Promise<void> {
    const active = await this.active(options)
    await cancellable(options.signal, "group filter revision", () =>
      this.group.filters().setRevisionEnabled(active.filterId, revision, enabled, options)
    )
  }

  /**
   * The group's own filter as a catalog row, bound or released: the caller's
   * name, state, latest revision and the owning group. `undefined` when the
   * group has no filter of its own.
   */
  async summary(options: CallOptions = {}): Promise<FilterSummary | undefined> {
    const signal = options.signal
    throwIfAborted(signal, "group filter summary")
    await abortable(this.group.filters().requireCatalog(), signal, "group filter summary")
    const native = await abortable(this.group.native(), signal, "group filter summary")
    return abortable(this.group.filters().owned(native.identity), signal, "group filter summary")
  }

  /**
   * Delete the group's own filter with every revision. A bound group is
   * released first, so its consumers receive every record from their next
   * poll. Nothing of the filter stays in the catalog. `false` when the group
   * has no filter of its own.
   */
  async delete(options: CallOptions = {}): Promise<boolean> {
    const own = await this.summary(options)
    if (own === undefined) return false
    this.group.remember(
      await cancellable(options.signal, "group filter delete", () =>
        this.group.filters().dropFilter(own.id, options)
      )
    )
    return true
  }

  /**
   * Release the group's policy. Its readers then receive every record. A
   * released group may only be configured with the digest it ran.
   */
  async release(options: CallOptions = {}): Promise<FilterBinding> {
    const active = await this.active(options)
    const released = await cancellable(options.signal, "group filter release", () =>
      this.group.filters().releaseGroup(active, options)
    )
    this.group.remember(released.catalogPosition)
    return released.binding
  }

  /**
   * Preview the active policy over the stored records of one partition. A
   * preview joins no group, stores no offset, and changes no consumer state.
   */
  async preview(
    partitionId: number,
    options: FilterPreviewOptions & CallOptions = {}
  ): Promise<FilterPreview> {
    const active = await this.active(options)
    return abortable(
      this.group
        .filters()
        .preview(
          this.group.streamName,
          this.group.topicName,
          partitionId,
          { kind: "revision", filterId: active.filterId, revision: active.revision },
          options
        ),
      options.signal,
      "group filter preview"
    )
  }

  /**
   * Evaluate the active policy against one supplied record and explain the
   * verdict. Nothing is read from a stream and nothing is stored.
   */
  async test(
    payload: Uint8Array | string,
    headers: readonly FilterHeader[] = [],
    options: CallOptions = {}
  ): Promise<FilterTestResult> {
    const active = await this.active(options)
    return abortable(
      this.group
        .filters()
        .test(
          { kind: "revision", filterId: active.filterId, revision: active.revision },
          payload,
          headers
        ),
      options.signal,
      "group filter test"
    )
  }

  private async active(options: CallOptions): Promise<FilterBinding> {
    const binding = await this.get(options)
    if (binding === undefined) {
      throw new FilterExecutionError("not_found: the consumer group has no filter policy", {
        code: { kind: "known", name: "NotFound" },
        reason: "not_found",
        message: "the consumer group has no filter policy"
      })
    }
    return binding
  }
}

function requireSameGroup(expected: FilterGroupIdentity, actual: FilterGroupIdentity): void {
  if (!sameGroupIdentity(expected, actual)) {
    throw new FilterExecutionError("the group incarnation changed while its policy was resolved", {
      code: { kind: "known", name: "StaleGeneration" },
      reason: "source_changed",
      message: "the group incarnation changed while its policy was resolved, refresh its identity"
    })
  }
}

// Builds a consumer with the configured init retries: the first attempt plus
// up to `retries` more, `intervalMs` apart, for a failure `retryable` admits.
async function withInitRetries<T>(
  initRetries: { readonly retries: number; readonly intervalMs: number },
  retryable: (error: unknown) => boolean,
  attempt: () => Promise<T>,
  signal: AbortSignal | undefined
): Promise<T> {
  for (let tried = 0; ; tried += 1) {
    try {
      return await attempt()
    } catch (error) {
      if (tried >= initRetries.retries || !retryable(error)) throw error
      await delay(initRetries.intervalMs, signal, "group consumer")
    }
  }
}

// Waits for `acquiring` like `abortable`. What it acquires after an abort is
// released, so an abandoned join leaves no membership behind.
async function abortableAcquire<T>(
  acquiring: Promise<T>,
  signal: AbortSignal | undefined,
  release: (acquired: T) => Promise<unknown>
): Promise<T> {
  try {
    return await abortable(acquiring, signal, "group consumer")
  } catch (error) {
    if (error instanceof CancelledError) void acquiring.then(release).catch(() => undefined)
    throw error
  }
}

/**
 * Whether `position` is a later catalog configuration than `known`, as Rust
 * `supersedes`. Catalog revisions order outcomes across a recreated control
 * log, where offsets start over. Without them, another proven operation,
 * another partition or a later offset counts as later.
 * @internal
 */
export function supersedesPosition(known: CatalogPosition, position: CatalogPosition): boolean {
  if (known.revision !== undefined && position.revision !== undefined) {
    return position.revision > known.revision
  }
  return (
    known.partitionId !== position.partitionId ||
    known.offset < position.offset ||
    (position.operationId !== undefined && known.operationId !== position.operationId)
  )
}
