import type { Capabilities } from "../client/capabilities.js"
import {
  ConsumerGroupSetupError,
  FilterExecutionError,
  InvalidError,
  TransportError,
  UnsupportedError
} from "../client/errors.js"
import { INTERNAL_NATIVE_CONSUMER } from "../client/internals.js"
import type { LaserTransport } from "../iggy/apache-iggy.js"
import {
  FilteredReaderBuilder,
  Filters,
  nativeGroup,
  type CatalogPageOptions,
  type FilterPreviewOptions,
  type FilterTransport,
  type NativeGroup
} from "../managed/filters.js"
import type {
  CatalogPosition,
  ConsumerFilter,
  FilterBinding,
  FilterConsumer,
  FilterGroupIdentity,
  FilterGroupRef,
  FilterHeader,
  FilterPreview,
  FilterRef,
  FilterRevisionPage,
  FilterRevisionRef,
  FilterTestResult,
  FilteredStart,
  GroupFilterSpec
} from "../wire/filter.js"
import { Consumer, resolveConsumerOptions, type ConsumerOptions } from "./consumer.js"
import type { PollingStrategy } from "./polling-strategy.js"

/** What a consumer group needs beyond the stream transport to reach its filter policy. */
export interface GroupContext {
  readonly transport: FilterTransport
  readonly capabilities: () => Promise<Capabilities>
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
  /** Configure the group with this definition, saved as the group's own filter. */
  readonly filter?: ConsumerFilter
  /** Configure the group with a definition or one of its own revisions. */
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

  constructor(
    private readonly transport: LaserTransport,
    readonly streamName: string,
    readonly topicName: string,
    private readonly target: GroupTarget,
    private readonly context?: GroupContext
  ) {
    if (target.kind === "id" && (target.id < 0n || target.id > 0xffff_ffffn)) {
      throw new InvalidError("consumer group id exceeds 32 bits")
    }
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
   * `ConsumerGroupSetupError` with reason `conflict`, and the group is left as
   * it is.
   */
  async create(options: CreateConsumerGroupOptions = {}): Promise<ConsumerGroupInfo> {
    if (this.target.kind !== "name") {
      throw new InvalidError(
        "a consumer group is created by name, address it with consumerGroup(name)"
      )
    }
    if (options.filter !== undefined && options.policy !== undefined) {
      throw new InvalidError("pass either `filter` or `policy`")
    }
    const policy: GroupFilterSpec | undefined =
      options.policy ??
      (options.filter !== undefined ? { kind: "definition", filter: options.filter } : undefined)
    if (policy !== undefined) await this.filters().requireCatalog()
    await this.transport.ensureConsumerGroup(this.streamName, this.topicName, this.target.name)
    const native = await this.native()
    if (policy === undefined) return this.described(native)
    try {
      const configured = await this.filters().configureGroup(
        { stream: this.streamName, topic: this.topicName, group: native.name },
        policy,
        options.operationId,
        native.identity
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
      throw new ConsumerGroupSetupError(native.id, native.name, native.identity, cause)
    }
  }

  /** The group as the server knows it: its id, exact identity and active policy. */
  async info(): Promise<ConsumerGroupInfo> {
    return this.described(await this.native())
  }

  /**
   * The group's filter policy: configure it, inspect it, draft and pause
   * revisions, release it, preview and sample-test it.
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
   * reads is refused with `UnsupportedError`, and a capability probe that
   * established nothing is a retryable `TransportError`, never a native read.
   */
  async consumer(options: ConsumerOptions = {}): Promise<Consumer> {
    const capabilities = await this.context?.capabilities()
    if (capabilities === undefined) {
      throw new TransportError(
        "consumer group policies require capability negotiation, build the topic from a Laser client",
        true
      )
    }
    if (!policyAware(capabilities)) {
      return this[INTERNAL_NATIVE_CONSUMER](options)
    }
    const resolved = resolveConsumerOptions(options, false)
    if (this.target.kind === "name") {
      await this.transport.ensureConsumerGroup(this.streamName, this.topicName, this.target.name)
    }
    const name = this.target.kind === "name" ? this.target.name : (await this.native()).name
    const reader = await this.readerWith({ kind: "group" })
      .start(filteredStart(resolved.startFrom))
      .count(resolved.batchLength)
      .maxExamined(resolved.batchLength)
      .idleInterval(resolved.pollIntervalMs)
      .build()
    return new Consumer(
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
  async [INTERNAL_NATIVE_CONSUMER](options: ConsumerOptions = {}): Promise<Consumer> {
    const name = this.target.kind === "name" ? this.target.name : (await this.native()).name
    await this.transport.joinConsumerGroup(this.streamName, this.topicName, name)
    return new Consumer(
      this.transport,
      this.streamName,
      this.topicName,
      { kind: "group", name },
      options
    )
  }

  /** @internal */
  private filters(): Filters {
    const context = this.context
    if (context === undefined) {
      throw new UnsupportedError("consumer group filters need a topic built from a Laser client")
    }
    return new Filters(context.transport, context.capabilities)
  }

  /** @internal */
  private remember(position: CatalogPosition | undefined): void {
    if (position === undefined) return
    const known = this.configuredAt
    if (
      known?.partitionId !== position.partitionId ||
      known.offset < position.offset ||
      (position.operationId !== undefined && known.operationId !== position.operationId)
    ) {
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
      throw new UnsupportedError("a group reader needs a topic built from a Laser client")
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

  private async described(native: NativeGroup): Promise<ConsumerGroupInfo> {
    const capabilities = await this.context?.capabilities()
    const filter = capabilities?.filters.catalog === true ? await this.filter().get() : undefined
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
 * Every verb needs a managed plane that serves the filter catalog.
 */
export class GroupFilter {
  private constructor(private readonly group: GroupFilterContext) {}

  /** @internal */
  static create(group: GroupFilterContext): GroupFilter {
    return new GroupFilter(group)
  }

  /**
   * Give the group `filter` as its policy. The definition is saved as the
   * group's own filter and the group is bound to it in one catalog
   * transaction. The same digest again keeps the binding. Another digest on a
   * group that runs a policy throws with reason `conflict`: create a new group
   * for another policy.
   */
  configure(filter: ConsumerFilter): Promise<FilterBinding> {
    return this.configureAs(undefined, { kind: "definition", filter })
  }

  /** `configure` with a definition or one of the group's own revisions. */
  configureWith(policy: GroupFilterSpec): Promise<FilterBinding> {
    return this.configureAs(undefined, policy)
  }

  /**
   * `configureWith` under a caller-chosen operation id, so a caller that
   * records the id first resumes the same configuration after a crash and
   * reads its first outcome.
   */
  async configureAs(
    operationId: bigint | undefined,
    policy: GroupFilterSpec
  ): Promise<FilterBinding> {
    const native = await this.group.native()
    const configured = await this.group
      .filters()
      .configureGroup(
        { stream: this.group.streamName, topic: this.group.topicName, group: native.name },
        policy,
        operationId,
        native.identity
      )
    requireSameGroup(native.identity, configured.binding.identity)
    this.group.remember(configured.catalogPosition)
    return configured.binding
  }

  /** The active policy, `undefined` for an unbound group. */
  async get(): Promise<FilterBinding | undefined> {
    // The catalog gate comes first, like Rust: without a plane the answer is
    // `unsupported`, whatever the native group looks like.
    await this.group.filters().requireCatalog()
    const native = await this.group.native()
    try {
      const binding = await this.group.filters().binding({
        stream: this.group.streamName,
        topic: this.group.topicName,
        group: native.name
      })
      requireSameGroup(native.identity, binding.identity)
      return binding
    } catch (error) {
      if (error instanceof FilterExecutionError && error.reason === "not_found") return undefined
      throw error
    }
  }

  /** One page of the group's own filter revisions, newest first. */
  async revisions(options: CatalogPageOptions = {}): Promise<FilterRevisionPage> {
    const active = await this.active()
    return this.group.filters().revisions(active.filterId, options)
  }

  /**
   * Draft a revision of the group's own filter. Readers keep running the
   * active revision. `expectedRevision` must still be the latest.
   */
  async revise(expectedRevision: number, filter: ConsumerFilter): Promise<FilterRevisionRef> {
    const active = await this.active()
    return this.group.filters().revise(active.filterId, expectedRevision, filter)
  }

  /**
   * Pause or resume a revision of the group's own filter. A paused active
   * revision refuses new reads, while records already delivered can still be
   * acknowledged.
   */
  async setRevisionEnabled(revision: number, enabled: boolean): Promise<void> {
    const active = await this.active()
    await this.group.filters().setRevisionEnabled(active.filterId, revision, enabled)
  }

  /**
   * Delete the group's own filter with every revision. A bound group is
   * released first, so its consumers receive every record from their next
   * poll. Nothing of the filter stays in the catalog. `false` when the group
   * has no filter of its own.
   */
  async delete(): Promise<boolean> {
    await this.group.filters().requireCatalog()
    const native = await this.group.native()
    const name = groupFilterName(native.identity)
    const page = await this.group.filters().list(name, { pageSize: GROUP_FILTER_LOOKUP_PAGE })
    const own = page.items.find((summary) => summary.name === name)
    if (own === undefined) return false
    this.group.remember(await this.group.filters().dropFilter(own.id))
    return true
  }

  /**
   * Release the group's policy. Its readers then receive every record. A
   * released group may only be configured with the digest it ran.
   */
  async release(): Promise<FilterBinding> {
    const released = await this.group.filters().releaseGroup(await this.active())
    this.group.remember(released.catalogPosition)
    return released.binding
  }

  /**
   * Preview the active policy over the stored records of one partition. A
   * preview joins no group, stores no offset, and changes no consumer state.
   */
  async preview(partitionId: number, options: FilterPreviewOptions = {}): Promise<FilterPreview> {
    const active = await this.active()
    return this.group
      .filters()
      .preview(
        this.group.streamName,
        this.group.topicName,
        partitionId,
        { kind: "revision", filterId: active.filterId, revision: active.revision },
        options
      )
  }

  /**
   * Evaluate the active policy against one supplied record and explain the
   * verdict. Nothing is read from a stream and nothing is stored.
   */
  async test(
    payload: Uint8Array | string,
    headers: readonly FilterHeader[] = []
  ): Promise<FilterTestResult> {
    const active = await this.active()
    return this.group
      .filters()
      .test(
        { kind: "revision", filterId: active.filterId, revision: active.revision },
        payload,
        headers
      )
  }

  private async active(): Promise<FilterBinding> {
    const binding = await this.get()
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

// The catalog names a group's own filter by the ids of the group incarnation
// it was configured for. The name is unique, so the first page holds it.
const GROUP_FILTER_LOOKUP_PAGE = 8

function groupFilterName(identity: FilterGroupIdentity): string {
  return `group:${String(identity.streamId)}:${identity.streamCreatedAtMicros.toString()}:${String(identity.topicId)}:${identity.topicCreatedAtMicros.toString()}:${identity.groupId.toString()}`
}

function requireSameGroup(expected: FilterGroupIdentity, actual: FilterGroupIdentity): void {
  if (
    expected.streamId !== actual.streamId ||
    expected.streamCreatedAtMicros !== actual.streamCreatedAtMicros ||
    expected.topicId !== actual.topicId ||
    expected.topicCreatedAtMicros !== actual.topicCreatedAtMicros ||
    expected.groupId !== actual.groupId
  ) {
    throw new FilterExecutionError("the group incarnation changed while its policy was resolved", {
      code: { kind: "known", name: "StaleGeneration" },
      reason: "source_changed",
      message: "the group incarnation changed while its policy was resolved, refresh its identity"
    })
  }
}

/**
 * Whether a group consumer reads through the group reader. A server that
 * advertises group-aware reads does. A server that serves consumer filters
 * without them must be upgraded, because its groups may be bound and a native
 * poll would read past their policies. A probe that established nothing is
 * not a server without filters. Only a server whose announcement, or refusal
 * to announce, positively lacks filters reads natively.
 */
export function policyAware(capabilities: Capabilities): boolean {
  if (capabilities.filters.groupPolicyReads) return true
  if (capabilities.filters.native) {
    throw new UnsupportedError(
      "this server serves consumer filters but not group-aware reads, upgrade it before consuming groups through the Laser SDK"
    )
  }
  if (capabilities.hello === "failed" || capabilities.hello === "unknown") {
    throw new TransportError(
      "the managed probe that decides whether this server resolves consumer group policies did not answer, reconnect and build the consumer again",
      true
    )
  }
  return false
}

function filteredStart(start: PollingStrategy): FilteredStart {
  switch (start.kind) {
    case "first":
    case "last":
    case "next":
      return { kind: start.kind }
    case "offset":
      return { kind: "offset", offset: start.value }
    case "timestamp":
      return { kind: "timestamp", micros: start.value }
  }
}
