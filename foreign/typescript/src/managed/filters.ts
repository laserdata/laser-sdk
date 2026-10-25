import {
  FILTER_MUTATION_ATTEMPTS,
  FILTER_MUTATION_BACKOFF_MS,
  FILTER_OUTCOME_WAIT_MS
} from "../retry.js"
import { BARE_SCOPE, type ResourceScope } from "../client/resource-scope.js"
import { resultCodeIsRetryable } from "../wire/result.js"
import { Json } from "../stream/codecs.js"
import { decodeBrowseReply, encodeGetSchema } from "../wire/browse.js"
import type { SchemaDef } from "../wire/control.js"
import { AGDX_GET_SCHEMA_CODE, QUERY_OP_VERSION } from "../wire/codes.js"
import type { Capabilities } from "../client/capabilities.js"
import {
  AmbiguousMutationError,
  ConfigError,
  FilterExecutionError,
  FilterFaultError,
  FilterOversizedRecordError,
  InvalidError,
  type LaserError,
  ProtocolError,
  QueryExecutionError,
  TimeoutError,
  TransportError,
  UnsupportedError
} from "../client/errors.js"
import { decodeManagedReply, executeManaged, requireManagedCommand } from "../client/managed.js"
import {
  type CoordinatorConnection,
  type HeaderFault,
  type LaserTransport,
  type ManagedTarget,
  POLLED_HEAD_BYTES,
  type PolledMessage,
  decodePolledBody,
  decodePolledHead,
  isRouteStale,
  serverErrorCode
} from "../iggy/apache-iggy.js"
import type { HeaderValue } from "../stream/header-value.js"
import type { ConsumerMessage } from "../stream/consumer.js"
import { withMessageJson } from "../stream/message.js"
import { mintUlidValue } from "../runtime/ulid.js"
import { decodeOne, encodeNamed } from "../wire/cbor.js"
import {
  AGDX_FILTERED_ACK_CODE,
  AGDX_FILTERED_POLL_CODE,
  FILTER_OP_VERSION
} from "../wire/codes.js"
import {
  FilterMutateCommand,
  FilterOperationCommand,
  FilterPreviewCommand,
  FilterTestCommand,
  GetFilterBindingCommand,
  ListFilterBindingsCommand,
  ListFilterRevisionsCommand,
  ListFiltersCommand
} from "../wire/commands.js"
import { CompiledFilter, type DecodeLimits } from "../wire/filter-eval.js"
import {
  encodeFilteredAck,
  type AckContext,
  type AppliedPolicy,
  type CatalogPosition,
  type ConsumerFilter,
  type FaultReason,
  type FilterBinding,
  type FilterBindingPage,
  type FilterCatalogOutcome,
  type FilterCatalogReply,
  type FilterConsumer,
  type FilterGroupIdentity,
  type FilterGroupRef,
  type FilterHeader,
  type FilterMutation,
  type FilterMutationOutcome,
  type FilterMutationResult,
  type FilterMutationStatus,
  type FilterOutcome,
  type FilterPreview,
  type FilterRef,
  type FilterReply,
  type FilterRevisionPage,
  type FilterRevisionRef,
  type FilterSource,
  type FilterTestResult,
  type FilteredPage,
  type FilteredPollRequest,
  type FilteredStart,
  type GroupFilterSpec,
  type SourceGeneration,
  type StopReason,
  decodeFilterReply,
  encodeFilteredPollRequest,
  filteredPageNextStart,
  validateCatalogPage,
  validateFilterGroupRef,
  validateFilterMutationRequest,
  validateFilterPreviewRequest,
  validateFilterTestRequest,
  validateFilteredPollRequest,
  type FilterPage,
  type ListFilters
} from "../wire/filter.js"
import {
  MAX_FILTER_CATALOG_PAGE,
  MAX_FILTER_PARSE_DEPTH,
  MAX_FILTER_PREVIEW_EXAMINED,
  MAX_FILTERED_PAGE_BYTES
} from "../wire/limits.js"

const DEFAULT_COUNT = 100
const DEFAULT_MAX_REPLY_BYTES = 1024 * 1024
const DEFAULT_IDLE_INTERVAL_MS = 250
const DEFAULT_PAGE_SIZE = 50
const DEFAULT_PREVIEW_RECORDS = 20
const DEFAULT_MAX_UNACKED_PAGES = 1024
const ASSIGNMENT_REFRESH_MS = 2_000
const MUTATION_ATTEMPTS = FILTER_MUTATION_ATTEMPTS
const MUTATION_RETRY_BACKOFF_MS = FILTER_MUTATION_BACKOFF_MS
const OUTCOME_POLL_INTERVAL_MS = 100
/** How long the typed catalog verbs wait for a mutation's authoritative outcome. */
export const DEFAULT_OUTCOME_WAIT_MS = FILTER_OUTCOME_WAIT_MS

const GET_STREAM_CODE = 200
const GET_TOPIC_CODE = 300
const GET_CONSUMER_GROUP_CODE = 600
const SYNC_CONSUMER_GROUP_CODE = 606
const HISTORY_UNAVAILABLE = 87
const CONSUMER_GROUP_PARTITION_NOT_OWNED = 5009
// Wider than any server bound, so a record the server could decode always
// decodes here too and a mismatch is a real disagreement.
const GUARD_LIMITS: DecodeLimits = {
  maxPayloadBytes: MAX_FILTERED_PAGE_BYTES,
  maxDepth: MAX_FILTER_PARSE_DEPTH
}
const UTF8 = new TextEncoder()

/**
 * Marks a page or record with the reader that read it, so an acknowledgment
 * can tell its own records from another reader's. A spread copy keeps it.
 */
export const READER_TAG: unique symbol = Symbol("laser.filters.reader")

/** The reader membership and read sequence behind one page or record. */
export interface ReaderTag {
  readonly owner: object
  readonly sequence: number
}

/** The transport a consumer group's filter handle and readers need. */
export type FilterTransport = Pick<
  LaserTransport,
  | "sendManaged"
  | "sendManagedTo"
  | "partitionPrimary"
  | "storePosition"
  | "joinConsumerGroup"
  | "leaveConsumerGroup"
  | "joinExistingConsumerGroup"
  | "openCoordinator"
  | "connectsNodes"
>

/** One page of matching records from one partition. */
export interface MatchedPage {
  readonly partitionId: number
  readonly records: readonly MatchedRecord[]
  /** The filter the server executed. */
  readonly policy: AppliedPolicy
  /** The source history the page was read from. */
  readonly generation: SourceGeneration
  readonly stop: StopReason
  readonly examined: number
  /** The partition head when the page was read. Reported, not progress. */
  readonly frontier: bigint
  /**
   * The offset acknowledging this page stores, once every earlier page is
   * handled too. Absent when the page examined nothing.
   */
  readonly safeAckOffset?: bigint
  readonly [READER_TAG]?: ReaderTag
}

/** One matching record, unchanged from the stream. Acknowledge the original reader-owned object. Cloning payload data does not transfer acknowledgment ownership. */
export interface MatchedRecord {
  readonly headersMalformed?: boolean
  /** Decode a JSON payload. Other codecs keep their original encoded bytes. */
  json(): unknown
  readonly partitionId: number
  readonly offset: bigint
  /** The partition head when the record was read. */
  readonly frontier: bigint
  /** `false` when the filter could not evaluate the record and a `pass` fault, foreign-record, or mismatch policy returned it unevaluated. */
  readonly evaluated: boolean
  /** The record as stored: payload, typed headers, id, and timestamps. Its
   * `currentOffset` is the partition head when the record was read. */
  readonly message: ConsumerMessage
  readonly [READER_TAG]?: ReaderTag
}

export interface FilterPreviewOptions {
  readonly fromOffset?: bigint
  readonly maxExamined?: number
  readonly maxRecords?: number
  readonly explain?: boolean
}

export interface CatalogPageOptions {
  readonly page?: number
  readonly pageSize?: number
}

/** A group mutation's binding with the control-log position the fold applied it at. */
export interface ConfiguredGroup {
  readonly binding: FilterBinding
  readonly catalogPosition?: CatalogPosition
}

/**
 * The catalog and native filter commands behind a consumer group's filter
 * handle, reached through `ConsumerGroup.filter()`. Native commands need a
 * server that serves consumer filters. The catalog also needs a managed
 * backend. Either missing throws `UnsupportedError`.
 */
export class Filters {
  /** @internal */
  constructor(
    private readonly transport: FilterTransport,
    private readonly capabilities: () => Promise<Capabilities>,
    private readonly scope: ResourceScope = BARE_SCOPE
  ) {}

  /** The stream whose schema registry this handle resolves writer schemas in.
   * @internal */
  schemaStream(): string | undefined {
    return this.scope.stream
  }

  // A filter with writer schemas resolves them in the registry this handle
  // registers schemas in, unless the caller named one.
  private scopedFilter(filter: ConsumerFilter): ConsumerFilter {
    const stream = this.scope.stream
    return filter.schemaRefs.length > 0 && filter.schemaStream === undefined && stream !== undefined
      ? { ...filter, schemaStream: stream }
      : filter
  }

  /** Throws `UnsupportedError` unless the group filter catalog is served. */
  async requireCatalog(): Promise<void> {
    const capabilities = await this.capabilities()
    requireManagedCommand(capabilities, FilterMutateCommand)
  }

  /** Evaluate a filter against one supplied record and explain the verdict. */
  async test(
    filter: FilterRef,
    payload: Uint8Array | string,
    headers: readonly FilterHeader[] = []
  ): Promise<FilterTestResult> {
    const request = {
      v: FILTER_OP_VERSION,
      filter,
      payload: typeof payload === "string" ? UTF8.encode(payload) : payload,
      headers
    }
    validateFilterTestRequest(request)
    const outcome = await this.native(FilterTestCommand, request)
    if (outcome.kind === "tested") return outcome.result
    throw unexpected("test")
  }

  /** Preview a filter over stored records of one partition. It joins no group and stores no offset. */
  async preview(
    stream: string,
    topic: string,
    partitionId: number,
    filter: FilterRef,
    options: FilterPreviewOptions = {}
  ): Promise<FilterPreview> {
    const request = {
      v: FILTER_OP_VERSION,
      source: { stream, topic },
      partitionId,
      filter,
      fromOffset: options.fromOffset ?? 0n,
      maxExamined: options.maxExamined ?? MAX_FILTER_PREVIEW_EXAMINED,
      maxRecords: options.maxRecords ?? DEFAULT_PREVIEW_RECORDS,
      explain: options.explain ?? false
    }
    validateFilterPreviewRequest(request)
    const target = await this.partitionTarget(stream, topic, partitionId)
    let outcome: FilterOutcome
    try {
      outcome = await this.native(FilterPreviewCommand, request, target)
    } catch (error) {
      // A preview the partition's primary no longer takes goes to the
      // caller's node, which serves it from its own copy.
      if (target.kind === "caller" || !isRouteStale(error)) throw error
      outcome = await this.native(FilterPreviewCommand, request, { kind: "caller" })
    }
    if (outcome.kind === "preview") return outcome.preview
    throw unexpected("preview")
  }

  /** One page of a filter's revisions, newest first. */
  async revisions(filterId: number, options: CatalogPageOptions = {}): Promise<FilterRevisionPage> {
    const request = {
      v: FILTER_OP_VERSION,
      filterId,
      page: options.page ?? 0,
      pageSize: options.pageSize ?? DEFAULT_PAGE_SIZE
    }
    validateCatalogPage(request.page, request.pageSize)
    const outcome = await this.catalog(ListFilterRevisionsCommand, request)
    if (outcome.kind === "revisions") return outcome.page
    throw unexpected("revisions")
  }

  /**
   * One page of saved filters whose name contains `nameContains`, newest first.
   *
   * @internal
   */
  async list(
    nameContains: string,
    options: CatalogPageOptions = {},
    beforeId?: number
  ): Promise<FilterPage> {
    const request: ListFilters = {
      v: FILTER_OP_VERSION,
      nameContains,
      ...(beforeId !== undefined ? { beforeId } : {}),
      page: options.page ?? 0,
      pageSize: options.pageSize ?? DEFAULT_PAGE_SIZE
    }
    validateCatalogPage(request.page, request.pageSize)
    const outcome = await this.catalog(ListFiltersCommand, request)
    if (outcome.kind === "filters") return outcome.page
    throw unexpected("filters")
  }

  /**
   * Drop a saved filter. The catalog releases every group bound to it.
   *
   * @internal
   */
  async dropFilter(filterId: number): Promise<CatalogPosition | undefined> {
    const applied = await this.apply({ kind: "drop", filterId })
    if (applied.result.kind === "dropped") return applied.catalogPosition
    throw unexpected("drop")
  }

  /** The binding of one consumer group. Throws with reason `not_found` when it is unbound. */
  async binding(group: FilterGroupRef): Promise<FilterBinding> {
    validateFilterGroupRef(group)
    const outcome = await this.catalog(GetFilterBindingCommand, { v: FILTER_OP_VERSION, group })
    if (outcome.kind === "binding") return outcome.binding
    throw unexpected("binding")
  }

  /** One page of bindings, optionally narrowed to one filter, one stream, or one stream and topic. */
  async bindings(
    options: CatalogPageOptions & {
      readonly filterId?: number
      readonly stream?: string
      readonly topic?: string
    } = {}
  ): Promise<FilterBindingPage> {
    const request = {
      v: FILTER_OP_VERSION,
      ...(options.filterId !== undefined ? { filterId: options.filterId } : {}),
      ...(options.stream !== undefined ? { stream: options.stream } : {}),
      ...(options.topic !== undefined ? { topic: options.topic } : {}),
      page: options.page ?? 0,
      pageSize: options.pageSize ?? DEFAULT_PAGE_SIZE
    }
    validateCatalogPage(request.page, request.pageSize)
    const outcome = await this.catalog(ListFilterBindingsCommand, request)
    if (outcome.kind === "bindings") return outcome.page
    throw unexpected("bindings")
  }

  /** The recorded outcome of a mutation. Throws with reason `not_found` until the catalog applies it. */
  async operation(operationId: bigint): Promise<FilterMutationOutcome> {
    const outcome = await this.catalog(FilterOperationCommand, {
      v: FILTER_OP_VERSION,
      operationId
    })
    if (outcome.kind === "mutation") return checkedMutationOutcome(operationId, outcome.outcome)
    throw unexpected("operation")
  }

  /** Add a revision. `expectedRevision` must still be the latest one. */
  async revise(
    filterId: number,
    expectedRevision: number,
    filter: ConsumerFilter
  ): Promise<FilterRevisionRef> {
    const applied = await this.apply({
      kind: "revise",
      filterId,
      expectedRevision,
      filter: this.scopedFilter(filter)
    })
    if (applied.result.kind === "revised") return applied.result.revision
    throw unexpected("revise")
  }

  /** Pause or resume a revision without changing its executable content or digest. */
  async setRevisionEnabled(filterId: number, revision: number, enabled: boolean): Promise<void> {
    await this.apply({ kind: "set_revision_enabled", filterId, revision, enabled })
  }

  /**
   * Give `group` its policy in one catalog transaction: a definition saved as
   * the group's own filter, or one of its existing revisions. A repeat with
   * the same digest keeps the binding, another digest conflicts. The returned
   * position is where the fold applied it, carried by the group's reads so
   * they never see this configuration as absent.
   */
  async configureGroup(
    group: FilterGroupRef,
    policy: GroupFilterSpec,
    operationId: bigint = mintUlidValue(),
    expectedIdentity?: FilterGroupIdentity
  ): Promise<ConfiguredGroup> {
    const identity =
      expectedIdentity ?? (await nativeGroup(this.transport, group, group.group)).identity
    const scoped: GroupFilterSpec =
      policy.kind === "definition"
        ? { kind: "definition", filter: this.scopedFilter(policy.filter) }
        : policy
    const applied = await this.apply(
      { kind: "configure_group", group, policy: scoped, expectedIdentity: identity },
      operationId
    )
    if (applied.result.kind === "bound") return bound(applied.result.binding, applied)
    throw unexpected("configure_group")
  }

  /**
   * Release this exact saved binding, including a group incarnation that was
   * deleted and recreated under the same name.
   */
  async releaseGroup(binding: FilterBinding): Promise<ConfiguredGroup> {
    const applied = await this.apply({
      kind: "unbind",
      group: binding.group,
      expectedDigest: binding.digest,
      expectedIdentity: binding.identity
    })
    if (applied.result.kind === "unbound") return bound(applied.result.binding, applied)
    throw unexpected("unbind")
  }

  /** Send one mutation under a caller-chosen `operationId`. The outcome may be pending. */
  async mutate(operationId: bigint, mutation: FilterMutation): Promise<FilterMutationOutcome> {
    const request = { v: FILTER_OP_VERSION, operationId, mutation }
    validateFilterMutationRequest(request)
    const outcome = await this.catalog(FilterMutateCommand, request)
    if (outcome.kind === "mutation") return checkedMutationOutcome(operationId, outcome.outcome)
    throw unexpected("mutate")
  }

  // Submit a mutation and wait for its authoritative outcome. A lost reply is
  // retried under the same id, which the catalog answers with the first
  // attempt's outcome. A rejection throws `FilterExecutionError`. An outcome
  // still pending after `DEFAULT_OUTCOME_WAIT_MS` throws
  // `AmbiguousMutationError` naming the operation id.
  private async apply(
    mutation: FilterMutation,
    operationId: bigint = mintUlidValue()
  ): Promise<AppliedMutation> {
    const submitted = await this.submit(operationId, mutation)
    const outcome =
      submitted.status.kind === "pending"
        ? await this.waitForOutcome(operationId, DEFAULT_OUTCOME_WAIT_MS)
        : submitted
    return {
      result: settled(operationId, outcome.status),
      ...(outcome.catalogPosition !== undefined ? { catalogPosition: outcome.catalogPosition } : {})
    }
  }

  private async waitForOutcome(
    operationId: bigint,
    timeoutMs: number
  ): Promise<FilterMutationOutcome> {
    const deadline = Date.now() + timeoutMs
    for (;;) {
      try {
        const outcome = await this.operation(operationId)
        if (outcome.status.kind !== "pending") return outcome
      } catch (error) {
        if (!isNotYetApplied(error)) throw error
      }
      if (Date.now() >= deadline) {
        throw new AmbiguousMutationError(
          `filter mutation ${operationId.toString()} has no outcome yet`
        )
      }
      await sleep(OUTCOME_POLL_INTERVAL_MS)
    }
  }

  private async submit(
    operationId: bigint,
    mutation: FilterMutation
  ): Promise<FilterMutationOutcome> {
    for (let attempt = 1; ; attempt += 1) {
      try {
        return await this.mutate(operationId, mutation)
      } catch (error) {
        if (!isTransient(error)) throw error
        if (attempt >= MUTATION_ATTEMPTS) {
          throw new AmbiguousMutationError(
            `filter mutation ${operationId.toString()}: outcome is unknown`,
            { cause: error }
          )
        }
        await sleep(MUTATION_RETRY_BACKOFF_MS)
      }
    }
  }

  private async native<Request>(
    command: Parameters<typeof executeManaged<Request, FilterReply>>[2],
    request: Request,
    target?: ManagedTarget
  ): Promise<FilterOutcome> {
    const reply = await executeManaged(
      this.transport,
      await this.capabilities(),
      command,
      request,
      target === undefined ? undefined : { target }
    )
    if (reply.kind === "ok") return reply.outcome
    throw filterError(reply.error)
  }

  // The partition's primary, or the caller's node when the route cannot be
  // read. Every node holds every partition, so either one answers.
  private async partitionTarget(
    stream: string,
    topic: string,
    partitionId: number
  ): Promise<ManagedTarget> {
    const lookup = this.transport.partitionPrimary
    if (lookup === undefined) return { kind: "caller" }
    try {
      return {
        kind: "replica",
        replica: await lookup.call(this.transport, stream, topic, partitionId)
      }
    } catch {
      return { kind: "caller" }
    }
  }

  private async catalog<Request>(
    command: Parameters<typeof executeManaged<Request, FilterCatalogReply>>[2],
    request: Request
  ): Promise<FilterCatalogOutcome> {
    const reply = await executeManaged(this.transport, await this.capabilities(), command, request)
    if (reply.kind === "ok") return reply.outcome
    throw filterError(reply.error)
  }
}

interface AppliedMutation {
  readonly result: FilterMutationResult
  readonly catalogPosition?: CatalogPosition
}

function checkedMutationOutcome(
  operationId: bigint,
  outcome: FilterMutationOutcome
): FilterMutationOutcome {
  if (
    outcome.v !== FILTER_OP_VERSION ||
    outcome.operationId !== operationId ||
    (outcome.catalogPosition?.operationId !== undefined &&
      outcome.catalogPosition.operationId !== operationId)
  ) {
    throw new ProtocolError("filter mutation outcome does not match the requested operation")
  }
  return outcome
}

function bound(binding: FilterBinding, applied: AppliedMutation): ConfiguredGroup {
  return {
    binding,
    ...(applied.catalogPosition !== undefined ? { catalogPosition: applied.catalogPosition } : {})
  }
}

/** Builds a {@link FilteredReader} over one consumer group. Start from `ConsumerGroup.reader()`. */
export class FilteredReaderBuilder {
  private startAt: FilteredStart = { kind: "next" }
  private pageCount = DEFAULT_COUNT
  private examinedBudget?: number
  private replyBytes = DEFAULT_MAX_REPLY_BYTES
  private guarded = false
  private idleMs = DEFAULT_IDLE_INTERVAL_MS
  private maxUnacked = DEFAULT_MAX_UNACKED_PAGES
  private readonly selectedPartitions = new Set<number>()

  private constructor(
    private readonly transport: FilterTransport,
    private readonly capabilities: () => Promise<Capabilities>,
    private readonly filters: Filters,
    private readonly source: FilterSource,
    private readonly consumer: Exclude<FilterConsumer, { readonly kind: "consumer" }>,
    private readonly filter: Extract<FilterRef, { readonly kind: "bound" | "group" }>,
    private readonly minCatalogPosition?: CatalogPosition
  ) {}

  /** @internal */
  static create(
    transport: FilterTransport,
    capabilities: () => Promise<Capabilities>,
    filters: Filters,
    source: FilterSource,
    consumer: Exclude<FilterConsumer, { readonly kind: "consumer" }>,
    filter: Extract<FilterRef, { readonly kind: "bound" | "group" }>,
    minCatalogPosition?: CatalogPosition
  ): FilteredReaderBuilder {
    return new FilteredReaderBuilder(
      transport,
      capabilities,
      filters,
      source,
      consumer,
      filter,
      minCatalogPosition
    )
  }

  /**
   * Where each partition read at the build starts. Defaults to after the
   * stored offset. A partition the group hands this reader later always resumes
   * after the group's stored offset, so the previous owner's unacknowledged
   * backlog is read, not skipped.
   */
  start(start: FilteredStart): this {
    this.startAt = start
    return this
  }

  /** Read this partition only while the group assigns it to this member. Repeat for several. */
  partition(partitionId: number): this {
    if (!Number.isSafeInteger(partitionId) || partitionId < 0 || partitionId > 0xffff_ffff) {
      throw new InvalidError("partition must be an unsigned 32-bit integer")
    }
    this.selectedPartitions.add(partitionId)
    return this
  }

  /** Most matching records per page. */
  count(count: number): this {
    this.pageCount = count
    return this
  }

  /**
   * Most source records one page examines, independent of `count`. Without it
   * the server's own budget bounds the scan. A page that examines its budget
   * without a match returns empty and the reader continues from where it
   * stopped.
   */
  maxExamined(records: number): this {
    this.examinedBudget = records
    return this
  }

  /** Most record bytes per page. The server may lower it. */
  maxReplyBytes(bytes: number): this {
    this.replyBytes = bytes
    return this
  }

  /** Re-evaluate returned records and fail on disagreement. Regex predicates cannot use this guard. */
  localGuard(enabled: boolean): this {
    this.guarded = enabled
    return this
  }

  /**
   * How long `nextPage` waits after a round that found nothing new, and how
   * long a failing or blocked partition waits before it is read again.
   */
  idleInterval(intervalMs: number): this {
    if (!Number.isFinite(intervalMs) || intervalMs < 0)
      throw new InvalidError("the idle interval must be finite and non-negative")
    this.idleMs = intervalMs
    return this
  }

  /** Most outstanding record-bearing pages per partition. Defaults to 1024. */
  maxUnackedPages(pages: number): this {
    if (!Number.isSafeInteger(pages) || pages < 1)
      throw new InvalidError("maxUnackedPages must be a positive integer")
    this.maxUnacked = pages
    return this
  }

  async build(): Promise<FilteredReader> {
    const capabilities = await this.capabilities()
    if (this.filter.kind === "group" && !capabilities.filters.groupPolicyReads) {
      throw new UnsupportedError(
        "this server serves consumer filters but not group-aware reads, upgrade it before reading groups through the Laser SDK",
        { surface: "filters", feature: "group_policy_reads" }
      )
    }
    if (this.filter.kind !== "group" && !capabilities.filters.native) {
      throw new UnsupportedError("consumer filters are not served by this server", {
        surface: "filters"
      })
    }
    const consumer = this.consumer
    const request: FilteredPollRequest = {
      v: FILTER_OP_VERSION,
      source: this.source,
      partitionId: 0,
      consumer,
      filter: this.filter,
      start: this.startAt,
      count: this.pageCount,
      maxReplyBytes: this.replyBytes,
      ...(this.examinedBudget !== undefined ? { maxExamined: this.examinedBudget } : {}),
      ...(this.minCatalogPosition !== undefined
        ? { minCatalogPosition: this.minCatalogPosition }
        : {})
    }
    validateFilteredPollRequest(request)
    const pinnedSource =
      consumer.kind === "group_id"
        ? await sourceIncarnation(this.transport, this.source)
        : undefined
    // A group reader owns the connection that holds its membership, so two
    // readers are two members. An injected client has only the shared one.
    const coordinator =
      this.transport.connectsNodes === true && this.transport.openCoordinator !== undefined
        ? await this.transport.openCoordinator()
        : sharedCoordinator(this.transport)
    let membership: Membership
    try {
      membership = await Membership.join(
        coordinator,
        this.source,
        consumer.kind === "group_id" ? Number(consumer.id) : consumer.name,
        this.transport,
        pinnedSource
      )
    } catch (error) {
      await coordinator.close()
      throw error
    }
    return FilteredReader.create({
      transport: this.transport,
      ...(pinnedSource === undefined ? {} : { pinnedSource }),
      filters: this.filters,
      coordinator,
      request,
      start: this.startAt,
      idleIntervalMs: this.idleMs,
      maxUnackedPages: this.maxUnacked,
      guard: undefined,
      guardEnabled: this.guarded,
      membership,
      selectedPartitions: new Set(this.selectedPartitions),
      partitionIds: membership.partitions.filter(
        (partition) => this.selectedPartitions.size === 0 || this.selectedPartitions.has(partition)
      )
    })
  }
}

type SourceIncarnation = Pick<
  SourceGeneration,
  "streamId" | "streamCreatedAtMicros" | "topicId" | "topicCreatedAtMicros"
>

/** What a filtered reader is built from. Builders assemble it, tests may construct it directly. */
interface ReaderSettings {
  readonly pinnedSource?: SourceIncarnation
  readonly transport: FilterTransport
  readonly filters: Filters
  readonly coordinator: CoordinatorConnection
  readonly request: FilteredPollRequest
  readonly start: FilteredStart
  readonly idleIntervalMs: number
  readonly maxUnackedPages?: number
  readonly guard: CompiledFilter | undefined
  readonly guardEnabled?: boolean
  readonly membership: Membership | undefined
  readonly partitionIds: readonly number[]
  readonly selectedPartitions?: ReadonlySet<number>
}

type Read =
  | { readonly kind: "page"; readonly page: MatchedPage }
  | { readonly kind: "empty"; readonly more: boolean }
  | { readonly kind: "saturated" }

/**
 * Reads the records a consumer group's policy selects, with their original
 * offsets, and stores progress through fenced acknowledgments. Pages are read
 * ahead of acknowledgments, and a page's safe offset is stored only after it
 * and every earlier page of the partition are fully handled. Pages that
 * matched nothing are stored by the reader itself. A partition that fails,
 * faults, or blocks waits one idle interval before it is read again, while
 * the others keep reading. A recreated partition or a new group binding
 * restarts the partition from its stored offset once, and the error that
 * reports it reaches the caller. Every read goes to the partition's primary
 * when its route is known, any other node serves it from its own copy
 * otherwise, and every acknowledgment is a consumer offset store fenced to the
 * partition incarnation and the group owner the page was read under. Drive one
 * reader from one task at a time.
 */
export class FilteredReader implements AsyncDisposable, AsyncIterable<MatchedRecord> {
  private initialAssignment: boolean
  private readonly progress = new Map<number, PartitionProgress>()
  private readonly revoked = new Set<number>()
  // The replica id of each partition's primary, looked up before the first
  // read and again after a read that found it moved or gone.
  private readonly leaders = new Map<number, number>()
  private readonly buffered: MatchedRecord[] = []
  private guard: CompiledFilter | undefined
  private cursor = 0
  private examined = 0
  private sequence = 0
  private owner: object = {}
  private readonly retiredOwners = new WeakSet<object>()
  private closed = false
  private deferredError: unknown
  private pinnedSource: SourceIncarnation | undefined

  // Partitions whose last read found the group assignment gone. The next such
  // read reaches the caller instead of syncing the group again.
  private readonly lostAssignments = new Set<number>()

  private constructor(private readonly settings: ReaderSettings) {
    this.initialAssignment =
      (settings.membership?.partitions.length ?? settings.partitionIds.length) === 0
    this.guard = settings.guard
    this.pinnedSource = settings.pinnedSource
    for (const partitionId of settings.partitionIds) {
      this.progress.set(partitionId, new PartitionProgress(settings.start))
    }
  }

  /** @internal */
  static create(settings: ReaderSettings): FilteredReader {
    return new FilteredReader(settings)
  }

  /** The next page with at least one match. Waits while nothing is new, up to `timeoutMs` when given. */
  async nextPage(options: { readonly timeoutMs?: number } = {}): Promise<MatchedPage> {
    const deadline = options.timeoutMs === undefined ? undefined : Date.now() + options.timeoutMs
    for (;;) {
      const round = await this.round()
      if (round.kind === "page") return round.page
      if (deadline !== undefined && Date.now() >= deadline) {
        throw new TimeoutError("a filtered page with a match")
      }
      if (round.kind === "idle") await sleep(this.settings.idleIntervalMs)
    }
  }

  /**
   * Read each partition in turn until a page matches or every partition has
   * nothing new, without the idle wait. `undefined` when nothing is new.
   */
  async tryNextPage(): Promise<MatchedPage | undefined> {
    for (;;) {
      const round = await this.round()
      if (round.kind === "page") return round.page
      if (round.kind === "idle") return undefined
    }
  }

  /** One bounded round without an idle wait. The flag means unmatched work remains. */
  async readRound(): Promise<readonly [MatchedPage | undefined, boolean]> {
    const round = await this.round()
    return [round.kind === "page" ? round.page : undefined, round.kind === "busy"]
  }

  /** Source records examined in the last bounded round, including empty pages. */
  examinedInRound(): number {
    return this.examined
  }

  /** The next matching record, one at a time over `nextPage`. */
  async nextRecord(options: { readonly timeoutMs?: number } = {}): Promise<MatchedRecord> {
    for (;;) {
      this.requireOpen()
      const record = this.buffered.shift()
      if (record !== undefined) return record
      const page = await this.nextPage(options)
      this.requireOpen()
      this.buffered.push(...page.records)
    }
  }

  /** Mark one record handled, and store the progress this completes. */
  async ack(record: MatchedRecord): Promise<void> {
    this.requireOpen()
    const sequence = this.requireSequence(record)
    this.partitionProgress(record.partitionId).completeRecord(sequence, record.offset)
    await this.flush(record.partitionId)
  }

  /** Mark this partition's preceding records through `record` handled. Later records stay pending. */
  async ackThrough(record: MatchedRecord): Promise<void> {
    this.requireOpen()
    const sequence = this.requireSequence(record)
    if (!this.partitionProgress(record.partitionId).completeThrough(sequence, record.offset)) {
      throw new InvalidError("the record is past the page acknowledgment boundary")
    }
    await this.flush(record.partitionId)
  }

  /** Mark every record of `page` handled, and store the progress this completes. */
  async ackPage(page: MatchedPage): Promise<void> {
    this.requireOpen()
    const sequence = this.requireSequence(page)
    this.partitionProgress(page.partitionId).completePage(sequence)
    await this.flush(page.partitionId)
  }

  /**
   * Mark this partition's records through `record` handled without storing
   * yet. The normal consumer delivers in order, so every delivery completes
   * the prefix through it, and it stores that prefix on its own cadence
   * through `flushCompleted`.
   *
   * @internal
   */
  handled(record: MatchedRecord): void {
    const tag = record[READER_TAG]
    if (tag?.owner !== this.owner) return
    this.progress.get(record.partitionId)?.completeThrough(tag.sequence, record.offset)
  }

  /**
   * Store every completed prefix now.
   *
   * @internal
   */
  async flushCompleted(): Promise<void> {
    let failure: unknown
    for (const partitionId of [...this.progress.keys()]) {
      try {
        await this.flush(partitionId)
      } catch (error) {
        failure ??= error
      }
    }
    if (failure !== undefined) {
      throw failure instanceof Error
        ? failure
        : new TransportError("storing group progress failed", true, { cause: failure })
    }
  }

  /** The partitions this reader reads now. */
  partitions(): readonly number[] {
    return [...this.progress.keys()].filter((partitionId) => !this.revoked.has(partitionId))
  }

  /** How long this reader waits, in milliseconds, when nothing is new. */
  idleIntervalMs(): number {
    return this.settings.idleIntervalMs
  }

  /**
   * Whether `value` was read by this reader in its current membership, so it
   * can still be acknowledged. A rejoin retires every earlier page.
   */
  owns(value: MatchedPage | MatchedRecord): boolean {
    const tag = value[READER_TAG]
    if (tag?.owner !== this.owner) return false
    return this.progress.get(value.partitionId)?.knows(tag.sequence) ?? false
  }

  /**
   * Store completed progress, leave the group, and close the coordinator
   * connection. A round still running stops at its next step.
   */
  async close(): Promise<void> {
    if (this.closed) return
    this.closed = true
    this.buffered.length = 0
    let failure: unknown
    for (const partitionId of [...this.progress.keys()]) {
      try {
        await this.flush(partitionId)
      } catch (error) {
        failure ??= error
      }
    }
    try {
      await this.settings.membership?.leave()
    } catch (error) {
      failure ??= error
    }
    await this.settings.coordinator.close()
    if (failure !== undefined) {
      throw failure instanceof Error
        ? failure
        : new TransportError("closing the filtered reader failed", false, { cause: failure })
    }
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close()
  }

  async *[Symbol.asyncIterator](): AsyncIterator<MatchedRecord> {
    for (;;) yield await this.nextRecord()
  }

  // One read of every partition that is not waiting, in turn from where the
  // last round stopped. A failing partition is deferred and the rest still
  // read, so one partition never starves the others, and its error reaches
  // the caller at most once per idle interval.
  private async round(): Promise<
    { readonly kind: "page"; readonly page: MatchedPage } | { readonly kind: "busy" | "idle" }
  > {
    this.requireOpen()
    this.examined = 0
    if (this.deferredError !== undefined) {
      const error = this.deferredError
      this.deferredError = undefined
      throw error instanceof Error
        ? error
        : new TransportError("a filtered partition failed", true, { cause: error })
    }
    if (this.settings.membership?.isDue() === true) await this.refreshMembership()
    const readable = this.partitions()
    if (readable.length === 0) return { kind: "idle" }
    const now = performance.now()
    let busy = false
    let failure: unknown
    const saturated: number[] = []
    for (let step = 0; step < readable.length; step += 1) {
      this.requireOpen()
      const index = (this.cursor + step) % readable.length
      const partitionId = readable[index] ?? 0
      if (this.progress.get(partitionId)?.waiting(now) === true) continue
      try {
        const read = await this.read(partitionId)
        if (read.kind === "page") {
          this.cursor = index + 1
          this.deferredError = failure
          return read
        }
        if (read.kind === "empty") busy ||= read.more
        if (read.kind === "saturated") saturated.push(partitionId)
      } catch (error) {
        if (this.closed) throw error
        this.progress.get(partitionId)?.defer(now + this.settings.idleIntervalMs)
        failure ??= error
      }
    }
    if (failure !== undefined) {
      throw failure instanceof Error
        ? failure
        : new TransportError("a filtered read failed", true, { cause: failure })
    }
    if (saturated.length > 0 && !busy) {
      throw new InvalidError(
        `partitions ${saturated.join(", ")} each have ${String(this.settings.maxUnackedPages ?? DEFAULT_MAX_UNACKED_PAGES)} unacknowledged pages. Acknowledge before reading more`
      )
    }
    return { kind: busy ? "busy" : "idle" }
  }

  private async read(partitionId: number): Promise<Read> {
    const progress = this.partitionProgress(partitionId)
    if (progress.inFlight >= (this.settings.maxUnackedPages ?? DEFAULT_MAX_UNACKED_PAGES))
      return { kind: "saturated" }
    const request: FilteredPollRequest = {
      ...this.settings.request,
      partitionId,
      start: progress.start
    }
    let outcome: FilterOutcome
    try {
      outcome = await this.exchange(partitionId, encodeFilteredPollRequest(request))
      this.lostAssignments.delete(partitionId)
    } catch (error) {
      if (isAssignmentLost(error)) {
        // The round ends empty and the next one follows the synced assignment,
        // so the other partitions of this round never wait. A second lost
        // assignment in a row reaches the caller.
        this.leaders.delete(partitionId)
        if (this.lostAssignments.has(partitionId)) throw error
        this.lostAssignments.add(partitionId)
        this.settings.membership?.expire()
        return { kind: "empty", more: false }
      }
      this.restartAfter(partitionId, error)
      // The group's policy changed under this read. The partition restarted
      // from its stored offset and is read again at once under the policy the
      // group holds now. Only an explicit acknowledgment of a record read
      // under the old policy reports the change.
      if (error instanceof FilterExecutionError && error.detail.reason === "conflict") {
        return { kind: "empty", more: true }
      }
      throw error
    }
    this.requireOpen()
    if (outcome.kind !== "page") throw unexpected("filtered poll")
    const page = outcome.page
    this.examined += page.examined
    if (page.records.byteLength > request.maxReplyBytes + POLLED_HEAD_BYTES)
      throw new ProtocolError("filtered page exceeds the requested record byte limit")
    if (page.records.byteLength < POLLED_HEAD_BYTES)
      throw new ProtocolError("filtered record body is truncated")
    if (decodePolledHead(page.records).count !== page.matched || page.matched > request.count)
      throw new ProtocolError("filtered record body has invalid counts")
    const messages = decodePolledBody(page.records)
    validatePage(request, page, messages)
    if (request.consumer.kind === "group_id") {
      const pinned = this.pinnedSource
      const current = page.generation
      if (
        pinned !== undefined &&
        (pinned.streamId !== current.streamId ||
          pinned.streamCreatedAtMicros !== current.streamCreatedAtMicros ||
          pinned.topicId !== current.topicId ||
          pinned.topicCreatedAtMicros !== current.topicCreatedAtMicros)
      )
        throw filterError({
          code: { kind: "known", name: "NotFound" },
          reason: "not_found",
          message:
            "the numeric consumer group belongs to a replaced stream or topic, build a new reader"
        })
      this.pinnedSource ??= current
    }
    const unevaluated = new Set(page.unevaluated)
    // An unfiltered page ran no filter, so there is nothing to check.
    if (page.policy.mode === "filtered") {
      if (this.settings.guardEnabled && this.guard === undefined) {
        this.guard = await compileGuard(
          this.settings.transport,
          await boundDefinition(this.settings.filters, request.source, page),
          this.settings.filters.schemaStream()
        )
      }
      if (this.guard !== undefined) {
        try {
          checkGuard(this.guard, page, messages, unevaluated)
        } catch (error) {
          this.restartAfter(partitionId, error)
          throw error
        }
      }
    }
    this.sequence += 1
    const sequence = this.sequence
    progress.record(
      sequence,
      page,
      messages.map((message) => message.offset)
    )
    const blocked = progress.blocked
    if (blocked !== undefined) progress.defer(performance.now() + this.settings.idleIntervalMs)
    if (messages.length === 0) {
      try {
        await this.flush(partitionId)
      } catch {
        // Kept unstored: the next acknowledgment of this partition sends it again.
      }
      if (blocked !== undefined) {
        throw blocked.stop === "fault"
          ? new FilterFaultError(partitionId, blocked.offset, blocked.reason)
          : new FilterOversizedRecordError(partitionId, blocked.offset)
      }
      return { kind: "empty", more: page.stop === "filled" || page.stop === "budget" }
    }
    const tag = { owner: this.owner, sequence }
    const records = messages.map((message): MatchedRecord => ({
      json: () => new Json().decode(message.payload),
      partitionId,
      offset: message.offset,
      frontier: page.frontier,
      evaluated: page.policy.mode === "filtered" && !unevaluated.has(message.offset),
      headersMalformed: message.headersMalformed !== undefined,
      message: matchedMessage(partitionId, page.frontier, message),
      [READER_TAG]: tag
    }))
    const matched: MatchedPage = {
      partitionId,
      records,
      policy: page.policy,
      generation: page.generation,
      stop: page.stop,
      examined: page.examined,
      frontier: page.frontier,
      ...(page.safeAckOffset !== undefined ? { safeAckOffset: page.safeAckOffset } : {}),
      [READER_TAG]: tag
    }
    return { kind: "page", page: matched }
  }

  // A recreated partition or group, or a new group binding, ends the history
  // or the policy the partition's continuation was issued for. The partition
  // starts over from its stored offset, the guard reloads a rebound filter,
  // and the error reaches the caller once.
  private restartAfter(partitionId: number, error: unknown): void {
    const reason =
      serverErrorCode(error) === HISTORY_UNAVAILABLE
        ? "source_changed"
        : error instanceof FilterExecutionError
          ? error.detail.reason
          : undefined
    if (reason !== "source_changed" && reason !== "conflict") return
    this.leaders.delete(partitionId)
    this.progress.get(partitionId)?.restart({ kind: "next" })
    for (let index = this.buffered.length - 1; index >= 0; index -= 1) {
      if (this.buffered[index]?.partitionId === partitionId) this.buffered.splice(index, 1)
    }
    if (reason === "source_changed") this.settings.membership?.expire()
    if (this.settings.guardEnabled) this.guard = undefined
  }

  // Store the completed prefix of one partition. A lost reply keeps the target
  // unstored and the next store sends it again. Stores are monotone, so a
  // repeat never moves progress back.
  private async flush(partitionId: number): Promise<void> {
    const target = this.partitionProgress(partitionId).unstored
    if (target === undefined) {
      this.retire(partitionId)
      return
    }
    try {
      const outcome = await this.exchange(
        partitionId,
        encodeFilteredAck({
          v: FILTER_OP_VERSION,
          source: this.settings.request.source,
          consumer: this.settings.request.consumer,
          partitionId,
          offset: target.offset,
          generation: target.generation,
          ackContext: target.ackContext,
          mode: target.policy.mode,
          policyGeneration: target.policy.policyGeneration,
          ...(target.groupId === undefined ? {} : { groupId: target.groupId }),
          ...(target.policy.digest === undefined ? {} : { digest: target.policy.digest })
        }),
        AGDX_FILTERED_ACK_CODE
      )
      if (
        outcome.kind !== "acknowledged" ||
        outcome.receipt.partitionId !== partitionId ||
        outcome.receipt.offset !== target.offset ||
        !sameGeneration(outcome.receipt.generation, target.generation)
      ) {
        throw new ProtocolError("the acknowledgment receipt does not match its page")
      }
    } catch (error) {
      if (isAssignmentLost(error)) this.settings.membership?.expire()
      this.restartAfter(partitionId, error)
      throw error
    }
    this.progress.get(partitionId)?.stored(target)
    this.retire(partitionId)
  }

  // One read on the partition's primary. A read the primary no longer takes
  // looks the route up again and goes to the node it names, once.
  private async exchange(
    partitionId: number,
    request: ReadonlyMap<string, unknown>,
    code = AGDX_FILTERED_POLL_CODE
  ): Promise<FilterOutcome> {
    const payload = encodeNamed(request)
    let rerouted = false
    let reply: Uint8Array
    for (;;) {
      const target = await this.readTarget(partitionId)
      try {
        reply = (await this.settings.coordinator.sendManagedTo(code, payload, target)).reply
        break
      } catch (error) {
        const moved = isRouteStale(error)
        if (moved) this.leaders.delete(partitionId)
        if (!moved || rerouted || target.kind === "caller") throw error
        rerouted = true
      }
    }
    const decoded = decodeManagedReply(
      (bytes) => decodeFilterReply(decodeOne(bytes, "filter reply"), "filter reply"),
      reply
    )
    if (decoded.kind === "ok") return decoded.outcome
    if (decoded.error.reason === "unavailable") this.leaders.delete(partitionId)
    throw filterError(decoded.error)
  }

  // The partition's primary, or the caller's node when the route cannot be
  // read. Every node holds every partition, so either one answers.
  private async readTarget(partitionId: number): Promise<ManagedTarget> {
    const known = this.leaders.get(partitionId)
    if (known !== undefined) return { kind: "replica", replica: known }
    const source = this.settings.request.source
    try {
      const replica = await this.settings.coordinator.partitionPrimary(
        source.stream,
        source.topic,
        partitionId
      )
      this.leaders.set(partitionId, replica)
      return { kind: "replica", replica }
    } catch {
      return { kind: "caller" }
    }
  }

  // Follow the group's current assignment. The whole change is applied to the
  // reader before anything awaits. A partition that left the assignment stops
  // being read but keeps its in-flight pages, so records already handed out
  // can still be acknowledged while the server's offset fence allows it.
  private async refreshMembership(): Promise<void> {
    const membership = this.settings.membership
    if (membership === undefined || !(await membership.sync())) return
    this.requireOpen()
    const selection = this.settings.selectedPartitions
    const assigned = new Set(
      membership.partitions.filter(
        (partition) => selection === undefined || selection.size === 0 || selection.has(partition)
      )
    )
    const rejoined = membership.takeRejoined()
    if (rejoined) {
      this.initialAssignment = false
      // A rejoin can land in a recreated group with another binding.
      if (this.settings.guardEnabled) this.guard = undefined
      this.retiredOwners.add(this.owner)
      this.owner = {}
      this.progress.clear()
      this.revoked.clear()
      this.buffered.length = 0
    }
    const left = [...this.progress.keys()].filter((partitionId) => !assigned.has(partitionId))
    for (const partitionId of left) {
      this.revoked.add(partitionId)
      this.retire(partitionId)
    }
    // A partition gained on a rebalance resumes after the group's stored
    // offset, which holds what the previous owner acknowledged. One that
    // comes back before its revoked entry retired starts over the same way,
    // so it never replays another member's work from an old continuation.
    const initialAssignment = this.initialAssignment
    if (assigned.size > 0) this.initialAssignment = false
    for (const partitionId of assigned) {
      if (this.revoked.delete(partitionId) || !this.progress.has(partitionId)) {
        for (let index = this.buffered.length - 1; index >= 0; index--) {
          if (this.buffered[index]?.partitionId === partitionId) this.buffered.splice(index, 1)
        }
        this.progress.set(
          partitionId,
          new PartitionProgress(initialAssignment ? this.settings.start : { kind: "next" })
        )
      }
    }
  }

  // Forget a revoked partition once nothing of it is in flight.
  private retire(partitionId: number): void {
    const progress = this.progress.get(partitionId)
    if (
      this.revoked.has(partitionId) &&
      progress?.inFlight === 0 &&
      progress.unstored === undefined
    ) {
      this.progress.delete(partitionId)
      this.revoked.delete(partitionId)
    }
  }

  private partitionProgress(partitionId: number): PartitionProgress {
    const progress = this.progress.get(partitionId)
    if (progress === undefined) {
      throw filterError({
        code: { kind: "known", name: "Stale" },
        reason: "membership_stale",
        message: `partition ${String(partitionId)} is no longer read by this reader. Its unacknowledged records are read again`
      })
    }
    return progress
  }

  // The owner is checked before the partition, so a foreign record reads as
  // foreign, and a record of this reader's earlier membership reads as retired.
  private requireSequence(value: MatchedPage | MatchedRecord): number {
    const sequence = this.sequenceOf(value)
    if (!this.partitionProgress(value.partitionId).knows(sequence))
      throw filterError({
        code: { kind: "known", name: "Stale" },
        reason: "membership_stale",
        message: "the acknowledgment belongs to a retired partition read"
      })
    return sequence
  }

  private sequenceOf(value: MatchedRecord | MatchedPage): number {
    const tag = value[READER_TAG]
    if (tag?.owner === this.owner) return tag.sequence
    if (tag !== undefined && this.retiredOwners.has(tag.owner)) {
      throw new InvalidError(
        "the page or record was read before this reader rejoined its group, so it can no longer be acknowledged"
      )
    }
    throw new InvalidError("the page or record belongs to another filtered reader")
  }

  private requireOpen(): void {
    if (this.closed) throw new ConfigError("the filtered reader is closed")
  }
}

interface AckTarget {
  readonly groupId?: bigint
  readonly offset: bigint
  readonly generation: SourceGeneration
  readonly ackContext: AckContext
  readonly policy: AppliedPolicy
}

interface PendingPage {
  readonly sequence: number
  target: AckTarget | undefined
  readonly outstanding: Set<bigint>
}

interface Blocked {
  readonly stop: "fault" | "oversized_record"
  readonly offset: bigint
  readonly reason?: FaultReason
}

/** One partition's read position and acknowledgment state. */
class PartitionProgress {
  private current: FilteredStart
  private pages: PendingPage[] = []
  private pendingStore: AckTarget | undefined
  private stoppedAt: Blocked | undefined
  private retryAt: number | undefined
  private firstSequence: number | undefined
  private settledSequence = 0

  constructor(private original: FilteredStart) {
    this.current = original
  }

  get start(): FilteredStart {
    return this.current
  }

  get blocked(): Blocked | undefined {
    return this.stoppedAt
  }

  get inFlight(): number {
    return this.pages.length
  }

  get unstored(): AckTarget | undefined {
    return this.pendingStore
  }

  /**
   * Start over at `start` after the source history or the executed filter
   * changed. Every page in flight is dropped, so its records are delivered
   * again from the stored offset.
   */
  restart(start: FilteredStart): void {
    this.original = start
    this.current = start
    this.pages = []
    this.pendingStore = undefined
    this.stoppedAt = undefined
    this.retryAt = undefined
    this.firstSequence = undefined
    this.settledSequence = 0
  }

  knows(sequence: number): boolean {
    return (
      this.pages.some((page) => page.sequence === sequence) ||
      (this.firstSequence !== undefined &&
        sequence >= this.firstSequence &&
        sequence <= this.settledSequence)
    )
  }

  /** Whether the partition waits out a delay after an error or a block. */
  waiting(now: number): boolean {
    return this.retryAt !== undefined && now < this.retryAt
  }

  defer(until: number): void {
    this.retryAt = until
  }

  /**
   * Track a page and its returned `offsets`, and continue after it. A blocked
   * partition is read again after its delay, so a block ends once the record
   * ages out of the partition or the policy stops faulting.
   */
  record(sequence: number, page: FilteredPage, offsets: readonly bigint[]): void {
    this.retryAt = undefined
    this.firstSequence ??= sequence
    this.current = filteredPageNextStart(page, this.original)
    this.stoppedAt =
      page.fault === undefined
        ? undefined
        : page.stop === "fault"
          ? { stop: "fault", offset: page.fault.offset, reason: page.fault.reason }
          : page.stop === "oversized_record"
            ? { stop: "oversized_record", offset: page.fault.offset }
            : undefined
    const target =
      page.safeAckOffset === undefined
        ? undefined
        : {
            offset: page.safeAckOffset,
            generation: page.generation,
            ackContext: page.ackContext,
            policy: page.policy,
            ...(page.policy.groupId !== undefined ? { groupId: page.policy.groupId } : {})
          }
    const newest = this.pages.at(-1)
    if (offsets.length === 0 && newest !== undefined) {
      if (target !== undefined) newest.target = target
    } else {
      this.pages.push({ sequence, target, outstanding: new Set(offsets) })
    }
    this.settle()
  }

  completeRecord(sequence: number, offset: bigint): void {
    this.pages.find((page) => page.sequence === sequence)?.outstanding.delete(offset)
    this.settle()
  }

  completeThrough(sequence: number, offset: bigint): boolean {
    const target = this.pages.find((page) => page.sequence === sequence)?.target
    if (target === undefined) return true
    if (offset > target.offset) return false
    for (const page of this.pages) {
      if (page.sequence < sequence) page.outstanding.clear()
      else if (page.sequence === sequence) {
        for (const pending of page.outstanding)
          if (pending <= offset) page.outstanding.delete(pending)
      }
    }
    this.settle()
    if (this.pendingStore === undefined || this.pendingStore.offset < offset) {
      this.pendingStore = { ...target, offset }
    }
    return true
  }

  completePage(sequence: number): void {
    this.pages.find((page) => page.sequence === sequence)?.outstanding.clear()
    this.settle()
  }

  stored(target: AckTarget): void {
    if (this.pendingStore === target) this.pendingStore = undefined
  }

  // Retire the completed prefix. Its newest safe offset supersedes any older
  // unstored one, because a stored offset covers everything before it.
  private settle(): void {
    while (this.pages[0]?.outstanding.size === 0) {
      const page = this.pages.shift()
      if (page !== undefined) this.settledSequence = page.sequence
      if (page?.target !== undefined) this.pendingStore = page.target
    }
  }
}

// The shared transport as a coordinator, for a reader that has no connection
// of its own: an injected client.
function sharedCoordinator(transport: FilterTransport): CoordinatorConnection {
  const missing = (verb: string): never => {
    throw new ConfigError(`this transport cannot ${verb}, which a group reader needs`)
  }
  return {
    send: (code, payload) => transport.sendManaged(code, payload),
    sendManagedTo: (code, payload, target) =>
      transport.sendManagedTo === undefined
        ? missing("send a managed request to a chosen node")
        : transport.sendManagedTo(code, payload, target),
    partitionPrimary: (streamId, topicId, partitionId) =>
      transport.partitionPrimary === undefined
        ? missing("look a partition route up")
        : transport.partitionPrimary(streamId, topicId, partitionId),
    storePosition: (position) =>
      transport.storePosition === undefined
        ? missing("store a fenced consumer position")
        : transport.storePosition(position),
    joinConsumerGroup: (streamId, topicId, name) =>
      transport.joinExistingConsumerGroup !== undefined
        ? transport.joinExistingConsumerGroup(streamId, topicId, name)
        : typeof name === "string"
          ? transport.joinConsumerGroup(streamId, topicId, name)
          : Promise.reject(
              new ConfigError("this transport does not support numeric group membership")
            ),
    leaveConsumerGroup: (streamId, topicId, name) =>
      transport.leaveConsumerGroup(streamId, topicId, name),
    close: () => Promise.resolve()
  }
}

/** This reader's membership of a consumer group, held by its coordinator connection. */
class Membership {
  private sourceReplaced = false
  private generation = 0n
  private assigned: readonly number[] = []
  private syncedAt: number | undefined
  private rejoined = false
  private readonly request: Uint8Array

  private constructor(
    private readonly coordinator: CoordinatorConnection,
    private readonly source: FilterSource,
    private readonly group: string | number,
    private readonly transport: FilterTransport,
    private readonly pinnedSource: SourceIncarnation | undefined
  ) {
    this.request = concat([identifier(source.stream), identifier(source.topic), identifier(group)])
  }

  static async join(
    coordinator: CoordinatorConnection,
    source: FilterSource,
    group: string | number,
    transport: FilterTransport,
    pinnedSource: SourceIncarnation | undefined
  ): Promise<Membership> {
    const membership = new Membership(coordinator, source, group, transport, pinnedSource)
    await coordinator.joinConsumerGroup(source.stream, source.topic, group)
    await membership.sync()
    return membership
  }

  get partitions(): readonly number[] {
    return this.assigned
  }

  isDue(): boolean {
    return (
      this.partitions.length === 0 ||
      this.syncedAt === undefined ||
      performance.now() - this.syncedAt >= ASSIGNMENT_REFRESH_MS
    )
  }

  expire(): void {
    this.syncedAt = undefined
  }

  /** Read the current assignment. `true` when it changed. */
  async sync(): Promise<boolean> {
    let reply = await this.coordinator.send(SYNC_CONSUMER_GROUP_CODE, this.request)
    if (reply.byteLength === 0) {
      if (this.pinnedSource !== undefined) {
        const current = await sourceIncarnation(this.transport, this.source)
        if (
          current.streamId !== this.pinnedSource.streamId ||
          current.streamCreatedAtMicros !== this.pinnedSource.streamCreatedAtMicros ||
          current.topicId !== this.pinnedSource.topicId ||
          current.topicCreatedAtMicros !== this.pinnedSource.topicCreatedAtMicros
        ) {
          this.sourceReplaced = true
          await this.coordinator
            .leaveConsumerGroup(this.source.stream, this.source.topic, this.group)
            .catch(() => undefined)
          throw filterError({
            code: { kind: "known", name: "NotFound" },
            reason: "not_found",
            message: "the numeric consumer group belongs to a replaced stream or topic"
          })
        }
      }
      await this.coordinator.joinConsumerGroup(this.source.stream, this.source.topic, this.group)
      this.rejoined = true
      reply = await this.coordinator.send(SYNC_CONSUMER_GROUP_CODE, this.request)
      if (reply.byteLength === 0)
        throw filterError({
          code: { kind: "known", name: "Stale" },
          reason: "membership_stale",
          message: "the consumer group has not accepted this reader's rejoin"
        })
    }
    this.syncedAt = performance.now()
    const view = new DataView(reply.buffer, reply.byteOffset, reply.byteLength)
    if (reply.byteLength < 12) throw new ProtocolError("the group assignment is truncated")
    const generation = view.getBigUint64(0, true)
    const count = view.getUint32(8, true)
    if (count > (reply.byteLength - 12) / 4 || reply.byteLength !== 12 + count * 4)
      throw new ProtocolError("the group assignment length is invalid")
    const partitions = Array.from({ length: count }, (_, index) =>
      view.getUint32(12 + index * 4, true)
    )
    const changed =
      this.rejoined ||
      generation !== this.generation ||
      partitions.length !== this.assigned.length ||
      partitions.some((partitionId, index) => partitionId !== this.assigned[index])
    this.generation = generation
    this.assigned = partitions
    return changed
  }

  takeRejoined(): boolean {
    const rejoined = this.rejoined
    this.rejoined = false
    return rejoined
  }

  async leave(): Promise<void> {
    if (this.sourceReplaced) return
    await this.coordinator.leaveConsumerGroup(this.source.stream, this.source.topic, this.group)
  }
}

async function sourceIncarnation(
  transport: Pick<FilterTransport, "sendManaged">,
  source: FilterSource
): Promise<SourceIncarnation> {
  // Native metadata responses begin with id:u32 and created_at:u64. Reading
  // the timestamp directly preserves microseconds that the Node SDK's Date loses.
  const read = (bytes: Uint8Array): { id: number; created: bigint } => {
    if (bytes.length < 12) throw new ProtocolError("source metadata is absent or truncated")
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
    return { id: view.getUint32(0, true), created: view.getBigUint64(4, true) }
  }
  const stream = read(await transport.sendManaged(GET_STREAM_CODE, identifier(source.stream)))
  const topic = read(
    await transport.sendManaged(
      GET_TOPIC_CODE,
      concat([identifier(stream.id), identifier(source.topic)])
    )
  )
  const current = read(await transport.sendManaged(GET_STREAM_CODE, identifier(stream.id)))
  if (current.created !== stream.created)
    throw new ProtocolError("source stream changed while building the reader")
  return {
    streamId: stream.id,
    streamCreatedAtMicros: stream.created,
    topicId: topic.id,
    topicCreatedAtMicros: topic.created
  }
}

/** A consumer group as the server stores it, with its exact identity. */
export interface NativeGroup {
  readonly id: number
  readonly name: string
  readonly identity: FilterGroupIdentity
}

/**
 * Read a consumer group's native id and name, and the stream and topic
 * incarnations that scope it. Throws with reason `not_found` when the group
 * does not exist.
 */
export async function nativeGroup(
  transport: Pick<FilterTransport, "sendManaged">,
  source: FilterSource,
  group: string | number
): Promise<NativeGroup> {
  const incarnation = await sourceIncarnation(transport, source)
  const reply = await transport.sendManaged(
    GET_CONSUMER_GROUP_CODE,
    concat([identifier(source.stream), identifier(source.topic), identifier(group)])
  )
  // The reply begins with id:u32, partitions:u32, members:u32, then the name
  // as one length byte and its bytes. A missing group answers an empty body.
  if (reply.byteLength === 0)
    throw filterError({
      code: { kind: "known", name: "NotFound" },
      reason: "not_found",
      message: `consumer group ${String(group)} does not exist`
    })
  if (reply.byteLength < 13) throw new ProtocolError("consumer group metadata is truncated")
  const view = new DataView(reply.buffer, reply.byteOffset, reply.byteLength)
  const id = view.getUint32(0, true)
  if (typeof group === "number" && id !== group)
    throw new ProtocolError("consumer group metadata identifies another group")
  const length = view.getUint8(12)
  if (reply.byteLength < 13 + length)
    throw new ProtocolError("consumer group metadata is truncated")
  return {
    id,
    name: new TextDecoder().decode(reply.subarray(13, 13 + length)),
    identity: { ...incarnation, groupId: BigInt(id) }
  }
}

function identifier(name: string | number): Uint8Array {
  if (typeof name === "number") {
    const encoded = new Uint8Array(6)
    encoded.set([1, 4])
    new DataView(encoded.buffer).setUint32(2, name, true)
    return encoded
  }
  const bytes = UTF8.encode(name)
  return concat([Uint8Array.of(2, bytes.byteLength), bytes])
}

function concat(parts: readonly Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, part) => sum + part.byteLength, 0)
  const out = new Uint8Array(total)
  let offset = 0
  for (const part of parts) {
    out.set(part, offset)
    offset += part.byteLength
  }
  return out
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.byteLength === right.byteLength && left.every((byte, index) => byte === right[index])
}

// Re-evaluate every returned record with the shared evaluator before the
// caller sees it.
function checkGuard(
  guard: CompiledFilter,
  page: FilteredPage,
  messages: readonly {
    readonly offset: bigint
    readonly payload: Uint8Array
    readonly headers: ReadonlyMap<string, HeaderValue>
    readonly headersMalformed?: HeaderFault
  }[],
  unevaluated: ReadonlySet<bigint>
): void {
  if (page.policy.digest === undefined || !sameBytes(page.policy.digest, guard.digest)) {
    throw filterError({
      code: { kind: "known", name: "Conflict" },
      reason: "conflict",
      message: "the server ran another filter than the one the local guard checks"
    })
  }
  const filter = guard.filter
  if (
    unevaluated.size > 0 &&
    filter.faultPolicy !== "pass" &&
    filter.foreignPolicy !== "pass" &&
    filter.mismatchPolicy !== "pass"
  ) {
    throw filterError({
      code: { kind: "known", name: "Backend" },
      reason: "backend",
      message: "the server returned unevaluated records without a pass policy"
    })
  }
  const limits = page.evaluationLimits ?? GUARD_LIMITS
  if (
    limits.maxPayloadBytes <= 0 ||
    limits.maxPayloadBytes > MAX_FILTERED_PAGE_BYTES ||
    limits.maxDepth <= 0 ||
    limits.maxDepth > MAX_FILTER_PARSE_DEPTH
  ) {
    throw filterError({
      code: { kind: "known", name: "Backend" },
      reason: "backend",
      message: "the server returned invalid decoder limits"
    })
  }
  if (
    unevaluated.size > 0 &&
    filter.faultPolicy === "pass" &&
    page.evaluationLimits === undefined
  ) {
    throw new UnsupportedError(
      "local verification of pass-through decode faults requires the server's evaluation limits"
    )
  }
  for (const message of messages) {
    // The server decodes only the headers the filter needs, so a broken part
    // it never reads does not fault the record there either.
    const outcome = headersFault(guard, message.headersMalformed)
      ? { verdict: "fault" as const, fault: "malformed" as const }
      : guard.evaluateWithFault(
          {
            payload: message.payload,
            headers: guard.headerNeed === "none" ? [] : filterHeaders(message.headers)
          },
          limits
        )
    const agrees = unevaluated.has(message.offset)
      ? outcome.verdict === "fault" &&
        outcome.fault !== undefined &&
        guard.policyFor(outcome.fault) === "pass"
      : outcome.verdict === "selected"
    if (!agrees) {
      throw filterError({
        code: { kind: "known", name: "Backend" },
        reason: "backend",
        message: `the server's evaluation of offset ${message.offset.toString()} on partition ${String(page.partitionId)} disagrees with the local guard`
      })
    }
  }
}

// Whether the part of the header block that did not decode is one the
// server reads for this filter: every entry for header predicates, only the
// structure and `agdx.ct` for a payload filter, nothing otherwise.
function headersFault(guard: CompiledFilter, fault: HeaderFault | undefined): boolean {
  if (fault === undefined) return false
  const need = guard.headerNeed
  return need === "all" || (need === "content_type" && fault !== "entry")
}

// The typed user headers, mapped exactly as the server maps them.
function filterHeaders(headers: ReadonlyMap<string, HeaderValue>): FilterHeader[] {
  return [...headers].map(([key, header]): FilterHeader => {
    switch (header.kind) {
      case "bool":
        return { key, value: { kind: "bool", value: header.value } }
      case "string":
        return { key, value: { kind: "string", value: header.value } }
      case "int8":
      case "int16":
      case "int32":
        return { key, value: { kind: "int", value: BigInt(header.value) } }
      case "int64":
        return { key, value: { kind: "int", value: header.value } }
      case "uint8":
      case "uint16":
      case "uint32":
        return { key, value: { kind: "uint", value: BigInt(header.value) } }
      case "uint64":
        return { key, value: { kind: "uint", value: header.value } }
      case "float":
      case "double":
        return { key, value: { kind: "float", value: header.value } }
      case "raw":
      case "int128":
      case "uint128":
        return { key, value: { kind: "raw", value: header.value } }
    }
  })
}

async function compileGuard(
  transport: FilterTransport,
  filter: ConsumerFilter,
  defaultSchemaStream: string | undefined
): Promise<CompiledFilter> {
  const schemaStream = filter.schemaStream ?? defaultSchemaStream
  const schemas: SchemaDef[] = []
  for (const id of filter.schemaRefs) {
    const bytes = await transport.sendManaged(
      AGDX_GET_SCHEMA_CODE,
      encodeNamed(
        encodeGetSchema({
          v: QUERY_OP_VERSION,
          id,
          ...(schemaStream !== undefined ? { stream: schemaStream } : {})
        })
      )
    )
    const reply = decodeBrowseReply(decodeOne(bytes, "writer schema"), "writer schema")
    if (reply.kind === "err")
      throw new QueryExecutionError("writer schema lookup failed", reply.error)
    if (
      reply.kind !== "ok" ||
      reply.outcome.kind !== "schema" ||
      reply.outcome.schema?.schema.id !== id
    ) {
      throw new ProtocolError(`writer schema ${String(id)} was not returned`)
    }
    schemas.push(reply.outcome.schema.schema)
  }
  const compiled = CompiledFilter.compile(filter, schemas)
  return compiled
}

// The full definition a reader executes, for the local guard.
async function boundDefinition(
  filters: Filters,
  requested: FilterSource,
  page: FilteredPage
): Promise<ConsumerFilter> {
  const source = page.generation
  for (let index = 0; ; index += 1) {
    const bindings = await filters.bindings({
      stream: requested.stream,
      topic: requested.topic,
      page: index,
      pageSize: MAX_FILTER_CATALOG_PAGE
    })
    const binding = bindings.items.find(
      ({ identity }) =>
        identity.groupId === page.policy.groupId &&
        identity.streamId === source.streamId &&
        identity.streamCreatedAtMicros === source.streamCreatedAtMicros &&
        identity.topicId === source.topicId &&
        identity.topicCreatedAtMicros === source.topicCreatedAtMicros
    )
    if (binding !== undefined) {
      if (page.policy.digest === undefined || !sameBytes(binding.digest, page.policy.digest))
        throw new ProtocolError("served policy differs from the group's catalog binding")
      return definition(filters, {
        kind: "revision",
        filterId: binding.filterId,
        revision: binding.revision
      })
    }
    if ((index + 1) * MAX_FILTER_CATALOG_PAGE >= bindings.total)
      throw filterError({
        code: { kind: "known", name: "NotFound" },
        reason: "not_found",
        message: "no readable catalog binding matches this group identity"
      })
  }
}

async function definition(filters: Filters, filter: FilterRef): Promise<ConsumerFilter> {
  if (filter.kind === "inline") return filter.filter
  if (filter.kind === "bound" || filter.kind === "group")
    throw new ConfigError("resolve a group guard from the served page")
  const { filterId, revision } = filter
  for (let page = 0; ; page += 1) {
    const revisions = await filters.revisions(filterId, { page, pageSize: MAX_FILTER_CATALOG_PAGE })
    const found = revisions.items.find((info) => info.revision === revision)
    if (found !== undefined) return found.filter
    if ((page + 1) * MAX_FILTER_CATALOG_PAGE >= revisions.total) {
      throw filterError({
        code: { kind: "known", name: "NotFound" },
        reason: "not_found",
        message: `filter ${String(filterId)} has no revision ${String(revision)}`
      })
    }
  }
}

function settled(operationId: bigint, status: FilterMutationStatus): FilterMutationResult {
  if (status.kind === "applied") return status.result
  if (status.kind === "rejected") throw filterError(status.error)
  throw new AmbiguousMutationError(`filter mutation ${operationId.toString()} has no outcome yet`)
}

function filterError(error: {
  readonly code: FilterExecutionError["detail"]["code"]
  readonly reason: FilterExecutionError["detail"]["reason"]
  readonly message: string
}): LaserError {
  if (error.reason === "unsupported") return new UnsupportedError(error.message)
  return new FilterExecutionError(`${error.reason}: ${error.message}`, error)
}

// The group no longer assigns the partition to this member, or its session
// ended. Reads are side-effect free, so the next one follows the synced
// assignment.
function isAssignmentLost(error: unknown): boolean {
  if (error instanceof FilterExecutionError) return error.detail.reason === "membership_stale"
  return serverErrorCode(error) === CONSUMER_GROUP_PARTITION_NOT_OWNED
}

// The consumer a target's offset belongs to. A group stores under the numeric
// id its page named, so a recreated group with the same name never receives it.

function isNotYetApplied(error: unknown): boolean {
  return (
    (error instanceof FilterExecutionError &&
      error.detail.code.kind === "known" &&
      error.detail.code.name === "NotFound") ||
    (error instanceof FilterExecutionError && resultCodeIsRetryable(error.detail.code)) ||
    (error instanceof TransportError && error.retryable)
  )
}

function isTransient(error: unknown): boolean {
  return (
    (error instanceof TransportError && error.retryable) ||
    (error instanceof FilterExecutionError && resultCodeIsRetryable(error.detail.code))
  )
}

function unexpected(verb: string): ProtocolError {
  return new ProtocolError(`filters ${verb}: unexpected reply variant`)
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds))
}

function sameGeneration(left: SourceGeneration, right: SourceGeneration): boolean {
  return (
    left.streamId === right.streamId &&
    left.streamCreatedAtMicros === right.streamCreatedAtMicros &&
    left.topicId === right.topicId &&
    left.topicCreatedAtMicros === right.topicCreatedAtMicros &&
    left.partitionId === right.partitionId &&
    left.partitionIncarnation === right.partitionIncarnation
  )
}

function validatePage(
  request: FilteredPollRequest,
  page: FilteredPage,
  messages: readonly PolledMessage[]
): void {
  const invalid = (): never => {
    throw new ProtocolError("filtered page does not match its request or scan boundary")
  }
  if (request.consumer.kind === "group_id" && page.policy.groupId !== request.consumer.id) invalid()
  if (
    page.v !== FILTER_OP_VERSION ||
    page.partitionId !== request.partitionId ||
    page.generation.partitionId !== request.partitionId ||
    page.matched !== messages.length ||
    page.matched > request.count ||
    page.matched > page.examined ||
    (request.maxExamined !== undefined && page.examined > request.maxExamined) ||
    (page.policy.mode === "filtered") !== (page.policy.digest !== undefined) ||
    (page.policy.digest !== undefined && page.policy.digest.length !== 32) ||
    (page.policy.mode === "unfiltered" && page.policy.groupId === undefined) ||
    (page.policy.mode === "unfiltered" && page.matched !== page.examined) ||
    (page.policy.mode === "unfiltered" &&
      (page.policy.filterId !== undefined || page.policy.revision !== undefined)) ||
    // A strict group read runs a binding and never accepts an unfiltered page.
    (page.policy.mode === "unfiltered" && request.filter.kind !== "group") ||
    (request.consumer.kind !== "consumer") !== (page.policy.groupId !== undefined)
  )
    invalid()
  // The record body was read under the context the page names.
  const head = decodePolledHead(page.records)
  if (
    head.partitionId !== page.partitionId ||
    head.currentOffset !== page.frontier ||
    head.context.incarnation !== page.generation.partitionIncarnation ||
    head.context.ownerGeneration !== page.ackContext.ownerGeneration ||
    head.context.metadataOp !== page.ackContext.metadataOp
  )
    invalid()
  if (
    request.filter.kind === "revision" &&
    (page.policy.filterId !== request.filter.filterId ||
      page.policy.revision !== request.filter.revision)
  )
    invalid()
  let start: bigint | undefined
  if (request.start.kind === "continue") {
    const cursor = request.start.continuation
    if (
      !sameGeneration(cursor.generation, page.generation) ||
      cursor.groupId !== page.policy.groupId ||
      cursor.mode !== page.policy.mode ||
      cursor.policyGeneration !== page.policy.policyGeneration ||
      (cursor.digest === undefined) !== (page.policy.digest === undefined) ||
      (cursor.digest !== undefined &&
        page.policy.digest !== undefined &&
        !sameBytes(cursor.digest, page.policy.digest))
    )
      invalid()
    start = cursor.nextScanOffset
  } else if (request.start.kind === "offset") start = request.start.offset
  else if (request.start.kind === "first") start = 0n
  // The last offset the page examined, which the page offers as its safe
  // acknowledgment offset.
  const scannedTo =
    page.nextScanOffset === undefined || page.nextScanOffset === 0n
      ? undefined
      : page.nextScanOffset - 1n
  if (page.examined === 0) {
    if (page.safeAckOffset !== undefined || page.nextScanOffset !== start) invalid()
  } else if (scannedTo === undefined || page.safeAckOffset !== scannedTo) invalid()
  if (scannedTo !== undefined && scannedTo > page.frontier) invalid()
  if (start !== undefined && page.nextScanOffset !== undefined && page.nextScanOffset < start)
    invalid()
  let previous: bigint | undefined
  const offsets = new Set<bigint>()
  for (const message of messages) {
    const offset = message.offset
    if (
      message.partitionId !== request.partitionId ||
      (previous !== undefined && offset <= previous) ||
      (start !== undefined && offset < start) ||
      scannedTo === undefined ||
      offset > scannedTo ||
      offset > page.frontier
    )
      invalid()
    previous = offset
    offsets.add(offset)
  }
  if (
    new Set(page.unevaluated).size !== page.unevaluated.length ||
    page.unevaluated.some((offset) => !offsets.has(offset))
  )
    invalid()
  if ((page.stop === "fault" || page.stop === "oversized_record") !== (page.fault !== undefined))
    invalid()
  if (page.fault !== undefined && scannedTo !== undefined && scannedTo >= page.fault.offset)
    invalid()
}

// The stored record as the consumer hands it out, with the page frontier as
// its partition head, like the Rust and Python readers.
function matchedMessage(
  partitionId: number,
  frontier: bigint,
  message: PolledMessage
): ConsumerMessage {
  const position = { partitionId, offset: message.offset }
  return withMessageJson({
    payload: message.payload,
    id: position,
    partitionId,
    headers: message.headers,
    position,
    messageId: message.messageId ?? 0n,
    checksum: message.checksum ?? 0n,
    currentOffset: frontier,
    timestampMicros: message.timestampMicros ?? 0n,
    originTimestampMicros: message.originTimestampMicros ?? 0n,
    ...(message.userHeaders !== undefined ? { userHeaders: message.userHeaders } : {}),
    headersMalformed: message.headersMalformed !== undefined
  })
}
