import {
  FILTER_MUTATION_ATTEMPTS,
  FILTER_MUTATION_BACKOFF_MS,
  FILTER_OUTCOME_WAIT_MS,
  MANAGED_MUTATION,
  MANAGED_MUTATION_DEADLINE_MS,
  backoffDelay
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
import { storeSeeds } from "../stream/consumer-start.js"
import { withMessageJson } from "../stream/message.js"
import { Mutex } from "../runtime/mutex.js"
import { mintUlidValue } from "../runtime/ulid.js"
import { warn } from "../runtime/warn.js"
import {
  type CallOptions,
  abortable,
  cancellable,
  delay,
  throwIfAborted
} from "../client/call-options.js"
import { decodeOne, encodeNamed } from "../wire/cbor.js"
import {
  AGDX_FILTERED_ACK_CODE,
  AGDX_FILTERED_POLL_CODE,
  AGDX_FILTERED_WAIT_CANCEL_CODE,
  AGDX_FILTERED_WAIT_CODE,
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
  type AckReceipt,
  type AppliedPolicy,
  type CatalogPosition,
  type ConsumerFilter,
  type ExecutionMode,
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
  type FilterSource,
  type FilterTestResult,
  type FilteredPage,
  type FilteredPollRequest,
  type FilteredStart,
  type FilteredWaitPartition,
  type GroupFilterSpec,
  type SourceGeneration,
  type StopReason,
  decodeFilterReply,
  encodeFilteredPollRequest,
  encodeFilteredWaitCancel,
  encodeFilteredWaitRequest,
  filteredPageNextStart,
  validateCatalogPage,
  validateFilterGroupRef,
  validateFilterMutationRequest,
  validateFilterPreviewRequest,
  validateFilterTestRequest,
  validateFilteredPollRequest,
  validateFilteredWaitCancel,
  validateFilteredWaitRequest,
  validateListFilters,
  type FilterSummary,
  type ListFilters
} from "../wire/filter.js"
import {
  MAX_FILTER_CATALOG_PAGE,
  MAX_FILTER_PARSE_DEPTH,
  MAX_FILTER_PREVIEW_EXAMINED,
  MAX_FILTERED_PAGE_BYTES,
  MAX_FILTERED_WAIT_PARTITIONS
} from "../wire/limits.js"

const DEFAULT_COUNT = 100
const DEFAULT_MAX_REPLY_BYTES = 1024 * 1024
const DEFAULT_IDLE_INTERVAL_MS = 250
const DEFAULT_MAX_WAIT_MS = 5_000
// A filtered wait gets this long past its server wait for the reply.
const WAIT_REPLY_MARGIN_MS = 5_000
const WAIT_CANCEL_DEADLINE_MS = 5_000
const DEFAULT_PAGE_RESULTS = 50
const DEFAULT_PREVIEW_RECORDS = 20
const DEFAULT_MAX_UNACKED_PAGES = 1024
const ASSIGNMENT_REFRESH_MS = 2_000
// How often a read still unanswered by its cached primary looks the route up.
const ROUTE_RECHECK_MS = 1_000
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
const NOT_PRIMARY = 89
const CONSUMER_GROUP_PARTITION_NOT_OWNED = 5009

// Wait ids count up across every reader of the process, so no two waits on
// one connection share an id.
let lastWaitId = 0n

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
> &
  Partial<Pick<LaserTransport, "storeOffset" | "getConsumerOffset">>

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

/**
 * How the group's policy treated a delivered record: `selected` when the
 * filter evaluated and selected it, `unevaluated` with its `reason` when a
 * `pass` fault, foreign-record or mismatch policy delivered it without
 * evaluating it, and `unfiltered` when the group has no policy and receives
 * every record.
 */
export type RecordEvaluation =
  | { readonly kind: "selected" }
  | { readonly kind: "unevaluated"; readonly reason: FaultReason }
  | { readonly kind: "unfiltered" }

/** One matching record, unchanged from the stream. Acknowledge the original reader-owned object. Cloning payload data does not transfer acknowledgment ownership. */
export interface MatchedRecord {
  readonly headersMalformed?: boolean
  /** Decode a JSON payload. Other codecs keep their original encoded bytes. */
  json(): unknown
  readonly partitionId: number
  readonly offset: bigint
  /** The partition head when the record was read. */
  readonly frontier: bigint
  /** How the group's policy treated the record. */
  readonly evaluation: RecordEvaluation
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

/** One catalog page: `page` counts from 1, and 0 reads the first page too.
 * `results` is the page length. */
export interface CatalogPageOptions {
  readonly page?: number
  readonly results?: number
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
      page: options.page ?? 1,
      results: options.results ?? DEFAULT_PAGE_RESULTS
    }
    validateCatalogPage(request.page, request.results)
    const outcome = await this.catalog(ListFilterRevisionsCommand, request)
    if (outcome.kind === "revisions") return outcome.page
    throw unexpected("revisions")
  }

  /**
   * The own filter of one consumer group incarnation, bound or released,
   * `undefined` when it has none.
   *
   * @internal
   */
  async owned(owner: FilterGroupIdentity): Promise<FilterSummary | undefined> {
    const request: ListFilters = { v: FILTER_OP_VERSION, owner, page: 1, results: 1 }
    validateListFilters(request)
    const outcome = await this.catalog(ListFiltersCommand, request)
    if (outcome.kind !== "filters") throw unexpected("filters")
    return outcome.page.items.find(
      (summary) => summary.owner !== undefined && sameGroupIdentity(summary.owner.identity, owner)
    )
  }

  /**
   * Drop a saved filter. The catalog releases every group bound to it.
   *
   * @internal
   */
  async dropFilter(
    filterId: number,
    options: CallOptions = {}
  ): Promise<CatalogPosition | undefined> {
    const applied = await this.apply({ kind: "drop", filterId }, undefined, options.signal)
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
      page: options.page ?? 1,
      results: options.results ?? DEFAULT_PAGE_RESULTS
    }
    validateCatalogPage(request.page, request.results)
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

  /** Pause or resume a revision without changing its executable content or digest. */
  async setRevisionEnabled(
    filterId: number,
    revision: number,
    enabled: boolean,
    options: CallOptions = {}
  ): Promise<void> {
    await this.apply(
      { kind: "set_revision_enabled", filterId, revision, enabled },
      undefined,
      options.signal
    )
  }

  /**
   * Give `group` its policy in one catalog transaction: a definition saved as
   * the group's own filter. A repeat with the same digest keeps the binding,
   * another digest conflicts. The returned
   * position is where the fold applied it, carried by the group's reads so
   * they never see this configuration as absent.
   */
  async configureGroup(
    group: FilterGroupRef,
    policy: GroupFilterSpec,
    operationId: bigint = mintUlidValue(),
    expectedIdentity?: FilterGroupIdentity,
    options: CallOptions = {}
  ): Promise<ConfiguredGroup> {
    throwIfAborted(options.signal, "filter mutation")
    const identity =
      expectedIdentity ?? (await nativeGroup(this.transport, group, group.group)).identity
    const scoped: GroupFilterSpec = {
      kind: "definition",
      name: policy.name,
      filter: this.scopedFilter(policy.filter)
    }
    const applied = await this.apply(
      { kind: "configure_group", group, policy: scoped, expectedIdentity: identity },
      operationId,
      options.signal
    )
    if (applied.result.kind === "bound") return bound(applied.result.binding, applied)
    throw unexpected("configure_group")
  }

  /**
   * Release this exact saved binding, including a group incarnation that was
   * deleted and recreated under the same name.
   */
  async releaseGroup(binding: FilterBinding, options: CallOptions = {}): Promise<ConfiguredGroup> {
    const applied = await this.apply(
      {
        kind: "unbind",
        group: binding.group,
        expectedDigest: binding.digest,
        expectedIdentity: binding.identity
      },
      undefined,
      options.signal
    )
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
    operationId: bigint = mintUlidValue(),
    signal?: AbortSignal
  ): Promise<AppliedMutation> {
    throwIfAborted(signal, "filter mutation")
    const submitted = await this.submit(operationId, mutation, signal)
    const outcome =
      submitted.status.kind === "pending"
        ? await this.waitForOutcome(operationId, DEFAULT_OUTCOME_WAIT_MS, signal)
        : submitted
    return {
      result: settled(operationId, outcome.status),
      ...(outcome.catalogPosition !== undefined ? { catalogPosition: outcome.catalogPosition } : {})
    }
  }

  private async waitForOutcome(
    operationId: bigint,
    timeoutMs: number,
    signal?: AbortSignal
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
          `filter mutation ${operationId.toString()} has no outcome yet`,
          { operationId }
        )
      }
      await delay(OUTCOME_POLL_INTERVAL_MS, signal, "filter mutation")
    }
  }

  private async submit(
    operationId: bigint,
    mutation: FilterMutation,
    signal?: AbortSignal
  ): Promise<FilterMutationOutcome> {
    for (let attempt = 1; ; attempt += 1) {
      try {
        return await this.mutate(operationId, mutation)
      } catch (error) {
        if (!isTransient(error)) throw error
        if (attempt >= MUTATION_ATTEMPTS) {
          throw new AmbiguousMutationError(
            `filter mutation ${operationId.toString()}: outcome is unknown`,
            { cause: error, operationId }
          )
        }
        await delay(MUTATION_RETRY_BACKOFF_MS, signal, "filter mutation")
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
  private waitMs = DEFAULT_MAX_WAIT_MS
  private maxUnacked = DEFAULT_MAX_UNACKED_PAGES
  private seed?: GroupSeed
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

  /**
   * Store `offsets` taken at a group consumer's join, each before this member
   * first reads its partition, where `group` still stored nothing.
   *
   * @internal
   */
  seedStart(group: string, offsets: ReadonlyMap<number, bigint>): this {
    if (offsets.size > 0) this.seed = { group, offsets }
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

  /** Re-evaluate returned records with the shared evaluator and fail the page on any disagreement. */
  localGuard(enabled: boolean): this {
    this.guarded = enabled
    return this
  }

  /**
   * How long the server holds a round that found nothing new until one of the
   * reader's partitions has a record, within the server's announced filtered
   * wait. Defaults to 5 seconds. A server that announces none paces idle
   * rounds with `idleInterval`.
   */
  maxWait(waitMs: number): this {
    if (!Number.isSafeInteger(waitMs) || waitMs < 0)
      throw new InvalidError("the maximum wait must be a non-negative whole number of milliseconds")
    this.waitMs = waitMs
    return this
  }

  /**
   * How long `nextPage` waits after a round that found nothing new on a server
   * that holds no filtered wait, and how long a failing or blocked partition
   * waits before it is read again.
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

  /** Opens the reader. A reader opened after an abort is closed, not leaked. */
  async build(options: CallOptions = {}): Promise<FilteredReader> {
    const signal = options.signal
    throwIfAborted(signal, "filtered reader build")
    const opening = this.open()
    try {
      return await abortable(opening, signal, "filtered reader build")
    } catch (error) {
      if (signal?.aborted === true) {
        void opening.then((reader) => reader.close()).catch(() => undefined)
      }
      throw error
    }
  }

  private async open(): Promise<FilteredReader> {
    const capabilities = await this.capabilities()
    // A group reader classifies the probe like the group consumer, and a
    // server that positively lacks filters has no filtered read to offer.
    if (this.filter.kind === "group" && !policyAware(capabilities)) {
      throw new UnsupportedError("consumer filters are not served by this server", {
        surface: "filters"
      })
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
      maxWaitMs: Math.min(this.waitMs, capabilities.filters.maxWaitMs),
      maxUnackedPages: this.maxUnacked,
      ...(this.seed === undefined ? {} : { seed: this.seed }),
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
  /** The server wait of an idle round, zero for none. */
  readonly maxWaitMs?: number
  readonly maxUnackedPages?: number
  readonly guard: CompiledFilter | undefined
  readonly guardEnabled?: boolean
  readonly membership: Membership | undefined
  readonly partitionIds: readonly number[]
  readonly selectedPartitions?: ReadonlySet<number>
  readonly seed?: GroupSeed
}

/** A group consumer's start, taken at its join for the partitions its group never stored. */
interface GroupSeed {
  readonly group: string
  readonly offsets: ReadonlyMap<number, bigint>
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
  // A server that refused a filtered wait as unsupported paces idle rounds
  // with the idle interval from then on.
  private waitsServed = true
  // The partitions the last filtered wait found ready. The next round reads
  // only these.
  private readyOnly: ReadonlySet<number> | undefined
  // A ready answer the next round could not confirm would repeat at once, so
  // the wait after that round is paced instead.
  private readyUnconfirmed = false
  // The filtered waits still held, by the slot of node partitions they name.
  // A slot holds at most one, kept across rounds until it answers or is
  // cancelled.
  private readonly heldWaits = new Map<string, HeldWait>()
  // Ends the idle wait in progress early.
  private idling: AbortController | undefined
  // One public call at a time, so overlapping calls never read the same
  // continuation twice.
  private readonly calls = new Mutex()
  // The last wait was refused and sent again at once, so another refusal in
  // a row is paced instead.
  private waitRefused = false

  // Partitions whose last read found the group assignment gone. The next such
  // read reaches the caller instead of syncing the group again.
  private readonly lostAssignments = new Set<number>()
  // Seeds still to store, each before the first read of its partition.
  private readonly seeds: Map<number, bigint>

  private constructor(private readonly settings: ReaderSettings) {
    this.initialAssignment =
      (settings.membership?.partitions.length ?? settings.partitionIds.length) === 0
    this.guard = settings.guard
    this.pinnedSource = settings.pinnedSource
    for (const partitionId of settings.partitionIds) {
      this.progress.set(partitionId, new PartitionProgress(settings.start))
    }
    this.seeds = new Map(settings.seed?.offsets)
  }

  /** @internal */
  static create(settings: ReaderSettings): FilteredReader {
    return new FilteredReader(settings)
  }

  /**
   * Store the start's seeds of the partitions this member owns now, so a
   * refused seed fails the group consumer's start.
   *
   * @internal
   */
  async storeOwnedSeeds(): Promise<void> {
    await this.storeSeeds([...this.progress.keys()])
  }

  /** The next page with at least one match. Waits while nothing is new, up to `timeoutMs` when given. */
  nextPage(options: CallOptions & { readonly timeoutMs?: number } = {}): Promise<MatchedPage> {
    return this.exclusive(options.signal, "filtered page", () => this.pageWithin(options))
  }

  /**
   * Read each partition in turn until a page matches or every partition has
   * nothing new, without the idle wait. `undefined` when nothing is new.
   */
  tryNextPage(options: CallOptions = {}): Promise<MatchedPage | undefined> {
    return this.exclusive(options.signal, "filtered page", async () => {
      for (;;) {
        const round = await this.round()
        if (round.kind === "page") return round.page
        if (round.kind === "idle") return undefined
      }
    })
  }

  /** One bounded round without an idle wait. The flag means unmatched work remains. */
  readRound(options: CallOptions = {}): Promise<readonly [MatchedPage | undefined, boolean]> {
    return this.exclusive(options.signal, "filtered round", async () => {
      const round = await this.round()
      return [round.kind === "page" ? round.page : undefined, round.kind === "busy"] as const
    })
  }

  /** Source records examined in the last bounded round, including empty pages. */
  examinedInRound(): number {
    return this.examined
  }

  /** The next matching record, one at a time over `nextPage`. */
  nextRecord(options: CallOptions & { readonly timeoutMs?: number } = {}): Promise<MatchedRecord> {
    return this.exclusive(options.signal, "filtered record", async () => {
      for (;;) {
        this.requireOpen()
        const record = this.buffered.shift()
        if (record !== undefined) return record
        const page = await this.pageWithin(options)
        this.requireOpen()
        this.buffered.push(...page.records)
      }
    })
  }

  /** Mark one record handled, and store the progress this completes. */
  ack(record: MatchedRecord, options: CallOptions = {}): Promise<void> {
    return this.exclusive(options.signal, "filtered ack", async () => {
      this.requireOpen()
      const sequence = this.requireSequence(record)
      this.partitionProgress(record.partitionId).completeRecord(sequence, record.offset)
      await this.store(record.partitionId)
    })
  }

  /** Mark this partition's preceding records through `record` handled. Later records stay pending. */
  ackThrough(record: MatchedRecord, options: CallOptions = {}): Promise<void> {
    return this.exclusive(options.signal, "filtered ack", async () => {
      this.requireOpen()
      const sequence = this.requireSequence(record)
      if (!this.partitionProgress(record.partitionId).completeThrough(sequence, record.offset)) {
        throw new InvalidError("the record is past the page acknowledgment boundary")
      }
      await this.store(record.partitionId)
    })
  }

  /** Mark every record of `page` handled, and store the progress this completes. */
  ackPage(page: MatchedPage, options: CallOptions = {}): Promise<void> {
    return this.exclusive(options.signal, "filtered ack", async () => {
      this.requireOpen()
      const sequence = this.requireSequence(page)
      this.partitionProgress(page.partitionId).completePage(sequence)
      await this.store(page.partitionId)
    })
  }

  /**
   * Pass the record a `FilterFaultError` or a `FilterOversizedRecordError`
   * stopped `partitionId` at, and read on after it, the way a native consumer
   * seeks past a record. The skip is a fenced acknowledgment of `offset` under
   * the policy and the owner the stopped page was read under, stored once
   * every earlier record of the partition is handled. A policy changed since
   * refuses it with a `conflict` and the partition restarts from its stored
   * offset. Fails with `InvalidError` when the partition is not stopped at
   * `offset`.
   */
  skip(partitionId: number, offset: bigint, options: CallOptions = {}): Promise<void> {
    return this.exclusive(options.signal, "filtered skip", () =>
      this.skipStopped(partitionId, offset)
    )
  }

  private async skipStopped(partitionId: number, offset: bigint): Promise<void> {
    this.requireOpen()
    this.sequence += 1
    if (!this.partitionProgress(partitionId).skip(this.sequence, offset)) {
      throw new InvalidError(
        `partition ${String(partitionId)} is not stopped at offset ${offset.toString()}`
      )
    }
    // A report of the block another partition's page held back is stale now.
    const deferred = this.deferredError
    if (
      (deferred instanceof FilterFaultError || deferred instanceof FilterOversizedRecordError) &&
      deferred.partitionId === partitionId &&
      deferred.offset === offset
    ) {
      this.deferredError = undefined
    }
    await this.store(partitionId)
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
  flushCompleted(): Promise<void> {
    return this.calls.runExclusive(async () => {
      let failure: unknown
      for (const partitionId of [...this.progress.keys()]) {
        try {
          await this.store(partitionId)
        } catch (error) {
          failure ??= error
        }
      }
      if (failure !== undefined) {
        throw failure instanceof Error
          ? failure
          : new TransportError("storing group progress failed", true, { cause: failure })
      }
    })
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
   * Wait after a round that found nothing new, for a caller that drives its
   * own loop with `readRound` or `tryNextPage`. The server holds one filtered
   * wait per node that serves this reader's partitions until one of them has a
   * record past what was examined, at most `maxWait` and what the server
   * announces, and the next round reads only the ready partitions. On a server
   * without filtered waits, or after a wait that failed or answered empty long
   * before its time, it sleeps one idle interval instead.
   */
  waitForRecords(options: CallOptions = {}): Promise<void> {
    return this.exclusive(options.signal, "filtered wait", async () => {
      await this.waitForRecordsWithin(undefined, options.signal)
    })
  }

  /**
   * `waitForRecords` bounded by `limitMs`, for a caller with work due sooner,
   * and ended early by `caller`. True when the wait ran out `limitMs`.
   *
   * @internal
   */
  async waitForRecordsWithin(limitMs: number | undefined, caller?: AbortSignal): Promise<boolean> {
    const idling = new AbortController()
    this.idling = idling
    const signal = idling.signal
    const pacing = Math.min(limitMs ?? Infinity, this.settings.idleIntervalMs)
    let expired = false
    // A held wait kept from an earlier round can outlast `limitMs`, so the
    // bound is a timer of its own.
    const bound =
      limitMs === undefined
        ? undefined
        : setTimeout(() => {
            expired = true
            idling.abort()
          }, limitMs)
    const ended = (): void => {
      idling.abort()
    }
    if (caller?.aborted === true) idling.abort()
    else caller?.addEventListener("abort", ended, { once: true })
    try {
      const waitMs = this.waitsServed
        ? Math.min(this.settings.maxWaitMs ?? 0, limitMs ?? Infinity)
        : 0
      const unconfirmed = this.readyUnconfirmed && this.examined === 0
      this.readyUnconfirmed = false
      if (waitMs <= 0 || unconfirmed) {
        await sleep(pacing, signal)
        return expired
      }
      const answer = await this.heldWait(Math.floor(waitMs), signal)
      switch (answer.kind) {
        case "ready":
          this.readyOnly = new Set(
            answer.ready.filter((partitionId) => this.progress.has(partitionId))
          )
          this.readyUnconfirmed = true
          return expired
        case "elapsed":
          return expired
        case "unsupported":
          this.waitsServed = false
          await sleep(pacing, signal)
          return expired
        case "sleep":
          await sleep(pacing, signal)
      }
      return expired
    } finally {
      clearTimeout(bound)
      caller?.removeEventListener("abort", ended)
      if (this.idling === idling) this.idling = undefined
    }
  }

  /**
   * End the idle wait in progress, so a consumer that shuts down does not sit
   * out a held wait.
   *
   * @internal
   */
  wake(): void {
    this.idling?.abort()
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
    this.idling?.abort()
    const cancelled = this.cancelHeldWaits()
    let failure: unknown
    for (const partitionId of [...this.progress.keys()]) {
      try {
        await this.store(partitionId)
      } catch (error) {
        failure ??= error
      }
    }
    await cancelled
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

  /** Yields matching records until `close()`, which ends the iteration like Python's. */
  async *[Symbol.asyncIterator](): AsyncIterator<MatchedRecord> {
    for (;;) {
      let record: MatchedRecord
      try {
        record = await this.nextRecord()
      } catch (error) {
        if (this.closed) return
        throw error
      }
      yield record
    }
  }

  // Runs `run` once every earlier public call has settled. The caller's
  // promise rejects at once on abort, while the lock is held until `run` ends.
  private exclusive<T>(
    signal: AbortSignal | undefined,
    what: string,
    run: () => Promise<T>
  ): Promise<T> {
    return cancellable(signal, what, () =>
      this.calls.runExclusive(() => {
        throwIfAborted(signal, what)
        return run()
      })
    )
  }

  // The page loop of `nextPage`, with the idle wait bounded by what is left
  // of `timeoutMs`.
  private async pageWithin(
    options: CallOptions & { readonly timeoutMs?: number }
  ): Promise<MatchedPage> {
    const deadline =
      options.timeoutMs === undefined ? undefined : performance.now() + options.timeoutMs
    // Timers count whole milliseconds, so a wait that ran out the time left
    // ends the call even when `performance.now()` is a fraction short of it.
    let expired = false
    for (;;) {
      throwIfAborted(options.signal, "filtered page")
      const round = await this.round()
      if (round.kind === "page") return round.page
      const remainingMs = deadline === undefined ? undefined : deadline - performance.now()
      if (expired || (remainingMs !== undefined && remainingMs <= 0)) {
        throw new TimeoutError("a filtered page with a match")
      }
      if (round.kind === "idle")
        expired = await this.waitForRecordsWithin(remainingMs, options.signal)
    }
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
    const only = this.readyOnly
    this.readyOnly = undefined
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
      if (only !== undefined && !only.has(partitionId)) continue
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
    // A partition not yet installed for this member keeps its seed and is
    // read once the next sync installed it.
    if (this.seeds.has(partitionId) && !(await this.storeSeeds([partitionId]))) {
      this.settings.membership?.expire()
      return { kind: "empty", more: false }
    }
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
      if (error instanceof FilterExecutionError && error.detail?.reason === "conflict") {
        return { kind: "empty", more: true }
      }
      throw error
    }
    this.requireOpen()
    if (outcome.kind !== "page") throw unexpected("filtered poll")
    const page = outcome.page
    this.examined += page.examined
    if (page.examined > 0) this.readyUnconfirmed = false
    // A first record larger than the requested cap comes alone, within the page cap.
    const recordCap =
      page.matched === 1
        ? Math.max(request.maxReplyBytes, MAX_FILTERED_PAGE_BYTES)
        : request.maxReplyBytes
    if (page.records.byteLength > recordCap + POLLED_HEAD_BYTES)
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
    const unevaluated = new Map(
      page.unevaluated.map((fault): [bigint, FaultReason] => [fault.offset, fault.reason])
    )
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
          ? new FilterFaultError(partitionId, blocked.offset, blocked.reason ?? "malformed")
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
      evaluation: recordEvaluation(page.policy.mode, unevaluated, message.offset),
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
          ? error.detail?.reason
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

  // Store the completed prefix like `flush`, again on the managed mutation
  // backoff while a failover leaves the partition without a primary that takes
  // it. Each repeat sends the same target, never past what was delivered.
  private async store(partitionId: number): Promise<void> {
    const deadline = performance.now() + MANAGED_MUTATION_DEADLINE_MS
    for (let attempt = 1; ; attempt += 1) {
      try {
        await this.flush(partitionId)
        return
      } catch (error) {
        if (!isRouteMoved(error) || performance.now() >= deadline) throw error
        await sleep(backoffDelay(MANAGED_MUTATION, attempt))
      }
    }
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
        !this.partitionProgress(partitionId).acknowledged(target, outcome.receipt)
      ) {
        throw new ProtocolError("the acknowledgment receipt does not match its page")
      }
    } catch (error) {
      if (isAssignmentLost(error)) this.settings.membership?.expire()
      this.restartAfter(partitionId, error)
      throw error
    }
    this.retire(partitionId)
  }

  // One read on the partition's primary. A read the primary no longer takes,
  // or a backup refuses as unavailable, looks the route up again and goes to
  // the node it names, once.
  private async exchange(
    partitionId: number,
    request: ReadonlyMap<string, unknown>,
    code = AGDX_FILTERED_POLL_CODE
  ): Promise<FilterOutcome> {
    const payload = encodeNamed(request)
    let rerouted = false
    let followed = false
    for (;;) {
      const target = await this.readTarget(partitionId)
      let reply: Uint8Array
      try {
        // A resend rechecks the route too: right after a crash the lookup can
        // still name the dead primary, whose link would hold the resend until
        // the call deadline. The read follows a partition that moves while it
        // waits once, a route that keeps moving is reported instead of chased.
        const sent = await this.sendUnlessMoved(partitionId, code, payload, target)
        if (sent === undefined) {
          if (followed) {
            throw filterError({
              code: { kind: "known", name: "Unavailable" },
              reason: "unavailable",
              message: `partition ${String(partitionId)} moved again during the read, retry`
            })
          }
          followed = true
          continue
        }
        reply = sent
      } catch (error) {
        const moved = isRouteStale(error)
        if (moved) this.leaders.delete(partitionId)
        // The caller's node moves on by itself after a lost link, but a node
        // that does not store the partition refuses it as not the primary.
        const elsewhere = target.kind !== "caller" || serverErrorCode(error) === NOT_PRIMARY
        if (!moved || rerouted || !elsewhere) throw error
        rerouted = true
        continue
      }
      const decoded = decodeManagedReply(
        (bytes) => decodeFilterReply(decodeOne(bytes, "filter reply"), "filter reply"),
        reply
      )
      if (decoded.kind === "ok") return decoded.outcome
      if (decoded.error.reason !== "unavailable") throw filterError(decoded.error)
      // A backup refuses a read of a partition it no longer leads as
      // unavailable, not as a transport error.
      this.leaders.delete(partitionId)
      if (rerouted || target.kind === "caller") throw filterError(decoded.error)
      rerouted = true
    }
  }

  // Send to the partition's known primary. A stopped primary holds the request
  // until the client's call deadline while the cluster names its successor
  // within moments, so a slow reply looks the route up again each
  // `ROUTE_RECHECK_MS` until it answers. `undefined` when another node leads
  // the partition now: the request is abandoned and every route through the
  // old node is read again.
  private async sendUnlessMoved(
    partitionId: number,
    code: number,
    payload: Uint8Array,
    target: ManagedTarget
  ): Promise<Uint8Array | undefined> {
    const answer = this.settings.coordinator
      .sendManagedTo(code, payload, target)
      .then((answered) => answered.reply)
    if (target.kind !== "replica") return answer
    // An abandoned request settles unobserved.
    answer.catch(() => undefined)
    const source = this.settings.request.source
    for (;;) {
      let timer: ReturnType<typeof setTimeout> | undefined
      const recheck = new Promise<undefined>((resolve) => {
        timer = setTimeout(() => {
          resolve(undefined)
        }, ROUTE_RECHECK_MS)
      })
      let reply: Uint8Array | undefined
      try {
        reply = await Promise.race([answer, recheck])
      } finally {
        clearTimeout(timer)
      }
      if (reply !== undefined) return reply
      const primary = await this.settings.coordinator
        .partitionPrimary(source.stream, source.topic, partitionId)
        .catch(() => undefined)
      if (primary !== undefined && primary !== target.replica) {
        for (const [partition, leader] of this.leaders) {
          if (leader === target.replica) this.leaders.delete(partition)
        }
        this.leaders.set(partitionId, primary)
        return undefined
      }
    }
  }

  // Store the seeds of `partitions` the group still stored nothing on. The
  // store goes through the member's own connection, the only one the server
  // lets store a partition this member owns.
  private async storeSeeds(partitions: Iterable<number>): Promise<boolean> {
    const group = this.settings.seed?.group
    if (group === undefined || this.seeds.size === 0) return true
    const read = this.settings.transport.getConsumerOffset?.bind(this.settings.transport)
    const store = this.settings.coordinator.storeOffset?.bind(this.settings.coordinator)
    if (read === undefined || store === undefined)
      throw new ConfigError(
        "this transport cannot seed a group's start, which a group consumer needs"
      )
    const { stream, topic } = this.settings.request.source
    const target = { kind: "group" as const, name: group }
    return storeSeeds(
      this.seeds,
      partitions,
      (partitionId) => read(stream, topic, target, partitionId),
      (partitionId, offset) => store(stream, topic, target, partitionId, offset)
    )
  }

  // Send one filtered wait for each slot of readable partitions, a node's
  // partitions in slices of at most the partitions one wait names, that holds
  // no wait naming all of them, and take the first answer of any held wait.
  private async heldWait(waitMs: number, signal: AbortSignal): Promise<WaitAnswer> {
    const now = performance.now()
    const groups = new Map<string, { target: ManagedTarget; partitions: FilteredWaitPartition[] }>()
    for (const partitionId of this.partitions()) {
      const progress = this.progress.get(partitionId)
      if (progress === undefined || progress.waiting(now)) continue
      const target = await this.readTarget(partitionId)
      const node = target.kind === "replica" ? `replica:${String(target.replica)}` : target.kind
      const group = groups.get(node) ?? { target, partitions: [] }
      group.partitions.push({ partitionId, offset: progress.waitOffset() })
      groups.set(node, group)
    }
    const replaced: HeldWait[] = []
    const slots = [...groups].flatMap(([node, group]) =>
      Array.from(
        { length: Math.ceil(group.partitions.length / MAX_FILTERED_WAIT_PARTITIONS) },
        (_, slice) => ({
          node: `${node}#${String(slice)}`,
          target: group.target,
          partitions: group.partitions.slice(
            slice * MAX_FILTERED_WAIT_PARTITIONS,
            (slice + 1) * MAX_FILTERED_WAIT_PARTITIONS
          )
        })
      )
    )
    for (const { node, target, partitions } of slots) {
      // A held wait stays while it names every partition of its slot now.
      // One that misses a partition moved to the slot is replaced.
      const held = this.heldWaits.get(node)
      if (held !== undefined) {
        if (partitions.every((partition) => held.partitions.has(partition.partitionId))) continue
        this.heldWaits.delete(node)
        replaced.push(held)
      }
      const sent = performance.now()
      lastWaitId += 1n
      const waitId = lastWaitId
      this.heldWaits.set(node, {
        target,
        waitId,
        partitions: new Set(partitions.map((partition) => partition.partitionId)),
        answer: this.waitOn({ target, partitions }, waitMs, waitId).then(
          (ready): HeldAnswer => ({ node, sent, ready }),
          (error: unknown): HeldAnswer => ({ node, sent, error, target, partitions })
        )
      })
    }
    if (replaced.length > 0) void this.cancelWaits(replaced)
    if (this.heldWaits.size === 0) return { kind: "sleep" }
    // The assignment is read again when it is due while every wait holds, so
    // partitions gained or lost meanwhile are read at once.
    let first: HeldAnswer | undefined
    for (;;) {
      const answered = Promise.race([...this.heldWaits.values()].map((held) => held.answer))
      const membership = this.settings.membership
      if (membership === undefined) {
        first = await untilAborted(answered, signal, undefined)
        break
      }
      let timer: ReturnType<typeof setTimeout> | undefined
      const due = new Promise<"due">((resolve) => {
        timer = setTimeout(() => {
          resolve("due")
        }, membership.refreshIn())
      })
      const outcome = await untilAborted(Promise.race([answered, due]), signal, undefined)
      clearTimeout(timer)
      if (outcome !== "due") {
        first = outcome
        break
      }
      try {
        if (await this.refreshMembership()) return { kind: "elapsed" }
      } catch {
        return { kind: "elapsed" }
      }
    }
    if (first === undefined) {
      void this.cancelHeldWaits()
      return { kind: "elapsed" }
    }
    this.heldWaits.delete(first.node)
    if (first.ready !== undefined) {
      this.waitRefused = false
      if (first.ready.length > 0) {
        void this.cancelHeldWaits()
        return { kind: "ready", ready: first.ready }
      }
      return performance.now() - first.sent >= waitMs / 2 ? { kind: "elapsed" } : { kind: "sleep" }
    }
    if (first.error instanceof UnsupportedError) return { kind: "unsupported" }
    // A node that no longer serves these partitions, or refuses for now as a
    // backup does, refused the whole wait: the next round looks their routes
    // up and waits again at once, as a read is resent once.
    const refused =
      (first.target?.kind === "replica" && isRouteStale(first.error)) ||
      (first.error instanceof FilterExecutionError && first.error.detail?.reason === "unavailable")
    if (refused && !this.waitRefused) {
      this.waitRefused = true
      for (const partition of first.partitions ?? []) this.leaders.delete(partition.partitionId)
      return { kind: "elapsed" }
    }
    return { kind: "sleep" }
  }

  // End every held wait on its node, so a node that lost the race does not
  // hold its wait to the deadline. An ended wait answers at once and nobody
  // reads that answer. A failed cancel only warns: the wait still ends at its
  // deadline.
  private async cancelHeldWaits(): Promise<void> {
    const held = [...this.heldWaits.values()]
    this.heldWaits.clear()
    await this.cancelWaits(held)
  }

  private async cancelWaits(waits: readonly HeldWait[]): Promise<void> {
    await Promise.all(
      waits.map(async (wait) => {
        try {
          await this.cancelOn(wait)
        } catch (error) {
          const node =
            wait.target.kind === "replica"
              ? `replica ${String(wait.target.replica)}`
              : `the ${wait.target.kind} node`
          warn(`cancelling a filtered wait on ${node} failed: ${String(error)}`)
        }
      })
    )
  }

  private async cancelOn(wait: HeldWait): Promise<void> {
    const request = { v: FILTER_OP_VERSION, waitIds: [wait.waitId] }
    validateFilteredWaitCancel(request)
    const answer = await this.settings.coordinator.sendManagedTo(
      AGDX_FILTERED_WAIT_CANCEL_CODE,
      encodeNamed(encodeFilteredWaitCancel(request)),
      wait.target,
      { deadline: Date.now() + WAIT_CANCEL_DEADLINE_MS }
    )
    const decoded = decodeManagedReply(
      (bytes) => decodeFilterReply(decodeOne(bytes, "filter reply"), "filter reply"),
      answer.reply
    )
    if (decoded.kind === "err") throw filterError(decoded.error)
    if (decoded.outcome.kind !== "cancelled") throw unexpected("filtered wait cancel")
  }

  // One filtered wait on one node. The call outlives the server's wait, which
  // never holds longer than the server announced.
  private async waitOn(
    group: WaitGroup,
    waitMs: number,
    waitId: bigint
  ): Promise<readonly number[]> {
    const request = {
      v: FILTER_OP_VERSION,
      source: this.settings.request.source,
      consumer: this.settings.request.consumer,
      partitions: group.partitions,
      maxWaitMs: waitMs,
      waitId
    }
    validateFilteredWaitRequest(request)
    const answer = await this.settings.coordinator.sendManagedTo(
      AGDX_FILTERED_WAIT_CODE,
      encodeNamed(encodeFilteredWaitRequest(request)),
      group.target,
      { deadline: Date.now() + waitMs + WAIT_REPLY_MARGIN_MS }
    )
    const decoded = decodeManagedReply(
      (bytes) => decodeFilterReply(decodeOne(bytes, "filter reply"), "filter reply"),
      answer.reply
    )
    if (decoded.kind === "err") throw filterError(decoded.error)
    if (decoded.outcome.kind !== "ready") throw unexpected("filtered wait")
    const asked = new Set(group.partitions.map((partition) => partition.partitionId))
    return decoded.outcome.result.ready.filter((partitionId) => asked.has(partitionId))
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

  // Follow the group's current assignment, `true` when it changed. The whole
  // change is applied to the reader before anything awaits. A partition that
  // left the assignment stops being read but keeps its in-flight pages, so
  // records already handed out can still be acknowledged while the server's
  // offset fence allows it.
  private async refreshMembership(): Promise<boolean> {
    const membership = this.settings.membership
    if (membership === undefined || !(await membership.sync())) return false
    this.requireOpen()
    // Held waits name the old assignment's partitions.
    void this.cancelHeldWaits()
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
    return true
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

interface WaitGroup {
  readonly target: ManagedTarget
  readonly partitions: readonly FilteredWaitPartition[]
}

// One filtered wait a node holds, named by its id on the connection.
interface HeldWait {
  readonly target: ManagedTarget
  readonly waitId: bigint
  readonly partitions: ReadonlySet<number>
  readonly answer: Promise<HeldAnswer>
}

// What one held filtered wait answered: the ready partitions, or its failure
// with where it went.
interface HeldAnswer {
  readonly node: string
  readonly sent: number
  readonly ready?: readonly number[]
  readonly error?: unknown
  readonly target?: ManagedTarget
  readonly partitions?: readonly FilteredWaitPartition[]
}

type WaitAnswer =
  | { readonly kind: "ready"; readonly ready: readonly number[] }
  | { readonly kind: "elapsed" }
  | { readonly kind: "sleep" }
  | { readonly kind: "unsupported" }

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
  private storedOffset: bigint | undefined
  // The durable offset the last receipt reported for a source generation,
  // which another consumer can have moved past this reader's own progress.
  private durable: { readonly generation: SourceGeneration; readonly offset: bigint } | undefined
  private stoppedAt: Blocked | undefined
  // The acknowledgment that skips the blocking record, fenced like the page
  // that reported the block.
  private skipTarget: AckTarget | undefined
  private retryAt: number | undefined
  private firstSequence: number | undefined
  private settledSequence = 0
  // The first offset no page examined yet, as the last page reported it.
  private waitFrom: bigint | undefined

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

  /** Where a filtered wait of this partition starts, as Rust `wait_offset`. */
  waitOffset(): bigint {
    if (this.waitFrom !== undefined) return this.waitFrom
    return this.original.kind === "offset" ? this.original.offset : 0n
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
    this.storedOffset = undefined
    this.durable = undefined
    this.stoppedAt = undefined
    this.skipTarget = undefined
    this.retryAt = undefined
    this.firstSequence = undefined
    this.settledSequence = 0
    this.waitFrom = undefined
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
    // The frontier is the partition's last offset, so the next record lands
    // after it. A head of 0 may also be an empty partition, whose next record
    // is 0.
    this.waitFrom = page.nextScanOffset ?? (page.frontier === 0n ? 0n : page.frontier + 1n)
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
    this.skipTarget =
      this.stoppedAt === undefined
        ? undefined
        : {
            offset: this.stoppedAt.offset,
            generation: page.generation,
            ackContext: page.ackContext,
            policy: page.policy,
            ...(page.policy.groupId !== undefined ? { groupId: page.policy.groupId } : {})
          }
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
      this.pend({ ...target, offset })
    }
    return true
  }

  completePage(sequence: number): void {
    this.pages.find((page) => page.sequence === sequence)?.outstanding.clear()
    this.settle()
  }

  /**
   * Pass the record the partition is blocked at, as a native consumer seeks
   * past it. The skip is acknowledged under the fence of the page that
   * reported the block and, like an empty page, only once every earlier page
   * is handled. `false` refuses an offset the partition is not blocked at.
   */
  skip(sequence: number, offset: bigint): boolean {
    const target = this.skipTarget
    if (target?.offset !== offset) return false
    const next = offset + 1n
    this.skipTarget = undefined
    this.stoppedAt = undefined
    this.retryAt = undefined
    this.waitFrom = next
    this.current =
      this.current.kind === "continue"
        ? { kind: "continue", continuation: { ...this.current.continuation, nextScanOffset: next } }
        : { kind: "offset", offset: next }
    const newest = this.pages.at(-1)
    if (newest !== undefined) newest.target = target
    else this.pages.push({ sequence, target, outstanding: new Set() })
    this.settle()
    return true
  }

  /**
   * `receipt` answered the store of `target`: the same source generation and
   * an offset at least the target's. Its offset is the durable progress, which
   * only covers later acknowledgments and never marks a local record handled.
   * `false` refuses a receipt that does not answer the target.
   */
  acknowledged(target: AckTarget, receipt: AckReceipt): boolean {
    if (!sameGeneration(receipt.generation, target.generation) || receipt.offset < target.offset) {
      return false
    }
    if (this.storedOffset === undefined || this.storedOffset < target.offset) {
      this.storedOffset = target.offset
    }
    const previous =
      this.durable !== undefined && sameGeneration(this.durable.generation, receipt.generation)
        ? this.durable.offset
        : undefined
    this.durable = {
      generation: receipt.generation,
      offset: previous !== undefined && previous > receipt.offset ? previous : receipt.offset
    }
    const pending = this.pendingStore
    if (pending !== undefined && (pending === target || this.covered(pending))) {
      this.pendingStore = undefined
    }
    return true
  }

  // Keep `target` to store unless stored or durable progress covers it. A
  // covered target also covers an older one still pending.
  private pend(target: AckTarget): void {
    if (!this.covered(target)) this.pendingStore = target
    else if (this.pendingStore !== undefined && this.pendingStore.offset <= target.offset) {
      this.pendingStore = undefined
    }
  }

  private covered(target: AckTarget): boolean {
    return (
      (this.storedOffset !== undefined && this.storedOffset >= target.offset) ||
      (this.durable !== undefined &&
        sameGeneration(this.durable.generation, target.generation) &&
        this.durable.offset >= target.offset)
    )
  }

  // Retire the completed prefix. Its newest safe offset supersedes any older
  // unstored one, because a stored offset covers everything before it.
  private settle(): void {
    while (this.pages[0]?.outstanding.size === 0) {
      const page = this.pages.shift()
      if (page !== undefined) this.settledSequence = page.sequence
      if (page?.target !== undefined) this.pend(page.target)
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
    sendManagedTo: (code, payload, target, options) =>
      transport.sendManagedTo === undefined
        ? missing("send a managed request to a chosen node")
        : transport.sendManagedTo(
            code,
            payload,
            target,
            options?.deadline === undefined ? undefined : { deadline: options.deadline }
          ),
    partitionPrimary: (streamId, topicId, partitionId) =>
      transport.partitionPrimary === undefined
        ? missing("look a partition route up")
        : transport.partitionPrimary(streamId, topicId, partitionId),
    storePosition: (position) =>
      transport.storePosition === undefined
        ? missing("store a fenced consumer position")
        : transport.storePosition(position),
    storeOffset: (streamId, topicId, target, partitionId, offset) =>
      transport.storeOffset === undefined
        ? missing("store a group offset")
        : transport.storeOffset(streamId, topicId, target, partitionId, offset),
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

  /** How long until the assignment is read again, zero once it is due. */
  refreshIn(): number {
    return this.syncedAt === undefined
      ? 0
      : Math.max(0, ASSIGNMENT_REFRESH_MS - (performance.now() - this.syncedAt))
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

function recordEvaluation(
  mode: ExecutionMode,
  unevaluated: ReadonlyMap<bigint, FaultReason>,
  offset: bigint
): RecordEvaluation {
  if (mode !== "filtered") return { kind: "unfiltered" }
  const reason = unevaluated.get(offset)
  return reason === undefined ? { kind: "selected" } : { kind: "unevaluated", reason }
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
      "this server serves consumer filters but not group-aware reads, upgrade it before consuming groups through the Laser SDK",
      { surface: "filters", feature: "group_policy_reads" }
    )
  }
  if (capabilities.hello === "failed" || capabilities.hello === "unknown") {
    throw new TimeoutError(
      "the managed probe that decides whether this server resolves consumer group policies, reconnect and build the consumer again"
    )
  }
  return false
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
  unevaluated: ReadonlyMap<bigint, FaultReason>
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
    const reason = unevaluated.get(message.offset)
    const agrees =
      reason === undefined
        ? outcome.verdict === "selected"
        : outcome.verdict === "fault" &&
          outcome.fault === reason &&
          guard.policyFor(reason) === "pass"
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
  for (let pageNumber = 1; ; pageNumber += 1) {
    const bindings = await filters.bindings({
      stream: requested.stream,
      topic: requested.topic,
      page: pageNumber,
      results: MAX_FILTER_CATALOG_PAGE
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
    if (pageNumber >= bindings.totalPages)
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
  for (let page = 1; ; page += 1) {
    const revisions = await filters.revisions(filterId, { page, results: MAX_FILTER_CATALOG_PAGE })
    const found = revisions.items.find((info) => info.revision === revision)
    if (found !== undefined) return found.filter
    if (page >= revisions.totalPages) {
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
  throw new AmbiguousMutationError(`filter mutation ${operationId.toString()} has no outcome yet`, {
    operationId
  })
}

function filterError(error: {
  readonly code: NonNullable<FilterExecutionError["detail"]>["code"]
  readonly reason: NonNullable<FilterExecutionError["detail"]>["reason"]
  readonly message: string
}): LaserError {
  if (error.reason === "unsupported") return new UnsupportedError(error.message)
  return new FilterExecutionError(`${error.reason}: ${error.message}`, error)
}

// The group no longer assigns the partition to this member, or its session
// ended. Reads are side-effect free, so the next one follows the synced
// assignment.
// No primary takes the request: the route moved, or a backup refused it.
function isRouteMoved(error: unknown): boolean {
  return (
    isRouteStale(error) ||
    (error instanceof FilterExecutionError && error.detail?.reason === "unavailable")
  )
}

function isAssignmentLost(error: unknown): boolean {
  if (error instanceof FilterExecutionError) return error.detail?.reason === "membership_stale"
  return serverErrorCode(error) === CONSUMER_GROUP_PARTITION_NOT_OWNED
}

// The consumer a target's offset belongs to. A group stores under the numeric
// id its page named, so a recreated group with the same name never receives it.

function isNotYetApplied(error: unknown): boolean {
  return (
    (error instanceof FilterExecutionError &&
      error.detail?.code.kind === "known" &&
      error.detail.code.name === "NotFound") ||
    (error instanceof FilterExecutionError &&
      error.detail !== undefined &&
      resultCodeIsRetryable(error.detail.code)) ||
    (error instanceof TransportError && error.retryable)
  )
}

function isTransient(error: unknown): boolean {
  return (
    (error instanceof TransportError && error.retryable) ||
    (error instanceof FilterExecutionError &&
      error.detail !== undefined &&
      resultCodeIsRetryable(error.detail.code))
  )
}

function unexpected(verb: string): ProtocolError {
  return new ProtocolError(`filters ${verb}: unexpected reply variant`)
}

function sleep(milliseconds: number, signal?: AbortSignal): Promise<void> {
  return untilAborted(
    new Promise((resolve) => setTimeout(resolve, milliseconds)),
    signal,
    undefined
  )
}

// `pending`, or `fallback` once `signal` aborts first.
function untilAborted<Value>(
  pending: Promise<Value>,
  signal: AbortSignal | undefined,
  fallback: Value
): Promise<Value> {
  if (signal === undefined) return pending
  if (signal.aborted) return Promise.resolve(fallback)
  return new Promise((resolve, reject) => {
    const abort = (): void => {
      resolve(fallback)
    }
    signal.addEventListener("abort", abort, { once: true })
    pending.then(
      (value) => {
        signal.removeEventListener("abort", abort)
        resolve(value)
      },
      (error: unknown) => {
        signal.removeEventListener("abort", abort)
        reject(error instanceof Error ? error : new Error(String(error)))
      }
    )
  })
}

/** Whether two group identities name the same group incarnation.
 * @internal */
export function sameGroupIdentity(left: FilterGroupIdentity, right: FilterGroupIdentity): boolean {
  return (
    left.streamId === right.streamId &&
    left.streamCreatedAtMicros === right.streamCreatedAtMicros &&
    left.topicId === right.topicId &&
    left.topicCreatedAtMicros === right.topicCreatedAtMicros &&
    left.groupId === right.groupId
  )
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
    new Set(page.unevaluated.map((fault) => fault.offset)).size !== page.unevaluated.length ||
    page.unevaluated.some((fault) => !offsets.has(fault.offset))
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
