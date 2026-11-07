import { InvalidError, UnsupportedError } from "../client/errors.js"
import { type CallOptions, cancellable, delay, throwIfAborted } from "../client/call-options.js"
import type { Capabilities } from "../client/capabilities.js"
import type { ResourceScope } from "../client/resource-scope.js"
import type { BackendBinding } from "../wire/destination.js"
import {
  type ControlCommand,
  type InvalidRecordPolicy,
  type OversizePolicy,
  type Projection,
  type RetentionPolicy,
  ProjectionBuilder,
  ProjectionBindingBuilder,
  parseProjectionId,
  validateProjection,
  validateControlCommand
} from "../wire/control.js"
import {
  type BindingStatus,
  type MaterializationReadiness,
  type ProjectionInfo,
  bindingStatusCaughtUpTo,
  bindingStatusIsBlocked,
  bindingStatusIsFailed,
  bindingStatusReadiness
} from "../wire/browse.js"
import { materializationOperationId } from "../wire/hashing.js"
import { mintUlidValue } from "../runtime/ulid.js"

/** The first pause between two status reads of a wait, in milliseconds. */
export const MATERIALIZATION_POLL_START_MS = 50
/** The longest pause between two status reads of a wait, in milliseconds.
 * Each pause doubles the one before it up to this cap. */
export const MATERIALIZATION_POLL_MAX_MS = 1000

/** @internal */
export interface MaterializationContext {
  readonly scope: ResourceScope
  capabilities(): Promise<Capabilities>
  publish(
    command: ControlCommand,
    stream: string,
    operationId: bigint,
    options?: CallOptions
  ): Promise<void>
  statusInfo(id: string, options?: CallOptions): Promise<ProjectionInfo | undefined>
}

/** The setup of one topic's materialization, built with
 * `Topic.materialize()`. `apply` registers the projection, applies its binding
 * and returns the `Materialization` to wait on. */
export class MaterializeRequest {
  private indexName: string | undefined
  private projectionName: string | undefined
  private projection: Projection | undefined
  private backendBinding: BackendBinding | undefined
  private retentionPolicy: RetentionPolicy | undefined
  private invalidRecordPolicy: InvalidRecordPolicy = "block"
  private oversizePolicy: OversizePolicy = "block"
  private setupId = mintUlidValue()
  private pinnedSetupId = false

  private constructor(
    private readonly context: MaterializationContext,
    private readonly stream: string,
    private readonly topic: string
  ) {}
  /** @internal */
  static create(
    context: MaterializationContext,
    stream: string,
    topic: string
  ): MaterializeRequest {
    return new MaterializeRequest(context, stream, topic)
  }
  index(name: string): this {
    this.indexName = name
    return this.changed()
  }
  projectionId(id: string): this {
    this.projectionName = id
    return this.changed()
  }
  typed(projection: Projection): this {
    this.projection = projection
    return this.changed()
  }
  backend(backend: BackendBinding): this {
    this.backendBinding = backend
    return this.changed()
  }
  retention(policy: RetentionPolicy): this {
    this.retentionPolicy = policy
    return this.changed()
  }
  invalidRecords(policy: InvalidRecordPolicy): this {
    this.invalidRecordPolicy = policy
    return this.changed()
  }
  oversize(policy: OversizePolicy): this {
    this.oversizePolicy = policy
    return this.changed()
  }
  /** Pin the setup identity. Without it every content change mints a fresh
   * one, so a corrected setup is never refused as a replay of a refused one. */
  operationId(id: bigint): this {
    this.setupId = id
    this.pinnedSetupId = true
    return this
  }
  /** The setup identity `apply` sends. Apply the same request again after
   * an uncertain response so the setup applies once. */
  operation(): bigint {
    return this.setupId
  }

  async apply(options: CallOptions = {}): Promise<Materialization> {
    const capabilities = await cancellable(options.signal, "materialize apply", () =>
      this.context.capabilities()
    )
    if (!capabilities.managed || !capabilities.materializations)
      throw new UnsupportedError("materializations are not served by this deployment", {
        surface: "materializations"
      })
    if (this.setupId <= 0n || this.setupId >= 1n << 128n)
      throw new InvalidError("operation id must be a nonzero u128")
    const index = this.indexName ?? this.topic
    const id = this.projectionName ?? this.projection?.id ?? index
    const projection = {
      ...(this.projection ?? new ProjectionBuilder(id).record().build()),
      id: parseProjectionId(id)
    }
    if (projection.kind.kind === "graph")
      throw new InvalidError("materialization requires a row or record projection")
    if (
      projection.history.kind === "keyed" &&
      this.retentionPolicy !== undefined &&
      !["keep", "keepUntilSourceDeleted", "timeToLive"].includes(this.retentionPolicy.kind)
    )
      throw new InvalidError(
        "keyed history requires Keep, KeepUntilSourceDeleted or TimeToLive retention"
      )
    if (this.oversizePolicy === "reference" && projection.kind.kind !== "record")
      throw new InvalidError("body references apply only to a whole-record projection")
    const builder = new ProjectionBindingBuilder()
      .source(this.stream, this.topic)
      .index(index)
      .allow(id)
      .defaultProjection(id)
      .invalidRecords(this.invalidRecordPolicy)
      .oversize(this.oversizePolicy)
    if (this.backendBinding !== undefined) builder.backend(this.backendBinding)
    if (this.retentionPolicy !== undefined) builder.retention(this.retentionPolicy)
    else if (projection.history.kind === "keyed") builder.retention({ kind: "keep" })
    const binding = builder.build()
    validateProjection(projection)
    validateControlCommand({ kind: "applyBinding", binding })
    const scopedId = parseProjectionId(this.context.scope.name(id))
    await cancellable(options.signal, "materialize apply", () =>
      this.context.publish(
        { kind: "registerProjection", projection: { ...projection, id: scopedId } },
        this.stream,
        materializationOperationId(this.setupId, 1),
        options
      )
    )
    await cancellable(options.signal, "materialize apply", () =>
      this.context.publish(
        {
          kind: "applyBinding",
          binding: {
            ...binding,
            index: this.context.scope.name(index),
            allowedProjections: [scopedId],
            defaultProjection: scopedId
          }
        },
        this.stream,
        materializationOperationId(this.setupId, 2),
        options
      )
    )
    return this.follow(id, index)
  }

  /** The handle `apply` returned for the setup whose projection is
   * `projectionId`, any index of this topic.
   * @internal */
  attach(projectionId: string): Materialization {
    return this.follow(projectionId, undefined)
  }

  private follow(id: string, index: string | undefined): Materialization {
    const follows = (source: { stream: string; topic: string }, bound: string) =>
      source.stream === this.stream &&
      source.topic === this.topic &&
      (index === undefined || bound === index)
    return Materialization.create(async (statusOptions) => {
      const info = await this.context.statusInfo(id, statusOptions)
      if (info === undefined) return undefined
      if (!info.bindings.some((binding) => follows(binding.source, binding.index))) return undefined
      return info.status.filter((status) => follows(status.source, status.index))
    })
  }

  private changed(): this {
    if (!this.pinnedSetupId) this.setupId = mintUlidValue()
    return this
  }
}

/** How a `Materialization.wait` ended. `failed` means creating a destination
 * table failed in a way no retry recovers, `not_found` that no projection has
 * the id or the followed source is no longer bound to it. */
export type MaterializationWait =
  | { readonly kind: "ready"; readonly status: readonly BindingStatus[] }
  | { readonly kind: "blocked"; readonly status: readonly BindingStatus[] }
  | { readonly kind: "failed"; readonly status: readonly BindingStatus[] }
  | { readonly kind: "not_found" }
  | { readonly kind: "timed_out"; readonly status: readonly BindingStatus[] }

/** A handle to one materialization's progress, from `MaterializeRequest.apply`,
 * `Topic.materialization` or `Laser.materialization`. */
export class Materialization {
  private constructor(
    private readonly readStatus: (
      options?: CallOptions
    ) => Promise<readonly BindingStatus[] | undefined>
  ) {}
  /** @internal */
  static create(
    status: (options?: CallOptions) => Promise<readonly BindingStatus[] | undefined>
  ): Materialization {
    return new Materialization(status)
  }
  /** The status of each binding this handle follows, or `undefined` when no
   * projection has the id or, for a handle tied to one source, when that
   * source is no longer bound to its index. */
  status(options: CallOptions = {}): Promise<readonly BindingStatus[] | undefined> {
    return cancellable(options.signal, "materialization status", () => this.readStatus(options))
  }
  async wait(
    level: MaterializationReadiness,
    timeoutMs: number,
    options: CallOptions = {}
  ): Promise<MaterializationWait> {
    throwIfAborted(options.signal, "materialization wait")
    const levels: readonly MaterializationReadiness[] = [
      "accepted",
      "applied",
      "realized",
      "caught_up"
    ]
    if (
      !levels.includes(level) ||
      !Number.isFinite(timeoutMs) ||
      timeoutMs < 0 ||
      timeoutMs > Number.MAX_SAFE_INTEGER
    )
      throw new InvalidError("invalid materialization readiness or timeout")
    const deadline = Date.now() + timeoutMs
    let target: readonly BindingStatus[] | undefined
    let last: readonly BindingStatus[] = []
    let pauseMs = MATERIALIZATION_POLL_START_MS
    const timedOut = Symbol("timed out")
    const matches = (left: BindingStatus, right: BindingStatus) =>
      left.index === right.index &&
      left.source.stream === right.source.stream &&
      left.source.topic === right.source.topic
    while (Date.now() < deadline) {
      let timer: ReturnType<typeof setTimeout> | undefined
      const statuses = await Promise.race([
        this.status(options),
        new Promise<typeof timedOut>((resolve) => {
          timer = setTimeout(
            () => {
              resolve(timedOut)
            },
            Math.min(deadline - Date.now(), 2_147_483_647)
          )
        })
      ]).finally(() => {
        clearTimeout(timer)
      })
      if (statuses === timedOut) return { kind: "timed_out", status: last }
      if (statuses === undefined) return { kind: "not_found" }
      if (statuses.some(bindingStatusIsBlocked)) return { kind: "blocked", status: statuses }
      if (statuses.some(bindingStatusIsFailed)) return { kind: "failed", status: statuses }
      // A recreated source no longer holds the heads the target captured.
      if (
        target?.some((wanted) =>
          statuses.some(
            (status) =>
              matches(status, wanted) &&
              status.sourceIncarnation !== undefined &&
              status.sourceIncarnation !== wanted.sourceIncarnation
          )
        ) === true
      )
        target = undefined
      if (
        target === undefined &&
        statuses.length > 0 &&
        statuses.every((status) => status.sourceIncarnation !== undefined)
      )
        target = statuses
      const ready =
        statuses.length > 0 &&
        statuses.every((status) =>
          level !== "caught_up"
            ? levels.indexOf(bindingStatusReadiness(status)) >= levels.indexOf(level)
            : target?.some(
                (wanted) => matches(status, wanted) && bindingStatusCaughtUpTo(status, wanted)
              ) === true
        ) &&
        (target === undefined ||
          target.every((wanted) => statuses.some((status) => matches(status, wanted))))
      if (ready) return { kind: "ready", status: statuses }
      last = statuses
      await delay(
        Math.max(0, Math.min(pauseMs, deadline - Date.now())),
        options.signal,
        "materialization wait"
      )
      pauseMs = Math.min(pauseMs * 2, MATERIALIZATION_POLL_MAX_MS)
    }
    return { kind: "timed_out", status: last }
  }
}
