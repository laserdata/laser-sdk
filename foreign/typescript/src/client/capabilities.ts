import { UnsupportedError } from "./errors.js"
import type { FilterCodec } from "../wire/filter.js"
import { feature, filterAnnounceEvaluates, opVersionsHasFeature } from "../wire/hello.js"
import type {
  BackendAnnounce,
  BackendDescriptor,
  BackendReadinessReason,
  FilterAnnounce,
  OpVersions
} from "../wire/hello.js"
import type { BackendResourceId } from "../wire/ids.js"
import type { Consistency } from "../wire/query.js"
import type { WireTopology } from "../wire/topology.js"

export interface QueryCaps {
  readonly available: boolean
  readonly consistency: Consistency
  readonly keyword: boolean
}

export interface KvCaps {
  readonly available: boolean
  readonly cas: boolean
  readonly casFenced: boolean
  /** The revocable fenced-lease contract: holder-scoped acquire, renewal,
   * fence-validated release, fenced compare-and-swap requiring a live lease,
   * and the barriered read. The lease, renew, release, and fenced-CAS calls
   * all gate on this bit. */
  readonly fencedLeases: boolean
}

/** Server-side consumer filters. */
export interface FilterCaps {
  /** Filtered reads, fenced acknowledgments, previews, sample tests, and
   * validation, served by the streaming server itself. */
  readonly native: boolean
  /** Saved filters, revisions, group bindings, and mutation outcomes, served
   * with a ready managed backend. */
  readonly catalog: boolean
  /** Group-aware reads: the server resolves a consumer group's own policy,
   * delivers an unbound group unfiltered, and fences acknowledgments by policy
   * generation. A group consumer needs it on a server that serves filters. */
  readonly groupPolicyReads: boolean
  /** The evaluator version and codecs the server announced. Absent from a
   * server that predates the announcement, which evaluates as this build does. */
  readonly evaluation?: FilterAnnounce
  /** The longest filtered wait the server holds, in milliseconds. Zero when it
   * serves none, so a group reader paces idle rounds itself. */
  readonly maxWaitMs: number
}

/**
 * What the connect-time `AGDX_HELLO` probe established. A group consumer reads
 * natively only when the managed surfaces are positively absent: `rejected`
 * is Apache Iggy refusing the command or an older server's empty body,
 * `failed` a probe that established nothing, `unknown` no probe at all.
 */
export type HelloOutcome = "unknown" | "answered" | "rejected" | "failed"

export interface Capabilities {
  readonly managed: boolean
  readonly query: QueryCaps
  readonly kv: KvCaps
  readonly graph: boolean
  readonly forks: boolean
  readonly a2aGateway: boolean
  readonly sessions: boolean
  readonly materializations: boolean
  /** The deployment scopes every managed name to one stream, and each
   * stream's change feed rides its own ops topic. */
  readonly streamTenancy: boolean
  readonly watch: boolean
  readonly authz: boolean
  readonly filters: FilterCaps
  readonly versions?: OpVersions
  readonly backends: readonly BackendDescriptor[]
  readonly hello: HelloOutcome
}

export type CapabilitySurface =
  | "managed"
  | "query"
  | "kv"
  | "kvCas"
  | "kvCasFenced"
  | "kvFencedLeases"
  | "graph"
  | "forks"
  | "sessions"
  | "watch"
  | "authz"
  | "filters"
  | "filterCatalog"

export const OPEN_CAPABILITIES: Capabilities = Object.freeze({
  managed: false,
  query: Object.freeze({
    available: false,
    consistency: "eventual",
    keyword: false
  }),
  kv: Object.freeze({ available: false, cas: false, casFenced: false, fencedLeases: false }),
  graph: false,
  forks: false,
  a2aGateway: false,
  sessions: false,
  materializations: false,
  streamTenancy: false,
  watch: false,
  authz: false,
  filters: Object.freeze({ native: false, catalog: false, groupPolicyReads: false, maxWaitMs: 0 }),
  backends: Object.freeze([]),
  hello: "unknown"
})

// How long an unsettled capability set is trusted before it is probed again.
const CAPABILITY_REPROBE_INTERVAL_MS = 1_000

const CONSISTENCY_RANK: Readonly<Record<Consistency, number>> = {
  eventual: 0,
  read_your_writes: 1
}

// The announcement each answered set came from, so a merge onto the builder's
// seed folds the announcement itself like Rust `merge_announcement`, and the
// client reads the announced topology without it being a capability.
const ANNOUNCEMENTS = new WeakMap<Capabilities, BackendAnnounce>()

export function managedCapabilitiesFrom(announce: BackendAnnounce): Capabilities {
  return mergeAnnouncement(OPEN_CAPABILITIES, announce)
}

/** The stream and topic names the deployment announced with `capabilities`,
 * if it announced any. */
export function advertisedTopology(capabilities: Capabilities): WireTopology | undefined {
  return ANNOUNCEMENTS.get(capabilities)?.topology
}

/** Merges a probe result onto the builder's capability seed. The seed is the
 * starting point, never a ceiling: an answered probe adds what the deployment
 * serves, and a rejected or failed one only records its outcome. */
export function mergeCapabilities(configured: Capabilities, announced: Capabilities): Capabilities {
  const announce = ANNOUNCEMENTS.get(announced)
  if (announce !== undefined) return mergeAnnouncement(configured, announce)
  return {
    ...configured,
    hello: announced.hello === "unknown" ? configured.hello : announced.hello
  }
}

/** True when the connected infrastructure advertised nothing beyond the open
 * SDK: every field but the probe outcome equals `OPEN_CAPABILITIES`, like Rust
 * `Capabilities::is_open_only`. */
export function isOpenOnly(capabilities: Capabilities): boolean {
  return sameCapability({ ...capabilities, hello: OPEN_CAPABILITIES.hello }, OPEN_CAPABILITIES)
}

// Structural equality over the plain records a capability set is made of. An
// absent field and an undefined one are the same.
function sameCapability(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true
  if (typeof left !== "object" || typeof right !== "object" || left === null || right === null)
    return false
  if (Array.isArray(left) !== Array.isArray(right)) return false
  const leftRecord = left as Readonly<Record<string, unknown>>
  const rightRecord = right as Readonly<Record<string, unknown>>
  const keys = new Set([...Object.keys(leftRecord), ...Object.keys(rightRecord)])
  return [...keys].every((key) => sameCapability(leftRecord[key], rightRecord[key]))
}

/** Whether the server evaluates a filter built for `evaluatorVersion` with
 * `codec` exactly as this build does, like Rust `FilterCaps::evaluates`. A
 * server that announced no evaluation contract accepts every filter. */
export function filterCapsEvaluates(
  filters: FilterCaps,
  evaluatorVersion: number,
  codec: FilterCodec
): boolean {
  return (
    filters.evaluation === undefined ||
    filterAnnounceEvaluates(filters.evaluation, evaluatorVersion, codec)
  )
}

export function servesConsistency(capabilities: Capabilities, level: Consistency): boolean {
  return CONSISTENCY_RANK[level] <= CONSISTENCY_RANK[capabilities.query.consistency]
}

export function backend(
  capabilities: Capabilities,
  resourceId: BackendResourceId
): BackendDescriptor | undefined {
  return capabilities.backends.find((candidate) => candidate.resourceId.equals(resourceId))
}

export function enabledBackends(capabilities: Capabilities): readonly BackendDescriptor[] {
  return capabilities.backends.filter((candidate) => candidate.desiredState === "enabled")
}

export function unreadyBackends(capabilities: Capabilities): readonly BackendDescriptor[] {
  return enabledBackends(capabilities).filter(
    (candidate) => candidate.observedState !== "ready" || !candidate.readiness.ready
  )
}

export function readinessReasons(
  capabilities: Capabilities,
  resourceId: BackendResourceId
): readonly BackendReadinessReason[] | undefined {
  return backend(capabilities, resourceId)?.readiness.reasons
}

/** Whether a set probed at `probedAtMs` is probed again at `nowMs`: one without a managed backend, or with an enabled backend that is not ready, once it is a second old, as Rust `reprobe_due`. */
export function capabilityReprobeDue(
  capabilities: Capabilities,
  probedAtMs: number | undefined,
  nowMs: number
): boolean {
  const unsettled = !capabilities.managed || unreadyBackends(capabilities).length > 0
  return (
    unsettled && (probedAtMs === undefined || nowMs - probedAtMs >= CAPABILITY_REPROBE_INTERVAL_MS)
  )
}

export function isReady(capabilities: Capabilities): boolean {
  const enabled = enabledBackends(capabilities)
  return (
    capabilities.managed &&
    enabled.length > 0 &&
    enabled.every((candidate) => candidate.observedState === "ready" && candidate.readiness.ready)
  )
}

export function requireCapability(capabilities: Capabilities, surface: CapabilitySurface): void {
  const available =
    surface === "managed"
      ? capabilities.managed
      : surface === "query"
        ? capabilities.query.available
        : surface === "kv"
          ? capabilities.kv.available
          : surface === "kvCas"
            ? capabilities.kv.available && capabilities.kv.cas
            : surface === "kvCasFenced"
              ? capabilities.kv.available && capabilities.kv.casFenced
              : surface === "kvFencedLeases"
                ? capabilities.kv.available && capabilities.kv.fencedLeases
                : surface === "graph"
                  ? capabilities.graph
                  : surface === "forks"
                    ? capabilities.forks
                    : surface === "sessions"
                      ? capabilities.sessions
                      : surface === "watch"
                        ? capabilities.watch
                        : surface === "filters"
                          ? capabilities.filters.native
                          : surface === "filterCatalog"
                            ? capabilities.filters.catalog
                            : capabilities.authz
  if (!available) {
    throw new UnsupportedError(`${surface} is not served by this deployment`, { surface })
  }
}

function strongerConsistency(left: Consistency, right: Consistency): Consistency {
  return CONSISTENCY_RANK[right] > CONSISTENCY_RANK[left] ? right : left
}

// Folds one hello announcement into `capabilities`, step for step like Rust
// `merge_announcement` and `merge_features`. Only a ready announcement lights
// up backend-served surfaces, and the query surface needs a ready backend that
// announces query capabilities. Additive, so a surface the seed set survives.
function mergeAnnouncement(capabilities: Capabilities, announce: BackendAnnounce): Capabilities {
  const merged = foldAnnouncement(capabilities, announce)
  ANNOUNCEMENTS.set(merged, announce)
  return merged
}

function foldAnnouncement(capabilities: Capabilities, announce: BackendAnnounce): Capabilities {
  const versions = announce.versions
  const has = (bit: bigint): boolean => opVersionsHasFeature(versions, bit)
  const native = capabilities.filters.native || has(feature.CONSUMER_FILTERS)
  const groupPolicyReads = capabilities.filters.groupPolicyReads || has(feature.GROUP_POLICY_READS)
  const evaluation = native ? announce.filters : capabilities.filters.evaluation
  const answered: Capabilities = {
    ...capabilities,
    versions,
    backends: announce.backends,
    authz: capabilities.authz || has(feature.AUTHZ),
    filters: {
      native,
      catalog: capabilities.filters.catalog,
      groupPolicyReads,
      ...(evaluation !== undefined ? { evaluation } : {}),
      maxWaitMs: evaluation?.maxWaitMs ?? capabilities.filters.maxWaitMs
    },
    hello: "answered"
  }
  if (announce.ready === false) return answered
  let consistency = capabilities.query.consistency
  if (has(feature.READ_YOUR_WRITES))
    consistency = strongerConsistency(consistency, "read_your_writes")
  let query = {
    ...capabilities.query,
    keyword: capabilities.query.keyword || has(feature.KEYWORD_SEARCH)
  }
  for (const backend of announce.backends) {
    const served = backend.readiness.ready ? backend.query : undefined
    if (served === undefined) continue
    query = {
      ...query,
      available: query.available || versions.query > 0
    }
    if (served.consistency.includes("read_your_writes"))
      consistency = strongerConsistency(consistency, "read_your_writes")
  }
  const fencedLeases = capabilities.kv.fencedLeases || has(feature.KV_FENCED_LEASES)
  return {
    ...answered,
    managed: true,
    query: { ...query, consistency },
    kv: {
      available: capabilities.kv.available || versions.kv > 0,
      cas: capabilities.kv.cas || has(feature.KV_CAS),
      casFenced: capabilities.kv.casFenced || has(feature.KV_CAS_FENCED) || fencedLeases,
      fencedLeases
    },
    graph: capabilities.graph || versions.graph > 0,
    forks: capabilities.forks || versions.fork > 0,
    sessions: capabilities.sessions || has(feature.SESSIONS),
    materializations: capabilities.materializations || has(feature.MATERIALIZATIONS),
    streamTenancy: capabilities.streamTenancy || has(feature.STREAM_TENANCY),
    watch: capabilities.watch || has(feature.WATCH),
    filters: {
      ...answered.filters,
      catalog:
        capabilities.filters.catalog || ((native || groupPolicyReads) && (versions.filter ?? 0) > 0)
    }
  }
}
