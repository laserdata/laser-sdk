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
import type { CheckpointReadConsistency } from "../wire/checkpoint.js"
import type { BackendResourceId } from "../wire/ids.js"
import type { Consistency } from "../wire/query.js"
import type { WireTopology } from "../wire/topology.js"

export interface QueryCaps {
  readonly available: boolean
  readonly consistency: Consistency
  readonly keyword: boolean
  readonly cursorPaging: boolean
  readonly cancellation: boolean
  readonly executionStatus: boolean
}

export interface DestinationCaps {
  readonly available: boolean
  readonly consistency: CheckpointReadConsistency
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
   * with a ready managed plane. */
  readonly catalog: boolean
  /** Group-aware reads: the server resolves a consumer group's own policy,
   * delivers an unbound group unfiltered, and fences acknowledgments by policy
   * generation. A group consumer needs it on a server that serves filters. */
  readonly groupPolicyReads: boolean
  /** The evaluator version and codecs the server announced. Absent from a
   * server that predates the announcement, which evaluates as this build does. */
  readonly evaluation?: FilterAnnounce
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
  readonly destinations: DestinationCaps
  readonly kv: KvCaps
  readonly graph: boolean
  readonly forks: boolean
  readonly a2aGateway: boolean
  readonly agentWorkflow: boolean
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
  | "destinations"
  | "kv"
  | "kvCas"
  | "kvCasFenced"
  | "kvFencedLeases"
  | "graph"
  | "forks"
  | "agentWorkflow"
  | "watch"
  | "authz"
  | "filters"
  | "filterCatalog"

export const OPEN_CAPABILITIES: Capabilities = Object.freeze({
  managed: false,
  query: Object.freeze({
    available: false,
    consistency: "eventual",
    keyword: false,
    cursorPaging: false,
    cancellation: false,
    executionStatus: false
  }),
  destinations: Object.freeze({ available: false, consistency: "potentially_stale" }),
  kv: Object.freeze({ available: false, cas: false, casFenced: false, fencedLeases: false }),
  graph: false,
  forks: false,
  a2aGateway: false,
  agentWorkflow: false,
  watch: false,
  authz: false,
  filters: Object.freeze({ native: false, catalog: false, groupPolicyReads: false }),
  backends: Object.freeze([]),
  hello: "unknown"
})

const CONSISTENCY_RANK: Readonly<Record<Consistency, number>> = {
  eventual: 0,
  read_your_writes: 1,
  strong: 2
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
 * SDK, field for field equal to `OPEN_CAPABILITIES`, like Rust
 * `Capabilities::is_open_only`. */
export function isOpenOnly(capabilities: Capabilities): boolean {
  const open = OPEN_CAPABILITIES
  const { query, destinations, kv, filters } = capabilities
  return (
    capabilities.managed === open.managed &&
    query.available === open.query.available &&
    query.consistency === open.query.consistency &&
    query.keyword === open.query.keyword &&
    query.cursorPaging === open.query.cursorPaging &&
    query.cancellation === open.query.cancellation &&
    query.executionStatus === open.query.executionStatus &&
    destinations.available === open.destinations.available &&
    destinations.consistency === open.destinations.consistency &&
    kv.available === open.kv.available &&
    kv.cas === open.kv.cas &&
    kv.casFenced === open.kv.casFenced &&
    kv.fencedLeases === open.kv.fencedLeases &&
    capabilities.graph === open.graph &&
    capabilities.forks === open.forks &&
    capabilities.a2aGateway === open.a2aGateway &&
    capabilities.agentWorkflow === open.agentWorkflow &&
    capabilities.watch === open.watch &&
    capabilities.authz === open.authz &&
    filters.native === open.filters.native &&
    filters.catalog === open.filters.catalog &&
    filters.groupPolicyReads === open.filters.groupPolicyReads &&
    filters.evaluation === undefined &&
    capabilities.versions === undefined &&
    capabilities.backends.length === 0
  )
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
        : surface === "destinations"
          ? capabilities.destinations.available
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
                      : surface === "agentWorkflow"
                        ? capabilities.agentWorkflow
                        : surface === "watch"
                          ? capabilities.watch
                          : surface === "filters"
                            ? capabilities.filters.native
                            : surface === "filterCatalog"
                              ? capabilities.filters.catalog
                              : capabilities.authz
  if (!available) {
    throw new UnsupportedError(`${surface} is not served by this deployment`, {
      cause: { surface }
    })
  }
}

function strongerConsistency(left: Consistency, right: Consistency): Consistency {
  return CONSISTENCY_RANK[right] > CONSISTENCY_RANK[left] ? right : left
}

// Folds one hello announcement into `capabilities`, step for step like Rust
// `merge_announcement` and `merge_features`. Only a ready announcement lights
// up plane-served surfaces, and the query surface needs a ready backend that
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
      ...(evaluation !== undefined ? { evaluation } : {})
    },
    hello: "answered"
  }
  if (announce.ready === false) return answered
  const destinations =
    capabilities.destinations.available ||
    ((versions.checkpoint ?? 0) > 0 && has(feature.DESTINATIONS))
  let consistency = capabilities.query.consistency
  if (has(feature.STRONG_CONSISTENCY)) consistency = "strong"
  else if (has(feature.READ_YOUR_WRITES))
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
      available: query.available || versions.query > 0,
      cursorPaging: query.cursorPaging || served.paging.includes("cursor"),
      cancellation: query.cancellation || served.cancellation,
      executionStatus: query.executionStatus || served.executionStatus
    }
    if (served.consistency.includes("strong")) consistency = "strong"
    else if (served.consistency.includes("read_your_writes"))
      consistency = strongerConsistency(consistency, "read_your_writes")
  }
  const fencedLeases = capabilities.kv.fencedLeases || has(feature.KV_FENCED_LEASES)
  return {
    ...answered,
    managed: true,
    query: { ...query, consistency },
    destinations: {
      available: destinations,
      consistency: destinations ? "linearizable" : capabilities.destinations.consistency
    },
    kv: {
      available: capabilities.kv.available || versions.kv > 0,
      cas: capabilities.kv.cas || has(feature.KV_CAS),
      casFenced: capabilities.kv.casFenced || has(feature.KV_CAS_FENCED) || fencedLeases,
      fencedLeases
    },
    graph: capabilities.graph || versions.graph > 0,
    forks: capabilities.forks || versions.fork > 0,
    agentWorkflow: capabilities.agentWorkflow || has(feature.AGENT_WORKFLOW),
    watch: capabilities.watch || has(feature.WATCH),
    filters: {
      ...answered.filters,
      catalog:
        capabilities.filters.catalog || ((native || groupPolicyReads) && (versions.filter ?? 0) > 0)
    }
  }
}
