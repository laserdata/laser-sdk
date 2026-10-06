import { UnsupportedError } from "./errors.js"
import type { FilterCodec } from "../wire/filter.js"
import { Feature, filterAnnounceEvaluates, opVersionsHasFeature } from "../wire/hello.js"
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

export interface QueryCapabilities {
  readonly available: boolean
  readonly consistency: Consistency
  readonly keyword: boolean
  readonly cursorPaging: boolean
  readonly cancellation: boolean
  readonly executionStatus: boolean
}

export interface DestinationCapabilities {
  readonly available: boolean
  readonly checkpointVersion: number
}

export interface KvCapabilities {
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
export interface FilterCapabilities {
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
  readonly query: QueryCapabilities
  readonly destinations: DestinationCapabilities
  readonly kv: KvCapabilities
  readonly graph: boolean
  readonly forks: boolean
  readonly a2aGateway: boolean
  readonly agentWorkflow: boolean
  readonly watch: boolean
  readonly authz: boolean
  readonly filters: FilterCapabilities
  readonly versions?: OpVersions
  readonly backends: readonly BackendDescriptor[]
  readonly topology?: WireTopology
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
  destinations: Object.freeze({ available: false, checkpointVersion: 0 }),
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

function managedBase(): Capabilities {
  return {
    ...OPEN_CAPABILITIES,
    managed: true,
    query: { ...OPEN_CAPABILITIES.query, available: true },
    kv: { ...OPEN_CAPABILITIES.kv, available: true },
    forks: true
  }
}

export function managedCapabilitiesWithUnknownVersions(): Capabilities {
  return managedBase()
}

export function managedCapabilitiesFrom(announce: BackendAnnounce): Capabilities {
  const versions = announce.versions
  const ready = announce.ready !== false
  const consistency: Consistency = opVersionsHasFeature(versions, Feature.STRONG_CONSISTENCY)
    ? "strong"
    : opVersionsHasFeature(versions, Feature.READ_YOUR_WRITES)
      ? "read_your_writes"
      : "eventual"
  const readyBackends = ready ? announce.backends.filter((backend) => backend.readiness.ready) : []
  const queryCapabilities = readyBackends.flatMap((backend) => backend.query ?? [])
  return {
    ...(ready ? managedBase() : OPEN_CAPABILITIES),
    query: {
      available: ready && versions.query > 0,
      consistency: ready ? consistency : "eventual",
      keyword: ready && opVersionsHasFeature(versions, Feature.KEYWORD_SEARCH),
      cursorPaging: queryCapabilities.some((query) => query.paging.includes("cursor")),
      cancellation: queryCapabilities.some((query) => query.cancellation),
      executionStatus: queryCapabilities.some((query) => query.executionStatus)
    },
    destinations: {
      available:
        ready &&
        (versions.checkpoint ?? 0) > 0 &&
        opVersionsHasFeature(versions, Feature.DESTINATIONS),
      checkpointVersion: versions.checkpoint ?? 0
    },
    kv: {
      available: ready && versions.kv > 0,
      cas: ready && opVersionsHasFeature(versions, Feature.KV_CAS),
      casFenced:
        ready &&
        (opVersionsHasFeature(versions, Feature.KV_CAS_FENCED) ||
          opVersionsHasFeature(versions, Feature.KV_FENCED_LEASES)),
      fencedLeases: ready && opVersionsHasFeature(versions, Feature.KV_FENCED_LEASES)
    },
    graph: ready && versions.graph > 0,
    forks: ready && versions.fork > 0,
    agentWorkflow: ready && opVersionsHasFeature(versions, Feature.AGENT_WORKFLOW),
    watch: ready && opVersionsHasFeature(versions, Feature.WATCH),
    authz: opVersionsHasFeature(versions, Feature.AUTHZ),
    filters: {
      native: opVersionsHasFeature(versions, Feature.CONSUMER_FILTERS),
      catalog:
        ready &&
        (opVersionsHasFeature(versions, Feature.CONSUMER_FILTERS) ||
          opVersionsHasFeature(versions, Feature.GROUP_POLICY_READS)) &&
        (versions.filter ?? 0) > 0,
      // Served by the streaming server itself, like the native reads: a server
      // without a managed plane resolves every group as unbound.
      groupPolicyReads: opVersionsHasFeature(versions, Feature.GROUP_POLICY_READS),
      ...(opVersionsHasFeature(versions, Feature.CONSUMER_FILTERS) && announce.filters !== undefined
        ? { evaluation: announce.filters }
        : {})
    },
    versions,
    backends: announce.backends,
    ...(announce.topology !== undefined ? { topology: announce.topology } : {}),
    hello: "answered"
  }
}

const CONSISTENCY_RANK: Readonly<Record<Consistency, number>> = {
  eventual: 0,
  read_your_writes: 1,
  strong: 2
}

export function mergeCapabilities(configured: Capabilities, announced: Capabilities): Capabilities {
  const consistency =
    CONSISTENCY_RANK[announced.query.consistency] > CONSISTENCY_RANK[configured.query.consistency]
      ? announced.query.consistency
      : configured.query.consistency
  return {
    managed: configured.managed || announced.managed,
    query: {
      available: configured.query.available || announced.query.available,
      consistency,
      keyword: configured.query.keyword || announced.query.keyword,
      cursorPaging: configured.query.cursorPaging || announced.query.cursorPaging,
      cancellation: configured.query.cancellation || announced.query.cancellation,
      executionStatus: configured.query.executionStatus || announced.query.executionStatus
    },
    destinations: {
      available: configured.destinations.available || announced.destinations.available,
      checkpointVersion: Math.max(
        configured.destinations.checkpointVersion,
        announced.destinations.checkpointVersion
      )
    },
    kv: {
      available: configured.kv.available || announced.kv.available,
      cas: configured.kv.cas || announced.kv.cas,
      casFenced: configured.kv.casFenced || announced.kv.casFenced,
      fencedLeases: configured.kv.fencedLeases || announced.kv.fencedLeases
    },
    graph: configured.graph || announced.graph,
    forks: configured.forks || announced.forks,
    a2aGateway: configured.a2aGateway || announced.a2aGateway,
    agentWorkflow: configured.agentWorkflow || announced.agentWorkflow,
    watch: configured.watch || announced.watch,
    authz: configured.authz || announced.authz,
    filters: {
      native: configured.filters.native || announced.filters.native,
      catalog: configured.filters.catalog || announced.filters.catalog,
      groupPolicyReads: configured.filters.groupPolicyReads || announced.filters.groupPolicyReads,
      ...((announced.filters.evaluation ?? configured.filters.evaluation) !== undefined
        ? { evaluation: announced.filters.evaluation ?? configured.filters.evaluation }
        : {})
    },
    ...(announced.versions !== undefined
      ? { versions: announced.versions }
      : configured.versions !== undefined
        ? { versions: configured.versions }
        : {}),
    backends: announced.backends.length > 0 ? announced.backends : configured.backends,
    ...(announced.topology !== undefined
      ? { topology: announced.topology }
      : configured.topology !== undefined
        ? { topology: configured.topology }
        : {}),
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
    destinations.checkpointVersion === open.destinations.checkpointVersion &&
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
    capabilities.backends.length === 0 &&
    capabilities.topology === undefined &&
    capabilities.hello === open.hello
  )
}

/** Whether the server evaluates a filter built for `evaluatorVersion` with
 * `codec` exactly as this build does, like Rust `FilterCaps::evaluates`. A
 * server that announced no evaluation contract accepts every filter. */
export function filterCapsEvaluates(
  filters: FilterCapabilities,
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
