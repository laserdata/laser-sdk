import { UnsupportedError } from "./errors.js"
import { Feature, opVersionsHasFeature } from "../wire/hello.js"
import type { BackendAnnounce, BackendDescriptor, OpVersions } from "../wire/hello.js"
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
}

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
  readonly sessions: boolean
  readonly durableDedup: boolean
  readonly versions?: OpVersions
  readonly backends: readonly BackendDescriptor[]
  readonly topology?: WireTopology
}

export type CapabilitySurface =
  | "managed"
  | "query"
  | "destinations"
  | "kv"
  | "kvCas"
  | "kvCasFenced"
  | "graph"
  | "forks"
  | "agentWorkflow"
  | "watch"
  | "authz"

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
  kv: Object.freeze({ available: false, cas: false, casFenced: false }),
  graph: false,
  forks: false,
  a2aGateway: false,
  agentWorkflow: false,
  watch: false,
  authz: false,
  sessions: false,
  durableDedup: false,
  backends: Object.freeze([])
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
      casFenced: ready && opVersionsHasFeature(versions, Feature.KV_CAS_FENCED)
    },
    graph: ready && versions.graph > 0,
    forks: ready && versions.fork > 0,
    agentWorkflow: ready && opVersionsHasFeature(versions, Feature.AGENT_WORKFLOW),
    watch: ready && opVersionsHasFeature(versions, Feature.WATCH),
    authz: opVersionsHasFeature(versions, Feature.AUTHZ),
    versions,
    backends: ready ? announce.backends : [],
    ...(announce.topology !== undefined ? { topology: announce.topology } : {})
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
      casFenced: configured.kv.casFenced || announced.kv.casFenced
    },
    graph: configured.graph || announced.graph,
    forks: configured.forks || announced.forks,
    a2aGateway: configured.a2aGateway || announced.a2aGateway,
    agentWorkflow: configured.agentWorkflow || announced.agentWorkflow,
    watch: configured.watch || announced.watch,
    authz: configured.authz || announced.authz,
    sessions: configured.sessions || announced.sessions,
    durableDedup: configured.durableDedup || announced.durableDedup,
    ...(announced.versions !== undefined
      ? { versions: announced.versions }
      : configured.versions !== undefined
        ? { versions: configured.versions }
        : {}),
    backends: announced.managed ? announced.backends : configured.backends,
    ...(announced.topology !== undefined
      ? { topology: announced.topology }
      : configured.topology !== undefined
        ? { topology: configured.topology }
        : {})
  }
}

export function servesConsistency(capabilities: Capabilities, level: Consistency): boolean {
  return CONSISTENCY_RANK[level] <= CONSISTENCY_RANK[capabilities.query.consistency]
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
                : surface === "graph"
                  ? capabilities.graph
                  : surface === "forks"
                    ? capabilities.forks
                    : surface === "agentWorkflow"
                      ? capabilities.agentWorkflow
                      : surface === "watch"
                        ? capabilities.watch
                        : capabilities.authz
  if (!available) {
    throw new UnsupportedError(`${surface} is not served by this deployment`, {
      cause: { surface }
    })
  }
}
