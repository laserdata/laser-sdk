import { UnsupportedError } from "./errors.js"
import { Feature, opVersionsHasFeature } from "../wire/hello.js"
import type { BackendAnnounce, BackendDescriptor, OpVersions } from "../wire/hello.js"
import type { Consistency } from "../wire/query.js"
import type { WireTopology } from "../wire/topology.js"

export interface QueryCapabilities {
  readonly available: boolean
  readonly consistency: Consistency
  readonly keyword: boolean
}

export interface KvCapabilities {
  readonly available: boolean
  readonly cas: boolean
  readonly casFenced: boolean
}

export interface Capabilities {
  readonly managed: boolean
  readonly query: QueryCapabilities
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
  query: Object.freeze({ available: false, consistency: "eventual", keyword: false }),
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
  const consistency: Consistency = opVersionsHasFeature(versions, Feature.STRONG_CONSISTENCY)
    ? "strong"
    : opVersionsHasFeature(versions, Feature.READ_YOUR_WRITES)
      ? "readYourWrites"
      : "eventual"
  return {
    ...managedBase(),
    query: {
      available: true,
      consistency,
      keyword: opVersionsHasFeature(versions, Feature.KEYWORD_SEARCH)
    },
    kv: {
      available: true,
      cas: opVersionsHasFeature(versions, Feature.KV_CAS),
      casFenced: opVersionsHasFeature(versions, Feature.KV_CAS_FENCED)
    },
    graph: versions.graph > 0,
    agentWorkflow: opVersionsHasFeature(versions, Feature.AGENT_WORKFLOW),
    watch: opVersionsHasFeature(versions, Feature.WATCH),
    authz: opVersionsHasFeature(versions, Feature.AUTHZ),
    versions,
    backends: announce.backends,
    ...(announce.topology !== undefined ? { topology: announce.topology } : {})
  }
}

export function servesConsistency(capabilities: Capabilities, level: Consistency): boolean {
  const rank: Readonly<Record<Consistency, number>> = {
    eventual: 0,
    readYourWrites: 1,
    strong: 2
  }
  return rank[level] <= rank[capabilities.query.consistency]
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
