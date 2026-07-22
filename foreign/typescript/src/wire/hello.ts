import { CodecError } from "../client/errors.js"
import { type CborMap, decodeOne, encodeNamed, expectMap, field } from "./cbor.js"
import { type WireTopology, decodeWireTopology, encodeWireTopology } from "./topology.js"

// Capability feature bits advertised in `OpVersions.features`. Each constant
// names one managed sub-feature a server serves beyond the base surface, so
// a client feature-detects it before attempting the op. Additive and pinned
// cross-repo: a new bit set by a newer server is ignored by an older client.
export const Feature = {
  KV_CAS: 1n << 0n,
  READ_YOUR_WRITES: 1n << 1n,
  STRONG_CONSISTENCY: 1n << 2n,
  KV_CAS_FENCED: 1n << 3n,
  AGENT_WORKFLOW: 1n << 4n,
  KEYWORD_SEARCH: 1n << 5n,
  WATCH: 1n << 6n,
  AUTHZ: 1n << 7n
} as const

// The wire op versions a server accepts, one per surface, plus the
// capability feature bits it advertises. `agent`/`graph` of 0 means "not
// advertised". `features` of 0n means no capability bits advertised.
export interface OpVersions {
  readonly query: number
  readonly control: number
  readonly kv: number
  readonly fork: number
  readonly agent: number
  readonly graph: number
  readonly features: bigint
}

export function newOpVersions(
  query: number,
  control: number,
  kv: number,
  fork: number
): OpVersions {
  return { query, control, kv, fork, agent: 0, graph: 0, features: 0n }
}

export function opVersionsHasFeature(versions: OpVersions, bit: bigint): boolean {
  return (versions.features & bit) === bit
}

export function encodeOpVersions(versions: OpVersions): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("query", versions.query)
  map.set("control", versions.control)
  map.set("kv", versions.kv)
  map.set("fork", versions.fork)
  if (versions.agent !== 0) map.set("agent", versions.agent)
  if (versions.graph !== 0) map.set("graph", versions.graph)
  if (versions.features !== 0n) map.set("features", versions.features)
  return map
}

export function decodeOpVersions(map: CborMap, context: string): OpVersions {
  return {
    query: field.requiredU32(map, "query", context),
    control: field.requiredU32(map, "control", context),
    kv: field.requiredU32(map, "kv", context),
    fork: field.requiredU32(map, "fork", context),
    agent: field.optionalU32(map, "agent", context) ?? 0,
    graph: field.optionalU32(map, "graph", context) ?? 0,
    features: field.optionalU64(map, "features", context) ?? 0n
  }
}

export interface HelloReply {
  readonly versions: OpVersions
}

// One materialization backend a server exposes. `id` is the stable handle a
// binding references, `kind` is the engine family as an opaque string.
// Carries identity only, never settings or secrets.
export interface BackendDescriptor {
  readonly id: string
  readonly kind: string
  readonly label?: string
  readonly version?: string
  readonly capabilities: readonly string[]
}

export function newBackendDescriptor(id: string, kind: string): BackendDescriptor {
  return { id, kind, capabilities: [] }
}

export function backendDescriptorHasCapability(backend: BackendDescriptor, tag: string): boolean {
  return backend.capabilities.includes(tag)
}

export function encodeBackendDescriptor(backend: BackendDescriptor): Map<string, unknown> {
  const map = new Map<string, unknown>()
  map.set("id", backend.id)
  map.set("kind", backend.kind)
  if (backend.label !== undefined) map.set("label", backend.label)
  if (backend.version !== undefined) map.set("version", backend.version)
  if (backend.capabilities.length > 0) map.set("capabilities", backend.capabilities)
  return map
}

function decodeCapability(item: unknown, index: number, context: string): string {
  if (typeof item !== "string") {
    throw new CodecError(
      `capabilities[${String(index)}] in ${context} must be a string`,
      context,
      "capabilities"
    )
  }
  return item
}

export function decodeBackendDescriptor(value: unknown, context: string): BackendDescriptor {
  const map = expectMap(value, context)
  const label = field.optionalString(map, "label", context)
  const version = field.optionalString(map, "version", context)
  return {
    id: field.requiredString(map, "id", context),
    kind: field.requiredString(map, "kind", context),
    ...(label !== undefined ? { label } : {}),
    ...(version !== undefined ? { version } : {}),
    capabilities: field.optionalArray(map, "capabilities", context, (item, index) =>
      decodeCapability(item, index, context)
    )
  }
}

// The managed backend's capability announcement to the streaming server, sent
// over their private socket on connect. The streaming server caches
// `versions` and `backends` and relays them verbatim when answering a client
// hello probe, so the backend is the single source of its own capability truth.
export interface BackendAnnounce {
  readonly versions: OpVersions
  readonly backends: readonly BackendDescriptor[]
  readonly topology?: WireTopology
}

export function newBackendAnnounce(versions: OpVersions): BackendAnnounce {
  return { versions, backends: [] }
}

const HELLO_REPLY_CONTEXT = "HelloReply"
const BACKEND_ANNOUNCE_CONTEXT = "BackendAnnounce"

export function encodeHelloReply(reply: HelloReply): Uint8Array {
  const map = new Map<string, unknown>()
  map.set("versions", encodeOpVersions(reply.versions))
  return encodeNamed(map)
}

export function decodeHelloReply(bytes: Uint8Array): HelloReply {
  const map = expectMap(decodeOne(bytes, HELLO_REPLY_CONTEXT), HELLO_REPLY_CONTEXT)
  return {
    versions: decodeOpVersions(
      field.requiredMap(map, "versions", HELLO_REPLY_CONTEXT),
      HELLO_REPLY_CONTEXT
    )
  }
}

export function encodeBackendAnnounce(announce: BackendAnnounce): Uint8Array {
  const map = new Map<string, unknown>()
  map.set("versions", encodeOpVersions(announce.versions))
  if (announce.backends.length > 0) {
    map.set("backends", announce.backends.map(encodeBackendDescriptor))
  }
  if (announce.topology !== undefined) {
    map.set("topology", encodeWireTopology(announce.topology))
  }
  return encodeNamed(map)
}

export function decodeBackendAnnounce(bytes: Uint8Array): BackendAnnounce {
  const map = expectMap(decodeOne(bytes, BACKEND_ANNOUNCE_CONTEXT), BACKEND_ANNOUNCE_CONTEXT)
  const versions = decodeOpVersions(
    field.requiredMap(map, "versions", BACKEND_ANNOUNCE_CONTEXT),
    BACKEND_ANNOUNCE_CONTEXT
  )
  const backends = field.optionalArray(map, "backends", BACKEND_ANNOUNCE_CONTEXT, (item) =>
    decodeBackendDescriptor(item, BACKEND_ANNOUNCE_CONTEXT)
  )
  const topologyMap = field.optionalMap(map, "topology", BACKEND_ANNOUNCE_CONTEXT)
  const topology =
    topologyMap === undefined
      ? undefined
      : decodeWireTopology(topologyMap, BACKEND_ANNOUNCE_CONTEXT)
  return {
    versions,
    backends,
    ...(topology !== undefined ? { topology } : {})
  }
}
