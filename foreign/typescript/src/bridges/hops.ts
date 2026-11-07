import { InvalidError } from "../client/errors.js"

/** The key an inbound request carries its `bridge_hops` path under: in MCP
 * `params._meta`, in A2A `params.metadata`. */
export const BRIDGE_HOPS_META_KEY = "laserdata.com/bridge_hops"

export function enterBridge(bridge: string, previous: readonly string[] = []): readonly string[] {
  if (bridge.length === 0) throw new InvalidError("bridge id must not be empty")
  if (previous.includes(bridge)) {
    throw new InvalidError(`bridge loop detected at \`${bridge}\``)
  }
  return [...previous, bridge]
}

export function bridgeHopMetadata(hops: readonly string[]): {
  readonly kind: "list"
  readonly value: readonly { readonly kind: "str"; readonly value: string }[]
} {
  return {
    kind: "list",
    value: hops.map((value) => ({ kind: "str", value }))
  }
}

// The path one call carries: the bridge's configured path with the call's
// inbound hops before this bridge, refused when either already holds it.
export function callHops(
  configured: readonly string[],
  inbound: readonly string[]
): readonly string[] {
  const bridge = configured.at(-1)
  if (bridge === undefined) throw new InvalidError("bridge id must not be empty")
  if (inbound.length === 0) return configured
  const previous = configured.slice(0, -1)
  return enterBridge(bridge, [...previous, ...inbound.filter((hop) => !previous.includes(hop))])
}

// The inbound hop path under `BRIDGE_HOPS_META_KEY` of a request's metadata
// object, empty when absent. Anything but a list of strings is refused.
export function inboundHops(metadata: unknown): readonly string[] {
  if (typeof metadata !== "object" || metadata === null || Array.isArray(metadata)) return []
  const hops = (metadata as Readonly<Record<string, unknown>>)[BRIDGE_HOPS_META_KEY]
  if (hops === undefined) return []
  if (!Array.isArray(hops) || !hops.every((hop): hop is string => typeof hop === "string")) {
    throw new InvalidError(`${BRIDGE_HOPS_META_KEY} must be a list of strings`)
  }
  return hops
}
