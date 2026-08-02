import { InvalidError } from "../client/errors.js"

export function enterBridge(bridge: string, previous: readonly string[] = []): readonly string[] {
  if (bridge.length === 0) throw new InvalidError("bridge id must not be empty")
  if (previous.includes(bridge)) {
    throw new InvalidError(`bridge loop detected at \`${bridge}\``)
  }
  return [...previous, bridge]
}

export function bridgeHopMetadata(hops: readonly string[]): {
  readonly kind: "list"
  readonly value: readonly { readonly kind: "string"; readonly value: string }[]
} {
  return {
    kind: "list",
    value: hops.map((value) => ({ kind: "string", value }))
  }
}
