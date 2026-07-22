import { RoutingError } from "../client/errors.js"
import type { Provenance } from "../provenance/provenance.js"
import type { AgentId, PrincipalId } from "../types/ids.js"
import type { CapabilityDescriptor } from "../wire/agent.js"
import type { AgentRegistry, RegisteredCard } from "./registry.js"

export type { RegisteredCard } from "./registry.js"

// How a capability route picks one agent from the agents advertising the
// skill. The advisory class fields are lower-is-better, and a candidate
// that does not advertise the ranked field sorts last, so an agent that
// publishes its class always beats one that omits it.
export type RoutePolicy =
  | { readonly kind: "cheapest" }
  | { readonly kind: "fastest" }
  | { readonly kind: "leastLoaded" }
  // Prefer this agent when it is among the candidates, else fall back to
  // "any". Keeps a conversation pinned to one agent.
  | { readonly kind: "sticky"; readonly agent: AgentId }
  // Any candidate (the first resolved). The default.
  | { readonly kind: "any" }
  // Your ranking over exactly the candidate view the shipped policies
  // read, so a custom scorer can never see less than a built-in.
  | { readonly kind: "custom"; readonly scorer: RouteScorer }

export const ANY_ROUTE_POLICY: RoutePolicy = { kind: "any" }

// One capability-route candidate as every policy sees it: the agent, its
// registered card, and the descriptor it advertises for the routed
// skill.
export interface RouteCandidate {
  readonly agent: AgentId
  readonly card: RegisteredCard
  readonly capability?: CapabilityDescriptor
}

// A user ranking over capability-route candidates, plugged in with
// `{kind: "custom"}`. `select` returns the index of the chosen candidate
// (`undefined`, or out of bounds, means no pick, so the route fails with a
// `RoutingError` exactly as an empty candidate list does).
export interface RouteScorer {
  select(skillId: string, candidates: readonly RouteCandidate[]): number | undefined
}

// A capability route: which skill to resolve and how to pick among the
// agents that advertise it.
export interface CapabilitySelector {
  readonly skill: string
  readonly policy: RoutePolicy
  // Require the selected live agent connection to authenticate as this
  // principal. Absent preserves claim-only discovery.
  readonly principal?: PrincipalId
}

export function capabilitySelector(
  skill: string,
  policy: RoutePolicy,
  principal?: PrincipalId
): CapabilitySelector {
  return { skill, policy, ...(principal !== undefined ? { principal } : {}) }
}

// Where a message is addressed: one specific agent, every listener on
// the topic, or resolved by capability against the card registry (one
// capable agent, or all of them for fan-out).
export type Router =
  | { readonly kind: "to"; readonly agent: AgentId }
  // Route to one named agent only when its live connection authenticated
  // as the required principal.
  | { readonly kind: "toPrincipal"; readonly agent: AgentId; readonly principal: PrincipalId }
  | { readonly kind: "broadcast" }
  // Route to the one capable agent the policy picks.
  | { readonly kind: "toCapable"; readonly selector: CapabilitySelector }
  // Route to every capable agent (fan-out: a diagnostic pool, a verifier
  // panel).
  | { readonly kind: "allCapable"; readonly selector: CapabilitySelector }

// Route to one agent (stamps `agdx.to`).
export function routeTo(agent: AgentId): Router {
  return { kind: "to", agent }
}

// Route to `agent` only when its live presence is bound to `principal`.
export function routeToPrincipal(agent: AgentId, principal: PrincipalId): Router {
  return { kind: "toPrincipal", agent, principal }
}

// Clear any target so every consumer-group member may handle the
// message.
export function routeBroadcast(): Router {
  return { kind: "broadcast" }
}

// Route to the one agent advertising `skill` that `policy` picks.
export function routeToCapable(skill: string, policy: RoutePolicy): Router {
  return { kind: "toCapable", selector: capabilitySelector(skill, policy) }
}

// Route to every agent advertising `skill` (fan-out).
export function routeAllCapable(skill: string, policy: RoutePolicy): Router {
  return { kind: "allCapable", selector: capabilitySelector(skill, policy) }
}

// Stamp (or clear) the target for the direct variants. Capability
// variants are resolved against the registry by `resolveTargets`, so
// they leave the target unset until then. Returns a new `Provenance`
// because this port's provenance is readonly.
export function applyRoute(router: Router, provenance: Provenance): Provenance {
  switch (router.kind) {
    case "to":
    case "toPrincipal":
      return { ...provenance, targetAgentId: router.agent }
    case "broadcast":
    case "toCapable":
    case "allCapable": {
      const { targetAgentId, ...rest } = provenance
      void targetAgentId
      return rest
    }
  }
}

// Whether resolving this route requires live, principal-bearing
// presence.
export function routeRequiresPresence(router: Router): boolean {
  if (router.kind === "toPrincipal") return true
  if (router.kind === "toCapable" || router.kind === "allCapable") {
    return router.selector.principal !== undefined
  }
  return false
}

// The authenticated principal a constrained route requires. Contract
// reply verification uses the same identity, joining discovery and
// authorship.
export function requiredPrincipal(router: Router): PrincipalId | undefined {
  switch (router.kind) {
    case "toPrincipal":
      return router.principal
    case "toCapable":
    case "allCapable":
      return router.selector.principal
    case "to":
    case "broadcast":
      return undefined
  }
}

// Keep only candidates whose resolved principal equals `required`.
export function filterCandidatesByPrincipal(
  candidates: readonly RegisteredCard[],
  required: PrincipalId,
  principalFor: (agent: AgentId) => PrincipalId | undefined
): readonly RegisteredCard[] {
  return candidates.filter((card) => principalFor(card.agent)?.get() === required.get())
}

const DEFAULT_COST_CLASS = 255
const DEFAULT_LATENCY_CLASS = 255
const DEFAULT_LOAD = 65_535

function minBy<T>(items: readonly T[], key: (item: T) => number): T | undefined {
  let best: T | undefined
  let bestKey = Number.POSITIVE_INFINITY
  for (const item of items) {
    const candidateKey = key(item)
    if (candidateKey < bestKey) {
      bestKey = candidateKey
      best = item
    }
  }
  return best
}

function candidateView(
  skillId: string,
  candidates: readonly RegisteredCard[]
): readonly RouteCandidate[] {
  return candidates.map((card) => {
    const capability = card.card.capabilities.find((entry) => entry.skillId === skillId)
    return { agent: card.agent, card, ...(capability !== undefined ? { capability } : {}) }
  })
}

// Pick one agent for `skillId` from `candidates` by `policy`, or
// `undefined` when there are no candidates.
export function selectRoute(
  skillId: string,
  candidates: readonly RegisteredCard[],
  policy: RoutePolicy
): AgentId | undefined {
  const view = candidateView(skillId, candidates)
  let chosen: RouteCandidate | undefined
  switch (policy.kind) {
    case "any":
      chosen = view[0]
      break
    case "sticky":
      chosen = view.find((candidate) => candidate.agent.equals(policy.agent)) ?? view[0]
      break
    case "cheapest":
      chosen = minBy(view, (candidate) => candidate.capability?.costClass ?? DEFAULT_COST_CLASS)
      break
    case "fastest":
      chosen = minBy(
        view,
        (candidate) => candidate.capability?.latencyClass ?? DEFAULT_LATENCY_CLASS
      )
      break
    case "leastLoaded":
      chosen = minBy(view, (candidate) => candidate.capability?.load ?? DEFAULT_LOAD)
      break
    case "custom": {
      const index = policy.scorer.select(skillId, view)
      chosen = index !== undefined ? view[index] : undefined
      break
    }
  }
  return chosen?.agent
}

type RegistryView = Pick<AgentRegistry, "principalFor" | "resolve">

function principalMismatch(
  registry: RegistryView,
  agent: AgentId,
  expected: PrincipalId
): RoutingError {
  const actual = registry.principalFor(agent)?.get()
  return new RoutingError(
    `agent \`${agent.asString()}\` is not authenticated as principal ${expected.get().toString()}`,
    {
      kind: "principalMismatch",
      agent: agent.asString(),
      expected: expected.get(),
      ...(actual !== undefined ? { actual } : {})
    }
  )
}

function trustedCandidates(
  registry: RegistryView,
  selector: CapabilitySelector,
  nowMicros: bigint
): readonly RegisteredCard[] {
  const candidates = registry.resolve(selector.skill, nowMicros)
  if (selector.principal === undefined) return candidates
  const trusted = filterCandidatesByPrincipal(candidates, selector.principal, (agent) =>
    registry.principalFor(agent)
  )
  const first = candidates[0]
  if (trusted.length === 0 && first !== undefined) {
    throw principalMismatch(registry, first.agent, selector.principal)
  }
  return trusted
}

// Resolve a route to concrete target agents. Broadcast resolves to an
// empty list, meaning no `agdx.to` header. Capability routes fail rather
// than silently broadcasting when no live card matches.
export function resolveTargets(
  router: Router,
  registry: RegistryView,
  nowMicros: bigint
): readonly AgentId[] {
  switch (router.kind) {
    case "to":
      return [router.agent]
    case "toPrincipal":
      if (registry.principalFor(router.agent)?.get() !== router.principal.get()) {
        throw principalMismatch(registry, router.agent, router.principal)
      }
      return [router.agent]
    case "broadcast":
      return []
    case "toCapable": {
      const candidates = trustedCandidates(registry, router.selector, nowMicros)
      const selected = selectRoute(router.selector.skill, candidates, router.selector.policy)
      if (selected === undefined) {
        throw new RoutingError(`no live agent advertises capability \`${router.selector.skill}\``, {
          kind: "noCapableAgent",
          skill: router.selector.skill
        })
      }
      return [selected]
    }
    case "allCapable": {
      const candidates = trustedCandidates(registry, router.selector, nowMicros)
      if (candidates.length === 0) {
        throw new RoutingError(`no live agent advertises capability \`${router.selector.skill}\``, {
          kind: "noCapableAgent",
          skill: router.selector.skill
        })
      }
      return candidates.map((card) => card.agent)
    }
  }
}

// How a resolved target agent is turned into the topic name its work is
// sent on. The substrate has no single well-known inbox, on purpose: a
// real deployment scopes each user to its own stream and each workflow
// to its own topic. So routing resolves a target to where that agent
// actually consumes, never a shared name baked into the SDK.
export type InboxRoute =
  // Resolve each target to the inbox topic it advertises in its live
  // presence. The default. A target advertising no inbox is a
  // per-target failure, surfaced, never silently rerouted to a shared
  // topic.
  | { readonly kind: "advertised" }
  // Send every routed message to this fixed topic regardless of
  // per-agent presence. The explicit escape hatch, honored ahead of
  // presence.
  | { readonly kind: "fixed"; readonly topic: string }

export const ADVERTISED_INBOX_ROUTE: InboxRoute = { kind: "advertised" }

// Resolve `agent`'s inbox topic name under this route. `advertised` is
// the agent's live-presence inbox, consulted only by the `"advertised"`
// path. Unlike Rust (which resolves to an Iggy `Identifier`), this port's
// `Topic`/`Stream` already take a plain topic-name string directly, so
// there is no separate identifier type to resolve into. Throws
// `RoutingError` when an advertised route finds no inbox for the agent,
// rather than inventing a destination.
export function resolveInboxRoute(
  route: InboxRoute,
  agent: AgentId,
  advertised: string | undefined
): string {
  if (route.kind === "fixed") return route.topic
  if (advertised === undefined) {
    throw new RoutingError(`no inbox advertised for agent \`${agent.asString()}\``, {
      kind: "noInbox",
      agent: agent.asString()
    })
  }
  return advertised
}
