import { RoutingError } from "../client/errors.js"
import type { Provenance } from "../provenance/provenance.js"
import type { AgentId, PrincipalId } from "../types/ids.js"
import type { CapabilityDescriptor } from "../wire/agent.js"
import type { AgentRegistry, RegisteredCard } from "./registry.js"

export type { RegisteredCard } from "./registry.js"

export type RoutePolicy =
  | { readonly kind: "cheapest" }
  | { readonly kind: "fastest" }
  | { readonly kind: "leastLoaded" }
  | { readonly kind: "sticky"; readonly agent: AgentId }
  | { readonly kind: "any" }
  | { readonly kind: "custom"; readonly scorer: RouteScorer }

export const ANY_ROUTE_POLICY: RoutePolicy = { kind: "any" }

export interface RouteCandidate {
  readonly agent: AgentId
  readonly card: RegisteredCard
  readonly capability?: CapabilityDescriptor
}

export interface RouteScorer {
  select(skillId: string, candidates: readonly RouteCandidate[]): number | undefined
}

export interface CapabilitySelector {
  readonly skill: string
  readonly policy: RoutePolicy
  readonly principal?: PrincipalId
}

export function capabilitySelector(
  skill: string,
  policy: RoutePolicy,
  principal?: PrincipalId
): CapabilitySelector {
  return { skill, policy, ...(principal !== undefined ? { principal } : {}) }
}

export type Router =
  | { readonly kind: "to"; readonly agent: AgentId }
  | { readonly kind: "toPrincipal"; readonly agent: AgentId; readonly principal: PrincipalId }
  | { readonly kind: "broadcast" }
  | { readonly kind: "toCapable"; readonly selector: CapabilitySelector }
  | { readonly kind: "allCapable"; readonly selector: CapabilitySelector }

export function routeTo(agent: AgentId): Router {
  return { kind: "to", agent }
}

export function routeToPrincipal(agent: AgentId, principal: PrincipalId): Router {
  return { kind: "toPrincipal", agent, principal }
}

export function routeBroadcast(): Router {
  return { kind: "broadcast" }
}

export function routeToCapable(skill: string, policy: RoutePolicy): Router {
  return { kind: "toCapable", selector: capabilitySelector(skill, policy) }
}

export function routeAllCapable(skill: string, policy: RoutePolicy): Router {
  return { kind: "allCapable", selector: capabilitySelector(skill, policy) }
}

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

export function routeRequiresPresence(router: Router): boolean {
  if (router.kind === "toPrincipal") return true
  if (router.kind === "toCapable" || router.kind === "allCapable") {
    return router.selector.principal !== undefined
  }
  return false
}

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

export type InboxRoute =
  { readonly kind: "advertised" } | { readonly kind: "fixed"; readonly topic: string }

export const ADVERTISED_INBOX_ROUTE: InboxRoute = { kind: "advertised" }

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
