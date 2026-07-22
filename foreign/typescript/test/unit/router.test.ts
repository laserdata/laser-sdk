import assert from "node:assert/strict"
import { test } from "node:test"
import { RoutingError } from "../../src/client/errors.js"
import { AgentTopic } from "../../src/provenance/agent-topic.js"
import type { Provenance } from "../../src/provenance/provenance.js"
import {
  ADVERTISED_INBOX_ROUTE,
  applyRoute,
  capabilitySelector,
  filterCandidatesByPrincipal,
  requiredPrincipal,
  resolveInboxRoute,
  resolveTargets,
  routeAllCapable,
  routeBroadcast,
  routeTo,
  routeToCapable,
  routeToPrincipal,
  selectRoute,
  type RegisteredCard,
  type RouteCandidate,
  type RouteScorer
} from "../../src/agent/router.js"
import { AgentId, ConversationId, PrincipalId } from "../../src/types/ids.js"

function registry(
  cards: readonly RegisteredCard[],
  principals: Readonly<Record<string, number>> = {}
): {
  resolve(skill: string, nowMicros: bigint): readonly RegisteredCard[]
  principalFor(agent: AgentId): PrincipalId | undefined
} {
  return {
    resolve(skill: string): readonly RegisteredCard[] {
      return cards.filter((candidate) =>
        candidate.card.capabilities.some((entry) => entry.skillId === skill)
      )
    },
    principalFor(agent: AgentId): PrincipalId | undefined {
      const value = principals[agent.asString()]
      return value !== undefined ? PrincipalId.new(value) : undefined
    }
  }
}

function card(
  agent: string,
  cost: number | undefined,
  latency: number | undefined,
  load: number | undefined
): RegisteredCard {
  return {
    agent: AgentId.new(agent),
    card: {
      capabilities: [
        {
          skillId: "diagnose",
          ...(cost !== undefined ? { costClass: cost } : {}),
          ...(latency !== undefined ? { latencyClass: latency } : {}),
          ...(load !== undefined ? { load } : {})
        }
      ]
    },
    observedAtMicros: 0n
  }
}

void test("given_a_route_to_an_agent_when_applied_then_should_set_the_target", () => {
  const provenance: Provenance = { conversationId: ConversationId.new() }
  const applied = applyRoute(routeTo(AgentId.new("executor")), provenance)
  assert.equal(applied.targetAgentId?.asString(), "executor")
})

void test("given_a_broadcast_route_when_applied_then_should_clear_the_target", () => {
  const provenance: Provenance = {
    conversationId: ConversationId.new(),
    targetAgentId: AgentId.new("executor")
  }
  const applied = applyRoute(routeBroadcast(), provenance)
  assert.equal(applied.targetAgentId, undefined)
  assert.ok(!("targetAgentId" in applied))
})

void test("given_candidates_when_selected_by_policy_then_should_pick_the_best_by_that_class", () => {
  const cheap = card("cheap", 1, 9, 900)
  const fast = card("fast", 9, 1, 500)
  const idle = card("idle", 5, 5, 10)
  const candidates = [cheap, fast, idle]

  assert.equal(selectRoute("diagnose", candidates, { kind: "cheapest" })?.asString(), "cheap")
  assert.equal(selectRoute("diagnose", candidates, { kind: "fastest" })?.asString(), "fast")
  assert.equal(selectRoute("diagnose", candidates, { kind: "leastLoaded" })?.asString(), "idle")
  assert.equal(
    selectRoute("diagnose", candidates, { kind: "sticky", agent: AgentId.new("fast") })?.asString(),
    "fast"
  )
  // Sticky to an absent agent falls back to a candidate.
  assert.ok(
    selectRoute("diagnose", candidates, { kind: "sticky", agent: AgentId.new("gone") }) !==
      undefined
  )
})

void test("given_a_principal_bound_selector_when_filtering_then_should_drop_foreign_claims", () => {
  const trusted = card("trusted", 1, undefined, undefined)
  const foreign = card("foreign", 2, undefined, undefined)
  const filtered = filterCandidatesByPrincipal([trusted, foreign], PrincipalId.new(7), (agent) => {
    if (agent.asString() === "trusted") return PrincipalId.new(7)
    if (agent.asString() === "foreign") return PrincipalId.new(9)
    return undefined
  })
  assert.equal(filtered.length, 1)
  assert.equal(filtered[0]?.agent.asString(), "trusted")
})

void test("given_a_principal_bound_route_when_reading_required_identity_then_should_return_principal", () => {
  const router = routeToPrincipal(AgentId.new("billing"), PrincipalId.new(42))
  assert.equal(requiredPrincipal(router)?.get(), 42)
})

void test("given_direct_and_broadcast_routes_when_resolved_then_should_return_explicit_targets", () => {
  const empty = registry([])
  assert.deepEqual(
    resolveTargets(routeTo(AgentId.new("billing")), empty, 0n).map((agent) => agent.asString()),
    ["billing"]
  )
  assert.deepEqual(resolveTargets(routeBroadcast(), empty, 0n), [])
})

void test("given_capable_agents_when_resolving_routes_then_should_apply_single_and_all_policies", () => {
  const cheap = card("cheap", 1, 9, 900)
  const fast = card("fast", 9, 1, 500)
  const view = registry([cheap, fast])

  assert.deepEqual(
    resolveTargets(routeToCapable("diagnose", { kind: "fastest" }), view, 0n).map((agent) =>
      agent.asString()
    ),
    ["fast"]
  )
  assert.deepEqual(
    resolveTargets(routeAllCapable("diagnose", { kind: "any" }), view, 0n).map((agent) =>
      agent.asString()
    ),
    ["cheap", "fast"]
  )
})

void test("given_no_capable_agent_when_resolving_then_should_fail_without_broadcasting", () => {
  assert.throws(
    () => resolveTargets(routeToCapable("diagnose", { kind: "any" }), registry([]), 0n),
    (error: unknown) => error instanceof RoutingError && error.reason.kind === "noCapableAgent"
  )
})

void test("given_principal_bound_routes_when_identity_differs_then_should_report_the_actual_identity", () => {
  const candidate = card("billing", 1, undefined, undefined)
  const view = registry([candidate], { billing: 9 })
  const direct = routeToPrincipal(AgentId.new("billing"), PrincipalId.new(7))
  const capable = {
    kind: "toCapable",
    selector: capabilitySelector("diagnose", { kind: "any" }, PrincipalId.new(7))
  } as const

  for (const route of [direct, capable]) {
    assert.throws(
      () => resolveTargets(route, view, 0n),
      (error: unknown) =>
        error instanceof RoutingError &&
        error.reason.kind === "principalMismatch" &&
        error.reason.actual === 9
    )
  }
})

void test("given_a_custom_scorer_when_selected_then_should_rank_over_the_same_candidate_view", () => {
  const cheap = card("cheap", 1, 9, 900)
  const idle = card("idle", 5, 5, 10)
  const candidates = [cheap, idle]

  const highestLoad: RouteScorer = {
    select(_skillId: string, view: readonly RouteCandidate[]): number | undefined {
      let bestIndex: number | undefined
      let bestLoad = -1
      view.forEach((candidate, index) => {
        const load = candidate.capability?.load ?? 0
        if (load > bestLoad) {
          bestLoad = load
          bestIndex = index
        }
      })
      return bestIndex
    }
  }
  const refuseAll: RouteScorer = {
    select(): number | undefined {
      return undefined
    }
  }

  assert.equal(
    selectRoute("diagnose", candidates, { kind: "custom", scorer: highestLoad })?.asString(),
    "cheap"
  )
  assert.equal(
    selectRoute("diagnose", candidates, { kind: "custom", scorer: refuseAll }),
    undefined
  )
  assert.equal(selectRoute("diagnose", [], { kind: "any" }), undefined)
})

void test("given_a_fixed_inbox_route_when_resolved_then_should_use_that_topic_ignoring_presence", () => {
  const agent = AgentId.new("billing")
  const route = { kind: "fixed", topic: AgentTopic.Commands } as const
  assert.equal(resolveInboxRoute(route, agent, "some.other.topic"), "agent.commands")
})

void test("given_an_advertised_route_when_an_inbox_is_present_then_should_resolve_to_it", () => {
  const agent = AgentId.new("billing")
  assert.equal(
    resolveInboxRoute(ADVERTISED_INBOX_ROUTE, agent, "billing.work.2026"),
    "billing.work.2026"
  )
})

void test("given_an_advertised_route_when_no_inbox_then_should_error_without_a_fallback", () => {
  const agent = AgentId.new("billing")
  assert.throws(() => resolveInboxRoute(ADVERTISED_INBOX_ROUTE, agent, undefined), RoutingError)
})
