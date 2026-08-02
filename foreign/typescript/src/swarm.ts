import type { PolicyEvidence, Verdict } from "./govern.js"

export class AgentActivity {
  private readonly byVerdict = new Map<Verdict["kind"], bigint>()
  decisions = 0n
  lastDecision: PolicyEvidence | undefined

  observe(evidence: PolicyEvidence): void {
    this.decisions += 1n
    this.byVerdict.set(evidence.decision, this.count(evidence.decision) + 1n)
    const last = this.lastDecision
    if (
      last === undefined ||
      last.atMicros < evidence.atMicros ||
      (last.atMicros === evidence.atMicros && last.decisionId < evidence.decisionId)
    ) {
      this.lastDecision = evidence
    }
  }

  count(verdict: Verdict["kind"]): bigint {
    return this.byVerdict.get(verdict) ?? 0n
  }
}

export class SwarmActivity {
  private readonly byAgent = new Map<string, AgentActivity>()
  private readonly seenDecisions = new Set<string>()

  observe(evidence: PolicyEvidence): void {
    if (evidence.source === undefined || this.seenDecisions.has(evidence.decisionId)) return
    this.seenDecisions.add(evidence.decisionId)
    const activity = this.byAgent.get(evidence.source) ?? new AgentActivity()
    activity.observe(evidence)
    this.byAgent.set(evidence.source, activity)
  }

  agent(name: string): AgentActivity | undefined {
    return this.byAgent.get(name)
  }

  agents(): readonly (readonly [string, AgentActivity])[] {
    return [...this.byAgent.entries()].toSorted(
      ([leftName, left], [rightName, right]) =>
        compareBigInt(right.decisions, left.decisions) || leftName.localeCompare(rightName)
    )
  }
}

function compareBigInt(left: bigint, right: bigint): number {
  return left < right ? -1 : left > right ? 1 : 0
}
