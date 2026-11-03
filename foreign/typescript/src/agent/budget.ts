import { BudgetExceededError, LaserError, SessionError } from "../client/errors.js"
import type { Session, Sessions } from "../session.js"
import { warn } from "../runtime/warn.js"
import { ConversationId } from "../types/ids.js"
import {
  type AgentEnvelope,
  type AgentErrorBody,
  AgentKind,
  type Budget,
  OPERATION_SESSION,
  decodeSessionStart
} from "../wire/agent.js"
import { decodeOne, expectMap } from "../wire/cbor.js"
import type { SessionInfo } from "../wire/session.js"
import type { Value } from "../wire/value.js"

/** The `SessionEnd.reason` of a session ended failed for passing its budget.
 * @internal */
export const BUDGET_END_REASON = "budget"

/** Which ceiling a session passed.
 * @internal */
export type BudgetDimension = "tokens" | "cost_micros"

/** How a session passed its budget.
 * @internal */
export interface BudgetBreach {
  readonly dimension?: BudgetDimension
  readonly ceiling: bigint
  readonly spent: bigint
}

/** A breach known only by its numbers, such as a workflow's own ceiling.
 * @internal */
export function exceededBreach(ceiling: bigint, spent: bigint): BudgetBreach {
  return { ceiling, spent }
}

/** The error a caller stopped by `breach` throws.
 * @internal */
export function breachError(breach: BudgetBreach): BudgetExceededError {
  return new BudgetExceededError(breach.ceiling, breach.spent)
}

/** How usage passes `budget`: the token ceiling is checked first, then the
 * cost ceiling, the rule the session index applies. `undefined` while within
 * it or without a budget.
 * @internal */
export function breachOf(
  budget: Budget | undefined,
  tokens: bigint,
  costMicros: bigint
): BudgetBreach | undefined {
  if (budget === undefined) return undefined
  if (budget.tokens !== undefined && tokens > budget.tokens) {
    return { dimension: "tokens", ceiling: budget.tokens, spent: tokens }
  }
  if (budget.costMicros !== undefined && costMicros > budget.costMicros) {
    return { dimension: "cost_micros", ceiling: budget.costMicros, spent: costMicros }
  }
  return undefined
}

/** The index's flag decides. Its counters name the ceiling that was passed.
 * @internal */
export function breachOfInfo(info: SessionInfo): BudgetBreach | undefined {
  if (!info.overBudget) return undefined
  const tokens = info.tokensIn + info.tokensOut
  return breachOf(info.budget, tokens, info.costMicros) ?? exceededBreach(0n, tokens)
}

/** The budget of the first start record on the lane against the usage summed
 * over every enveloped record, as the session index folds them.
 * @internal */
export function laneBreach(envelopes: Iterable<AgentEnvelope>): BudgetBreach | undefined {
  let started = false
  let budget: Budget | undefined
  let tokens = 0n
  let costMicros = 0n
  for (const envelope of envelopes) {
    if (envelope.usage !== undefined) {
      tokens += envelope.usage.inputTokens + envelope.usage.outputTokens
      costMicros += envelope.usage.costMicros ?? 0n
    }
    if (
      !started &&
      envelope.kind === AgentKind.Status &&
      envelope.operation === OPERATION_SESSION
    ) {
      try {
        budget = decodeSessionStart(
          expectMap(decodeOne(envelope.body, "start"), "start"),
          "start"
        ).budget
        started = true
      } catch {
        // A transition or end record is not the start.
      }
    }
  }
  return breachOf(budget, tokens, costMicros)
}

/** The error body of a session ended for `breach`.
 * @internal */
export function breachErrorBody(breach: BudgetBreach): AgentErrorBody {
  const message =
    breach.dimension === "tokens"
      ? `the session went over its token budget: ${breach.spent.toString()} of ${breach.ceiling.toString()} tokens`
      : breach.dimension === "cost_micros"
        ? `the session went over its cost budget: ${breach.spent.toString()} of ${breach.ceiling.toString()} micro-units`
        : `the session went over its budget: spent ${breach.spent.toString()} of ceiling ${breach.ceiling.toString()}`
  const detail = new Map<string, Value>([
    ["ceiling", { kind: "int", value: breach.ceiling }],
    ["spent", { kind: "int", value: breach.spent }]
  ])
  if (breach.dimension !== undefined) detail.set("budget", { kind: "str", value: breach.dimension })
  return { code: { kind: "known", name: "Internal" }, message, retryable: false, detail }
}

/** Whether `error` says the index does not know the session or its stream.
 * @internal */
export function unknownToIndex(error: unknown): boolean {
  return (
    error instanceof SessionError &&
    (error.detail.kind === "notFound" || error.detail.kind === "notRegistered")
  )
}

interface Verdict {
  readonly breach?: BudgetBreach
  readonly ended: boolean
}

interface Ending {
  readonly lens: Session
  written: boolean
}

/** The runtime's budget check before a work record reaches the handler. It
 * reads the session index only, one read per session per poll batch, so a
 * deployment without the index never enforces in the runtime.
 * @internal */
export class BudgetGate {
  // Per partition, the head offset of the poll the verdicts came from and
  // each session's verdict. Records of one poll share their partition's head
  // offset, so a later poll that moved the head reads again.
  private readonly batches = new Map<
    number,
    { readonly head: bigint; readonly verdicts: Map<string, Verdict> }
  >()
  // The handles of sessions this runtime fails for their budget, kept until
  // the index shows them ended, so a retried failure repeats the same
  // terminal record and a confirmed one is not written again.
  private readonly ending = new Map<string, Ending>()

  /** Whether a work record of `session`, delivered from `partition` by a poll
   * that saw `head`, may reach the handler. A session over its budget is ended
   * failed with reason `budget` through `lens`, once, and its record is not
   * handled. A failed index read lets the record through, and a failed
   * terminal write is retried by the session's next record. */
  async admit(
    sessions: Sessions,
    session: string,
    partition: number,
    head: bigint,
    lens: () => Session
  ): Promise<boolean> {
    if (!(await sessions.indexesSessions())) return true
    let verdict = this.cached(partition, head, session)
    if (verdict === undefined) {
      try {
        const info = await sessions.get(ConversationId.parse(session))
        const breach = breachOfInfo(info)
        verdict = {
          ...(breach !== undefined ? { breach } : {}),
          ended:
            info.status === "completed" || info.status === "failed" || info.status === "canceled"
        }
      } catch (error) {
        if (!unknownToIndex(error)) {
          warn(`reading the session budget failed, handling the record: ${String(error)}`)
          return true
        }
        verdict = { ended: false }
      }
      this.store(partition, head, session, verdict)
    }
    const breach = verdict.breach
    if (breach === undefined) return true
    if (verdict.ended) {
      this.ending.delete(session)
      return false
    }
    let entry = this.ending.get(session)
    if (entry === undefined) {
      entry = { lens: lens(), written: false }
      this.ending.set(session, entry)
    }
    if (!entry.written) {
      try {
        await entry.lens.failOverBudget(breach)
        entry.written = true
      } catch (error) {
        warn(
          `failed to end a session over its budget, its next record retries: ${error instanceof LaserError ? error.message : String(error)}`
        )
      }
    }
    return false
  }

  private cached(partition: number, head: bigint, session: string): Verdict | undefined {
    const batch = this.batches.get(partition)
    return batch?.head === head ? batch.verdicts.get(session) : undefined
  }

  private store(partition: number, head: bigint, session: string, verdict: Verdict): void {
    let batch = this.batches.get(partition)
    if (batch?.head !== head) {
      batch = { head, verdicts: new Map() }
      this.batches.set(partition, batch)
    }
    batch.verdicts.set(session, verdict)
  }
}
