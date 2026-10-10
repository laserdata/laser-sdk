import type { SessionLayout } from "../session.js"
import { type AgentId, AgentKind } from "../wire/agent.js"
import { AGENT_SESSIONS } from "../wire/topics.js"

/**
 * The partition one agent record lands on under the stream's `layout`, or
 * `undefined` for the session's own partition keyed by the conversation. A
 * command is keyed by its addressee and a reply by its requester, which is the
 * reply's addressee. Everything else, lifecycle and state included, rides the
 * session's partition. Only a declared per-agent partition on `agent.sessions`
 * moves a record off the session partition.
 * @internal
 */
export function resolveAgentPartition(
  layout: SessionLayout | undefined,
  topic: string,
  kind: AgentKind,
  target: AgentId | undefined
): number | undefined {
  if (layout?.kind !== "perAgentPartition" || topic !== AGENT_SESSIONS) return undefined
  const directed =
    kind === AgentKind.Command ||
    kind === AgentKind.Response ||
    kind === AgentKind.Error ||
    kind === AgentKind.Chunk
  return directed && target !== undefined ? layout.partitions.get(target) : undefined
}

/**
 * The topic one agent record lands on under a declared per-agent topic
 * layout, or `undefined` to keep the topic it was sent on. Only
 * `agent.sessions` sends move: an envelope command, response, error, or chunk
 * (`kind` set), or a plain record (`kind` undefined), addressed to a declared
 * agent goes to that agent's topic. Everything else, lifecycle and state
 * included, stays on the lane.
 * @internal
 */
export function resolveAgentTopic(
  layout: SessionLayout | undefined,
  topic: string,
  kind: AgentKind | undefined,
  target: string | undefined
): string | undefined {
  if (layout?.kind !== "perAgentTopic" || topic !== AGENT_SESSIONS) return undefined
  const directed =
    kind === undefined ||
    kind === AgentKind.Command ||
    kind === AgentKind.Response ||
    kind === AgentKind.Error ||
    kind === AgentKind.Chunk
  return directed && target !== undefined ? layout.topics.get(target) : undefined
}

/**
 * The topic `agent` reads instead of `agent.sessions` under a declared
 * per-agent topic layout, the one its addressed work and replies land on.
 * @internal
 */
export function declaredAgentTopic(
  layout: SessionLayout | undefined,
  agent: string
): string | undefined {
  return layout?.kind === "perAgentTopic" ? layout.topics.get(agent) : undefined
}

/**
 * Where a requester waits for replies sent to `replyTopic`: its declared
 * topic when replies would ride `agent.sessions`, else `replyTopic`.
 * @internal
 */
export function replyTopicFor(
  layout: SessionLayout | undefined,
  replyTopic: string,
  requester: string | undefined
): string {
  if (replyTopic !== AGENT_SESSIONS || requester === undefined) return replyTopic
  return declaredAgentTopic(layout, requester) ?? replyTopic
}
