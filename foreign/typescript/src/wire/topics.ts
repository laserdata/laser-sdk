export const OPS_STREAM = "_agdx"
export const CONTROL_TOPIC = "control.commands"
export const DLQ_TOPIC = "dlq"
export const CHANGES_TOPIC = "changes"

/** The ops stream topic that holds one stream's records of an ops `surface`
 * (such as `CHANGES_TOPIC` or `DLQ_TOPIC`) under stream tenancy,
 * `stream:<stream>/_agdx/<surface>`. */
export function streamOpsTopic(stream: string, surface: string): string {
  return `stream:${stream}/_agdx/${surface}`
}

export const AGENT_SESSIONS = "agent.sessions"
export const AGENT_STREAMS = "agent.streams"
export const AGENT_HEARTBEATS = "agent.heartbeats"
export const AGENT_CONTROL = "agent.control"
export const AGENT_MEMORY = "agent.memory"
export const AGENT_DLQ = "agent.dlq"
export const AGENT_AUDIT = "agent.audit"
export const AGENT_JOURNAL = "agent.workflow_journal"
export const AGENT_REGISTRY = "agent.registry"
