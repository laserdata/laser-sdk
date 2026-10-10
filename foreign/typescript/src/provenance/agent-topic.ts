import {
  AGENT_AUDIT,
  AGENT_CONTROL,
  AGENT_DLQ,
  AGENT_HEARTBEATS,
  AGENT_JOURNAL,
  AGENT_MEMORY,
  AGENT_REGISTRY,
  AGENT_SESSIONS,
  AGENT_STREAMS
} from "../wire/topics.js"

export const AgentTopic = {
  /** The session lane: commands, replies, tool and model records, user
   * turns, lifecycle, and state, keyed by session. */
  Sessions: AGENT_SESSIONS,
  /** High-volume chunk streams, collapsed into their response on a timeline. */
  Streams: AGENT_STREAMS,
  /** Process heartbeats listing the sessions a process holds leases on. */
  Heartbeats: AGENT_HEARTBEATS,
  /** Session control requests, sent by operators only. */
  Control: AGENT_CONTROL,
  /** Memory records. */
  Memory: AGENT_MEMORY,
  /** Policy evidence and other audit records. */
  Audit: AGENT_AUDIT,
  /** The agent card registry. */
  Registry: AGENT_REGISTRY,
  /** The workflow journal, keyed by the workflow run. */
  WorkflowJournal: AGENT_JOURNAL,
  /** The dead-letter topic. */
  Dlq: AGENT_DLQ,
  /** Any other topic, by name. */
  Custom: (name: string): string => name
} as const

export type AgentTopic = Exclude<
  (typeof AgentTopic)[keyof typeof AgentTopic],
  typeof AgentTopic.Custom
>
