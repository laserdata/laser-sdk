// Well-known agent topic names, pinned from `sdk/src/provenance/topic.rs`.
// Unlike Rust, which wraps a raw Iggy `Identifier` for the `Custom` case,
// a TS `Topic`/`Stream` already takes a plain topic name string directly
// (see `stream/topic.ts`), so there is no separate "custom" variant here:
// any topic name this dictionary doesn't list is just passed as a string.
export const AgentTopic = {
  Commands: "agent.commands",
  Responses: "agent.responses",
  ToolCalls: "agent.tool_calls",
  ToolResults: "agent.tool_results",
  LlmIo: "agent.llm_io",
  HumanInput: "agent.human_input",
  Audit: "agent.audit",
  Registry: "agent.registry",
  WorkflowJournal: "agent.workflow_journal",
  Dlq: "agent.dlq"
} as const

export type AgentTopic = (typeof AgentTopic)[keyof typeof AgentTopic]
