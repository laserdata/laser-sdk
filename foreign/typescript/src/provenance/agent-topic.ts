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
