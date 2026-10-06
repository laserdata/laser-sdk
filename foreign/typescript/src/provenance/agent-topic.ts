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
  Dlq: "agent.dlq",
  /** Any other topic, by name. */
  Custom: (name: string): string => name
} as const

export type AgentTopic = Exclude<
  (typeof AgentTopic)[keyof typeof AgentTopic],
  typeof AgentTopic.Custom
>
