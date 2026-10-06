import assert from "node:assert/strict"
import { test } from "node:test"
import { AgentTopic } from "../../src/provenance/agent-topic.js"

void test("given_the_well_known_agent_topics_when_compared_to_sdk_src_provenance_topic_rs_then_should_match_exactly", () => {
  const { Custom: _custom, ...named } = AgentTopic
  assert.deepEqual(named, {
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
  })
})

void test("given_a_custom_topic_name_when_spelled_as_an_agent_topic_then_should_be_that_name", () => {
  assert.equal(AgentTopic.Custom("readings.audit"), "readings.audit")
})
