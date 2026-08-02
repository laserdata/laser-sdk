import {
  A2aBridge,
  Agent,
  AgentId,
  AgentTopic,
  ConversationId,
  CorrelationId,
  McpBridge,
  OPERATION_CHAT,
  type AgentHandle,
  type Laser
} from "@laserdata/laser-sdk"

import { AsyncResourceGroup, phase, runExample, utf8 } from "../common.js"
import { defaultLlm } from "../llm.js"

export const EXAMPLE = "interop"
const decoder = new TextDecoder()

async function completedTask(bridge: A2aBridge, id: string): Promise<string> {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    const task = await bridge.task(id)
    const state = task.status.state
    if (state.kind === "known" && state.name === "Completed") {
      return task.artifacts[0]?.text ?? ""
    }
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  throw new Error("A2A task did not complete before the deadline")
}

function worker(laser: Laser, id: string, input: string, output: string): AgentHandle {
  const llm = defaultLlm()
  const agent = AgentId.new(id)
  return Agent.builder()
    .id(agent)
    .listenOn(input)
    .respondOn(output)
    .pollInterval(5)
    .handler({
      async handle(message, context): Promise<void> {
        const envelope = message.envelope
        if (envelope?.correlation === undefined) {
          throw new Error("bridge command must carry an AGDX correlation")
        }
        const prompt = decoder.decode(envelope.body)
        await context.laser
          .agdx(output, agent, message.provenance.conversationId)
          .respond(envelope.correlation, utf8(await llm.complete(prompt)))
          .send()
      }
    })
    .spawn(laser)
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  phase("connecting")
  await laser.bootstrap(1)
  await using agents = new AsyncResourceGroup()
  const handles = [
    agents.add(worker(laser, "assistant", AgentTopic.Commands, AgentTopic.Responses)),
    agents.add(worker(laser, "tool-runner", AgentTopic.ToolCalls, AgentTopic.ToolResults)),
    agents.add(
      Agent.builder()
        .id(AgentId.new("approver"))
        .listenOn(AgentTopic.HumanInput)
        .pollInterval(5)
        .handler({
          handle(_message, context): Promise<void> {
            return context.respondInput(AgentTopic.Responses, utf8("approved"))
          }
        })
        .spawn(laser)
    )
  ]
  await Promise.all(handles.map((handle) => handle.ready()))

  phase("A2A: SendMessage -> GetTask")
  const a2a = new A2aBridge(
    laser,
    AgentId.new("a2a-gateway"),
    AgentTopic.Commands,
    AgentTopic.Responses
  ).withCapabilities([{ skillId: "summarize" }])
  const submitted = await a2a.submit({
    message: { role: "user", text: "summarize incident" }
  })
  console.log(`A2A completed: ${await completedTask(a2a, submitted.id)}`)

  phase("MCP: initialize / tools/list / tools/call")
  const mcp = new McpBridge(
    laser,
    AgentId.new("mcp-gateway"),
    AgentTopic.ToolCalls,
    AgentTopic.ToolResults,
    "laser-mcp"
  )
    .withTool("ask", "Ask the assistant", {
      type: "object",
      properties: { q: { type: "string" } },
      required: ["q"]
    })
    .withResource("laser://protocol", "AGDX", "text/plain", "Agent Data Exchange Protocol")
    .withPrompt({ name: "incident", description: "Summarize an incident" }, [
      ["user", "Summarize {{incident}}"]
    ])
    .withTimeout(15_000)
  const tool = await mcp.callTool("ask", { q: "what is AGDX?" })
  console.log(`MCP result: ${tool.content[0]?.text ?? ""}`)

  phase("AG-UI: render a chat stream as events")
  const conversation = ConversationId.new()
  const stream = laser
    .agdx(AgentTopic.LlmIo, AgentId.new("assistant"), conversation)
    .stream(CorrelationId.parse(conversation.toString()), OPERATION_CHAT)
  await stream.write(utf8("incident "))
  await stream.write(utf8("stable"))
  await stream.finish("stop")
  const events = await laser.aguiEvents(conversation, AgentTopic.LlmIo)
  console.log(`AG-UI events: ${String(events.length)}`)

  phase("Human-in-the-loop: request_input -> respond_input")
  const decision = await laser
    .agdx(AgentTopic.HumanInput, AgentId.new("orchestrator"), ConversationId.new())
    .requestInput(AgentTopic.Responses, utf8("approve credit?"), 15_000)
  console.log(`human decision: ${decoder.decode(decision)}`)
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
