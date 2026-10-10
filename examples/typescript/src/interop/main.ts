import {
  A2aBridge,
  Agent,
  AgentId,
  AgentTopic,
  ConversationId,
  CorrelationId,
  McpBridge,
  type AgentHandle,
  type Laser
} from "@laserdata/laser-sdk"
import { wire } from "@laserdata/laser-sdk/full"

import {
  AsyncResourceGroup,
  PARTITIONS,
  phase,
  runExample,
  SESSION_RETENTION,
  utf8
} from "../common.js"
import { defaultLlm } from "../llm.js"

export const EXAMPLE = "interop"
const decoder = new TextDecoder()

async function completedTask(
  bridge: A2aBridge,
  id: string
): Promise<{ readonly state: string; readonly text: string }> {
  const deadline = Date.now() + 15_000
  while (Date.now() < deadline) {
    const task = await bridge.task(id)
    const state = task.status.state
    if (state.kind === "known" && state.name === "Completed") {
      return { state: state.name.toLowerCase(), text: task.artifacts[0]?.text ?? "(no artifact)" }
    }
    await new Promise((resolve) => setTimeout(resolve, 25))
  }
  throw new Error("A2A task did not complete before the deadline")
}

// The worker behind every bridge: it reads the decoded AGDX command, asks the
// model, and answers with an AGDX response echoing the correlation.
async function spawnWorker(laser: Laser, id: string): Promise<AgentHandle> {
  const llm = defaultLlm()
  const agent = AgentId.new(id)
  const handle = Agent.builder()
    .id(agent)
    .listenOn(AgentTopic.Sessions)
    .pollInterval(5)
    .handler({
      async handle(message, context): Promise<void> {
        const envelope = message.envelope
        if (envelope?.correlation === undefined) {
          throw new Error("bridge command must carry an AGDX correlation")
        }
        const prompt = decoder.decode(envelope.body)
        await context.laser
          .agdx(AgentTopic.Sessions, agent, message.provenance.conversationId)
          .respond(envelope.correlation, utf8(await llm.complete(prompt)))
          .send()
      }
    })
    .build()
    .spawn(laser)
  await handle.ready()
  return handle
}

/** The tool names of an MCP `tools/list` result. */
function toolNames(listed: unknown): readonly string[] {
  if (typeof listed !== "object" || listed === null || !("tools" in listed)) return []
  const { tools } = listed
  if (!Array.isArray(tools)) return []
  return tools.flatMap((tool: unknown) =>
    typeof tool === "object" && tool !== null && "name" in tool && typeof tool.name === "string"
      ? [tool.name]
      : []
  )
}

/** What each bridge flow returned. */
export interface InteropSummary {
  readonly a2a: string
  readonly mcp: string
  readonly aguiEvents: number
  readonly decision: string
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<InteropSummary> {
  phase("connecting")
  await laser.bootstrap(PARTITIONS, SESSION_RETENTION)
  const llm = defaultLlm()

  // Three responders share `agent.sessions`. Each bridge call and the input
  // request name the agent they are for, so only that agent takes them as work.
  await using agents = new AsyncResourceGroup()
  agents.add(await spawnWorker(laser, "assistant"))
  agents.add(await spawnWorker(laser, "tool-runner"))
  const approver = agents.add(
    Agent.builder()
      .id(AgentId.new("approver"))
      .listenOn(AgentTopic.Sessions)
      .pollInterval(5)
      .handler({
        handle(_message, context): Promise<void> {
          return context.respondInput(AgentTopic.Sessions, utf8("approved"))
        }
      })
      .build()
      .spawn(laser)
  )
  await approver.ready()

  phase("A2A: SendMessage -> GetTask")
  const a2a = new A2aBridge(
    laser,
    AgentId.new("a2a-gateway"),
    AgentTopic.Sessions,
    AgentTopic.Sessions
  )
  const submitted = await a2a.submit(
    { message: { role: "user", parts: [{ kind: "text", text: "summarize the incident" }] } },
    { target: AgentId.new("assistant") }
  )
  const completed = await completedTask(a2a, submitted.id)
  const a2aText = completed.text
  console.log(`A2A task ${submitted.id} -> ${completed.state}: ${a2aText}`)

  phase("MCP: initialize / tools/list / tools/call")
  const mcp = new McpBridge(
    laser,
    AgentId.new("mcp-gateway"),
    AgentTopic.Sessions,
    AgentTopic.Sessions,
    "laser-mcp"
  )
    .withTool("ask", "ask the assistant a question", {
      type: "object",
      properties: { q: { type: "string" } }
    })
    .withTimeout(15_000)
  const names = toolNames(mcp.listTools())
  console.log(`MCP tools/list: [${names.join(", ")}]`)
  // The MCP `tools/call` params ride the command body unchanged.
  const tool = await mcp.callTool(
    "ask",
    { name: "ask", arguments: { q: "what is the Agent Data Exchange Protocol?" } },
    { target: AgentId.new("tool-runner") }
  )
  const mcpText = tool.content[0]?.text ?? "(empty)"
  console.log(`MCP tools/call -> isError=${String(tool.isError === true)}, content: ${mcpText}`)

  phase("AG-UI: render a chat stream as events")
  const conversation = ConversationId.new()
  const stream = laser
    .agdx(AgentTopic.Sessions, AgentId.new("assistant"), conversation)
    .stream(CorrelationId.parse(conversation.toString()), wire.OPERATION_CHAT)
  const answer = await llm.complete("give a one-line status update")
  for (const token of answer.split(/(?<= )/)) await stream.write(utf8(token))
  await stream.finish("stop")
  const events = await laser.aguiEvents(conversation, AgentTopic.Sessions)
  console.log(`AG-UI rendered ${String(events.length)} event(s) from the chat stream`)

  // Human-in-the-loop: the orchestrator pauses for a human decision, the
  // approver resolves the interrupt it is handling. Built on AGDX
  // command/response, so it rides the same log as everything above.
  phase("Human-in-the-loop: request_input -> respond_input")
  const decision = decoder.decode(
    await laser
      .agdx(AgentTopic.Sessions, AgentId.new("orchestrator"), ConversationId.new())
      .requestInput(AgentTopic.Sessions, utf8("approve draining node-7?"), 15_000, {
        target: AgentId.new("approver")
      })
  )
  console.log(`HITL decision: ${decision}`)
  return { a2a: a2aText, mcp: mcpText, aguiEvents: events.length, decision }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
