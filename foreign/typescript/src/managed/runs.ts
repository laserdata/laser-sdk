import type { Capabilities } from "../client/capabilities.js"
import { AgentWorkflowExecutionError, ProtocolError, UnsupportedError } from "../client/errors.js"
import { executeManaged, type ManagedTransport } from "../client/managed.js"
import type {
  AgentOutcome,
  AgentReply,
  AgentRunInfo,
  AgentRunState,
  RunBudget,
  RunPage
} from "../wire/agent-workflow.js"
import {
  AgentCancelCommand,
  AgentListCommand,
  AgentStatusCommand,
  AgentSubmitCommand,
  type ManagedCommand
} from "../wire/commands.js"
import type { PublishControl } from "./projections.js"

export type RunsBackend = ManagedTransport

function unexpected(op: string, outcome: AgentOutcome): ProtocolError {
  return new ProtocolError(`agent ${op}: unexpected outcome \`${outcome.kind}\``)
}

async function executeAgent<Request>(
  backend: RunsBackend,
  capabilities: Capabilities,
  command: ManagedCommand<Request, AgentReply>,
  request: Request
): Promise<AgentOutcome> {
  const reply = await executeManaged(backend, capabilities, command, request)
  if (reply.kind === "ok") return reply.outcome
  if (reply.kind === "err") {
    if (reply.error.kind === "unsupported") throw new UnsupportedError(reply.error.message)
    throw new AgentWorkflowExecutionError(`agent command failed: ${reply.error.kind}`, reply.error)
  }
  throw new ProtocolError(`agent: unrecognized reply variant \`${reply.tag}\``, {
    commandCode: command.code
  })
}

export interface SubmitOptions {
  readonly runId?: string
  readonly input?: Uint8Array
  readonly params?: ReadonlyMap<string, string>
  readonly budget?: RunBudget
}

export class Runs {
  constructor(
    private readonly backend: RunsBackend,
    private readonly getCapabilities: () => Promise<Capabilities>,
    private readonly publishControl: PublishControl
  ) {}

  async submit(agentId: string, input?: Uint8Array): Promise<AgentRunInfo> {
    return this.submitWith(agentId, { ...(input !== undefined ? { input } : {}) })
  }

  async submitWith(agentId: string, options: SubmitOptions = {}): Promise<AgentRunInfo> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeAgent(this.backend, capabilities, AgentSubmitCommand, {
      agentId,
      params: options.params ?? new Map(),
      ...(options.runId !== undefined ? { runId: options.runId } : {}),
      ...(options.input !== undefined ? { input: options.input } : {}),
      ...(options.budget !== undefined ? { budget: options.budget } : {})
    })
    if (outcome.kind === "submitted") return outcome.run
    throw unexpected("submit", outcome)
  }

  async submitBudgeted(
    agentId: string,
    budget: RunBudget,
    input?: Uint8Array
  ): Promise<AgentRunInfo> {
    return this.submitWith(agentId, { budget, ...(input !== undefined ? { input } : {}) })
  }

  async cancel(runId: string): Promise<AgentRunInfo> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeAgent(this.backend, capabilities, AgentCancelCommand, { runId })
    if (outcome.kind === "cancelled") return outcome.run
    throw unexpected("cancel", outcome)
  }

  async status(runId: string): Promise<AgentRunInfo> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeAgent(this.backend, capabilities, AgentStatusCommand, { runId })
    if (outcome.kind === "status") return outcome.run
    throw unexpected("status", outcome)
  }

  list(): RunListRequest {
    return new RunListRequest(this.backend, this.getCapabilities)
  }

  async registerSource(stream: string, topic: string): Promise<void> {
    await this.publishControl({ kind: "registerRunSource", source: { stream, topic } })
  }

  async removeSource(stream: string, topic: string): Promise<void> {
    await this.publishControl({ kind: "removeRunSource", source: { stream, topic } })
  }
}

export class RunListRequest {
  private agentIdFilter: string | undefined
  private stateFilter: AgentRunState | undefined
  private limitValue: number | undefined
  private cursorValue: Uint8Array | undefined

  constructor(
    private readonly backend: RunsBackend,
    private readonly getCapabilities: () => Promise<Capabilities>
  ) {}

  agent(agentId: string): this {
    this.agentIdFilter = agentId
    return this
  }

  state(state: AgentRunState): this {
    this.stateFilter = state
    return this
  }

  limit(limit: number): this {
    this.limitValue = limit
    return this
  }

  cursor(cursor: Uint8Array): this {
    this.cursorValue = cursor
    return this
  }

  async fetch(): Promise<RunPage> {
    const capabilities = await this.getCapabilities()
    const outcome = await executeAgent(this.backend, capabilities, AgentListCommand, {
      ...(this.agentIdFilter !== undefined ? { agentId: this.agentIdFilter } : {}),
      ...(this.stateFilter !== undefined ? { state: this.stateFilter } : {}),
      ...(this.limitValue !== undefined ? { limit: this.limitValue } : {}),
      ...(this.cursorValue !== undefined ? { cursor: this.cursorValue } : {})
    })
    if (outcome.kind === "list") return outcome.page
    throw unexpected("list", outcome)
  }
}
