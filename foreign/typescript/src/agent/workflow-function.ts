import { InvalidError } from "../client/errors.js"
import { Laser } from "../client/laser.js"
import { ConversationId } from "../types/ids.js"
import {
  Workflow,
  type JsonRoundTrip,
  type WorkflowOutcome,
  currentWorkflowRun
} from "./workflow.js"

/** A step function from `step(name, fn)`: call it inside a workflow function. */
export interface Step<Args extends readonly unknown[], Result> {
  /** Run the step and resolve to its value after a JSON round trip, or to the value a resumed run recorded. */
  (...args: Args): Promise<JsonRoundTrip<Awaited<Result>>>
  /** The plain function, which runs without a workflow and journals nothing. */
  readonly fn: (...args: Args) => Result
}

/** Where a workflow function from `workflow(name, fn, options)` runs. */
export interface WorkflowOptions {
  /** The stream of the run, ahead of the client's default stream and `LASER_STREAM`. */
  readonly stream?: string
}

/** A workflow function from `workflow(name, fn)`: call it to run the workflow
 * and resolve to what `fn` returned. */
export interface WorkflowFunction<Args extends readonly unknown[], Result> {
  /** Run the workflow and resolve to what its function returned. */
  (...args: Args): Promise<Awaited<Result>>
  /** Run the workflow and resolve to its outcome. */
  run(...args: Args): Promise<WorkflowOutcome<Awaited<Result>>>
  /** Resume the run `runId`, or start a run under a new id, and resolve to its outcome. */
  resume(runId: ConversationId | string, ...args: Args): Promise<WorkflowOutcome<Awaited<Result>>>
  /** A copy that runs on `laser` and leaves it open. */
  bind(laser: Laser): WorkflowFunction<Args, Result>
}

interface Definition<Args extends readonly unknown[], Result> {
  readonly name: string
  readonly fn: (...args: Args) => Result
  readonly options: WorkflowOptions
}

/** Make `fn` a step named `name`, journaled when a workflow function calls it.
 * A call is known by the step's name and its arguments, so a call with other
 * arguments runs as a new call. */
export function step<Args extends readonly unknown[], Result>(
  name: string,
  fn: (...args: Args) => Result
): Step<Args, Result> {
  const call = (...args: Args): Promise<JsonRoundTrip<Awaited<Result>>> => {
    const run = currentWorkflowRun()
    if (run === undefined) {
      return Promise.reject(
        new InvalidError(
          `step \`${name}\` was called outside a workflow: call it inside a workflow function, or call ${name}.fn(...) for the plain function`
        )
      )
    }
    return run.callStep(name, args, () => fn(...args))
  }
  return Object.assign(Object.defineProperty(call, "name", { value: name }), { fn })
}

/** Make `fn` a durable workflow named `name` whose steps are journaled. */
export function workflow<Args extends readonly unknown[], Result>(
  name: string,
  fn: (...args: Args) => Result,
  options: WorkflowOptions = {}
): WorkflowFunction<Args, Result> {
  return workflowFunction({ name, fn, options }, undefined)
}

function workflowFunction<Args extends readonly unknown[], Result>(
  definition: Definition<Args, Result>,
  laser: Laser | undefined
): WorkflowFunction<Args, Result> {
  const call = async (...args: Args): Promise<Awaited<Result>> =>
    (await runDefinition(definition, laser, undefined, args)).value
  return Object.assign(Object.defineProperty(call, "name", { value: definition.name }), {
    run: (...args: Args) => runDefinition(definition, laser, undefined, args),
    resume: async (runId: ConversationId | string, ...args: Args) =>
      runDefinition(
        definition,
        laser,
        typeof runId === "string" ? ConversationId.parse(runId) : runId,
        args
      ),
    bind: (client: Laser) => workflowFunction(definition, client)
  })
}

async function runDefinition<Args extends readonly unknown[], Result>(
  definition: Definition<Args, Result>,
  laser: Laser | undefined,
  runId: ConversationId | undefined,
  args: Args
): Promise<WorkflowOutcome<Awaited<Result>>> {
  if (laser !== undefined) return runOn(laser, definition, runId, args)
  const connected = await Laser.connectEnv()
  try {
    return await runOn(connected, definition, runId, args)
  } finally {
    // The run's own result outlives a failed close.
    await connected.close().catch(() => undefined)
  }
}

function runOn<Args extends readonly unknown[], Result>(
  client: Laser,
  definition: Definition<Args, Result>,
  runId: ConversationId | undefined,
  args: Args
): Promise<WorkflowOutcome<Awaited<Result>>> {
  // The stream option runs as the default stream, which `runWith` keeps ahead of the rest.
  const { stream } = definition.options
  const run = Workflow.create(
    stream === undefined ? client : client.withDefaultStream(stream),
    definition.name
  )
  if (runId !== undefined) run.runId(runId)
  return run.runWith(() => definition.fn(...args))
}
