import {
  type AgentEnvelope,
  type AgentId,
  AgentKind,
  decodeSessionPauseRequestJson
} from "../wire/agent.js"
import {
  OPERATION_SESSION_CANCEL,
  OPERATION_SESSION_PAUSE,
  OPERATION_SESSION_RESUME
} from "../wire/dispatch.js"
import type { RecordId } from "../wire/ids.js"

/** The operator requests a session has on `agent.control`, as this process
 * observed them. A handler reads them through `Session.pendingControl` and
 * decides itself when to stop: the runtime never interrupts a handler. */
export interface PendingControl {
  /** A pause was requested and no later resume lifted it. */
  readonly pauseRequested: boolean
  /** A cancel was requested, or an operator forced the session canceled. */
  readonly cancelRequested: boolean
}

/** No pending request.
 * @internal */
export const NO_PENDING_CONTROL: PendingControl = Object.freeze({
  pauseRequested: false,
  cancelRequested: false
})

/** One control request a session record carries.
 * @internal */
export type ControlAction = "pause" | "resume" | "cancel"

/** One control request as one agent reads it.
 * @internal */
export interface ControlRequest {
  readonly action: ControlAction
  /** The request names this agent: it is addressed to it, or it is a pause
   * that lists it among the participants whose acknowledgments it needs. */
  readonly named: boolean
  /** The request's record id, for the causal link of an acknowledgment. */
  readonly record?: RecordId
}

/** Where a control record sits: its partition and offset on `agent.control`.
 * @internal */
export type ControlPosition = readonly [number, bigint]

/** A control request at its position on `agent.control`.
 * @internal */
export interface PlacedRequest {
  readonly at: ControlPosition
  readonly named: boolean
  readonly record?: RecordId
}

/** The control state of one session: its flags, the pause request in force,
 * and the latest resume request.
 * @internal */
export interface ControlState {
  readonly flags: PendingControl
  /** The pause request in force, set while a pause is requested. */
  readonly pause?: PlacedRequest
  /** The latest resume request. */
  readonly resume?: PlacedRequest
}

/** No request.
 * @internal */
export const NO_CONTROL: ControlState = Object.freeze({ flags: NO_PENDING_CONTROL })

/** The pause request in force, `undefined` when no pause is requested.
 * @internal */
export function pausedBy(state: ControlState): PlacedRequest | undefined {
  return state.flags.pauseRequested ? state.pause : undefined
}

/** The control request `envelope` read from `agent.control` makes, when it is
 * addressed to `me` or to every agent. A forced cancel is a canceled session
 * status on the control topic. A pause applies to every agent it reaches, and
 * its participant list only decides which agents it names.
 * @internal */
export function controlRequest(
  envelope: AgentEnvelope,
  me: AgentId | undefined
): ControlRequest | undefined {
  if (envelope.target !== undefined && me !== undefined && envelope.target !== me) return undefined
  const action = controlAction(envelope)
  if (action === undefined) return undefined
  const addressed = envelope.target !== undefined && me !== undefined
  const listed = action === "pause" && me !== undefined && lists(envelope.body, me)
  return {
    action,
    named: addressed || listed,
    ...(envelope.record !== undefined ? { record: envelope.record } : {})
  }
}

function controlAction(envelope: AgentEnvelope): ControlAction | undefined {
  if (envelope.kind === AgentKind.Command) {
    if (envelope.operation === OPERATION_SESSION_PAUSE) return "pause"
    if (envelope.operation === OPERATION_SESSION_RESUME) return "resume"
    return envelope.operation === OPERATION_SESSION_CANCEL ? "cancel" : undefined
  }
  return envelope.kind === AgentKind.Status &&
    envelope.taskState?.kind === "known" &&
    envelope.taskState.name === "Canceled"
    ? "cancel"
    : undefined
}

function lists(body: Uint8Array, me: AgentId): boolean {
  try {
    return decodeSessionPauseRequestJson(body).participants.includes(me)
  } catch {
    return false
  }
}

function applied(state: ControlState, at: ControlPosition, request: ControlRequest): ControlState {
  const placed: PlacedRequest = {
    at,
    named: request.named,
    ...(request.record !== undefined ? { record: request.record } : {})
  }
  switch (request.action) {
    case "pause":
      return { ...state, flags: { ...state.flags, pauseRequested: true }, pause: placed }
    case "resume": {
      const { pause: _lifted, ...rest } = state
      return { ...rest, flags: { ...state.flags, pauseRequested: false }, resume: placed }
    }
    case "cancel":
      return { ...state, flags: { ...state.flags, cancelRequested: true } }
  }
}

// Offsets order records within one partition. A session's control records
// share one partition, so a record on another partition is taken as newer.
function newer(at: ControlPosition, last: ControlPosition): boolean {
  return at[0] !== last[0] || at[1] > last[1]
}

class Entry {
  state: ControlState = NO_CONTROL
  last: ControlPosition | undefined
  folded = false
  pending: (readonly [ControlPosition, ControlRequest])[] = []

  apply(at: ControlPosition, request: ControlRequest): void {
    if (this.last !== undefined && !newer(at, this.last)) return
    this.state = applied(this.state, at, request)
    this.last = at
  }
}

/** Fold control records in log order into a state and the position of the
 * last one.
 * @internal */
export function foldControl(
  records: Iterable<readonly [ControlPosition, ControlRequest]>
): readonly [ControlState, ControlPosition | undefined] {
  const entry = new Entry()
  for (const [at, request] of records) entry.apply(at, request)
  return [entry.state, entry.last]
}

/** The control state of the sessions one runtime has seen. The runtime loads
 * it from a bounded read of `agent.control` at startup, and the control
 * follower records every later request, so a process that joined after a
 * request still sees it. A book that was never loaded folds a session on its
 * first read, and live records that arrive before that fold are kept and
 * replayed on top of it.
 * @internal */
export class ControlBook {
  private readonly entries = new Map<string, Entry>()
  // Whether a follower keeps folded entries current. Without one every read
  // folds the log again.
  private live = false
  // Whether the startup read loaded every session, so a session without an
  // entry has no requests.
  private loaded = false

  /** Mark whether a follower is keeping the book current. */
  setLive(live: boolean): void {
    this.live = live
  }

  /** Whether a follower is keeping the book current. */
  isLive(): boolean {
    return this.live
  }

  /** Load the requests a bounded read of `agent.control` found, in log order.
   * Every session is folded afterwards: one without an entry has no
   * requests. */
  load(records: Iterable<readonly [string, ControlPosition, ControlRequest]>): void {
    for (const [conversation, at, request] of records) {
      const entry = this.entry(conversation)
      entry.folded = true
      entry.apply(at, request)
    }
    this.loaded = true
  }

  /** Record one live control request. */
  observe(conversation: string, at: ControlPosition, request: ControlRequest): void {
    const entry = this.entry(conversation)
    if (entry.folded || this.loaded) {
      entry.folded = true
      entry.apply(at, request)
    } else {
      entry.pending.push([at, request])
    }
  }

  /** The folded control state of `conversation`, `undefined` when it must be
   * folded from the log first. */
  state(conversation: string): ControlState | undefined {
    if (!this.live) return undefined
    const entry = this.entries.get(conversation)
    if (entry === undefined) return this.loaded ? NO_CONTROL : undefined
    return entry.folded ? entry.state : undefined
  }

  /** Whether any session has a pause or resume request, so the runtime has
   * held work to look for. */
  hasPauseHistory(): boolean {
    return [...this.entries.values()].some(
      (entry) => entry.state.pause !== undefined || entry.state.resume !== undefined
    )
  }

  /** The sessions with a pause or resume request. */
  pausedOrResumed(): readonly string[] {
    return [...this.entries]
      .filter(([, entry]) => entry.state.pause !== undefined || entry.state.resume !== undefined)
      .map(([conversation]) => conversation)
  }

  /** Install the state folded from the log through `last`, replay the live
   * requests that arrived after it, and return the result. An entry a live
   * follower keeps current is kept as it is. */
  install(
    conversation: string,
    state: ControlState,
    last: ControlPosition | undefined
  ): ControlState {
    const entry = this.entry(conversation)
    if (entry.folded && this.live) return entry.state
    const pending = entry.pending
    entry.pending = []
    entry.state = state
    entry.last = last
    entry.folded = true
    for (const [at, request] of pending) entry.apply(at, request)
    return entry.state
  }

  private entry(conversation: string): Entry {
    let entry = this.entries.get(conversation)
    if (entry === undefined) {
      entry = new Entry()
      this.entries.set(conversation, entry)
    }
    return entry
  }
}
