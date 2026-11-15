const MAX_RECORDED_WRITES = 4_096
const DEFAULT_WAIT_MS = 10_000

/** One log-memory write a default recall waits for the view to apply. `view`
 * names the stream, topic, and namespace. A remember carries the scope it was
 * written under. A forget carries only the `conversation` it is limited to,
 * absent for any.
 * @internal */
export interface MemoryWrite {
  readonly view: string
  readonly id: string
  readonly forgotten: boolean
  readonly conversation?: string
  readonly user?: string
  readonly agent?: string
  readonly app?: string
}

/** The lenses a recall reads through, each unset when the recall names none.
 * @internal */
export interface RecallLenses {
  readonly conversation?: string
  readonly user?: string
  readonly agent?: string
  readonly app?: string
}

/** The log-memory writes one connection made, shared by its clones and every
 * memory handle built from it.
 * @internal */
export class MemoryWrites {
  private writes: MemoryWrite[] = []

  /** `waitMs` bounds how long a recall waits for the view. */
  constructor(readonly waitMs = DEFAULT_WAIT_MS) {}

  /** Records `write`. A later write on the same id replaces the earlier one,
   * so a remember and a forget of one item never wait for opposite views. */
  record(write: MemoryWrite): void {
    this.writes = this.writes.filter(
      (earlier) => earlier.view !== write.view || earlier.id !== write.id
    )
    this.writes.push(write)
    if (this.writes.length > MAX_RECORDED_WRITES) this.writes.shift()
  }

  /** The recorded writes on `view` a recall through `lenses` returns or
   * leaves out: a remembered item inside every lens the recall sets, and
   * every forgotten one, since a forget names no user, agent, or application. */
  within(view: string, lenses: RecallLenses): readonly MemoryWrite[] {
    return this.writes.filter(
      (write) =>
        write.view === view &&
        (write.forgotten ||
          (inLens(lenses.conversation, write.conversation) &&
            inLens(lenses.user, write.user) &&
            inLens(lenses.agent, write.agent) &&
            inLens(lenses.app, write.app)))
    )
  }

  /** Drops `settled` from the record. */
  settle(settled: readonly MemoryWrite[]): void {
    if (settled.length === 0) return
    const gone = new Set(settled)
    this.writes = this.writes.filter((write) => !gone.has(write))
  }
}

function inLens(lens: string | undefined, value: string | undefined): boolean {
  return lens === undefined || lens === value
}
