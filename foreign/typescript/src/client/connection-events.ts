import type { CallOptions } from "./call-options.js"

/** A change in the state of the connection a `Laser` holds. `disconnected`
 * means the client lost its session and reconnects by the policy of the
 * connection string. `signed_in` follows a reconnect that restored it. A
 * leader change that moves the client to another node is not a disconnect. */
export type ConnectionEvent = "connected" | "disconnected" | "signed_in" | "signed_out" | "shutdown"

/** Registers `listener` for the connection events from now on, and returns
 * what removes it.
 * @internal */
export type ConnectionWatch = (listener: (event: ConnectionEvent) => void) => () => void

// A subscriber that falls behind loses its oldest events, as the Rust client's
// diagnostic channel does.
const CONNECTION_EVENT_CAPACITY = 1000

/** The connection events of one `Laser` from the moment of the call, for
 * `for await`. The iteration ends after `shutdown` or when the signal aborts. */
export class ConnectionEvents implements AsyncIterable<ConnectionEvent> {
  private readonly queue: ConnectionEvent[] = []
  private wake: (() => void) | undefined
  private ended = false
  private readonly unwatch: () => void

  private constructor(watch: ConnectionWatch | undefined, signal: AbortSignal | undefined) {
    this.unwatch =
      watch?.((event) => {
        this.push(event)
      }) ?? (() => undefined)
    if (signal?.aborted === true) this.end()
    else
      signal?.addEventListener(
        "abort",
        () => {
          this.queue.length = 0
          this.end()
        },
        { once: true }
      )
  }

  /** @internal */
  static create(watch: ConnectionWatch | undefined, options: CallOptions = {}): ConnectionEvents {
    return new ConnectionEvents(watch, options.signal)
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ConnectionEvent> {
    try {
      for (;;) {
        const event = this.queue.shift()
        if (event !== undefined) {
          yield event
          continue
        }
        if (this.ended) return
        await new Promise<void>((resolve) => {
          this.wake = resolve
        })
      }
    } finally {
      this.end()
    }
  }

  private push(event: ConnectionEvent): void {
    if (this.ended) return
    if (this.queue.length === CONNECTION_EVENT_CAPACITY) this.queue.shift()
    this.queue.push(event)
    if (event === "shutdown") this.end()
    else this.notify()
  }

  private end(): void {
    if (this.ended) return
    this.ended = true
    this.unwatch()
    this.notify()
  }

  private notify(): void {
    const wake = this.wake
    this.wake = undefined
    wake?.()
  }
}
