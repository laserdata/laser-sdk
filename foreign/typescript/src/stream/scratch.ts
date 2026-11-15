import type { CallOptions } from "../client/call-options.js"
import { INTERNAL_TRANSPORT } from "../client/internals.js"
import type { Laser } from "../client/laser.js"
import { administer } from "../client/server.js"
import { createStream } from "../iggy/server-admin.js"

/** A uniquely named stream, created on open and deleted when the scope that
 * holds it with `await using` ends. */
export class ScratchStream implements AsyncDisposable {
  private deleted = false

  private constructor(
    /** The stream's name, `{prefix}-{8 lowercase hex}`. */
    readonly name: string,
    /** A view of the connection with this stream as its default stream. */
    readonly laser: Laser
  ) {}

  /** @internal An existing stream of the drawn name is refused, never reused. */
  static async open(
    laser: Laser,
    prefix: string,
    options: CallOptions = {}
  ): Promise<ScratchStream> {
    const name = `${prefix}-${randomHex(4)}`
    await administer(
      laser[INTERNAL_TRANSPORT](),
      "scratch stream create",
      false,
      options,
      (client) => createStream(client, name)
    )
    return new ScratchStream(name, laser.withDefaultStream(name))
  }

  /** Deletes the stream with every topic and message in it. Later calls do nothing. */
  async delete(options: CallOptions = {}): Promise<void> {
    if (this.deleted) return
    await this.laser.stream(this.name).delete(options)
    this.deleted = true
  }

  [Symbol.asyncDispose](): Promise<void> {
    return this.delete()
  }
}

function randomHex(bytes: number): string {
  return Array.from(crypto.getRandomValues(new Uint8Array(bytes)), (byte) =>
    byte.toString(16).padStart(2, "0")
  ).join("")
}
