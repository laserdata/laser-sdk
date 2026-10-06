import type { HeaderValue } from "./header-value.js"
import type { MessageId } from "../types/ids.js"
import { Json } from "./codecs.js"

/** Exact Apache Iggy user headers, preserving each value's wire type. */
export type Headers = ReadonlyMap<string, HeaderValue>

/** A message read off the log: its payload, where it sits on the log, and its
 * user headers. No agent decoding, the agent layer reads provenance from the
 * same headers. */
export interface Message {
  readonly payload: Uint8Array
  /** Where the message sits on the log (partition and offset). */
  readonly id: MessageId
  readonly headers: Headers
  /** Decodes the payload as JSON, through `decodeValue` when given. A payload
   * that is not JSON fails with `CodecError`. */
  json<T = unknown>(decodeValue?: (value: unknown) => T): T
}

/** Gives a read message its `json` decoder. Not enumerable, so the message
 * still compares equal to its plain fields.
 * @internal */
export function withMessageJson<M extends Omit<Message, "json">>(message: M): M & Message {
  const json = <T = unknown>(decodeValue?: (value: unknown) => T): T =>
    new Json<T>(decodeValue).decode(message.payload)
  return Object.defineProperty(message, "json", { value: json }) as M & Message
}
