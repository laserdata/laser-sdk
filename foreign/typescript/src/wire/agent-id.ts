import { InvalidError } from "../client/errors.js"
import { MAX_AGENT_STRING_BYTES } from "./limits.js"

const textEncoder = new TextEncoder()

export type AgentId = string & { readonly __brand: "AgentId" }

export function parseAgentId(value: string): AgentId {
  if (value.length === 0) {
    throw new InvalidError("agent id must not be empty")
  }
  const bytes = textEncoder.encode(value).length
  if (bytes > MAX_AGENT_STRING_BYTES) {
    throw new InvalidError(
      `agent id is ${String(bytes)}B, exceeds cap ${String(MAX_AGENT_STRING_BYTES)}B`
    )
  }
  for (const char of value) {
    const code = char.charCodeAt(0)
    if (code < 0x20 || code === 0x7f || (code >= 0x80 && code <= 0x9f)) {
      throw new InvalidError(`agent id must not contain control characters (found ${char})`)
    }
  }
  return value as AgentId
}
