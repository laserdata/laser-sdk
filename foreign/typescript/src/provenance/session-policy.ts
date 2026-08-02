import { ConversationId } from "../types/ids.js"

export type SessionPolicy = "perCall" | "perUser"

export function conversationFor(policy: SessionPolicy, key: string): ConversationId {
  return policy === "perCall" ? ConversationId.new() : ConversationId.derive(key)
}
