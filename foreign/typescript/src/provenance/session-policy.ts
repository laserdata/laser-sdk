import { ConversationId } from "../types/ids.js"

// How a user key maps to a conversation: a fresh one per call, or a
// stable one per user.
export type SessionPolicy = "perCall" | "perUser"

// The conversation id for `key` (random for `perCall`, derived
// deterministically for `perUser`).
export function conversationFor(policy: SessionPolicy, key: string): ConversationId {
  return policy === "perCall" ? ConversationId.new() : ConversationId.derive(key)
}
