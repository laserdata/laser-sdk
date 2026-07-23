import type { ContextMessage } from "./context.js"
import type { PolicyEvidence } from "./govern.js"
import type { AgentDeadLetter, DeadLetterReason } from "./wire/agent.js"

const MAX_PREVIEW_CHARS = 200

export class CrashContext {
  constructor(
    readonly journal: readonly ContextMessage[],
    readonly deadLetter?: AgentDeadLetter,
    readonly lastDecision?: PolicyEvidence
  ) {}

  summarize(): string {
    let output = `journal (${String(this.journal.length)} message(s)):\n`
    for (const message of this.journal) {
      output += `  - [${message.provenance.agent?.toString() ?? "unknown"}] ${previewBytes(message.payload)}\n`
    }
    if (this.deadLetter === undefined) output += "dead letter: none\n"
    else {
      const detail =
        this.deadLetter.detail === undefined ? "" : `: ${safePreview(this.deadLetter.detail)}`
      output += `dead letter: ${String(this.deadLetter.attempts)} attempt(s), reason ${reasonName(this.deadLetter.reason)}${detail}\n`
    }
    if (this.lastDecision === undefined) output += "last decision: none\n"
    else {
      const reason =
        this.lastDecision.reason === undefined ? "" : ` - ${safePreview(this.lastDecision.reason)}`
      output += `last decision: ${safePreview(this.lastDecision.decision)} (${safePreview(this.lastDecision.outcome)})${reason}\n`
    }
    return output
  }
}

function reasonName(reason: DeadLetterReason): string {
  return reason.kind === "known" ? reason.name : `Unrecognized(${String(reason.code)})`
}

function previewBytes(payload: Uint8Array): string {
  return safePreview(new TextDecoder().decode(payload))
}

function safePreview(text: string): string {
  const escaped: string[] = []
  let count = 0
  let truncated = false
  for (const character of text) {
    if (count === MAX_PREVIEW_CHARS) {
      truncated = true
      break
    }
    const code = character.codePointAt(0) ?? 0
    if (character === "\n") escaped.push("\\n")
    else if (character === "\r") escaped.push("\\r")
    else if (character === "\t") escaped.push("\\t")
    else if (code < 0x20 || (code >= 0x7f && code <= 0x9f)) {
      escaped.push(`\\u{${code.toString(16)}}`)
    } else escaped.push(character)
    count += 1
  }
  return `${escaped.join("")}${truncated ? "..." : ""}`
}
