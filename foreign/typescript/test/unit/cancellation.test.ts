import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import { test } from "node:test"
import type { Capabilities } from "../../src/client/capabilities.js"
import { managedCapabilitiesFrom } from "../../src/client/capabilities.js"
import { CancelledError } from "../../src/client/errors.js"
import { Kv } from "../../src/managed/kv.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { encodeKvReply } from "../../src/wire/kv.js"

const CAPS: Capabilities = managedCapabilitiesFrom({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
  backends: []
})

// Interfaces the application implements and the SDK calls, and the SDK's own
// implementations of them. They are not calls an application awaits, but
// none of them takes a positional signal either.
const SEAMS = new Set([
  "ActionGovernor",
  "AgentHandler",
  "AgentMiddleware",
  "BlobStore",
  "Consolidator",
  "ConversationStateLoader",
  "DeadLetterSink",
  "DedicatedKvTransport",
  "Deduplicator",
  "Embedder",
  "FileStore",
  "InMemoryStore",
  "KvSnapshotStore",
  "KvStore",
  "LogMemory",
  "ManagedKvTransport",
  "Memory",
  "MemoryHandler",
  "ProducerBackgroundOptions",
  "QuorumGovernor",
  "RerankedMemory",
  "Reranker",
  "SlidingWindow",
  "SnapshotStore",
  "StateStore",
  "Summarizer",
  "SwappableGovernor",
  "TopicSnapshotStore",
  "VectorMemory"
])
// Teardown takes no signal: a cancelled teardown would leak the handle.
const TEARDOWN = new Set(["close", "shutdown", "[Symbol.asyncDispose]", "LeaseGuard.release"])
const SIGNAL = "signal?: AbortSignal"
const POSITIONAL_SIGNAL = /(?:\(|, )signal\?: AbortSignal/
const METHOD =
  /^(?:export function |static |protected |abstract )*(\[Symbol\.\w+\]|[A-Za-z_$][\w$]*)(?:<[^(]*>)?\??\(/

interface Declaration {
  readonly kind: string
  readonly name: string
  readonly head: string
  readonly body: readonly string[]
}

void test("given_an_aborted_signal_when_a_kv_read_is_called_then_should_reject_with_cancelled_before_sending", async () => {
  const sent: number[] = []
  const store = Kv.create(
    {
      sendManaged(code) {
        sent.push(code)
        return Promise.resolve(okFrame())
      }
    },
    () => Promise.resolve(CAPS),
    "sessions"
  )
  const controller = new AbortController()
  controller.abort()
  await assert.rejects(store.get(Uint8Array.of(1), { signal: controller.signal }), CancelledError)
  assert.deepEqual(sent, [])
})

void test(
  "given_a_kv_read_waiting_for_its_reply_when_the_signal_aborts_then_should_reject_with_cancelled",
  { timeout: 5_000 },
  async () => {
    const sent: number[] = []
    // The reply never comes, and the timer keeps the test alive while it waits.
    let held: ReturnType<typeof setTimeout> | undefined
    const store = Kv.create(
      {
        sendManaged(code) {
          sent.push(code)
          return new Promise<Uint8Array>(() => {
            held = setTimeout(() => undefined, 60_000)
          })
        }
      },
      () => Promise.resolve(CAPS),
      "sessions"
    )
    const controller = new AbortController()
    const read = store.get(Uint8Array.of(1), { signal: controller.signal })
    await new Promise((resolve) => setImmediate(resolve))
    assert.equal(sent.length, 1)
    controller.abort()
    try {
      await assert.rejects(read, CancelledError)
    } finally {
      clearTimeout(held)
    }
  }
)

void test("given_the_public_api_when_listed_then_should_give_every_awaiting_call_a_trailing_signal", () => {
  const report = readFileSync(new URL("../../../api/laser-sdk.api.md", import.meta.url), "utf8")
  const missing = awaitingCallsWithoutSignal(report)
  assert.deepEqual(missing, [])
})

function okFrame(): Uint8Array {
  const value = encodeKvReply({ kind: "ok", outcome: { kind: "value" } })
  if (!(value instanceof Map)) throw new Error("expected a map-shaped reply")
  return encodeNamed(value)
}

// Every method or function of the API report that returns a promise and takes
// no signal, as `Declaration.member`.
function awaitingCallsWithoutSignal(report: string): readonly string[] {
  const declarations = declarationsOf(report)
  // An options interface that carries a signal as its own field.
  const carriers = new Set(
    declarations
      .filter(
        (declaration) =>
          declaration.kind === "interface" &&
          declaration.body.some((member) => member.replace(/^readonly /, "").startsWith(SIGNAL))
      )
      .map((declaration) => declaration.name)
  )
  const takesSignal = (parameters: string): boolean =>
    parameters.includes(SIGNAL) ||
    parameters.includes("CallOptions") ||
    [...carriers].some((carrier) => new RegExp(`\\b${carrier}\\b`).test(parameters))
  const missing: string[] = []
  for (const declaration of declarations) {
    const seam = SEAMS.has(declaration.name)
    const members = declaration.kind === "function" ? [declaration.head] : declaration.body
    for (const member of members) {
      const call = METHOD.exec(member)
      if (call === null) continue
      const name = call[1] ?? ""
      const end = member.lastIndexOf("): Promise<")
      if (end < 0) continue
      const label = declaration.kind === "function" ? name : `${declaration.name}.${name}`
      if (TEARDOWN.has(name) || TEARDOWN.has(label)) continue
      const parameters = member.slice(member.indexOf("("), end)
      if (POSITIONAL_SIGNAL.test(parameters)) missing.push(`${label} (positional signal)`)
      else if (!seam && !takesSignal(parameters)) missing.push(label)
    }
  }
  return missing.sort()
}

// The report's top-level declarations, each class or interface with its
// members joined onto one line apiece.
function declarationsOf(report: string): readonly Declaration[] {
  const declarations: Declaration[] = []
  const lines = report.split(/\r?\n/)
  for (let index = 0; index < lines.length; index += 1) {
    const line = lines[index] ?? ""
    const head = /^export (?:declare )?(?:abstract )?(class|interface|function) ([\w$]+)/.exec(line)
    if (head === null) continue
    const kind = head[1] ?? ""
    const name = head[2] ?? ""
    if (kind === "function") {
      let text = line.slice("export ".length)
      while (!text.trimEnd().endsWith(";") && index + 1 < lines.length) {
        index += 1
        text += ` ${(lines[index] ?? "").trim()}`
      }
      declarations.push({
        kind,
        name,
        head: text.replace(/^function /, "export function "),
        body: []
      })
      continue
    }
    if (!line.trimEnd().endsWith("{")) continue
    const body: string[] = []
    for (index += 1; index < lines.length && lines[index] !== "}"; index += 1) {
      const member = lines[index] ?? ""
      const trimmed = member.trim()
      if (trimmed === "" || trimmed.startsWith("//")) continue
      const starts = /^ {4}\S/.test(member) && !trimmed.startsWith("}")
      if (starts || body.length === 0) body.push(trimmed)
      else body[body.length - 1] = `${body[body.length - 1] ?? ""} ${trimmed}`
    }
    declarations.push({ kind, name, head: line, body })
  }
  return declarations
}
