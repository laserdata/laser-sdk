import assert from "node:assert/strict"
import { randomUUID } from "node:crypto"
import { test } from "node:test"
import {
  ActionDecision,
  ActionKind,
  ConversationId,
  GovernorMode,
  Laser,
  MemoryBackend,
  PolicyBlockedError,
  type ActionGovernor
} from "../../src/index.js"

const CONNECTION_STRING =
  process.env["LASER_CONNECTION_STRING"] ?? "iggy://iggy:iggy@127.0.0.1:8090"

void test("given_a_blocking_governor_when_vector_memory_writes_then_should_record_evidence_without_mutating_the_index", async () => {
  const stream = `laser-ts-test-${randomUUID()}`
  const connected = await Laser.connectWithStream(CONNECTION_STRING, stream)
  const governor: ActionGovernor = {
    decide(action) {
      return Promise.resolve(
        action.kind === ActionKind.MemoryWrite
          ? ActionDecision.block("memory is read-only")
          : ActionDecision.allow()
      )
    }
  }
  const laser = connected.withGovernor(governor, GovernorMode.Enforce)
  try {
    await laser.bootstrap(1)
    const conversation = ConversationId.new()
    const memory = laser.memoryWith("governed-vector", MemoryBackend.Vector)
    await assert.rejects(
      memory
        .remember(new TextEncoder().encode("must not persist"))
        .conversation(conversation)
        .send(),
      PolicyBlockedError
    )
    assert.deepEqual(await memory.recall().conversation(conversation).fetch(), [])
    const evidence = await laser.policyEvidence(conversation)
    assert.equal(evidence.length, 1)
    const decision = evidence[0]
    assert.ok(decision !== undefined)
    assert.equal(decision.kind, ActionKind.MemoryWrite)
    assert.equal(decision.outcome, "blocked")
  } finally {
    await laser.close()
  }
})
