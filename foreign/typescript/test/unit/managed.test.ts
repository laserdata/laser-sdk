import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { OPEN_CAPABILITIES, managedCapabilitiesFrom } from "../../src/client/capabilities.js"
import { ProtocolError, UnsupportedError } from "../../src/client/errors.js"
import { executeManaged } from "../../src/client/managed.js"
import { Destinations } from "../../src/managed/destinations.js"
import { decodeCheckpointRequestFrame } from "../../src/wire/checkpoint.js"
import { KvSetCommand } from "../../src/wire/commands.js"
import { Feature } from "../../src/wire/hello.js"
import { CheckpointRequestId, DestinationId } from "../../src/wire/ids.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

const managed = managedCapabilitiesFrom({
  versions: { query: 1, control: 1, kv: 1, fork: 1, agent: 1, graph: 1, features: 0n },
  backends: []
})

void test("given_a_managed_command_when_executed_then_should_send_the_exact_code_and_bytes_and_decode_the_reply", async () => {
  const replyBytes = await readFixture("kv_reply_committed.bin")
  let capturedCode: number | undefined
  let capturedPayload: Uint8Array | undefined
  const transport = {
    sendManaged(code: number, payload: Uint8Array): Promise<Uint8Array> {
      capturedCode = code
      capturedPayload = payload
      return Promise.resolve(replyBytes)
    }
  }
  const request = {
    namespace: "sessions",
    key: Uint8Array.of(1),
    value: Uint8Array.of(2)
  }

  const reply = await executeManaged(transport, managed, KvSetCommand, request)

  assert.equal(capturedCode, 1_000_301)
  assert.deepEqual(capturedPayload, KvSetCommand.encode(request))
  assert.deepEqual(reply, { kind: "ok", outcome: { kind: "committed", version: 8n } })
})

void test("given_open_capabilities_when_executing_then_should_reject_before_encoding_or_transport", async () => {
  let sent = false
  const transport = {
    sendManaged(): Promise<Uint8Array> {
      sent = true
      return Promise.resolve(new Uint8Array())
    }
  }
  await assert.rejects(
    executeManaged(transport, OPEN_CAPABILITIES, KvSetCommand, {
      namespace: "sessions",
      key: Uint8Array.of(1),
      value: Uint8Array.of(2)
    }),
    UnsupportedError
  )
  assert.equal(sent, false)
})

void test("given_an_advertised_version_skew_when_executing_then_should_reject_before_transport", async () => {
  let sent = false
  const transport = {
    sendManaged(): Promise<Uint8Array> {
      sent = true
      return Promise.resolve(new Uint8Array())
    }
  }
  const skewed = managedCapabilitiesFrom({
    versions: { query: 1, control: 1, kv: 2, fork: 1, agent: 1, graph: 1, features: 0n },
    backends: []
  })
  await assert.rejects(
    executeManaged(transport, skewed, KvSetCommand, {
      namespace: "sessions",
      key: Uint8Array.of(1),
      value: Uint8Array.of(2)
    }),
    ProtocolError
  )
  assert.equal(sent, false)
})

void test("given_a_supervisor_assertion_when_mutating_then_should_reuse_its_signed_request_id", async () => {
  const replyBytes = await readFixture("checkpoint_reply_destination.bin")
  const requestId = CheckpointRequestId.fromU128(41n)
  const destinationId = DestinationId.fromU128(42n)
  let capturedPayload: Uint8Array | undefined
  const transport = {
    sendManaged(_code: number, payload: Uint8Array): Promise<Uint8Array> {
      capturedPayload = payload
      return Promise.resolve(replyBytes)
    }
  }
  const capabilities = managedCapabilitiesFrom({
    versions: {
      query: 1,
      control: 1,
      kv: 1,
      fork: 1,
      agent: 1,
      graph: 1,
      checkpoint: 1,
      features: Feature.DESTINATIONS
    },
    backends: []
  })
  const destinations = new Destinations(transport, () => Promise.resolve(capabilities))

  await destinations.acceptRetentionGap(3n, destinationId, 4n, 5n, 6n, {
    claims: {
      v: 1,
      requestId,
      deploymentId: 7,
      cloudUserId: 8,
      action: "accept_retention_gap",
      destinationId,
      destinationGeneration: 4n,
      expectedRevision: 5n,
      issuedAtMicros: 9n,
      expiresAtMicros: 10n
    },
    keyId: new Uint8Array(8),
    signature: new Uint8Array(64)
  })

  assert.ok(capturedPayload !== undefined)
  assert.equal(decodeCheckpointRequestFrame(capturedPayload).requestId.asU128(), requestId.asU128())
})
