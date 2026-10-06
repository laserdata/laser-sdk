import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { OPEN_CAPABILITIES, managedCapabilitiesFrom } from "../../src/client/capabilities.js"
import {
  CodecError,
  InvalidError,
  KvExecutionError,
  ProtocolError,
  UnsupportedError
} from "../../src/client/errors.js"
import { isVersionSkew } from "../../src/client/error-classify.js"
import { executeManaged } from "../../src/client/managed.js"
import { encodeNamed } from "../../src/wire/cbor.js"
import { Destinations } from "../../src/managed/destinations.js"
import { decodeCheckpointRequestFrame } from "../../src/wire/checkpoint.js"
import { KvSetCommand } from "../../src/wire/commands.js"
import { feature } from "../../src/wire/hello.js"
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
    (error: unknown) => error instanceof KvExecutionError && isVersionSkew(error)
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
      features: feature.DESTINATIONS
    },
    backends: []
  })
  const destinations = Destinations.create(transport, () => Promise.resolve(capabilities))

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

function commandError(code: string, message: string): Uint8Array {
  return encodeNamed(
    new Map<string, unknown>([
      ["code", code],
      ["message", message]
    ])
  )
}

function answering(reply: Uint8Array) {
  return { sendManaged: (): Promise<Uint8Array> => Promise.resolve(reply) }
}

const SET_REQUEST = { namespace: "sessions", key: Uint8Array.of(1), value: Uint8Array.of(2) }

void test("given_a_command_error_reply_when_executing_then_should_classify_its_result_code", async () => {
  await assert.rejects(
    executeManaged(
      answering(commandError("unsupported", "not served")),
      managed,
      KvSetCommand,
      SET_REQUEST
    ),
    (error: unknown) =>
      error instanceof UnsupportedError &&
      error.message === "not served" &&
      error.surface === "managed"
  )
  for (const code of ["invalid_argument", "version_skew"]) {
    await assert.rejects(
      executeManaged(answering(commandError(code, "bad")), managed, KvSetCommand, SET_REQUEST),
      InvalidError
    )
  }
  await assert.rejects(
    executeManaged(
      answering(commandError("not_found", "gone")),
      managed,
      KvSetCommand,
      SET_REQUEST
    ),
    (error: unknown) =>
      error instanceof ProtocolError && error.message === "NotFound: gone" && error.resultCode === 2
  )
})

void test("given_an_undecodable_reply_that_is_no_command_error_when_executing_then_should_keep_the_codec_error", async () => {
  await assert.rejects(
    executeManaged(
      answering(commandError("no_such_code", "x")),
      managed,
      KvSetCommand,
      SET_REQUEST
    ),
    CodecError
  )
  await assert.rejects(
    executeManaged(answering(Uint8Array.of(0xff)), managed, KvSetCommand, SET_REQUEST),
    CodecError
  )
})

void test("given_a_command_error_with_a_newer_code_when_executing_then_should_report_it_as_unrecognized", async () => {
  const reply = encodeNamed(
    new Map<string, unknown>([
      ["code", new Map<string, unknown>([["unrecognized", 42]])],
      ["message", "later"]
    ])
  )
  await assert.rejects(
    executeManaged(answering(reply), managed, KvSetCommand, SET_REQUEST),
    (error: unknown) =>
      error instanceof ProtocolError &&
      error.message === "Unrecognized(42): later" &&
      error.resultCode === 42
  )
})
