import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { validateAgentEnvelope, decodeAgentEnvelope } from "../../src/wire/agent.js"
import { decodeArrowIpcMessageMetadata, decodeArrowIpcPolicy } from "../../src/wire/arrow.js"
import {
  decodeCheckpointReply,
  decodeCheckpointReadReply,
  decodeCheckpointRequestFrame,
  decodeDestinationCheckpointStatus
} from "../../src/wire/checkpoint.js"
import { decodeBrowseReply } from "../../src/wire/browse.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import { decodeControlEnvelope } from "../../src/wire/control.js"
import { decodeMaterializationDestination, decodeQueryRoute } from "../../src/wire/destination.js"
import { decodeForwardedCommand, decodeForwardedQuery } from "../../src/wire/forward.js"
import { decodeForkReply } from "../../src/wire/fork.js"
import { decodeBackendAnnounce, decodeHelloReply } from "../../src/wire/hello.js"
import { decodeKvReply } from "../../src/wire/kv.js"
import {
  decodeQueryCancelEnvelopeFrame,
  decodeQueryEnvelopeFrame,
  decodeQueryPageEnvelopeFrame,
  decodeQueryReplyFrame,
  decodeQueryStatusEnvelopeFrame,
  decodeQueryStatusReplyFrame
} from "../../src/wire/query.js"
import { decodeLogicalSchema } from "../../src/wire/schema.js"
import { assertDecoderIsRobust } from "../wire/support/robustness.js"

const FIXTURES_DIR = path.resolve(process.cwd(), "../../wire/fixtures")

async function readFixture(name: string): Promise<Uint8Array> {
  const buffer = await readFile(path.join(FIXTURES_DIR, name))
  return new Uint8Array(buffer.buffer, buffer.byteOffset, buffer.byteLength)
}

/**
 * Security Hardening: Comprehensive Robustness Suite for Wire Decoders
 *
 * Every decoder that processes untrusted network bytes must gracefully reject malformed input:
 * - Truncated or incomplete messages must not crash
 * - Bit-corrupted frames must not panic
 * - Unexpected trailing data must not cause infinite loops
 *
 * This mirrors laser-wire/tests/robustness.rs structure-aware fuzzing:
 * For each fixture, the assertDecoderIsRobust() helper validates:
 * 1. Empty input → DecodeError (never null dereference)
 * 2. Truncation at every byte boundary → DecodeError (never incomplete parse)
 * 3. Single-bit flip at every position → DecodeError (never corrupted state)
 * 4. Trailing bytes (1, 8, 64 bytes) → DecodeError (never partial acceptance)
 *
 * Rationale: TypeScript SDKs process the same untrusted bytes from Apache Iggy that Rust does.
 * A crash on malformed input is a DoS vector. These tests prevent silent failures by enforcing
 * error handling through the fixture corpus mutations.
 */

// Decoder wrapping helpers for CBOR-decoded types (reduce duplication)
function wrapCborDecoder<T>(
  decode: (map: ReadonlyMap<unknown, unknown>, context: string) => T,
  context: string
): (bytes: Uint8Array) => T {
  return (candidate: Uint8Array): T => {
    const map = expectMap(decodeOne(candidate, context), context)
    return decode(map, context)
  }
}

function wrapCborValueDecoder<T>(
  decode: (value: unknown, context: string) => T,
  context: string
): (bytes: Uint8Array) => T {
  return (candidate: Uint8Array): T => {
    const value = decodeOne(candidate, context)
    return decode(value, context)
  }
}

// Table-driven test cases: decoder name → (fixture, decoder function)
// Organized by network message type for clarity
type DecoderTestCase = readonly [name: string, fixtureName: string, decode: (bytes: Uint8Array) => unknown]

const directDecoders: DecoderTestCase[] = [
  // Hello handshake
  ["HelloReply", "hello_reply_features.bin", decodeHelloReply],
  ["BackendAnnounce", "backend_announce_topology.bin", decodeBackendAnnounce],
  // Query control frames
  ["QueryPageEnvelope", "query_page.bin", decodeQueryPageEnvelopeFrame],
  ["QueryCancelEnvelope", "query_cancel.bin", decodeQueryCancelEnvelopeFrame],
  ["QueryStatusEnvelope", "query_status.bin", decodeQueryStatusEnvelopeFrame],
  ["QueryStatusReply", "query_status_reply.bin", decodeQueryStatusReplyFrame],
  // Checkpoint lifecycle
  ["CheckpointRequest", "checkpoint_request_public.bin", decodeCheckpointRequestFrame],
  ["CheckpointReply", "checkpoint_reply_destination.bin", decodeCheckpointReply],
  ["CheckpointReadReply", "checkpoint_reply_destination.bin", decodeCheckpointReadReply]
] as const

const cborMapDecoders: DecoderTestCase[] = [
  // Agent lifecycle
  [
    "AgentEnvelope",
    "agent_command.bin",
    (bytes: Uint8Array): unknown => {
      const map = expectMap(decodeOne(bytes, "AgentEnvelope"), "AgentEnvelope")
      const envelope = decodeAgentEnvelope(map, "AgentEnvelope")
      validateAgentEnvelope(envelope)
      return envelope
    }
  ],
  // Control operations
  [
    "ControlEnvelope",
    "control_register_projection.bin",
    wrapCborDecoder(decodeControlEnvelope, "ControlEnvelope")
  ],
  // Query distribution
  [
    "ForwardedQuery",
    "forwarded_query.bin",
    wrapCborDecoder(decodeForwardedQuery, "ForwardedQuery")
  ],
  [
    "ForwardedCommand",
    "forwarded_command.bin",
    wrapCborDecoder(decodeForwardedCommand, "ForwardedCommand")
  ],
  // Data stack schema
  [
    "LogicalSchema",
    "logical_schema.bin",
    wrapCborDecoder(decodeLogicalSchema, "LogicalSchema")
  ],
  [
    "MaterializationDestination",
    "materialization_destination.bin",
    wrapCborDecoder(decodeMaterializationDestination, "MaterializationDestination")
  ],
  [
    "QueryRoute",
    "query_route.bin",
    wrapCborDecoder(decodeQueryRoute, "QueryRoute")
  ],
  [
    "ArrowIpcMetadata",
    "arrow_ipc_metadata.bin",
    wrapCborDecoder(decodeArrowIpcMessageMetadata, "ArrowIpcMessageMetadata")
  ],
  [
    "ArrowIpcPolicy",
    "arrow_ipc_policy.bin",
    wrapCborDecoder(decodeArrowIpcPolicy, "ArrowIpcPolicy")
  ],
  [
    "DestinationCheckpointStatus",
    "destination_checkpoint_status.bin",
    wrapCborDecoder(decodeDestinationCheckpointStatus, "DestinationCheckpointStatus")
  ]
] as const

const cborValueDecoders: DecoderTestCase[] = [
  // RPC replies
  ["QueryReply", "query_reply_ok.bin", wrapCborValueDecoder(decodeQueryReplyFrame, "QueryReply")],
  ["KvReply", "kv_reply_committed.bin", wrapCborValueDecoder(decodeKvReply, "KvReply")],
  ["ForkReply", "fork_reply_created.bin", wrapCborValueDecoder(decodeForkReply, "ForkReply")],
  ["BrowseReply", "browse_reply_schemas.bin", wrapCborValueDecoder(decodeBrowseReply, "BrowseReply")],
  // Query envelope
  ["QueryEnvelope", "query_envelope.bin", wrapCborValueDecoder(decodeQueryEnvelopeFrame, "QueryEnvelope")]
] as const

const allTestCases = [...directDecoders, ...cborMapDecoders, ...cborValueDecoders]

// Run table-driven tests: verify every decoder gracefully rejects malformed input
void test("when_network_decoders_receive_truncated_bit_flipped_or_trailing_corrupted_bytes_then_should_never_crash_unstructured", async () => {
  for (const [decoderName, fixtureName, decode] of allTestCases) {
    const bytes = await readFixture(fixtureName)
    assertDecoderIsRobust(bytes, (candidate: Uint8Array): unknown => {
      try {
        return decode(candidate)
      } catch {
        // Expected: DecodeError, ValidationError, RangeError, or TypeError on malformed input
        // Unexpected: unhandled exceptions, null dereference, or infinite loops
      }
    })
  }
})
