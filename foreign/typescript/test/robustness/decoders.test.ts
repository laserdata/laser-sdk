import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { decodeArrowIpcMessageMetadata, decodeArrowIpcPolicy } from "../../src/wire/arrow.js"
import {
  decodeCheckpointRequestFrame,
  decodeDestinationCheckpointStatus
} from "../../src/wire/checkpoint.js"
import { decodeOne, expectMap } from "../../src/wire/cbor.js"
import { decodeMaterializationDestination, decodeQueryRoute } from "../../src/wire/destination.js"
import { decodeBackendAnnounce, decodeHelloReply } from "../../src/wire/hello.js"
import {
  decodeQueryCancelEnvelopeFrame,
  decodeQueryPageEnvelopeFrame,
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

void test("given_truncated_bit_flipped_or_trailing_corrupted_bytes_when_decoding_any_cbor_value_then_should_never_crash_unstructured", async () => {
  const bytes = await readFixture("backend_announce.bin")
  assertDecoderIsRobust(bytes, (candidate) => decodeOne(candidate, "robustness"))
})

void test("given_truncated_bit_flipped_or_trailing_corrupted_bytes_when_decoding_a_backend_announce_then_should_never_crash_unstructured", async () => {
  const bytes = await readFixture("backend_announce_topology.bin")
  assertDecoderIsRobust(bytes, decodeBackendAnnounce)
})

void test("given_truncated_bit_flipped_or_trailing_corrupted_bytes_when_decoding_a_hello_reply_then_should_never_crash_unstructured", async () => {
  const bytes = await readFixture("hello_reply_features.bin")
  assertDecoderIsRobust(bytes, decodeHelloReply)
})

void test("data stack decoders reject malformed fixture mutations without unstructured failures", async () => {
  const mapDecoder =
    <T>(context: string, decode: (map: ReadonlyMap<unknown, unknown>, context: string) => T) =>
    (bytes: Uint8Array): T =>
      decode(expectMap(decodeOne(bytes, context), context), context)

  const cases = [
    ["logical_schema.bin", mapDecoder("LogicalSchema", decodeLogicalSchema)],
    [
      "materialization_destination.bin",
      mapDecoder("MaterializationDestination", decodeMaterializationDestination)
    ],
    ["query_route.bin", mapDecoder("QueryRoute", decodeQueryRoute)],
    [
      "arrow_ipc_metadata.bin",
      mapDecoder("ArrowIpcMessageMetadata", decodeArrowIpcMessageMetadata)
    ],
    ["arrow_ipc_policy.bin", mapDecoder("ArrowIpcPolicy", decodeArrowIpcPolicy)],
    [
      "destination_checkpoint_status.bin",
      mapDecoder("DestinationCheckpointStatus", decodeDestinationCheckpointStatus)
    ],
    ["checkpoint_request_public.bin", decodeCheckpointRequestFrame],
    ["query_page.bin", decodeQueryPageEnvelopeFrame],
    ["query_cancel.bin", decodeQueryCancelEnvelopeFrame],
    ["query_status.bin", decodeQueryStatusEnvelopeFrame],
    ["query_status_reply.bin", decodeQueryStatusReplyFrame]
  ] as const

  for (const [name, decode] of cases) {
    assertDecoderIsRobust(await readFixture(name), decode)
  }
})
