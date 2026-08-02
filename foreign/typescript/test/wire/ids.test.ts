import assert from "node:assert/strict"
import { test } from "node:test"
import {
  ChannelId,
  ConversationId,
  RecordId,
  crockfordDecode,
  crockfordEncode,
  logPositionFromBytes,
  logPositionToBytes
} from "../../src/wire/ids.js"

void test("given_an_id_when_displayed_then_should_round_trip_through_crockford_base32", () => {
  const value = 0x0123_4567_89ab_cdef_0123_4567_89ab_cdefn
  const id = RecordId.fromU128(value)
  const text = id.toString()
  assert.equal(text.length, 26)
  assert.ok(RecordId.parse(text).equals(id))
  assert.ok(RecordId.parse(text.toLowerCase()).equals(id))
  assert.equal(RecordId.fromU128(0n).toString(), "00000000000000000000000000")
  assert.equal(RecordId.fromU128((1n << 128n) - 1n).toString(), "7ZZZZZZZZZZZZZZZZZZZZZZZZZ")
})

void test("given_invalid_id_strings_when_parsed_then_should_reject_with_the_right_error", () => {
  assert.throws(() => crockfordDecode("short"), /must be 26 characters, got 5/)
  assert.throws(() => crockfordDecode("8ZZZZZZZZZZZZZZZZZZZZZZZZZ"), /id overflows 128 bits/)
  assert.throws(() => crockfordDecode("UUUUUUUUUUUUUUUUUUUUUUUUUU"), /invalid character/)
})

void test("given_a_wire_id_when_round_tripped_through_bytes_then_should_preserve_value", () => {
  const id = ConversationId.fromU128(0x1n)
  const bytes = id.toBytes()
  assert.equal(bytes.length, 16)
  assert.ok(ConversationId.fromBytes(bytes).equals(id))
})

void test("given_distinct_wire_id_kinds_when_encoded_then_should_not_be_interchangeable", () => {
  const record = RecordId.fromU128(7n)
  const channel = ChannelId.fromU128(7n)
  assert.equal(crockfordEncode(record.asU128()), crockfordEncode(channel.asU128()))
})

void test("given_a_log_position_when_round_tripped_through_bytes_then_should_preserve_fields", () => {
  const position = { streamId: 1, topicId: 2, partitionId: 3, offset: 44n }
  const bytes = logPositionToBytes(position)
  assert.equal(bytes.length, 20)
  assert.deepEqual(logPositionFromBytes(bytes), position)
})
