import assert from "node:assert/strict"
import { test } from "node:test"
import { IdError, InvalidError } from "../../src/client/errors.js"
import {
  AgentId,
  ConsumerGroupName,
  ConversationId,
  IntentId,
  MintUlid,
  PrincipalId,
  parseMessageId,
  messageIdToString
} from "../../src/types/ids.js"
import {
  RecordId,
  logPositionFromBytes,
  logPositionToBytes,
  newLogPosition
} from "../../src/wire/ids.js"

void test("given_a_conversation_id_when_round_tripped_through_a_string_then_should_be_equal", () => {
  const id = ConversationId.new()
  const parsed = ConversationId.parse(id.toString())
  assert.ok(parsed.equals(id))
})

void test("given_seeds_when_deriving_conversation_ids_then_should_be_stable_and_distinct", () => {
  assert.ok(ConversationId.derive("user-1").equals(ConversationId.derive("user-1")))
  assert.ok(!ConversationId.derive("user-1").equals(ConversationId.derive("user-2")))
})

void test("given_the_rust_golden_seed_when_derived_then_should_match_the_pinned_id", () => {
  assert.equal(ConversationId.derive("user-1").toString(), "6X4VM88293CP9BFK3H58TFMMS7")
})

void test("given_an_invalid_string_when_parsing_a_conversation_id_then_should_error", () => {
  assert.throws(() => ConversationId.parse("not-a-ulid"), {
    name: "IdError",
    message: "invalid ULID `not-a-ulid`"
  })
})

void test("given_a_fresh_intent_id_when_round_tripped_then_should_be_equal", () => {
  const id = IntentId.new()
  assert.ok(IntentId.parse(id.toString()).equals(id))
})

void test("given_valid_and_invalid_agent_id_strings_when_constructed_then_should_accept_or_reject", () => {
  assert.equal(AgentId.new("executor-v1").asStr(), "executor-v1")
  assert.equal(AgentId.new("planner@acme.example").asStr(), "planner@acme.example")
  assert.equal(AgentId.new("team/planner one").asStr(), "team/planner one")
  assert.throws(
    () => AgentId.new(""),
    (error: unknown) => {
      return error instanceof IdError && error.kind === "id"
    }
  )
  assert.throws(() => AgentId.new(""), { message: "identifier must not be empty" })
  assert.throws(() => AgentId.new("bad\u0000name"), {
    message: "identifier contains invalid character `\u0000`"
  })
  assert.throws(() => AgentId.new("bad\tname"), {
    message: "identifier contains invalid character `\t`"
  })
  const tooLong = "x".repeat(256)
  assert.throws(() => AgentId.new(tooLong), { message: "identifier length 256B exceeds max 255B" })
})

void test("given_an_agent_id_when_used_for_a_consumer_group_then_should_share_its_spelling", () => {
  const agent = AgentId.new("planner")
  assert.equal(ConsumerGroupName.forAgent(agent).asStr(), "planner")
})

void test("given_a_principal_id_when_constructed_then_should_round_trip_the_raw_value", () => {
  assert.equal(PrincipalId.new(42).get(), 42)
  assert.throws(() => PrincipalId.new(-1), InvalidError)
  assert.throws(() => PrincipalId.new(0x1_0000_0000), InvalidError)
  assert.throws(() => PrincipalId.new(1.5), InvalidError)
})

void test("given_a_message_id_string_when_parsed_then_should_round_trip", () => {
  const id = parseMessageId("3:100")
  assert.deepEqual(id, { partitionId: 3, offset: 100n })
  assert.equal(messageIdToString(id), "3:100")
})

void test("given_a_malformed_message_id_string_when_parsed_then_should_error", () => {
  assert.throws(() => parseMessageId("nope"), {
    message: "invalid message id `nope`, expected `<partition_id>:<offset>`"
  })
  assert.throws(() => parseMessageId("01:5"), IdError)
  assert.throws(() => parseMessageId("1:-5"), IdError)
})

void test("given_a_message_id_past_the_wire_widths_when_parsed_then_should_error", () => {
  assert.deepEqual(parseMessageId("4294967295:18446744073709551615"), {
    partitionId: 4_294_967_295,
    offset: 18_446_744_073_709_551_615n
  })
  assert.throws(() => parseMessageId("4294967296:0"), IdError)
  assert.throws(() => parseMessageId("0:18446744073709551616"), IdError)
})

void test("given_an_agent_id_when_taken_to_the_wire_then_should_be_the_same_string", () => {
  const planner = AgentId.new("planner")
  assert.equal(planner.wireId(), "planner")
  assert.notEqual(planner.wireId(), AgentId.new("executor").wireId())
})

void test("given_a_wire_id_type_when_minted_then_should_be_distinct_ulids_from_the_source", () => {
  assert.ok(!MintUlid.mint(RecordId).equals(MintUlid.mint(RecordId)))
  const fixed = {
    nowMilliseconds: () => 1,
    fillRandom: (bytes: Uint8Array) => {
      bytes.fill(0)
    }
  }
  assert.equal(MintUlid.mint(RecordId, fixed).asU128(), 1n << 80n)
})

void test("given_log_position_fields_when_built_then_should_round_trip_and_refuse_out_of_range_values", () => {
  const position = newLogPosition(1, 2, 3, 4n)
  assert.deepEqual(logPositionFromBytes(logPositionToBytes(position)), position)
  assert.throws(() => newLogPosition(-1, 2, 3, 4n), InvalidError)
  assert.throws(() => newLogPosition(1, 0x1_0000_0000, 3, 4n), InvalidError)
  assert.throws(() => newLogPosition(1, 2, 3, -1n), InvalidError)
})
