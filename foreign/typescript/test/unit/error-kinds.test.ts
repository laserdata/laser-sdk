import assert from "node:assert/strict"
import { test } from "node:test"
import {
  AgentWorkflowExecutionError,
  AuthzExecutionError,
  BudgetExceededError,
  CancelledError,
  CodecError,
  ConfigError,
  ForkExecutionError,
  GraphExecutionError,
  HandlerConfigError,
  HandlerError,
  IntegrityError,
  InvalidError,
  KvExecutionError,
  LaserError,
  NoStreamError,
  PolicyBlockedError,
  PolicyDeferredError,
  PresenceConflictError,
  ProtocolError,
  QueryExecutionError,
  RejectedError,
  RoutingError,
  SignatureError,
  StateStoreError,
  StepUpRequiredError,
  TimeoutError,
  TransportError,
  TypedDecodeError,
  UnsupportedError,
  assertNever
} from "../../src/client/errors.js"

// Every error carries a discriminating `kind`, so a caller classifies a failure
// without matching on message text.
const MESSAGE_ONLY = [
  [ConfigError, "config"],
  [NoStreamError, "no-stream"],
  [TimeoutError, "timeout"],
  [CancelledError, "cancelled"],
  [UnsupportedError, "unsupported"],
  [SignatureError, "signature"],
  [HandlerError, "handler"],
  [HandlerConfigError, "handler-config"],
  [StateStoreError, "state-store"],
  [PolicyBlockedError, "policy-blocked"],
  [PolicyDeferredError, "policy-deferred"],
  [RejectedError, "rejected"]
] as const

void test("given_every_message_only_error_when_constructed_then_should_carry_its_kind_and_name", () => {
  for (const [Kind, kind] of MESSAGE_ONLY) {
    const error = new Kind("boom")
    assert.equal(error.kind, kind, `${Kind.name} kind`)
    assert.equal(error.name, Kind.name)
    assert.ok(error instanceof LaserError)
    assert.equal(error.message, "boom")
  }
})

void test("given_a_surface_scoped_execution_error_when_constructed_then_should_keep_the_detail", () => {
  const surfaces = [
    QueryExecutionError,
    KvExecutionError,
    ForkExecutionError,
    GraphExecutionError,
    AuthzExecutionError,
    AgentWorkflowExecutionError
  ] as const
  for (const Kind of surfaces) {
    const error = new Kind("rejected by the engine", { code: 7 })
    assert.deepEqual(error.detail, { code: 7 }, `${Kind.name} detail`)
    assert.ok(error instanceof LaserError)
  }
})

void test("given_a_codec_error_when_constructed_then_should_name_the_surface_and_operation", () => {
  const error = new CodecError("body is not json", "kv", "set")
  assert.equal(error.kind, "codec")
  assert.equal(error.surface, "kv")
  assert.equal(error.operation, "set")
})

void test("given_a_typed_decode_error_when_constructed_then_should_carry_the_log_position", () => {
  const position = { partitionId: 2, offset: 41n }
  const error = new TypedDecodeError("order fields are invalid", position)
  assert.equal(error.kind, "typed-decode")
  assert.deepEqual(error.position, position)
  assert.equal(new TypedDecodeError("no position", undefined).position, undefined)
})

void test("given_a_protocol_error_when_constructed_then_should_expose_the_result_and_command_codes", () => {
  const error = new ProtocolError("unexpected reply", { resultCode: 3, commandCode: 1_000_301 })
  assert.equal(error.kind, "protocol")
  assert.equal(error.resultCode, 3)
  assert.equal(error.commandCode, 1_000_301)
})

void test("given_a_transport_error_when_constructed_then_should_report_whether_a_retry_is_worthwhile", () => {
  assert.equal(new TransportError("socket closed", true).retryable, true)
  assert.equal(new TransportError("bad credentials", false).retryable, false)
})

void test("given_a_routing_error_when_constructed_then_should_carry_the_structured_reason", () => {
  const error = new RoutingError("no capable agent", { kind: "noCapableAgent", skill: "triage" })
  assert.equal(error.kind, "routing")
  assert.deepEqual(error.reason, { kind: "noCapableAgent", skill: "triage" })
})

void test("given_a_presence_conflict_when_constructed_then_should_name_both_agents", () => {
  const error = new PresenceConflictError("triage", "billing")
  assert.equal(error.kind, "presence-conflict")
  assert.equal(error.advertised, "triage")
  assert.equal(error.requested, "billing")
})

void test("given_the_self_describing_errors_when_constructed_then_should_build_their_own_message", () => {
  const integrity = new IntegrityError("blob://abc")
  assert.equal(integrity.reference, "blob://abc")
  assert.match(integrity.message, /blob:\/\/abc/u)

  const stepUp = new StepUpRequiredError("refund:approve")
  assert.equal(stepUp.scope, "refund:approve")
  assert.match(stepUp.message, /refund:approve/u)

  const budget = new BudgetExceededError(100n, 140n)
  assert.equal(budget.ceiling, 100n)
  assert.equal(budget.spent, 140n)
  assert.match(budget.message, /spent 140, ceiling 100/u)
})

void test("given_an_unreachable_variant_when_asserted_then_should_throw_invalid_with_the_value", () => {
  assert.throws(
    () => assertNever("surprise" as never),
    (error: unknown) => error instanceof InvalidError && error.context?.["value"] === "surprise"
  )
})
