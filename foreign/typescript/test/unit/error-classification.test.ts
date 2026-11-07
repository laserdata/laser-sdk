import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import path from "node:path"
import { test } from "node:test"
import { isRetryable } from "../../src/client/error-classify.js"
import {
  code,
  isAmbiguousMutation,
  isBudgetExceeded,
  isFenceViolation,
  isLeaseLost,
  isNoCapableAgent,
  isNotFound,
  isNotLeader,
  isPermissionDenied,
  isQuarantined,
  isStale,
  isStreamOrTopicNotFound,
  isUnavailable,
  isUnsupported,
  isVersionConflict,
  isVersionSkew
} from "../../src/client/error-classify.js"
import {
  AmbiguousMutationError,
  AuthzExecutionError,
  BudgetExceededError,
  CancelledError,
  CodecError,
  ConfigError,
  ConsumerGroupSetupError,
  FenceViolationError,
  FilterExecutionError,
  FilterFaultError,
  FilterOversizedRecordError,
  ForkExecutionError,
  GraphExecutionError,
  HandlerConfigError,
  HandlerError,
  IdError,
  IntegrityError,
  InvalidError,
  KvExecutionError,
  type LaserError,
  NoCapableAgentError,
  NoInboxError,
  NoRespondTopicError,
  NoStreamError,
  PolicyBlockedError,
  PolicyDeferredError,
  PresenceConflictError,
  ProtocolError,
  ProvenanceError,
  PublishFailedError,
  QuarantinedError,
  QueryExecutionError,
  RejectedError,
  RoutePrincipalMismatchError,
  SessionError,
  SignatureError,
  StateStoreError,
  StepUpRequiredError,
  TimeoutError,
  TransportError,
  TypedDecodeError,
  UnsupportedError
} from "../../src/client/errors.js"
import { decodeManagedReply } from "../../src/client/managed.js"
import { decodeAuthzError } from "../../src/wire/authz.js"
import { decodeOne, encodeOne } from "../../src/wire/cbor.js"
import { decodeFilterError } from "../../src/wire/filter.js"
import { decodeForkError } from "../../src/wire/fork.js"
import { decodeGraphError } from "../../src/wire/graph.js"
import { decodeKvError } from "../../src/wire/kv.js"
import { decodeQueryError } from "../../src/wire/query.js"
import { decodeSessionError } from "../../src/wire/session.js"

interface ClassificationCase {
  readonly name: string
  readonly failure: Readonly<Record<string, unknown>>
  readonly code: string
  readonly flags: readonly string[]
}

interface ClassificationCorpus {
  readonly classifiers: readonly string[]
  readonly cases: readonly ClassificationCase[]
}

const CORPUS = path.resolve(process.cwd(), "../../wire/fixtures/error_classification_cases.json")

const CLASSIFIERS: Readonly<Record<string, (error: unknown) => boolean>> = {
  retryable: isRetryable,
  unavailable: isUnavailable,
  unsupported: isUnsupported,
  not_found: isNotFound,
  version_skew: isVersionSkew,
  permission_denied: isPermissionDenied,
  stale: isStale,
  version_conflict: isVersionConflict,
  ambiguous_mutation: isAmbiguousMutation,
  not_leader: isNotLeader,
  lease_lost: isLeaseLost,
  fence_violation: isFenceViolation,
  budget_exceeded: isBudgetExceeded,
  quarantined: isQuarantined,
  no_capable_agent: isNoCapableAgent,
  stream_or_topic_not_found: isStreamOrTopicNotFound
}

void test("given_the_shared_classification_corpus_when_classified_then_should_answer_every_case", async () => {
  const corpus = JSON.parse(await readFile(CORPUS, "utf8")) as ClassificationCorpus
  const mismatches: string[] = []
  for (const entry of corpus.cases) {
    for (const [form, error] of failuresOf(entry.failure)) {
      const name = `${entry.name}${form}`
      const result = code(error)
      const spelled = result.kind === "known" ? result.name : `Unrecognized(${String(result.code)})`
      if (spelled !== entry.code)
        mismatches.push(`${name}: code ${spelled}, expected ${entry.code}`)
      for (const classifier of corpus.classifiers) {
        const classify = CLASSIFIERS[classifier]
        assert.ok(classify !== undefined, `the corpus names an unknown classifier ${classifier}`)
        const expected = entry.flags.includes(classifier)
        if (classify(error) !== expected)
          mismatches.push(`${name}: ${classifier} should be ${String(expected)}`)
      }
    }
  }
  assert.deepEqual(mismatches, [])
})

// Every form a client meets the failure in: an Apache Iggy server code also
// arrives as the raw client error, outside any SDK wrapper.
function failuresOf(failure: Readonly<Record<string, unknown>>): readonly [string, unknown][] {
  const forms: [string, unknown][] = [["", failureOf(failure)]]
  const serverCode = failure["transport"]
  if (typeof serverCode === "number") forms.push([" (raw)", serverError(serverCode)])
  return forms
}

function failureOf(failure: Readonly<Record<string, unknown>>): unknown {
  const [entry] = Object.entries(failure)
  assert.ok(entry !== undefined, "a failure names one kind")
  const [kind, detail] = entry
  switch (kind) {
    case "query":
      return new QueryExecutionError("refused", decodeQueryError(wire(detail), "corpus"))
    case "kv":
      return new KvExecutionError("refused", decodeKvError(wire(detail), "corpus"))
    case "fork":
      return new ForkExecutionError("refused", decodeForkError(wire(detail), "corpus"))
    case "graph":
      return new GraphExecutionError("refused", decodeGraphError(wire(detail), "corpus"))
    case "session":
      return new SessionError("refused", decodeSessionError(wire(detail), "corpus"))
    case "authz":
      return new AuthzExecutionError("refused", decodeAuthzError(wire(detail), "corpus"))
    case "filter":
      return new FilterExecutionError("refused", decodeFilterError(wire(detail), "corpus"))
    case "command":
      return commandFailure(detail)
    case "transport":
      return new TransportError("server refused", false, { cause: serverError(detail as number) })
    case "local":
      return localFailure(detail as string)
    case "publish_failed":
      return new PublishFailedError("orders", "placed", [], [], nested(detail))
    case "consumer_group_setup":
      return new ConsumerGroupSetupError(
        1,
        "billing",
        {
          streamId: 1,
          streamCreatedAtMicros: 1n,
          topicId: 1,
          topicCreatedAtMicros: 1n,
          groupId: 1n
        },
        nested(detail)
      )
    case "typed_decode":
      return detail === null
        ? new TypedDecodeError(
            "record at 0:7 does not decode",
            { partitionId: 0, offset: 7n },
            new CodecError("not json", "typed", "decode")
          )
        : new TypedDecodeError("typed read failed to poll", undefined, nested(detail) as LaserError)
    default:
      throw new Error(`the corpus names an unknown failure ${kind}`)
  }
}

function localFailure(kind: string): LaserError {
  switch (kind) {
    case "link_lost":
      return new TransportError("link lost", true)
    case "no_stream":
      return new NoStreamError("no default stream set")
    case "no_respond_topic":
      return new NoRespondTopicError()
    case "config":
      return new ConfigError("refused")
    case "handler_config":
      return new HandlerConfigError("refused")
    case "handler":
      return new HandlerError("refused")
    case "rejected":
      return new RejectedError("refused")
    case "timeout":
      return new TimeoutError("a reply")
    case "ambiguous_mutation":
      return new AmbiguousMutationError("refused")
    case "unsupported":
      return new UnsupportedError("refused", { surface: "kv" })
    case "invalid":
      return new InvalidError("refused")
    case "codec":
      return new CodecError("refused", "kv", "decode")
    case "protocol":
      return new ProtocolError("refused")
    case "state_store":
      return new StateStoreError("refused")
    case "integrity":
      return new IntegrityError("blob://refused")
    case "signature":
      return new SignatureError("refused")
    case "policy_blocked":
      return new PolicyBlockedError("refused")
    case "step_up_required":
      return new StepUpRequiredError("storage:rotate")
    case "policy_deferred":
      return new PolicyDeferredError("refused")
    case "no_capable_agent":
      return new NoCapableAgentError("triage")
    case "no_inbox":
      return new NoInboxError("planner")
    case "presence_conflict":
      return new PresenceConflictError("planner", "writer")
    case "route_principal_mismatch":
      return new RoutePrincipalMismatchError("planner", 7, 8)
    case "fence_violation":
      return new FenceViolationError(3n, 5n)
    case "budget_exceeded":
      return new BudgetExceededError(10n, 12n)
    case "cancelled":
      return new CancelledError("run cancelled", { run: "refused" })
    case "quarantined":
      return new QuarantinedError("planner")
    case "filter_fault":
      return new FilterFaultError(1, 7n, "malformed")
    case "filter_oversized_record":
      return new FilterOversizedRecordError(1, 7n)
    case "id":
      return IdError.empty()
    case "provenance":
      return ProvenanceError.missingRequired("gen_ai.conversation.id")
    default:
      throw new Error(`the corpus names an unknown local failure ${kind}`)
  }
}

function nested(detail: unknown): unknown {
  return failureOf(detail as Readonly<Record<string, unknown>>)
}

function wire(detail: unknown): unknown {
  return decodeOne(encodeOne(detail), "corpus")
}

function serverError(errorCode: number): Error {
  return Object.assign(new Error("server refused"), { errorCode })
}

// The surface-neutral reply a server answers in place of the surface's own.
function commandFailure(detail: unknown): unknown {
  try {
    decodeManagedReply(() => {
      throw new CodecError("not this surface's reply", "corpus", "decode")
    }, encodeOne(detail))
  } catch (error) {
    return error
  }
  throw new Error("a command error reply must not decode as a typed reply")
}
