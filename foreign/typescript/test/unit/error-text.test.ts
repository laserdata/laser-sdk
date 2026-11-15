import assert from "node:assert/strict"
import { test } from "node:test"
import { ADVERTISED_INBOX_ROUTE, resolveInboxRoute } from "../../src/agent/router.js"
import { isStreamOrTopicNotFound } from "../../src/client/error-classify.js"
import {
  NoInboxError,
  NoStreamError,
  PublishFailedError,
  TransportError
} from "../../src/client/errors.js"
import { Laser } from "../../src/client/laser.js"
import { type IggyClient, acceptStage, missingTopic } from "../../src/iggy/apache-iggy.js"
import { AgentId } from "../../src/types/ids.js"

const NO_STREAM_HINT =
  "pass a stream, or set a default stream when connecting (stream option) or with withDefaultStream"
const MISSING_TOPIC_HINT =
  "the topic does not exist: create the agent topics with bootstrap, or a memory topic with memoryTopic"
const NO_INBOX_HINT =
  'start the agent with capabilities so it advertises its inbox, or give the workflow a fixed inbox topic (inboxRoute({ kind: "fixed", topic }))'

void test("given_no_default_stream_when_a_shortcut_needs_one_then_should_name_the_next_call", async () => {
  await using laser = await Laser.fromClient({
    clientProvider: () => Promise.resolve({ protocol: "vsr" }),
    destroy: () => Promise.resolve()
  } as unknown as IggyClient)

  assert.throws(
    () => laser.topic("readings"),
    (error: unknown) =>
      error instanceof NoStreamError &&
      error.message === `topic() requires a default stream: ${NO_STREAM_HINT}`
  )
  const bare = new NoStreamError()
  assert.deepEqual(
    [bare.kind, bare.name, bare.message],
    ["no-stream", "NoStreamError", `no default stream set: ${NO_STREAM_HINT}`]
  )
})

void test("given_a_publish_to_a_missing_topic_when_it_fails_then_should_name_bootstrap_and_memory_topic", () => {
  const failure = new PublishFailedError(
    "fleet",
    "readings",
    [],
    [],
    missingTopic("fleet", "readings")
  )
  const rewrapped = new PublishFailedError("fleet", "readings", [], [], failure)
  const unrelated = new PublishFailedError(
    "fleet",
    "readings",
    [],
    [],
    new TransportError("connection reset", true)
  )

  assert.equal(
    failure.message,
    `publish failed to fleet/readings: topic \`readings\` on stream \`fleet\` does not exist: ${MISSING_TOPIC_HINT}`
  )
  assert.equal(isStreamOrTopicNotFound(failure), true)
  assert.equal(rewrapped.message.split(MISSING_TOPIC_HINT).length, 2)
  assert.equal(unrelated.message, "publish failed to fleet/readings: connection reset")
})

void test("given_a_laserdata_host_when_the_connection_is_never_accepted_then_should_name_the_access_rule", () => {
  const hint =
    "the deployment admits only addresses its access rules allow: add an access rule for this address"
  assert.equal(
    acceptStage("starter-1.us-west-1.aws.laserdata.cloud"),
    `timed out waiting for the Iggy server to accept the connection: ${hint}`
  )
  assert.equal(
    acceptStage("EU.LASERDATA.COM"),
    `timed out waiting for the Iggy server to accept the connection: ${hint}`
  )
  assert.equal(
    acceptStage("127.0.0.1"),
    "timed out waiting for the Iggy server to accept the connection"
  )
  assert.equal(
    acceptStage("laserdata.cloud.example.org"),
    "timed out waiting for the Iggy server to accept the connection"
  )
})

void test("given_an_agent_without_an_advertised_inbox_when_routed_then_should_name_capabilities_and_the_fixed_route", () => {
  assert.throws(
    () => resolveInboxRoute(ADVERTISED_INBOX_ROUTE, AgentId.new("worker"), undefined),
    (error: unknown) =>
      error instanceof NoInboxError &&
      error.agent === "worker" &&
      error.message === `no inbox advertised for agent \`worker\`: ${NO_INBOX_HINT}`
  )
})
