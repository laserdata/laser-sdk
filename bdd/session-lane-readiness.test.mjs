import assert from "node:assert/strict";
import { test } from "node:test";
import { setTimeout as wait } from "node:timers/promises";
import { TransportError } from "../foreign/typescript/dist/index.js";
import { INTERNAL_TRANSPORT } from "../foreign/typescript/dist/client/internals.js";
import { waitForPartitions } from "./session-lane-readiness.mjs";

test("given_one_refused_partition_when_waiting_then_should_retry_only_that_partition", async () => {
  const reads = [0, 0];
  const client = fixture((partition) => {
    reads[partition] += 1;
    if (partition === 1 && reads[partition] === 1)
      throw new TransportError("partition is materializing", true);
  });
  await waitForPartitions(client, "stream");
  assert.deepEqual(reads, [1, 2]);
});

test("given_a_partition_that_never_answers_in_time_when_waiting_then_should_fail_at_the_deadline", async () => {
  let answered = false;
  const client = fixture(async () => {
    await wait(100);
    answered = true;
  });
  await assert.rejects(waitForPartitions(client, "stream", 10), /readiness/i);
  assert.equal(answered, false);
});

test("given_partitions_that_answer_after_six_seconds_when_waiting_then_should_keep_probing_until_they_answer", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const started = Date.now();
  const client = fixture(() => {
    if (Date.now() - started < 6000)
      throw new TransportError("partition is materializing", true);
  });
  await settled(context, waitForPartitions(client, "stream"));
  assert.ok(Date.now() - started >= 6000);
});

test("given_one_probe_that_never_answers_when_waiting_then_should_probe_that_partition_again", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const reads = [0, 0];
  const client = fixture((partition) => {
    reads[partition] += 1;
    if (partition === 1 && reads[partition] === 1) return new Promise(() => {});
  });
  await settled(context, waitForPartitions(client, "stream"));
  assert.deepEqual(reads, [1, 2]);
});

test("given_a_probe_that_answers_after_a_second_and_a_half_when_waiting_then_should_give_it_longer_and_succeed", async (context) => {
  context.mock.timers.enable({ apis: ["setTimeout", "Date"] });
  const client = fixture(
    () => new Promise((resolve) => setTimeout(resolve, 1500)),
  );
  await settled(context, waitForPartitions(client, "stream"));
});

test("given_a_permanent_error_when_waiting_for_partitions_then_should_report_it", async () => {
  const refused = new TransportError("forbidden", false);
  await assert.rejects(
    waitForPartitions(
      fixture(() => {
        throw refused;
      }),
      "stream",
    ),
    (error) => error === refused,
  );
});

test("given_a_new_session_topic_missing_from_metadata_when_waiting_then_should_probe_it_before_returning", async () => {
  const client = fixture(() => {});
  const sessions = client.client.topic.list;
  let listings = 0;
  client.client.topic.list = async () =>
    ++listings === 1
      ? [{ name: "agent.heartbeats", partitionsCount: 1 }]
      : sessions();
  await waitForPartitions(client, "stream");
  assert.equal(listings, 2);
});

// Advances the mocked clock until `waiting` settles, then returns its outcome.
async function settled(context, waiting) {
  let done = false;
  const outcome = waiting.finally(() => {
    done = true;
  });
  outcome.catch(() => undefined);
  while (!done) {
    context.mock.timers.tick(100);
    await new Promise((resolve) => setImmediate(resolve));
  }
  return outcome;
}

function fixture(probe) {
  const source = { name: "agent.sessions", partitionsCount: 2 };
  return {
    client: { topic: { list: async () => [source] } },
    [INTERNAL_TRANSPORT]: () => ({
      pollMessages: async (_stream, _topic, target) => {
        await probe(target.partitionId);
        return [];
      },
    }),
  };
}
