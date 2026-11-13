import assert from "node:assert/strict";
import { After, Given, Then, When } from "@cucumber/cucumber";
import {
  Agent,
  AgentId,
  AgentTopic,
  type AgentHandle,
  type WarmDedupPolicy,
} from "@laserdata/laser-sdk";
import type { LaserWorld } from "../world.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

// How far ahead a resend's deadline lies, far enough that it never expires
// in the scenario.
const RESEND_DEADLINE_MICROS = 60_000_000n;

/** The worker of the warm dedup scenarios: its id and policy, the running
 * incarnation, the bodies it handled, and the deadline each key was sent with. */
interface Warm {
  agent: AgentId;
  policy: WarmDedupPolicy;
  worker: AgentHandle | undefined;
  readonly handled: string[];
  readonly deadlines: Map<string, bigint>;
}

const scenarios = new WeakMap<LaserWorld, Warm>();

function warm(world: LaserWorld): Warm {
  const state = scenarios.get(world);
  if (state === undefined) throw new Error("scenario has no warm-dedup worker");
  return state;
}

After(async function (this: LaserWorld) {
  const state = scenarios.get(this);
  await state?.worker?.shutdown();
  scenarios.delete(this);
});

Given(
  /^a warm-dedup worker "([^"]+)" with policy "([^"]+)"$/,
  async function (this: LaserWorld, name: string, policy: string) {
    scenarios.set(this, {
      agent: AgentId.new(name),
      policy: policy as WarmDedupPolicy,
      worker: undefined,
      handled: [],
      deadlines: new Map(),
    });
    await start(this);
  },
);

When(
  /^the worker is sent a command under key "([^"]+)" due in (\d+) seconds$/,
  async function (this: LaserWorld, key: string, seconds: string) {
    await send(this, key, nowMicros() + BigInt(seconds) * 1_000_000n);
  },
);

When(
  /^the worker is sent a command under key "([^"]+)" already past its deadline$/,
  async function (this: LaserWorld, key: string) {
    await send(this, key, nowMicros() - 1_000_000n);
  },
);

When(
  /^the worker is sent a command under key "([^"]+)" without a deadline$/,
  async function (this: LaserWorld, key: string) {
    await send(this, key, undefined);
  },
);

Then(
  /^the worker handled the keys "([^"]+)"$/,
  async function (this: LaserWorld, keys: string) {
    await assertHandled(this, keys);
  },
);

When(
  /^the worker stops and the deadline of key "([^"]+)" passes$/,
  async function (this: LaserWorld, key: string) {
    const state = warm(this);
    await state.worker?.shutdown();
    state.worker = undefined;
    const deadline = state.deadlines.get(key);
    assert.ok(deadline !== undefined, `key ${key} was sent with a deadline`);
    for (let now = nowMicros(); now <= deadline; now = nowMicros()) {
      await new Promise((resolve) =>
        setTimeout(resolve, Number((deadline + 1_000n - now) / 1_000n)),
      );
    }
  },
);

When(
  /^resends arrive under the keys "([^"]+)" and then "([^"]+)"$/,
  async function (this: LaserWorld, keys: string, last: string) {
    const due = nowMicros() + RESEND_DEADLINE_MICROS;
    for (const key of [...keys.split(", "), last]) await send(this, key, due);
  },
);

When("the worker restarts", async function (this: LaserWorld) {
  warm(this).handled.length = 0;
  await start(this);
});

Then(
  /^the restarted worker handled the keys "([^"]+)"$/,
  async function (this: LaserWorld, keys: string) {
    await assertHandled(this, keys);
    const state = warm(this);
    await state.worker?.shutdown();
    state.worker = undefined;
  },
);

async function start(world: LaserWorld): Promise<void> {
  const state = warm(world);
  const worker = Agent.builder()
    .id(state.agent)
    .listenOn(AgentTopic.Sessions)
    .warmDedup()
    .warmDedupPolicy(state.policy)
    .handler({
      handle: (message) => {
        state.handled.push(decoder.decode(message.payload));
        return Promise.resolve();
      },
    })
    .build()
    .spawn(world.requireLaser());
  await worker.ready();
  state.worker = worker;
}

async function send(
  world: LaserWorld,
  key: string,
  deadline: bigint | undefined,
): Promise<void> {
  const state = warm(world);
  await world.requireLaser().sendAgent(AgentTopic.Sessions, encoder.encode(key), {
    conversationId: world.requireConversation(),
    agent: AgentId.new("warm-sender"),
    targetAgentId: state.agent,
    idempotencyKey: key,
    ...(deadline !== undefined ? { deadlineAtMicros: deadline } : {}),
  });
  if (deadline !== undefined) state.deadlines.set(key, deadline);
}

// Waits until the worker handled the last listed key, then compares the
// whole list. Every record rides one partition and the worker is serial, so
// the records before the last one were decided before it.
async function assertHandled(world: LaserWorld, keys: string): Promise<void> {
  const state = warm(world);
  const expected = keys.split(", ");
  const last = expected.at(-1);
  for (let attempt = 0; attempt < 200; attempt += 1) {
    if (last !== undefined && state.handled.includes(last)) break;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  assert.deepEqual(state.handled, expected);
}

function nowMicros(): bigint {
  return BigInt(Date.now()) * 1_000n;
}
