import assert from "node:assert/strict";
import { After, Given, Then, When } from "@cucumber/cucumber";
import {
  Agent,
  AgentId,
  AgentTopic,
  CorrelationId,
  ConversationId,
  RecordId,
  TopicRetention,
  UnsupportedError,
  code,
  eventEnvelope,
  isUnavailable,
  PresenceConflictError,
  requiring,
  unmetRequirements,
  type AgentHandle,
} from "@laserdata/laser-sdk";
import { wire } from "@laserdata/laser-sdk/full";
import { eventual } from "../support/eventual.js";
import type { LaserWorld } from "../world.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

Given("a running data platform", function (this: LaserWorld) {
  assert.ok(this.endpoint.length > 0);
});

Given("a fresh stream", async function (this: LaserWorld) {
  await this.connect();
});

Given(
  /^a fresh stream bootstrapped with (\d+) partitions$/,
  async function (this: LaserWorld, partitions: string) {
    await this.connect();
    await this.requireLaser().bootstrap(
      Number(partitions),
      TopicRetention.expireAfter(86_400_000),
    );
    const context = this.requireLaser().context(ConversationId.new());
    await eventual(async () => {
      try {
        await context.fetch([AgentTopic.Sessions], 1);
        return true;
      } catch (error) {
        if (isUnavailable(error)) return undefined;
        throw error;
      }
    }, "session partitions are readable");
  },
);

Given("a new conversation", function (this: LaserWorld) {
  this.newConversation();
});

When(
  /^I send agent commands "([^"]+)", "([^"]+)", "([^"]+)"$/,
  async function (
    this: LaserWorld,
    first: string,
    second: string,
    third: string,
  ) {
    for (const body of [first, second, third]) await sendCommand(this, body);
  },
);

When(
  /^I send an agent command "([^"]+)" with agent "([^"]+)" and idempotency key "([^"]+)"$/,
  async function (this: LaserWorld, body: string, agent: string, key: string) {
    await sendCommand(this, body, agent, { idempotencyKey: key });
  },
);

When(
  /^I send an agent command "([^"]+)" with agent "([^"]+)" and correlation id "([^"]+)"$/,
  async function (
    this: LaserWorld,
    body: string,
    agent: string,
    correlationId: string,
  ) {
    await sendCommand(this, body, agent, { correlationId });
  },
);

When("I start another conversation", function (this: LaserWorld) {
  this.newConversation();
});

When("I assemble the conversation", async function (this: LaserWorld) {
  this.assembled = await eventual(async () => {
    const messages = await this.requireLaser()
      .context(this.requireConversation())
      .fetch([AgentTopic.Sessions], 100);
    return messages.length > 0 ? messages : undefined;
  }, "conversation assembly");
});

When(
  /^I publish an AGDX command "([^"]+)" via the typed producer$/,
  async function (this: LaserWorld, body: string) {
    await this.requireLaser()
      .agdx(
        AgentTopic.Sessions,
        AgentId.new("producer"),
        this.requireConversation(),
      )
      .command(CorrelationId.fromU128(1n), encoder.encode(body))
      .send();
  },
);

Then(
  /^the assembled payloads are "([^"]+)", "([^"]+)", "([^"]+)" in order$/,
  function (this: LaserWorld, first: string, second: string, third: string) {
    assert.deepEqual(
      this.assembled.map((message) => decoder.decode(message.payload)),
      [first, second, third],
    );
  },
);

Then(
  /^the assembled message payload is "([^"]+)"$/,
  function (this: LaserWorld, body: string) {
    assert.deepEqual(
      this.assembled.map((message) => decoder.decode(message.payload)),
      [body],
    );
  },
);

Then(
  /^the AGDX command body is "([^"]+)"$/,
  function (this: LaserWorld, body: string) {
    assert.ok(
      this.assembled.some(
        (message) => decoder.decode(message.envelope?.body) === body,
      ),
    );
  },
);

Then(
  /^the assembled message agent is "([^"]+)"$/,
  function (this: LaserWorld, agent: string) {
    assert.equal(this.assembled[0]?.provenance.agent?.asStr(), agent);
  },
);

Then(
  /^the assembled message idempotency key is "([^"]+)"$/,
  function (this: LaserWorld, key: string) {
    assert.equal(this.assembled[0]?.provenance.idempotencyKey, key);
  },
);

Then(
  /^the assembled message correlation id is "([^"]+)"$/,
  function (this: LaserWorld, correlation: string) {
    assert.equal(this.assembled[0]?.provenance.correlationId, correlation);
  },
);

Then(
  "the assembled message belongs to the conversation",
  function (this: LaserWorld) {
    assert.ok(
      this.assembled[0]?.provenance.conversationId.equals(
        this.requireConversation(),
      ),
    );
  },
);

When(
  "I build an agent event requiring feature bits the receiver lacks",
  function (this: LaserWorld) {
    const envelope = eventEnvelope(
      RecordId.fromU128(1n),
      wire.ConversationId.fromU128(2n),
      AgentId.new("sender").wireId(),
      encoder.encode("event"),
    );
    this.understood =
      unmetRequirements(requiring(envelope, 1n << 8n), 0n) === 0n;
  },
);

When("I build a plain agent event", function (this: LaserWorld) {
  const envelope = eventEnvelope(
    RecordId.fromU128(1n),
    wire.ConversationId.fromU128(2n),
    AgentId.new("sender").wireId(),
    encoder.encode("event"),
  );
  this.understood = unmetRequirements(envelope, 0n) === 0n;
});

Then("the receiver rejects it as not understood", function (this: LaserWorld) {
  assert.equal(this.understood, false);
});

Then("the receiver understands it", function (this: LaserWorld) {
  assert.equal(this.understood, true);
});

Then("the call fails as unsupported", function (this: LaserWorld) {
  assert.ok(this.error instanceof UnsupportedError);
});

Then("the unified result code is unsupported", function (this: LaserWorld) {
  assert.deepEqual(code(this.error), { kind: "known", name: "Unsupported" });
});

async function sendCommand(
  world: LaserWorld,
  body: string,
  agent = "agent",
  extra: {
    readonly idempotencyKey?: string;
    readonly correlationId?: string;
  } = {},
): Promise<void> {
  await world
    .requireLaser()
    .sendAgent(AgentTopic.Sessions, encoder.encode(body), {
      conversationId: world.requireConversation(),
      agent: AgentId.new(agent),
      ...extra,
    });
}

/** The agents of the presence scenario: the first one ready, the second
 * started on the same connection. */
interface Presence {
  first?: AgentHandle;
  second?: AgentHandle;
}

const presences = new WeakMap<LaserWorld, Presence>();

function presence(world: LaserWorld): Presence {
  let state = presences.get(world);
  if (state === undefined) {
    state = {};
    presences.set(world, state);
  }
  return state;
}

function spawnAdvertising(
  world: LaserWorld,
  name: string,
  skill: string,
): AgentHandle {
  return Agent.builder()
    .id(AgentId.new(name))
    .listenOn(AgentTopic.Sessions)
    .capabilities([{ skillId: skill }])
    .handler({ handle: () => Promise.resolve() })
    .build()
    .spawn(world.requireLaser());
}

After(async function (this: LaserWorld) {
  const state = presences.get(this);
  if (state === undefined) return;
  await state.second?.shutdown().catch(() => undefined);
  await state.first?.shutdown();
  presences.delete(this);
});

Given(
  /^agent "([^"]+)" advertising the skill "([^"]+)" is ready on this connection$/,
  async function (this: LaserWorld, name: string, skill: string) {
    const first = spawnAdvertising(this, name, skill);
    presence(this).first = first;
    await first.ready();
  },
);

When(
  /^agent "([^"]+)" advertising the skill "([^"]+)" starts on the same connection$/,
  function (this: LaserWorld, name: string, skill: string) {
    presence(this).second = spawnAdvertising(this, name, skill);
  },
);

Then(
  "waiting for the second agent to be ready fails with a presence conflict",
  async function (this: LaserWorld) {
    const second = presence(this).second;
    assert.ok(second !== undefined, "a second agent started");
    await assert.rejects(second.ready(), PresenceConflictError);
  },
);
