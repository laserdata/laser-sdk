import assert from "node:assert/strict";
import { After, Given, Then, When } from "@cucumber/cucumber";
import {
  type LaserError,
  ActionDecision,
  Agent,
  AgentId,
  AgentTopic,
  ConversationId,
  CorrelationId,
  GovernorMode,
  HandlerError,
  InvalidError,
  LastN,
  MintUlid,
  ModelRequest,
  RecordId,
  SessionConfig,
  TopicRetention,
  deriveSessionId,
  eventEnvelope,
  sessionTurnText,
  type ActionGovernor,
  type AgentHandle,
  type AssembledContext,
  type SessionLease,
  type SessionTurn,
  type Sessions,
} from "@laserdata/laser-sdk";
import { wire } from "@laserdata/laser-sdk/full";
import { eventual } from "../support/eventual.js";
import type { LaserWorld } from "../world.js";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function names(list: string): string[] {
  return list.split(", ").map((name) => name.replace(/^"|"$/g, ""));
}

// The records of `session` on its lane, read until `done` holds.
function lane(
  world: LaserWorld,
  session: ConversationId,
  done: (turns: readonly SessionTurn[]) => boolean,
): Promise<readonly SessionTurn[]> {
  const laser = world.requireLaser();
  return eventual(
    async () => {
      const turns = await laser
        .sessions()
        .open(session)
        .contextWith(new LastN(200));
      return done(turns) ? turns : undefined;
    },
    "the session lane",
    10_000,
  );
}

const displays = (turns: readonly SessionTurn[]): string[] =>
  turns.map((turn) => turn.display);

When(
  /^agent "([^"]+)" starts the session "([^"]+)"$/,
  async function (this: LaserWorld, owner: string, label: string) {
    const { session, lease } = await sessionsOf(this)
      .create(label)
      .agent(AgentId.new(owner))
      .begin();
    this.session = session;
    this.sessionLease = lease;
  },
);

When("the session ends", async function (this: LaserWorld) {
  await this.requireSession().end();
});

When("I cancel the session", async function (this: LaserWorld) {
  await this.capture(() => this.requireSession().cancel());
});

When(
  /^agent "([^"]+)" runs the session "([^"]+)" with work that fails with "([^"]+)"$/,
  async function (
    this: LaserWorld,
    owner: string,
    label: string,
    message: string,
  ) {
    const { session, lease } = await this.requireLaser()
      .sessions()
      .create(label)
      .agent(AgentId.new(owner))
      .begin();
    this.session = session;
    await assert.rejects(
      session.run(lease, () => Promise.reject(new HandlerError(message))),
    );
  },
);

Then(
  /^the session lane shows (".+")$/,
  async function (this: LaserWorld, list: string) {
    const expected = names(list);
    const turns = await lane(
      this,
      this.requireSession().conversation,
      (turns) => turns.length >= expected.length,
    );
    assert.deepEqual(displays(turns), expected);
  },
);

Then(
  /^the session failure message is "([^"]+)"$/,
  async function (this: LaserWorld, message: string) {
    const turns = await lane(
      this,
      this.requireSession().conversation,
      (turns) => turns.some((turn) => turn.display === "session.failed"),
    );
    const failed = turns.find((turn) => turn.display === "session.failed")
      ?.message.envelope;
    assert.ok(failed !== undefined);
    const context = "session end";
    const end = wire.decodeSessionEnd(
      wire.expectMap(wire.decodeOne(failed.body, context), context),
      context,
    );
    assert.ok(end.error?.message?.includes(message), end.error?.message);
  },
);

Then(
  /^the session "([^"]+)" has the same id when created twice$/,
  function (this: LaserWorld, label: string) {
    const sessions = this.requireLaser().sessions();
    const first = sessions.create(label).id();
    assert.ok(first.equals(sessions.create(label).id()));
    assert.ok(first.equals(deriveSessionId(sessions.stream(), "", label)));
  },
);

Then(
  /^the sessions "([^"]+)" and "([^"]+)" have different ids$/,
  function (this: LaserWorld, first: string, second: string) {
    const sessions = this.requireLaser().sessions();
    assert.ok(
      !sessions.create(first).id().equals(sessions.create(second).id()),
    );
  },
);

When(
  /^the session records the message "([^"]+)"$/,
  async function (this: LaserWorld, text: string) {
    const session = this.requireSession();
    const agent = session.agent;
    assert.ok(agent !== undefined, "the session has an agent");
    await session.append(
      wire.withOperation(
        eventEnvelope(
          MintUlid.mint(RecordId),
          wire.ConversationId.parse(session.conversation.toString()),
          agent.wireId(),
          encoder.encode(text),
        ),
        "note",
      ),
    );
  },
);

When(
  /^I take a session checkpoint after (\d+) records?$/,
  async function (this: LaserWorld, records: string) {
    const session = this.requireSession();
    this.checkpoint = await eventual(
      async () => {
        const checkpoint = await session.checkpoint();
        const seen = await session.turnsAt(checkpoint);
        return seen.length === Number(records) ? checkpoint : undefined;
      },
      "the session checkpoint",
      10_000,
    );
  },
);

Then(
  /^the records since the checkpoint are "([^"]+)"$/,
  async function (this: LaserWorld, text: string) {
    const session = this.requireSession();
    const checkpoint = this.requireCheckpoint();
    const turns = await eventual(
      async () => {
        const turns = await session.turnsSince(checkpoint);
        return turns.length === 1 ? turns : undefined;
      },
      "the records since the checkpoint",
      10_000,
    );
    assert.deepEqual(turns.map(sessionTurnText), [text]);
  },
);

Then(
  /^the last record at the checkpoint is "([^"]+)"$/,
  async function (this: LaserWorld, text: string) {
    const turns = await this.requireSession().turnsAt(this.requireCheckpoint());
    const last = turns.at(-1);
    assert.ok(last !== undefined);
    assert.equal(sessionTurnText(last), text);
  },
);

When(
  /^the session records a model call to "([^"]+)" whose request carries an api key$/,
  async function (this: LaserWorld, model: string) {
    await this.requireSession().recordModelCall(
      new ModelRequest(
        model,
        encoder.encode('{"prompt":"hi","api_key":"k-secret"}'),
      ),
      { body: encoder.encode("hello") },
    );
  },
);

When(
  /^the session records a call of tool "([^"]+)" whose arguments carry a token$/,
  async function (this: LaserWorld, tool: string) {
    const call = await this.requireSession().tool(tool, {
      id: 7,
      token: "t-secret",
    });
    await call.complete(encoder.encode("found"));
  },
);

Then(
  "no recorded call carries a secret value",
  async function (this: LaserWorld) {
    const turns = await lane(
      this,
      this.requireSession().conversation,
      (turns) => turns.length >= 5,
    );
    for (const turn of turns) {
      const text = sessionTurnText(turn);
      assert.ok(!text.includes("k-secret") && !text.includes("t-secret"), text);
    }
  },
);

When(
  /^the session state sets "([^"]+)" to (.+)$/,
  async function (this: LaserWorld, key: string, value: string) {
    await this.requireSession()
      .state()
      .set(key, JSON.parse(value) as unknown);
  },
);

Then(
  /^the folded session state has "([^"]+)" (\S+) and "([^"]+)" (\S+) at revision (\d+)$/,
  async function (
    this: LaserWorld,
    first: string,
    firstValue: string,
    second: string,
    secondValue: string,
    revision: string,
  ) {
    const state = this.requireSession().state();
    const view = await eventual(
      async () => {
        const view = await state.get();
        return view.revision === BigInt(revision) ? view : undefined;
      },
      "the folded session state",
      10_000,
    );
    const document = view.document as Record<string, unknown>;
    assert.deepEqual(document[first], JSON.parse(firstValue));
    assert.deepEqual(document[second], JSON.parse(secondValue));
    assert.equal(view.complete, true);
  },
);

Then(
  /^the session lane shows (\d+) "([^"]+)" records$/,
  async function (this: LaserWorld, count: string, display: string) {
    const matching = (turns: readonly SessionTurn[]): number =>
      turns.filter((turn) => turn.display === display).length;
    const turns = await lane(
      this,
      this.requireSession().conversation,
      (turns) => matching(turns) >= Number(count),
    );
    assert.equal(matching(turns), Number(count));
  },
);

Given(
  /^agent "([^"]+)" ends every session it handles$/,
  async function (this: LaserWorld, name: string) {
    const builder = Agent.builder()
      .id(AgentId.new(name))
      .listenOn(AgentTopic.Sessions)
      .pollInterval(5)
      .handler({ handle: (_message, context) => context.session().end() });
    const config = native(this).config;
    const worker = (config === undefined ? builder : builder.sessions(config))
      .build()
      .spawn(this.requireLaser());
    await worker.ready();
    this.worker = worker;
  },
);

When(
  /^"([^"]+)" submits a session to agent "([^"]+)"$/,
  async function (this: LaserWorld, submitter: string, target: string) {
    const submitted = await this.requireLaser()
      .sessions()
      .submit(AgentId.new(target), encoder.encode("{}"))
      .from(AgentId.new(submitter))
      .send();
    this.otherSession = submitted.session;
  },
);

Then(
  /^the submitted session starts as "([^"]+)" and reaches "([^"]+)"$/,
  async function (this: LaserWorld, first: string, last: string) {
    const session = this.otherSession;
    assert.ok(session !== undefined, "a session was submitted");
    const turns = await lane(this, session, (turns) =>
      turns.some((turn) => turn.display === last),
    );
    assert.equal(turns[0]?.display, first);
  },
);

When(
  /^operator "([^"]+)" asks to cancel the session "([^"]+)"$/,
  async function (this: LaserWorld, operator: string, label: string) {
    const laser = this.requireLaser();
    await laser.topic(AgentTopic.Control).ensure(1);
    const stream = laser.defaultStream ?? "";
    const session = deriveSessionId(stream, "", label);
    await laser
      .sessions()
      .control(stream, session)
      .asOperator(AgentId.new(operator))
      .cancel();
    this.otherSession = session;
  },
);

Then(
  /^the control topic holds a "([^"]+)" request for the session$/,
  async function (this: LaserWorld, operation: string) {
    const session = this.otherSession;
    assert.ok(session !== undefined, "a session was controlled");
    const laser = this.requireLaser();
    const records = await eventual(
      async () => {
        const records = await laser
          .context(session)
          .fetchWith([AgentTopic.Control], new LastN(10));
        return records.length > 0 ? records : undefined;
      },
      "the control records",
      10_000,
    );
    assert.equal(records[0]?.envelope?.operation, operation);
  },
);

Given(
  /^the agents use a declared partition layout with "([^"]+)" on (\d+) and "([^"]+)" on (\d+)$/,
  function (
    this: LaserWorld,
    first: string,
    firstPartition: string,
    second: string,
    secondPartition: string,
  ) {
    this.requireLaser().sessions(
      new SessionConfig().layout({
        kind: "perAgentPartition",
        partitions: new Map([
          [first, Number(firstPartition)],
          [second, Number(secondPartition)],
        ]),
      }),
    );
  },
);

When(
  /^"([^"]+)" sends a command to "([^"]+)" in a new session and "([^"]+)" replies$/,
  async function (
    this: LaserWorld,
    requester: string,
    target: string,
    _responder: string,
  ) {
    const laser = this.requireLaser();
    const session = ConversationId.new();
    const correlation = CorrelationId.fromU128(7n);
    const command = await laser
      .agdx(AgentTopic.Sessions, AgentId.new(requester), session)
      .command(correlation, encoder.encode("{}"))
      .withTarget(AgentId.new(target))
      .sendReceipt();
    const reply = await laser
      .agdx(AgentTopic.Sessions, AgentId.new(target), session)
      .respond(correlation, encoder.encode("{}"))
      .withTarget(AgentId.new(requester))
      .sendReceipt();
    this.routed = [command.partitionId, reply.partitionId];
    this.routedSession = session;
  },
);

Given(
  /^the agents use a declared topic layout with "([^"]+)" on "([^"]+)" and "([^"]+)" on "([^"]+)"$/,
  async function (
    this: LaserWorld,
    first: string,
    firstTopic: string,
    second: string,
    secondTopic: string,
  ) {
    await this.requireLaser()
      .sessions(
        new SessionConfig().layout({
          kind: "perAgentTopic",
          topics: new Map([
            [first, firstTopic],
            [second, secondTopic],
          ]),
        }),
      )
      .bootstrap(1, TopicRetention.expireAfter(86_400_000));
  },
);

// The envelope kinds of the routed session's records on `topic`.
async function routedKinds(
  world: LaserWorld,
  topic: string,
): Promise<readonly wire.AgentKind[]> {
  const session = world.routedSession;
  assert.ok(session !== undefined, "a routed exchange ran");
  const messages = await world
    .requireLaser()
    .context(session)
    .fetchWith([topic], new LastN(10));
  return messages.flatMap((message) =>
    message.envelope === undefined ? [] : [message.envelope.kind],
  );
}

Then(
  /^the topic "([^"]+)" holds the command and the topic "([^"]+)" holds the reply$/,
  async function (this: LaserWorld, work: string, reply: string) {
    const commands = await eventual(
      async () => {
        const kinds = await routedKinds(this, work);
        return kinds.length > 0 ? kinds : undefined;
      },
      "the routed command",
      10_000,
    );
    assert.deepEqual(commands, [wire.AgentKind.Command]);
    const replies = await eventual(
      async () => {
        const kinds = await routedKinds(this, reply);
        return kinds.length > 0 ? kinds : undefined;
      },
      "the routed reply",
      10_000,
    );
    assert.deepEqual(replies, [wire.AgentKind.Response]);
  },
);

Then("the session lane holds neither", async function (this: LaserWorld) {
  assert.deepEqual(await routedKinds(this, AgentTopic.Sessions), []);
});

Then(
  /^the command landed on partition (\d+) and the reply on partition (\d+)$/,
  function (this: LaserWorld, command: string, reply: string) {
    assert.deepEqual(this.routed, [Number(command), Number(reply)]);
  },
);

When(
  /^the session remembers "([^"]+)" through its linked memory$/,
  async function (this: LaserWorld, text: string) {
    await this.requireSession()
      .linkedMemory()
      .remember(encoder.encode(text))
      .send();
  },
);

Then(
  /^the remembered item names producer "([^"]+)"$/,
  async function (this: LaserWorld, producer: string) {
    const memory = this.requireSession().memory();
    const items = await eventual(
      async () => {
        const items = await memory.recall().recent().folded().fetch();
        return items.length > 0 ? items : undefined;
      },
      "the remembered item",
      10_000,
    );
    assert.equal(items[0]?.producer?.name, producer);
  },
);

When(
  /^I publish (\d+) unrelated records to topic "([^"]+)"$/,
  async function (this: LaserWorld, count: string, topic: string) {
    const batch = this.requireLaser().topic(topic).publishBatch();
    for (let index = 0; index < Number(count); index += 1) {
      batch.addPayload(encoder.encode(`unrelated-${String(index)}`));
    }
    await batch.send();
  },
);

Then(
  /^the session context holds only the message "([^"]+)"$/,
  async function (this: LaserWorld, text: string) {
    const session = this.requireSession();
    const texts = await eventual(
      async () => {
        const texts = (await session.context()).map(sessionTurnText);
        return texts.length === 1 && texts[0] === text ? texts : undefined;
      },
      "the session context",
      10_000,
    );
    assert.deepEqual(texts, [text]);
  },
);

// The state of the native session scenarios that the shared world fields do
// not cover.
interface Native {
  config?: SessionConfig;
  refusals?: { remaining: number; snapshots: number };
  outcomes: (readonly ["end" | "cancel", unknown])[];
  winner?: "end" | "cancel";
  agents: AgentHandle[];
  handled: Map<string, string[]>;
  partitions: (number | undefined)[];
  assembled?: AssembledContext;
  secondStream?: string;
  leases: Map<string, SessionLease>;
  labels: Map<string, readonly [string, ConversationId]>;
}

const scenarios = new WeakMap<LaserWorld, Native>();

function native(world: LaserWorld): Native {
  let state = scenarios.get(world);
  if (state === undefined) {
    state = {
      outcomes: [],
      agents: [],
      handled: new Map(),
      partitions: [],
      leases: new Map(),
      labels: new Map(),
    };
    scenarios.set(world, state);
  }
  return state;
}

After(async function (this: LaserWorld) {
  const state = scenarios.get(this);
  if (state === undefined) return;
  for (const lease of state.leases.values()) lease.release();
  state.leases.clear();
  for (const agent of state.agents) await agent.shutdown();
  state.agents = [];
});

// The session factory of the scenario: the configured one, or the default.
function sessionsOf(world: LaserWorld): Sessions {
  const config = native(world).config;
  return config === undefined
    ? world.requireLaser().sessions()
    : world.requireLaser().sessions(config);
}

const ONE_DAY_MS = 86_400_000;
const TERMINAL = ["session.completed", "session.failed", "session.canceled"];

const terminal = (turns: readonly SessionTurn[]): SessionTurn[] =>
  turns.filter((turn) => TERMINAL.includes(turn.display));

const recordIds = (turns: readonly SessionTurn[]): Set<string | undefined> =>
  new Set(
    terminal(turns).map((turn) => turn.message.envelope?.record?.toString()),
  );

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

Given(
  "session publishes pass through a policy that can refuse them",
  function (this: LaserWorld) {
    const refusals = { remaining: 0, snapshots: 0 };
    // Blocks the next `remaining` session status publishes.
    const governor: ActionGovernor = {
      decide: (action) => {
        if (
          action.kind === "status" &&
          action.operation === "session" &&
          refusals.remaining > 0
        ) {
          refusals.remaining -= 1;
          return Promise.resolve(
            ActionDecision.block("injected publish refusal"),
          );
        }
        if (
          action.operation === wire.OPERATION_STATE_SNAPSHOT &&
          refusals.snapshots > 0
        ) {
          refusals.snapshots -= 1;
          return Promise.resolve(
            ActionDecision.block("injected publish refusal"),
          );
        }
        return Promise.resolve(ActionDecision.allow());
      },
    };
    this.laser = this.requireLaser().withGovernor(
      governor,
      GovernorMode.Enforce,
    );
    native(this).refusals = refusals;
  },
);

When(
  "the policy refuses the next session status publish",
  function (this: LaserWorld) {
    const refusals = native(this).refusals;
    assert.ok(refusals !== undefined, "a refusing policy is installed");
    refusals.remaining = 1;
  },
);

When("I end the session", async function (this: LaserWorld) {
  await this.capture(() => this.requireSession().end());
});

Then(
  "every terminal record on the lane carries one record id",
  async function (this: LaserWorld) {
    const turns = await lane(
      this,
      this.requireSession().conversation,
      (turns) => terminal(turns).length > 0,
    );
    const records = recordIds(turns);
    assert.equal(records.size, 1, [...records].join(", "));
    assert.ok(!records.has(undefined));
  },
);

When(
  /^(\d+) clones of the session end it while (\d+) other clones cancel it at once$/,
  async function (this: LaserWorld, enders: string, cancelers: string) {
    const session = this.requireSession();
    const agent = session.agent;
    assert.ok(agent !== undefined, "the session has an agent");
    // Alternate the verbs so neither side gets a head start.
    const verbs: ("end" | "cancel")[] = [];
    for (
      let index = 0;
      index < Math.max(Number(enders), Number(cancelers));
      index += 1
    ) {
      if (index < Number(enders)) verbs.push("end");
      if (index < Number(cancelers)) verbs.push("cancel");
    }
    const settled = await Promise.allSettled(
      verbs.map((verb) => {
        const clone = session.asAgent(agent);
        return verb === "end" ? clone.end() : clone.cancel();
      }),
    );
    native(this).outcomes = settled.map(
      (outcome, index) =>
        [
          verbs[index] ?? "end",
          outcome.status === "rejected" ? outcome.reason : undefined,
        ] as const,
    );
  },
);

Then(
  "the calls of one verb all succeed and the calls of the other all fail as invalid",
  function (this: LaserWorld) {
    const state = native(this);
    const winners = new Set(
      state.outcomes
        .filter(([, error]) => error === undefined)
        .map(([verb]) => verb),
    );
    assert.equal(winners.size, 1);
    const [winner] = winners;
    assert.ok(winner !== undefined, "a winning verb");
    for (const [verb, error] of state.outcomes) {
      if (verb === winner) assert.equal(error, undefined);
      else assert.ok(error instanceof InvalidError, String(error));
    }
    state.winner = winner;
  },
);

Then(
  /^the lane holds (\d+) terminal records of the winning verb under one record id$/,
  async function (this: LaserWorld, count: string) {
    const display =
      native(this).winner === "end" ? "session.completed" : "session.canceled";
    const turns = await lane(
      this,
      this.requireSession().conversation,
      (turns) => terminal(turns).length >= Number(count),
    );
    const records = terminal(turns);
    assert.equal(records.length, Number(count));
    assert.ok(
      records.every((turn) => turn.display === display),
      displays(turns).join(", "),
    );
    const ids = recordIds(turns);
    assert.equal(ids.size, 1);
    assert.ok(!ids.has(undefined));
  },
);

Given(
  "a fresh stream bootstrapped for sessions in the single-partition layout",
  async function (this: LaserWorld) {
    await this.laser?.close();
    await this.connect();
    await this.requireLaser()
      .sessions(
        new SessionConfig()
          .layout({ kind: "singlePartition" })
          .registerSource(false),
      )
      .bootstrap(4, TopicRetention.expireAfter(ONE_DAY_MS));
  },
);

Given(
  /^agents "([^"]+)" and "([^"]+)" each record the commands they handle$/,
  async function (this: LaserWorld, first: string, second: string) {
    const state = native(this);
    for (const name of [first, second]) {
      const seen: string[] = [];
      const agent = Agent.builder()
        .id(AgentId.new(name))
        .listenOn(AgentTopic.Sessions)
        .pollInterval(5)
        .handler({
          handle: (message) => {
            seen.push(
              decoder.decode(message.envelope?.body ?? message.payload),
            );
            return Promise.resolve();
          },
        })
        .build()
        .spawn(this.requireLaser());
      await agent.ready();
      state.agents.push(agent);
      state.handled.set(name, seen);
    }
  },
);

When(
  /^"([^"]+)" sends (.+) in separate sessions$/,
  async function (this: LaserWorld, sender: string, list: string) {
    const state = native(this);
    for (const pair of list.split(", ")) {
      const [body, target] = pair
        .split(" to ")
        .map((part) => part.replace(/^"|"$/g, ""));
      assert.ok(body !== undefined && target !== undefined, pair);
      const receipt = await this.requireLaser()
        .agdx(AgentTopic.Sessions, AgentId.new(sender), ConversationId.new())
        .command(MintUlid.mint(CorrelationId), encoder.encode(body))
        .withTarget(AgentId.new(target))
        .sendReceipt();
      state.partitions.push(receipt.partitionId);
    }
  },
);

Then("every command landed on the same partition", function (this: LaserWorld) {
  const partitions = new Set(native(this).partitions);
  assert.equal(partitions.size, 1, [...partitions].join(", "));
  assert.ok(!partitions.has(undefined));
});

Then(
  /^agent "([^"]+)" handled exactly (".+")$/,
  async function (this: LaserWorld, name: string, list: string) {
    const expected = names(list);
    const seen = native(this).handled.get(name);
    assert.ok(seen !== undefined, "a recording agent");
    await eventual(
      () =>
        Promise.resolve(
          seen.join("\n") === expected.join("\n") ? true : undefined,
        ),
      `agent ${name}`,
      10_000,
    );
    // The other agent's records share the partition, so give a wrongly
    // handled one time to show up before the final check.
    await sleep(500);
    assert.deepEqual(seen, expected);
  },
);

When(
  /^"([^"]+)" writes the control requests (".+") for agent "([^"]+)" on the session lane$/,
  async function (
    this: LaserWorld,
    sender: string,
    list: string,
    target: string,
  ) {
    const lane = this.requireLaser().agdx(
      AgentTopic.Sessions,
      AgentId.new(sender),
      this.requireSession().conversation,
    );
    for (const operation of names(list)) {
      await lane
        .command(MintUlid.mint(CorrelationId), encoder.encode("{}"))
        .withOperation(operation)
        .withTarget(AgentId.new(target))
        .send();
    }
  },
);

When(
  /^"([^"]+)" sends work to agent "([^"]+)" in the session$/,
  async function (this: LaserWorld, sender: string, target: string) {
    await this.requireLaser()
      .agdx(
        AgentTopic.Sessions,
        AgentId.new(sender),
        this.requireSession().conversation,
      )
      .command(MintUlid.mint(CorrelationId), encoder.encode("work"))
      .withTarget(AgentId.new(target))
      .send();
  },
);

Then(
  /^agent "([^"]+)" sees no pending pause or cancel for the session$/,
  async function (this: LaserWorld, name: string) {
    const lens = sessionsOf(this)
      .open(this.requireSession().conversation)
      .asAgent(AgentId.new(name));
    const pending = await lens.pendingControl();
    assert.equal(pending.pauseRequested, false, "a pause was applied");
    assert.equal(pending.cancelRequested, false, "a cancel was applied");
    assert.equal(await lens.cancelRequested(), false, "a cancel was applied");
  },
);

When(
  /^the session records a model call to "([^"]+)" with the context assembled from its (\d+) records$/,
  async function (this: LaserWorld, model: string, records: string) {
    const session = this.requireSession();
    const assembled = await eventual(
      async () => {
        const assembled = await session.assemble(new LastN(50));
        return assembled.fragments.length === Number(records)
          ? assembled
          : undefined;
      },
      "the assembled context",
      10_000,
    );
    await session.recordModelCall(
      new ModelRequest(model, encoder.encode('{"prompt":"hi"}')),
      { body: encoder.encode("hello") },
      assembled,
    );
    native(this).assembled = assembled;
  },
);

When(
  /^the session records a model call to "([^"]+)" without an assembled context$/,
  async function (this: LaserWorld, model: string) {
    await this.requireSession().recordModelCall(
      new ModelRequest(model, encoder.encode('{"prompt":"hi"}')),
      { body: encoder.encode("hello") },
    );
  },
);

Then(
  "the context manifest lists the address of every assembled fragment",
  async function (this: LaserWorld) {
    const session = this.requireSession().conversation;
    const turns = await lane(this, session, (turns) =>
      turns.some((turn) => turn.display === "context.assembled"),
    );
    const envelope = turns.find((turn) => turn.display === "context.assembled")
      ?.message.envelope;
    const request = turns.find((turn) => turn.display === "model.request")
      ?.message.envelope;
    assert.ok(envelope !== undefined && request !== undefined);
    const context = "context manifest";
    const manifest = wire.decodeContextManifest(
      wire.expectMap(wire.decodeOne(envelope.body, context), context),
      context,
    );
    assert.equal(
      manifest.correlation?.toString(),
      request.correlation?.toString(),
    );
    const listed = manifest.fragments.map((fragment) => {
      assert.ok(fragment.kind === "message", JSON.stringify(fragment.kind));
      const at = fragment.at;
      assert.ok(at.kind === "message", JSON.stringify(at.kind));
      assert.ok(at.generation !== undefined, "a fragment without generation");
      assert.equal(at.conversation, session.toString());
      return `${String(at.partition)}:${String(at.offset)}`;
    });
    const assembled = native(this).assembled;
    assert.ok(assembled !== undefined, "an assembled context");
    const fragments = assembled.fragments.map(
      (message) =>
        `${String(message.id.partitionId)}:${String(message.id.offset)}`,
    );
    const earlier = turns
      .slice(0, fragments.length)
      .map(
        (turn) =>
          `${String(turn.message.id.partitionId)}:${String(turn.message.id.offset)}`,
      );
    assert.deepEqual(listed, fragments);
    assert.deepEqual(listed, earlier);
  },
);

Given(
  /^sessions publish heartbeats every (\d+) milliseconds$/,
  function (this: LaserWorld, millis: string) {
    native(this).config = new SessionConfig().heartbeat(Number(millis));
  },
);

When(
  /^agent "([^"]+)" opens the session "([^"]+)"$/,
  function (this: LaserWorld, owner: string, label: string) {
    const sessions = sessionsOf(this);
    this.session = sessions
      .open(sessions.create(label).id())
      .asAgent(AgentId.new(owner));
  },
);

interface Beat {
  readonly stream: string;
  readonly sessions: Set<string>;
}

// Every heartbeat on `stream`, in log order.
async function heartbeats(world: LaserWorld, stream: string): Promise<Beat[]> {
  const cursor = await world
    .requireLaser()
    .stream(stream)
    .topic(AgentTopic.Heartbeats)
    .replay();
  const beats: Beat[] = [];
  for (;;) {
    const batch = await cursor.poll();
    if (batch.length === 0) return beats;
    for (const message of batch) {
      const envelope = wire.decodeAgentEnvelope(
        wire.expectMap(
          wire.decodeOne(message.payload, "heartbeat"),
          "heartbeat",
        ),
        "heartbeat",
      );
      const beat = wire.decodeSessionHeartbeat(
        wire.expectMap(wire.decodeOne(envelope.body, "heartbeat"), "heartbeat"),
        "heartbeat",
      );
      beats.push({
        stream: beat.stream,
        sessions: new Set(beat.sessions.map((session) => session.toString())),
      });
    }
  }
}

const sameSet = (left: Set<string>, right: Set<string>): boolean =>
  left.size === right.size && [...left].every((value) => right.has(value));

Then(
  /^no heartbeat is published within (\d+) seconds?$/,
  async function (this: LaserWorld, seconds: string) {
    await sleep(Number(seconds) * 1000);
    const beats = await heartbeats(
      this,
      this.requireLaser().defaultStream ?? "",
    );
    assert.deepEqual(beats, []);
  },
);

async function startLeased(
  world: LaserWorld,
  sessions: Sessions,
  owner: string,
  label: string,
): Promise<void> {
  const { session, lease } = await sessions
    .create(label)
    .agent(AgentId.new(owner))
    .begin();
  const state = native(world);
  state.labels.set(label, [sessions.stream(), session.conversation]);
  state.leases.set(label, lease);
}

When(
  /^agent "([^"]+)" starts the sessions "([^"]+)" and "([^"]+)"$/,
  async function (
    this: LaserWorld,
    owner: string,
    first: string,
    second: string,
  ) {
    for (const label of [first, second])
      await startLeased(this, sessionsOf(this), owner, label);
  },
);

Then(
  /^a heartbeat lists the sessions (".+")$/,
  async function (this: LaserWorld, list: string) {
    const state = native(this);
    const expected = new Set(
      names(list).map((label) => {
        const started = state.labels.get(label);
        assert.ok(started !== undefined, `a started session ${label}`);
        return started[1].toString();
      }),
    );
    const stream = this.requireLaser().defaultStream ?? "";
    await eventual(
      async () =>
        (await heartbeats(this, stream)).some(
          (beat) => beat.stream === stream && sameSet(beat.sessions, expected),
        )
          ? true
          : undefined,
      "a heartbeat listing the sessions",
      10_000,
    );
  },
);

When(
  /^the lease on the session "([^"]+)" is released$/,
  function (this: LaserWorld, label: string) {
    const leases = native(this).leases;
    const lease = leases.get(label);
    assert.ok(lease !== undefined, `a leased session ${label}`);
    lease.release();
    leases.delete(label);
  },
);

Then("the heartbeats stop", async function (this: LaserWorld) {
  const stream = this.requireLaser().defaultStream ?? "";
  // A beat already in flight when the last lease dropped may still land.
  await sleep(500);
  const settled = (await heartbeats(this, stream)).length;
  assert.ok(settled > 0, "no heartbeat was ever published");
  await sleep(1000);
  assert.equal((await heartbeats(this, stream)).length, settled);
});

function configOf(world: LaserWorld): SessionConfig {
  return native(world).config ?? new SessionConfig();
}

Given(
  "a second stream bootstrapped for sessions on the same connection",
  async function (this: LaserWorld) {
    const stream = `${this.requireLaser().defaultStream ?? ""}-second`;
    await this.requireLaser()
      .sessions(configOf(this).stream(stream).registerSource(false))
      .bootstrap(4, TopicRetention.expireAfter(ONE_DAY_MS));
    native(this).secondStream = stream;
  },
);

When(
  /^agent "([^"]+)" starts the session "([^"]+)" on this stream and the session "([^"]+)" on the second stream$/,
  async function (
    this: LaserWorld,
    owner: string,
    here: string,
    there: string,
  ) {
    const second = native(this).secondStream;
    assert.ok(second !== undefined, "a second stream");
    await startLeased(this, sessionsOf(this), owner, here);
    await startLeased(
      this,
      this.requireLaser().sessions(configOf(this).stream(second)),
      owner,
      there,
    );
  },
);

Then(
  "the heartbeats of each stream list only that stream's session",
  async function (this: LaserWorld) {
    const streams = new Map(native(this).labels.values());
    assert.equal(streams.size, 2, "one session on each of two streams");
    // The later session's stream beats only while both leases are held.
    await eventual(
      async () => {
        for (const stream of streams.keys())
          if ((await heartbeats(this, stream)).length < 2) return undefined;
        return true;
      },
      "heartbeats on both streams",
      10_000,
    );
    for (const [stream, session] of streams) {
      for (const beat of await heartbeats(this, stream)) {
        assert.equal(beat.stream, stream);
        assert.ok(
          sameSet(beat.sessions, new Set([session.toString()])),
          [...beat.sessions].join(", "),
        );
      }
    }
  },
);

When(
  "the policy refuses the next state snapshot publish",
  function (this: LaserWorld) {
    const refusals = native(this).refusals;
    assert.ok(refusals !== undefined);
    refusals.snapshots = 1;
  },
);

When("I write a state snapshot", async function (this: LaserWorld) {
  this.error = undefined;
  try {
    await this.requireSession().state().snapshot();
  } catch (error) {
    this.error = error as LaserError;
  }
});
