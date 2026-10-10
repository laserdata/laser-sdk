import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { After, Given, Then, When } from "@cucumber/cucumber";
import {
  A2aBridge,
  Agent,
  AgentId,
  AgentTopic,
  ConversationId,
  CorrelationId,
  Laser,
  McpBridge,
  ModelRequest,
  SessionConfig,
  SessionError,
  TopicRetention,
  deriveSessionId,
  isPermissionDenied,
  routeTo,
  type AgentHandle,
  type MemoryItem,
  type Session,
  type SessionInfo,
  type SessionLayout,
  type SessionLease,
  type Sessions,
} from "@laserdata/laser-sdk";
import { wire } from "@laserdata/laser-sdk/full";
import type { LaserWorld } from "../world.js";

// Folds run on their own schedule, so every index read waits for its answer.
const CONVERGE_MS = 20_000;
const ONE_DAY_MS = 86_400_000;
const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** The streams, sessions, and reads of one managed-session scenario. */
interface Managed {
  laser?: Laser;
  sessions?: Sessions;
  layout: string;
  secondLaser?: Laser;
  second?: Sessions;
  secondSession?: ConversationId;
  readonly labels: Map<string, ConversationId>;
  readonly children: ConversationId[];
  workflowRun?: ConversationId;
  linkedError?: unknown;
  remembered?: MemoryItem;
  worker?: AgentHandle;
  lease?: SessionLease;
  watch?: Promise<unknown>;
  watchAbort?: AbortController;
  readonly handled: ConversationId[];
  writer?: Laser;
  retained?: Session;
  retainedState?: ReturnType<Session["state"]>;
  retainedSource?: Extract<wire.SourceRef, { kind: "message" }>;
}

const scenarios = new WeakMap<LaserWorld, Managed>();

function managed(world: LaserWorld): Managed {
  let state = scenarios.get(world);
  if (state === undefined) {
    state = { layout: "shared", labels: new Map(), children: [], handled: [] };
    scenarios.set(world, state);
  }
  return state;
}

function sessionsOf(world: LaserWorld): Sessions {
  const sessions = managed(world).sessions;
  assert.ok(sessions !== undefined, "a managed session stream");
  return sessions;
}

function laserOf(world: LaserWorld): Laser {
  const laser = managed(world).laser;
  assert.ok(laser !== undefined, "a managed session stream");
  return laser;
}

// A labeled session id derives from the stream and the label, so a session
// the shared session steps started resolves the same way.
function idOf(world: LaserWorld, label: string): ConversationId {
  return (
    managed(world).labels.get(label) ??
    deriveSessionId(sessionsOf(world).stream(), "", label)
  );
}

const same = (
  left: { toString(): string },
  right: { toString(): string },
): boolean => left.toString() === right.toString();

async function eventually<Value>(
  description: string,
  read: () => Promise<Value | undefined>,
  show: () => Promise<string> = () => Promise.resolve(""),
): Promise<Value> {
  const deadline = performance.now() + CONVERGE_MS;
  while (performance.now() < deadline) {
    let value: Value | undefined;
    try {
      value = await read();
    } catch {
      value = undefined;
    }
    if (value !== undefined) return value;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(
    `${description} did not converge within ${String(CONVERGE_MS)}ms: ${await show()}`,
  );
}

async function indexed(
  sessions: Sessions,
  id: ConversationId,
): Promise<SessionInfo | undefined> {
  try {
    return await sessions.get(id);
  } catch {
    return undefined;
  }
}

const json = (value: unknown): string =>
  JSON.stringify(value, (_key, item: unknown) =>
    typeof item === "bigint" ? item.toString() : item,
  );

async function managedStream(
  world: LaserWorld,
  config: SessionConfig,
): Promise<{ laser: Laser; sessions: Sessions }> {
  const laser = await Laser.connectWithStream(
    world.endpoint,
    `bdd-ts-${randomUUID().slice(0, 12)}`,
  );
  const sessions = laser.sessions(config);
  const bootstrap = await sessions.bootstrap(
    4,
    TopicRetention.expireAfter(ONE_DAY_MS),
  );
  assert.ok(
    bootstrap.registered,
    "a deployment that serves sessions registers the stream",
  );
  return { laser, sessions };
}

const LAYOUTS: Readonly<Record<string, SessionLayout>> = {
  shared: { kind: "shared" },
  "per-agent topic": {
    kind: "perAgentTopic",
    topics: new Map([
      ["planner", "planner.inbox"],
      ["worker", "worker.inbox"],
    ]),
  },
  "declared partitions": {
    kind: "perAgentPartition",
    partitions: new Map([
      ["planner", 0],
      ["worker", 2],
    ]),
  },
  "single partition": { kind: "singlePartition" },
};

Given(
  /^a managed session stream in the (.+) layout$/,
  async function (this: LaserWorld, layout: string) {
    const state = managed(this);
    const chosen = LAYOUTS[layout];
    assert.ok(chosen !== undefined, `unknown layout ${layout}`);
    const { laser, sessions } = await managedStream(
      this,
      new SessionConfig().layout(chosen),
    );
    Object.assign(state, { laser, sessions, layout });
    // The shared session steps start sessions on the scenario's connection.
    this.laser = laser;
  },
);

Given(
  /^a managed session stream whose agents heartbeat every (\d+) seconds?$/,
  async function (this: LaserWorld, seconds: string) {
    const { laser, sessions } = await managedStream(
      this,
      new SessionConfig().heartbeat(Number(seconds) * 1000),
    );
    Object.assign(managed(this), { laser, sessions });
    this.laser = laser;
  },
);

Given("a second managed session stream", async function (this: LaserWorld) {
  const { laser, sessions } = await managedStream(this, new SessionConfig());
  Object.assign(managed(this), { secondLaser: laser, second: sessions });
});

async function begin(
  sessions: Sessions,
  owner: string,
  label: string,
  idleTimeoutMs?: number,
): Promise<{ session: Session; lease: SessionLease }> {
  let builder = sessions.create(label).agent(AgentId.new(owner));
  if (idleTimeoutMs !== undefined) builder = builder.idleTimeout(idleTimeoutMs);
  return builder.begin();
}

When(
  /^agent "([^"]+)" runs the session "([^"]+)" with one command answered by "([^"]+)"$/,
  async function (
    this: LaserWorld,
    owner: string,
    label: string,
    worker: string,
  ) {
    const state = managed(this);
    const { session, lease } = await begin(sessionsOf(this), owner, label);
    // Every layout sends on the lane. A per-agent topic layout moves the
    // addressed command and reply onto the declared topics.
    const [work, reply] = [AgentTopic.Sessions, AgentTopic.Sessions];
    const laser = laserOf(this);
    const conversation = session.conversation;
    const correlation = CorrelationId.fromU128(BigInt(state.labels.size + 1));
    await laser
      .agdx(work, AgentId.new(owner), conversation)
      .command(correlation, encoder.encode("{}"))
      .withTarget(AgentId.new(worker))
      .send();
    await laser
      .agdx(reply, AgentId.new(worker), conversation)
      .respond(correlation, encoder.encode("{}"))
      .withTarget(AgentId.new(owner))
      .send();
    await session.end();
    lease.release();
    state.labels.set(label, conversation);
  },
);

Then(
  /^the indexed session "([^"]+)" is completed by "([^"]+)" with (\d+) events$/,
  async function (
    this: LaserWorld,
    label: string,
    owner: string,
    events: string,
  ) {
    const sessions = sessionsOf(this);
    const id = idOf(this, label);
    const info = await eventually("the completed session row", async () => {
      const info = await indexed(sessions, id);
      return info?.status === "completed" && info.events >= BigInt(events)
        ? info
        : undefined;
    });
    assert.equal(info.events, BigInt(events), json(info));
    assert.equal(info.label, label);
    assert.equal(info.agent?.toString(), owner);
    assert.equal(info.flags.laneConflict, false, json(info));
  },
);

Then(
  /^the stream counts (\d+) completed sessions?$/,
  async function (this: LaserWorld, count: string) {
    const sessions = sessionsOf(this);
    const page = await eventually("the completed count", async () => {
      const page = await sessions.list().status("completed").total().fetch();
      return page.total === BigInt(count) ? page : undefined;
    });
    assert.equal(page.items.length, Number(count));
  },
);

Given(
  /^agent "([^"]+)" answers every command it receives$/,
  async function (this: LaserWorld, name: string) {
    const worker = Agent.builder()
      .id(AgentId.new(name))
      .listenOn(AgentTopic.Sessions)
      .respondOn(AgentTopic.Sessions)
      .pollInterval(5)
      .handler({
        handle: (_message, context) =>
          context.respond(encoder.encode('{"ok":true}')),
      })
      .build()
      .spawn(laserOf(this));
    await worker.ready();
    managed(this).worker = worker;
  },
);

When(
  /^agent "([^"]+)" starts the session "([^"]+)" on the second stream$/,
  async function (this: LaserWorld, owner: string, label: string) {
    const state = managed(this);
    assert.ok(state.second !== undefined, "a second managed session stream");
    const { session, lease } = await begin(state.second, owner, label);
    state.secondSession = session.conversation;
    await session.end();
    lease.release();
  },
);

When(
  /^agent "([^"]+)" starts the session "([^"]+)" with an idle timeout of (\d+) seconds$/,
  async function (
    this: LaserWorld,
    owner: string,
    label: string,
    seconds: string,
  ) {
    const state = managed(this);
    const { session, lease } = await begin(
      sessionsOf(this),
      owner,
      label,
      Number(seconds) * 1000,
    );
    state.labels.set(label, session.conversation);
    state.lease = lease;
    this.session = session;
  },
);

When(
  /^the session fans out one branch to "([^"]+)"$/,
  async function (this: LaserWorld, target: string) {
    const session = this.requireSession();
    const laser = laserOf(this);
    const author = session.agent;
    assert.ok(author !== undefined, "the session has an agent");
    // One fan-out branch: a subconversation of the session addressed to its
    // target, the request the gather awaits.
    const spawned = laser.spawnSubconversation(
      { conversationId: session.conversation, agent: author },
      author,
    );
    const branch = {
      ...spawned,
      targetAgentId: AgentId.new(target),
      correlationId: `branch-${spawned.conversationId.toString()}`,
    };
    await laser.request(
      AgentTopic.Sessions,
      AgentTopic.Sessions,
      encoder.encode("branch"),
      branch,
      20_000,
    );
    managed(this).children.push(branch.conversationId);
  },
);

When(
  "the session submits an A2A task as a child",
  async function (this: LaserWorld) {
    const session = this.requireSession().conversation;
    const bridge = new A2aBridge(
      laserOf(this),
      AgentId.new("a2a-gateway"),
      AgentTopic.Sessions,
      AgentTopic.Sessions,
    );
    const task = await bridge.submitIn(session, session, {
      message: { role: "user", text: "look" },
    });
    managed(this).children.push(ConversationId.parse(task.id));
  },
);

Given(
  /^agent "([^"]+)" has a bound addressee filter$/,
  async function (this: LaserWorld, name: string) {
    const laser = laserOf(this);
    assert.equal((await laser.capabilities()).filters.groupPolicyReads, true);
    const binding = await laser
      .topic(AgentTopic.Sessions)
      .consumerGroup(name)
      .filter()
      .get();
    assert.ok(binding !== undefined, "the worker's group has a filter");
  },
);

When(
  /^agent "([^"]+)" sends untargeted low-altitude work with send_agent and request$/,
  async function (this: LaserWorld, owner: string) {
    const laser = laserOf(this);
    const provenance = {
      conversationId: this.requireSession().conversation,
      agent: AgentId.new(owner),
    };
    await laser.sendAgent(
      AgentTopic.Sessions,
      encoder.encode("send"),
      provenance,
    );
    const reply = await laser.request(
      AgentTopic.Sessions,
      AgentTopic.Sessions,
      encoder.encode("request"),
      provenance,
      20_000,
    );
    assert.equal(decoder.decode(reply.payload), "{}");
  },
);

Then(
  /^agent "([^"]+)" handles both untargeted records for the session "([^"]+)"$/,
  async function (this: LaserWorld, _name: string, label: string) {
    const state = managed(this);
    await eventually("both untargeted records are handled", () =>
      Promise.resolve(state.handled.length >= 2 ? true : undefined),
    );
    const expected = idOf(this, label).toString();
    assert.deepEqual(
      state.handled.map((id) => id.toString()),
      [expected, expected],
    );
  },
);

When(
  /^the session calls the MCP tool "([^"]+)" as a child$/,
  async function (this: LaserWorld, tool: string) {
    const session = this.requireSession().conversation;
    const bridge = new McpBridge(
      laserOf(this),
      AgentId.new("mcp-gateway"),
      AgentTopic.Sessions,
      AgentTopic.Sessions,
      "bdd",
    ).withTimeout(20_000);
    await bridge.callToolIn(session, session, tool, { q: "auth" });
  },
);

async function tree(
  sessions: Sessions,
  root: ConversationId,
): Promise<readonly SessionInfo[]> {
  try {
    const page = await sessions.list().root(root).fetch();
    return page.items.filter((item) => !same(item.id, root));
  } catch {
    return [];
  }
}

Then(
  /^the session tree of "([^"]+)" holds (\d+) child(?:ren)?$/,
  async function (this: LaserWorld, label: string, count: string) {
    const sessions = sessionsOf(this);
    const root = idOf(this, label);
    const children = await eventually(
      "the session tree",
      async () => {
        const children = await tree(sessions, root);
        return children.length >= Number(count) ? children : undefined;
      },
      async () =>
        `tree ${json(await tree(sessions, root))}, stream ${json(await sessions.list().fetch())}`,
    );
    assert.equal(children.length, Number(count), json(children));
    for (const child of managed(this).children) {
      assert.ok(
        children.some((item) => same(item.id, child)),
        `child ${child.toString()} is in the tree`,
      );
    }
  },
);

Then(
  /^every child names "([^"]+)" as its parent and root$/,
  async function (this: LaserWorld, label: string) {
    const root = idOf(this, label);
    for (const child of await tree(sessionsOf(this), root)) {
      assert.equal(child.parent?.toString(), root.toString());
      assert.equal(child.root?.toString(), root.toString());
    }
  },
);

async function runWorkflow(
  laser: Laser,
  name: string,
  worker: string,
  run?: ConversationId,
): Promise<ConversationId> {
  const workflow = laser
    .workflow(name)
    .inboxRoute({ kind: "fixed", topic: AgentTopic.Sessions });
  if (run !== undefined) workflow.runId(run);
  workflow.step("check", routeTo(AgentId.new(worker)), () =>
    encoder.encode("incident"),
  );
  return (await workflow.run()).runId;
}

When(
  /^the workflow "([^"]+)" runs its one step on "([^"]+)"$/,
  async function (this: LaserWorld, name: string, worker: string) {
    managed(this).workflowRun = await runWorkflow(laserOf(this), name, worker);
  },
);

When(
  /^the workflow "([^"]+)" resumes the same run$/,
  async function (this: LaserWorld, name: string) {
    const state = managed(this);
    await runWorkflow(laserOf(this), name, "worker", state.workflowRun);
  },
);

Then(
  /^the session tree of the workflow run holds (\d+) child(?:ren)?$/,
  async function (this: LaserWorld, count: string) {
    const sessions = sessionsOf(this);
    const run = managed(this).workflowRun;
    assert.ok(run !== undefined, "a workflow ran");
    await eventually("the workflow tree", async () => {
      const children = await tree(sessions, run);
      return children.length >= Number(count) ? children : undefined;
    });
    const info = await eventually("the workflow run row", async () => {
      const info = await indexed(sessions, run);
      return info !== undefined &&
        ["completed", "failed", "canceled"].includes(info.status)
        ? info
        : undefined;
    });
    assert.equal(info.status, "completed", json(info));
    const children = await tree(sessions, run);
    assert.equal(children.length, Number(count), json(children));
  },
);

When(
  "the session records that it recalled the remembered item",
  async function (this: LaserWorld) {
    const session = this.requireSession();
    const memory = session.memory();
    const items = await eventually("the remembered item", async () => {
      const items = await memory.recall().recent().folded().fetch();
      return items.length > 0 ? items : undefined;
    });
    const [first] = items;
    assert.ok(first !== undefined);
    await session.recordRetrieval("deploy key", [first]);
    managed(this).remembered = first;
  },
);

Then(
  "the session links the remembered item as written and recalled",
  async function (this: LaserWorld) {
    const sessions = sessionsOf(this);
    const id = this.requireSession().conversation;
    const item = managed(this).remembered?.id.toString();
    assert.ok(item !== undefined, "an item was recalled");
    const view = await eventually("the memory links", async () => {
      const view = await sessions.links(id, "memory");
      const relations = new Set(
        view.links
          .filter((link) => link.item === item)
          .map((link) => link.relation),
      );
      return relations.has("wrote") && relations.has("recalled")
        ? view
        : undefined;
    });
    assert.ok(view.links.every((link) => link.surface === "memory"));
  },
);

When(
  /^the session state is replaced by (.+)$/,
  async function (this: LaserWorld, document: string) {
    await this.requireSession()
      .state()
      .replace(JSON.parse(document) as Record<string, unknown>);
  },
);

When("the session writes a state snapshot", async function (this: LaserWorld) {
  await this.requireSession().state().snapshot();
});

Then(
  /^the indexed state of the session is (.+)$/,
  async function (this: LaserWorld, document: string) {
    const expected = JSON.stringify(JSON.parse(document));
    const sessions = sessionsOf(this);
    const id = this.requireSession().conversation;
    const view = await eventually("the indexed state", async () => {
      const view = await sessions.state(id, 0);
      return JSON.stringify(view.document) === expected ? view : undefined;
    });
    assert.equal(view.complete, true, json(view));
  },
);

const hex = (digest: Uint8Array | undefined): string | undefined =>
  digest === undefined ? undefined : Buffer.from(digest).toString("hex");

Then(
  /^the indexed state history chains its digests over (\d+) deltas and the snapshot$/,
  async function (this: LaserWorld, count: string) {
    const deltas = Number(count);
    const sessions = sessionsOf(this);
    const id = this.requireSession().conversation;
    // The snapshot folds after the document already matched, so wait for it.
    const view = await eventually("the indexed state history", async () => {
      const view = await sessions.state(id, 0);
      return view.history.length === deltas + 1 ? view : undefined;
    });
    const history = view.history;
    for (const change of history) {
      assert.equal(change.outcome, "applied", json(change));
      assert.ok(
        change.oldDigest !== undefined && change.newDigest !== undefined,
        json(change),
      );
    }
    for (let index = 1; index < history.length; index += 1) {
      assert.equal(
        hex(history[index]?.oldDigest),
        hex(history[index - 1]?.newDigest),
      );
    }
    history.slice(0, deltas).forEach((delta, index) => {
      assert.ok(delta.opId !== undefined, json(delta));
      assert.equal(delta.revision, BigInt(index + 1), json(delta));
      assert.notEqual(hex(delta.oldDigest), hex(delta.newDigest), json(delta));
    });
    // A snapshot of the document the deltas built keeps the revision and the
    // digest.
    const snapshot = history[deltas];
    assert.ok(snapshot !== undefined);
    assert.equal(snapshot.opId, undefined, json(snapshot));
    assert.equal(snapshot.revision, BigInt(deltas), json(snapshot));
    assert.equal(
      hex(snapshot.oldDigest),
      hex(snapshot.newDigest),
      json(snapshot),
    );
  },
);

Then(
  /^the indexed session "([^"]+)" is active$/,
  async function (this: LaserWorld, label: string) {
    const sessions = sessionsOf(this);
    const id = idOf(this, label);
    await eventually("the active session row", async () => {
      const info = await indexed(sessions, id);
      return info?.status === "active" ? info : undefined;
    });
  },
);

When(
  /^the session links "([^"]+)" to "([^"]+)" in graph "([^"]+)"$/,
  async function (this: LaserWorld, from: string, to: string, graph: string) {
    await this.requireSession().linkedGraph(graph).link(from, "depends_on", to);
  },
);

async function graphNodes(
  sessions: Sessions,
  id: ConversationId,
): Promise<Set<string>> {
  const view = await sessions.links(id, "graph_node");
  return new Set(view.links.map((link) => link.item));
}

Then(
  /^the sessions "([^"]+)" and "([^"]+)" both link the graph node "([^"]+)"$/,
  async function (
    this: LaserWorld,
    first: string,
    second: string,
    node: string,
  ) {
    const sessions = sessionsOf(this);
    const [firsts, seconds] = await eventually(
      "the graph node links",
      async () => {
        const firsts = await graphNodes(sessions, idOf(this, first));
        const seconds = await graphNodes(sessions, idOf(this, second));
        return firsts.size === 2 && seconds.size === 2
          ? [firsts, seconds]
          : undefined;
      },
    );
    // Each session linked its own pair, and only the re-observed node is shared.
    const shared = [...firsts].filter((item) => seconds.has(item));
    assert.equal(
      shared.length,
      1,
      `${node}: ${json([...firsts])} ${json([...seconds])}`,
    );
  },
);

When(
  /^the session sets key "([^"]+)" to "([^"]+)" in namespace "([^"]+)"$/,
  async function (
    this: LaserWorld,
    key: string,
    value: string,
    namespace: string,
  ) {
    await this.requireSession()
      .kv(namespace)
      .set(encoder.encode(key))
      .bytes(encoder.encode(value))
      .send();
  },
);

Then(
  /^the session links key "([^"]+)" in namespace "([^"]+)"$/,
  async function (this: LaserWorld, key: string, namespace: string) {
    const sessions = sessionsOf(this);
    const id = this.requireSession().conversation;
    await eventually("the key-value link", async () => {
      const view = await sessions.links(id, "kv");
      return view.links.some(
        (link) => link.item === key && link.resource.endsWith(namespace),
      )
        ? view
        : undefined;
    });
  },
);

Then(
  /^the key-value lens of the session in namespace "([^"]+)" lists "([^"]+)"$/,
  async function (this: LaserWorld, namespace: string, key: string) {
    const laser = laserOf(this);
    const id = this.requireSession().conversation;
    const keys = await eventually("the key-value lens", async () => {
      const entries = await laser
        .kv(namespace)
        .scan()
        .conversation(id.toString())
        .entries();
      const keys = entries.map((entry) => decoder.decode(entry.key));
      return keys.length > 0 ? keys : undefined;
    });
    assert.deepEqual(keys, [key]);
  },
);

When(
  /^I set key "([^"]+)" in namespace "([^"]+)" linked to the second stream's session$/,
  async function (this: LaserWorld, key: string, namespace: string) {
    const state = managed(this);
    assert.ok(state.second !== undefined && state.secondSession !== undefined);
    const victim = state.second.open(state.secondSession).reference();
    try {
      await laserOf(this)
        .kv(namespace)
        .inSession(victim)
        .set(encoder.encode(key))
        .bytes(encoder.encode("stolen"))
        .send();
      state.linkedError = undefined;
    } catch (error) {
      state.linkedError = error;
    }
  },
);

Then("the linked write is refused", function (this: LaserWorld) {
  assert.ok(
    managed(this).linkedError !== undefined,
    "a write linked to another stream's session is refused",
  );
});

Then(
  "the second stream's session links no key-value item",
  async function (this: LaserWorld) {
    const state = managed(this);
    const second = state.second;
    const id = state.secondSession;
    assert.ok(second !== undefined && id !== undefined);
    await eventually("the second session row", () => indexed(second, id));
    const view = await second.links(id, "kv");
    assert.deepEqual(view.links, [], json(view));
  },
);

When(
  /^the session stays quiet for (\d+) seconds$/,
  async function (this: LaserWorld, seconds: string) {
    await new Promise((resolve) => setTimeout(resolve, Number(seconds) * 1000));
  },
);

Then(
  "the indexed session is live with a recent heartbeat",
  async function (this: LaserWorld) {
    const sessions = sessionsOf(this);
    const id = this.requireSession().conversation;
    const info = await eventually("the heartbeat liveness", async () => {
      const info = await indexed(sessions, id);
      return info !== undefined &&
        !info.flags.livenessUnknown &&
        info.lastHeartbeatAt !== undefined
        ? info
        : undefined;
    });
    assert.equal(info.status, "active", json(info));
    assert.equal(info.idle, false, json(info));
    assert.ok(
      (info.lastHeartbeatAt ?? 0n) > (info.lastEventAt ?? 0n),
      json(info),
    );
  },
);

Then(
  "no indexed event of the session is a heartbeat",
  async function (this: LaserWorld) {
    const page = await sessionsOf(this)
      .events(this.requireSession().conversation)
      .fetch();
    assert.ok(page.items.length > 0);
    for (const event of page.items) {
      assert.ok(
        !event.display.includes("heartbeat") &&
          !event.kind.includes("heartbeat"),
        json(event),
      );
    }
  },
);

When("the session lease is released", function (this: LaserWorld) {
  const state = managed(this);
  const lease = state.lease ?? this.sessionLease;
  assert.ok(lease !== undefined, "a held lease");
  lease.release();
  delete state.lease;
});

Then("the indexed session becomes idle", async function (this: LaserWorld) {
  const sessions = sessionsOf(this);
  const id = this.requireSession().conversation;
  const info = await eventually("the idle session", async () => {
    const info = await indexed(sessions, id);
    return info?.idle === true ? info : undefined;
  });
  assert.equal(info.status, "active", "idle never overwrites a status");
});

When(
  /^the session streams (\d+) chunks in one batch$/,
  async function (this: LaserWorld, chunks: string) {
    const session = this.requireSession();
    const author = session.agent;
    assert.ok(author !== undefined, "the session has an agent");
    const stream = laserOf(this)
      .agdx(AgentTopic.Sessions, author, session.conversation)
      .stream(CorrelationId.fromU128(99n), "chat")
      .buffered(Number(chunks), 60_000);
    for (let index = 0; index < Number(chunks); index += 1) {
      await stream.write(encoder.encode(`chunk-${String(index)}`));
    }
    await stream.flush();
  },
);

Then(
  /^the index counts (\d+) records for the session$/,
  async function (this: LaserWorld, records: string) {
    const sessions = sessionsOf(this);
    const id = this.requireSession().conversation;
    const info = await eventually("the session counters", async () => {
      const info = await indexed(sessions, id);
      return info !== undefined && info.events >= BigInt(records)
        ? info
        : undefined;
    });
    assert.equal(info.events, BigInt(records), json(info));
  },
);

Then(
  /^the change feed names the session in at most (\d+) rows$/,
  async function (this: LaserWorld, rows: string) {
    const id = this.requireSession().conversation;
    const changes = await sessionsOf(this).changes(0n, 0);
    const naming = changes.rows.filter((row) =>
      row.sessions.some((session) => same(session, id)),
    );
    assert.ok(naming.length >= 1, json(changes));
    assert.ok(
      naming.length <= Number(rows),
      `one row per fold batch, not per record: ${json(changes)}`,
    );
  },
);

Then(
  "the change feed never names the second stream's session",
  async function (this: LaserWorld) {
    const state = managed(this);
    const second = state.second;
    const other = state.secondSession;
    assert.ok(second !== undefined && other !== undefined);
    // The second stream's own feed names it, so both folds have run.
    await eventually("the second stream's feed", async () => {
      const changes = await second.changes(0n, 0);
      return changes.rows.some((row) =>
        row.sessions.some((session) => same(session, other)),
      )
        ? changes
        : undefined;
    });
    const changes = await sessionsOf(this).changes(0n, 0);
    assert.ok(
      changes.rows.every(
        (row) => !row.sessions.some((session) => same(session, other)),
      ),
      json(changes),
    );
  },
);

When(
  "a watch follows the stream's session changes",
  async function (this: LaserWorld) {
    const state = managed(this);
    const abort = new AbortController();
    const watch = await sessionsOf(this).watch(250);
    // A follower awaits its next change from the start, as a console would.
    state.watch = watch.next({ signal: abort.signal });
    state.watch.catch(() => undefined);
    state.watchAbort = abort;
  },
);

Then(
  /^listing the stream finds "([^"]+)" as completed$/,
  async function (this: LaserWorld, label: string) {
    const sessions = sessionsOf(this);
    const id = idOf(this, label);
    const page = await eventually("the listed session", async () => {
      const page = await sessions.list().fetch();
      return page.items.some(
        (item) => same(item.id, id) && item.status === "completed",
      )
        ? page
        : undefined;
    });
    assert.equal(page.items.length, 1, json(page));
  },
);

Then(
  /^the indexed events of the session are (".+")$/,
  async function (this: LaserWorld, list: string) {
    const expected = list.split(", ").map((name) => name.replace(/^"|"$/g, ""));
    const sessions = sessionsOf(this);
    const id = this.requireSession().conversation;
    const displays = await eventually("the indexed events", async () => {
      const page = await sessions.events(id).fetch();
      const displays = page.items.map((event) => event.display);
      return displays.length >= expected.length ? displays : undefined;
    });
    assert.deepEqual(displays, expected);
  },
);

Then(
  "the session sources show folded offsets within their heads",
  async function (this: LaserWorld) {
    const sources = await sessionsOf(this).sources(
      this.requireSession().conversation,
    );
    assert.ok(
      sources.sources.some((source) => source.folded !== undefined),
      json(sources),
    );
    for (const source of sources.sources) {
      if (source.folded !== undefined && source.head !== undefined)
        assert.ok(source.folded <= source.head, json(source));
    }
  },
);

Then("the session has no links", async function (this: LaserWorld) {
  const view = await sessionsOf(this).links(this.requireSession().conversation);
  assert.deepEqual(view.links, [], json(view));
});

Then("the watch reports the session", async function (this: LaserWorld) {
  const state = managed(this);
  assert.ok(state.watch !== undefined, "a watch");
  let timer: NodeJS.Timeout | undefined;
  const change = (await Promise.race([
    state.watch,
    new Promise((_resolve, reject) => {
      timer = setTimeout(() => {
        reject(new Error("the watch did not report within the deadline"));
      }, CONVERGE_MS);
    }),
  ]).finally(() => {
    clearTimeout(timer);
  })) as { kind: string; sessions?: readonly { toString(): string }[] };
  assert.equal(change.kind, "changed", json(change));
  const id = this.requireSession().conversation;
  assert.ok(
    change.sessions?.some((session) => same(session, id)),
    json(change),
  );
});

Given(
  /^agent "([^"]+)" counts the work it handles$/,
  async function (this: LaserWorld, name: string) {
    const state = managed(this);
    const worker = Agent.builder()
      .id(AgentId.new(name))
      .listenOn(AgentTopic.Sessions)
      .respondOn(AgentTopic.Sessions)
      .pollInterval(5)
      .handler({
        handle: (message, context) => {
          // Records the session of every work record it is handed.
          state.handled.push(message.provenance.conversationId);
          return context.respond(encoder.encode("{}"));
        },
      })
      .build()
      .spawn(laserOf(this));
    await worker.ready();
    state.worker = worker;
  },
);

When(
  /^agent "([^"]+)" starts the session "([^"]+)" with a budget of (\d+) tokens$/,
  async function (
    this: LaserWorld,
    owner: string,
    label: string,
    tokens: string,
  ) {
    const { session, lease } = await sessionsOf(this)
      .create(label)
      .agent(AgentId.new(owner))
      .budget({ tokens: BigInt(tokens) })
      .begin();
    managed(this).labels.set(label, session.conversation);
    this.session = session;
    managed(this).lease = lease;
  },
);

When(
  /^the session records a model call that used (\d+) tokens$/,
  async function (this: LaserWorld, tokens: string) {
    await this.requireSession().recordModelCall(
      new ModelRequest("gpt-test", encoder.encode("q")),
      {
        body: encoder.encode("a"),
        usage: { inputTokens: BigInt(tokens), outputTokens: 0n },
      },
    );
  },
);

Then(
  /^the indexed session "([^"]+)" is over its budget$/,
  async function (this: LaserWorld, label: string) {
    const sessions = sessionsOf(this);
    const id = idOf(this, label);
    await eventually("the over-budget flag", async () =>
      (await indexed(sessions, id))?.overBudget === true ? true : undefined,
    );
    assert.ok(
      await sessions.open(id).overBudget(),
      "the session lens reads the flag from the index",
    );
  },
);

When(
  /^agent "([^"]+)" sends the session (\d+) work records? for "([^"]+)"$/,
  async function (
    this: LaserWorld,
    owner: string,
    count: string,
    target: string,
  ) {
    const laser = laserOf(this);
    const conversation = this.requireSession().conversation;
    for (let index = 0; index < Number(count); index += 1) {
      await laser
        .agdx(AgentTopic.Sessions, AgentId.new(owner), conversation)
        .command(
          CorrelationId.fromU128(BigInt(Date.now()) * 1000n + BigInt(index)),
          encoder.encode("{}"),
        )
        .withTarget(AgentId.new(target))
        .send();
    }
  },
);

Then(
  /^agent "([^"]+)" handles only the work of the session "([^"]+)"$/,
  async function (this: LaserWorld, _name: string, label: string) {
    const id = idOf(this, label);
    const handled = managed(this).handled;
    await eventually("the handled work", () =>
      Promise.resolve(
        handled.some((session) => same(session, id)) ? true : undefined,
      ),
    );
    assert.ok(
      handled.every((session) => same(session, id)),
      json(handled.map(String)),
    );
  },
);

Then(
  /^the session "([^"]+)" ends failed once with reason "([^"]+)"$/,
  async function (this: LaserWorld, label: string, reason: string) {
    const lens = sessionsOf(this).open(idOf(this, label));
    const context = "session end";
    const ends = await eventually("the failed terminal", async () => {
      const failed = (await lens.context()).flatMap((turn) => {
        const envelope = turn.message.envelope;
        return envelope?.kind === wire.AgentKind.Status &&
          envelope.operation === "session" &&
          envelope.taskState?.kind === "known" &&
          envelope.taskState.name === "Failed"
          ? [
              wire.decodeSessionEnd(
                wire.expectMap(wire.decodeOne(envelope.body, context), context),
                context,
              ),
            ]
          : [];
      });
      return failed.length > 0 ? failed : undefined;
    });
    assert.equal(ends.length, 1, json(ends));
    assert.equal(ends[0]?.reason, reason);
    assert.ok(ends[0]?.error?.message?.includes("budget"), json(ends));
  },
);

After(async function (this: LaserWorld) {
  const state = scenarios.get(this);
  if (state === undefined) return;
  state.watchAbort?.abort("scenario cleanup");
  state.lease?.release();
  if (state.worker !== undefined) await state.worker.shutdown();
  if (state.secondLaser !== undefined) await state.secondLaser.close();
  scenarios.delete(this);
});

When(
  "I retain the session and state handles for lane identity checks",
  async function (this: LaserWorld) {
    const state = managed(this);
    const session = this.requireSession();
    await session.state().set("before", 1);
    state.retained = session.asAgent(AgentId.new("planner"));
    state.retainedState = session.state();
    state.lease?.release();
    delete state.lease;
    this.sessionLease?.release();
    delete this.sessionLease;
  },
);

When(
  /^the session source changes its (.+)$/,
  async function (this: LaserWorld, change: string) {
    const state = managed(this);
    const count = state.layout === "single partition" ? 1 : 4;
    const helper = fileURLToPath(
      new URL("../../../session-lane-admin.mjs", import.meta.url),
    );
    await promisify(execFile)(
      "rtk",
      [
        "proxy",
        "node",
        helper,
        this.endpoint,
        this.requireSession().stream(),
        change,
        String(count),
      ],
      { timeout: 60_000 },
    );
  },
);

const staleLaneError = (error: unknown): boolean =>
  error instanceof SessionError && error.detail.kind === "stale";

async function firstSourceReference(
  sessions: Sessions,
  session: Session,
): Promise<Extract<wire.SourceRef, { kind: "message" }>> {
  const first = await eventually(
    "the session's first source record",
    async () => {
      return (await session.context())[0]?.message;
    },
  );
  const [topic, generation] = await eventually(
    "the registered lane identity",
    async () => {
      return (await sessions.sources(session.conversation)).lane;
    },
  );
  assert.equal(topic, first.topicId);
  return {
    kind: "message",
    stream: first.streamId,
    topic,
    partition: first.id.partitionId,
    offset: first.id.offset,
    generation,
  };
}

When(
  "I retain a generation-bearing reference to its first record",
  async function (this: LaserWorld) {
    const at = await firstSourceReference(
      sessionsOf(this),
      this.requireSession(),
    );
    assert.ok((await laserOf(this).readAt(at)) !== undefined);
    const state = managed(this);
    state.retainedSource = at;
    state.lease?.release();
    delete state.lease;
    this.sessionLease?.release();
    delete this.sessionLease;
  },
);

When(
  "a replacement session records its start at the retained source address",
  async function (this: LaserWorld) {
    const state = managed(this);
    const sessions = state.second;
    assert.ok(sessions !== undefined && state.retainedSource !== undefined);
    const { session, lease } = await sessions
      .start()
      .withId(this.requireSession().conversation)
      .agent(AgentId.new("planner"))
      .begin();
    lease.release();
    const at = await firstSourceReference(sessions, session);
    const original = state.retainedSource;
    for (const key of ["stream", "topic", "partition", "offset"] as const) {
      assert.equal(
        original[key],
        at[key],
        "the recreated source reuses the record address",
      );
    }
    assert.notEqual(original.generation, at.generation);
    assert.ok((await laserOf(this).readAt(at)) !== undefined);
  },
);

Then(
  "reading the retained source reference returns no replacement record",
  async function (this: LaserWorld) {
    const at = managed(this).retainedSource;
    assert.ok(at !== undefined);
    assert.equal(await laserOf(this).readAt(at), undefined);
  },
);

Then(
  "retained session handles refuse lifecycle and state writes as stale",
  async function (this: LaserWorld) {
    const state = managed(this);
    const session = this.requireSession();
    assert.ok(
      state.retained !== undefined && state.retainedState !== undefined,
    );
    const before = (await session.context()).length;
    await assert.rejects(session.cancel(), staleLaneError);
    await assert.rejects(state.retained.cancel(), staleLaneError);
    await assert.rejects(state.retainedState.set("after", 2), staleLaneError);
    await assert.rejects(state.retainedState.snapshot(), staleLaneError);
    const envelope = wire.eventEnvelope(
      wire.RecordId.fromU128(1n),
      wire.ConversationId.parse(session.conversation.toString()),
      wire.parseAgentId("planner"),
      new Uint8Array([1]),
    );
    await assert.rejects(session.append(envelope), staleLaneError);
    await assert.rejects(
      sessionsOf(this).start().agent(AgentId.new("planner")).begin(),
      staleLaneError,
    );
    await assert.rejects(
      sessionsOf(this)
        .submit(AgentId.new("worker"), new Uint8Array([1]))
        .from(AgentId.new("planner"))
        .send(),
      staleLaneError,
    );
    assert.equal(
      (await session.context()).length,
      before,
      "refused writes append no records",
    );
  },
);

Then(
  "a fresh handle refuses the stale lane registration",
  async function (this: LaserWorld) {
    const session = this.requireSession();
    const before = (await session.context()).length;
    await assert.rejects(
      laserOf(this)
        .sessions()
        .open(session.conversation)
        .asAgent(AgentId.new("planner"))
        .cancel(),
      staleLaneError,
    );
    assert.equal((await session.context()).length, before);
  },
);

When(
  "the session source is explicitly removed and registered again",
  async function (this: LaserWorld) {
    const laser = laserOf(this);
    const session = this.requireSession();
    const payload = wire.encodeNamed(
      wire.encodeControlEnvelope({
        v: wire.CONTROL_OP_VERSION,
        timestampMicros: BigInt(Date.now()) * 1000n,
        command: { kind: "removeSessionSource", stream: session.stream() },
      }),
    );
    await laser
      .stream(laser.opsStream)
      .topic(laser.controlTopic)
      .send(payload, { key: new TextEncoder().encode("control") });
    await eventually("source registration removal", async () => {
      try {
        await laser.sessions().sources(session.conversation);
      } catch (error) {
        if (
          error instanceof SessionError &&
          error.detail.kind === "notRegistered"
        )
          return true;
      }
      return undefined;
    });
    const state = managed(this);
    const config = sessionsOf(this).config;
    const sessions = laser.sessions(config);
    assert.ok(
      (await sessions.bootstrap(4, TopicRetention.expireAfter(ONE_DAY_MS)))
        .registered,
    );
    state.second = sessions;
  },
);

Then(
  "a fresh session can write lifecycle and state on the recovered lane",
  async function (this: LaserWorld) {
    const sessions = managed(this).second;
    assert.ok(sessions !== undefined);
    const { session, lease } = await sessions
      .start()
      .agent(AgentId.new("planner"))
      .begin();
    await session.state().set("after", 2);
    await session.end();
    lease.release();
    await eventually("the recovered session state", async () => {
      const view = await sessions.state(session.conversation, 10);
      return json(view.document) === json({ after: 2 }) ? true : undefined;
    });
    const info = await eventually(
      "the recovered session completion",
      async () => {
        const info = await sessions.get(session.conversation);
        return info.status === "completed" ? info : undefined;
      },
    );
    assert.equal(info.flags.laneConflict, false);
  },
);

Then(
  "retained session handles still refuse the recovered lane",
  async function (this: LaserWorld) {
    const state = managed(this);
    assert.ok(
      state.retained !== undefined && state.retainedState !== undefined,
    );
    await assert.rejects(state.retained.cancel(), staleLaneError);
    await assert.rejects(state.retainedState.set("after", 3), staleLaneError);
  },
);

Then(
  "the old lease publishes no heartbeat into the recreated stream",
  async function (this: LaserWorld) {
    await new Promise((resolve) => setTimeout(resolve, 3000));
    const cursor = await laserOf(this).topic(AgentTopic.Heartbeats).replay();
    assert.deepEqual(
      await cursor.poll(),
      [],
      "an old lease must not publish a new-generation heartbeat",
    );
    managed(this).lease?.release();
    delete managed(this).lease;
    this.sessionLease?.release();
    delete this.sessionLease;
  },
);

When(
  "a native lane writer without session read grants writes lifecycle and state",
  async function (this: LaserWorld) {
    const stream = sessionsOf(this).stream();
    const username = `writer-${stream}`;
    const helper = fileURLToPath(
      new URL("../../../session-lane-admin.mjs", import.meta.url),
    );
    await promisify(execFile)(
      "rtk",
      [
        "proxy",
        "node",
        helper,
        this.endpoint,
        stream,
        "writer credentials",
        "4",
        username,
      ],
      { timeout: 60_000 },
    );
    const address = this.endpoint.split("@").at(-1);
    assert.ok(address !== undefined);
    const writer = await Laser.connectWithStream(
      `${username}:lane-writer-test@${address}`,
      stream,
    );
    const { session, lease } = await writer
      .sessions()
      .create("writer")
      .agent(AgentId.new("planner"))
      .begin();
    await session.state().set("step", 1);
    await session.end();
    lease.release();
    managed(this).labels.set("writer", session.conversation);
    managed(this).writer = writer;
  },
);

Then(
  "aggregate session reads remain refused for that writer",
  async function (this: LaserWorld) {
    const state = managed(this);
    const writer = state.writer;
    assert.ok(writer !== undefined);
    const id = idOf(this, "writer");
    await assert.rejects(writer.sessions().sources(id), isPermissionDenied);
    await assert.rejects(writer.sessions().get(id), isPermissionDenied);
    await writer.close();
    delete state.writer;
  },
);
