import assert from "node:assert/strict";
import { Given, Then, When } from "@cucumber/cucumber";
import {
  Agent,
  AgentId,
  AgentTopic,
  ConversationId,
  Laser,
  type MemoryHandle,
  StepFailedError,
  type InboxRoute,
  type ScratchStream,
  type StepContext,
  type Workflow,
  isUnavailable,
  retryBackoff,
  routeTo,
  routeToCapable,
} from "@laserdata/laser-sdk";
import { eventual } from "../support/eventual.js";
import type { LaserWorld } from "../world.js";

const decoder = new TextDecoder();
const FIXED_SESSIONS: InboxRoute = { kind: "fixed", topic: AgentTopic.Sessions };

Given(
  "a fresh stream bootstrapped with the defaults",
  async function (this: LaserWorld) {
    await this.connect();
    await this.requireLaser().bootstrap();
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

Given("a client without a default stream", async function (this: LaserWorld) {
  this.laser = await Laser.connect(this.endpoint);
});

When(
  "I bootstrap the stream with the defaults",
  async function (this: LaserWorld) {
    await this.requireLaser().bootstrap();
  },
);

Then(
  /^topic "([^"]+)" has (\d+) partitions? and expires records after (\d+) seconds$/,
  async function (
    this: LaserWorld,
    topic: string,
    partitions: string,
    seconds: string,
  ) {
    const info = await this.requireLaser().topic(topic).get();
    assert.deepEqual(
      [info?.partitionsCount, info?.messageExpiryMs],
      [Number(partitions), Number(seconds) * 1_000],
    );
  },
);

When(
  /^I publish "([^"]+)" to topic "([^"]+)" of the default stream$/,
  async function (this: LaserWorld, body: string, topic: string) {
    await this.capture(() =>
      this.requireLaser()
        .topic(topic)
        .publish()
        .payload(new TextEncoder().encode(body))
        .send(),
    );
  },
);

Then(
  "the error names the call that sets a default stream",
  function (this: LaserWorld) {
    const error = errorText(this);
    assert.ok(error.includes("scratchStream(prefix)"), error);
    assert.ok(error.includes("withDefaultStream(name)"), error);
  },
);

Then(
  "the error names the agent capabilities and the fixed inbox route",
  function (this: LaserWorld) {
    const text = errorText(this);
    assert.match(text, /start the agent with capabilities/u);
    assert.ok(
      text.includes('inboxRoute({ kind: "fixed", topic })'),
      `the error names no fixed inbox route: ${text}`,
    );
  },
);

When(
  /^I open a scratch stream with the prefix "([^"]+)"$/,
  async function (this: LaserWorld, prefix: string) {
    this.scratches.push(await this.requireLaser().scratchStream(prefix));
  },
);

When(
  /^I open two scratch streams with the prefix "([^"]+)"$/,
  async function (this: LaserWorld, prefix: string) {
    for (const _ of [1, 2])
      this.scratches.push(await this.requireLaser().scratchStream(prefix));
  },
);

Then(
  /^the scratch stream is named "([^"]+)" and 8 lowercase hex characters$/,
  function (this: LaserWorld, prefix: string) {
    const name = currentScratch(this).name;
    assert.ok(name.startsWith(prefix), name);
    assert.match(name.slice(prefix.length), /^[0-9a-f]{8}$/u);
  },
);

Then("the scratch stream exists", async function (this: LaserWorld) {
  assert.equal(
    await this.requireLaser().stream(currentScratch(this).name).exists(),
    true,
  );
});

Then("the scratch stream no longer exists", async function (this: LaserWorld) {
  assert.equal(
    await this.requireLaser().stream(currentScratch(this).name).exists(),
    false,
  );
});

Then(
  "the scratch handle's default stream is the scratch stream",
  function (this: LaserWorld) {
    const scratch = currentScratch(this);
    assert.equal(scratch.laser.defaultStream, scratch.name);
  },
);

When("I delete the scratch stream", async function (this: LaserWorld) {
  await currentScratch(this).delete();
});

Then(
  "deleting the scratch stream again succeeds",
  async function (this: LaserWorld) {
    await currentScratch(this).delete();
  },
);

Then(
  "the two scratch streams have different names",
  function (this: LaserWorld) {
    const [first, second] = this.scratches.slice(-2);
    assert.ok(first !== undefined && second !== undefined);
    assert.notEqual(first.name, second.name);
  },
);

Then("the memory topic exists on the stream", async function (this: LaserWorld) {
  assert.equal(
    await this.requireLaser().topic(AgentTopic.Memory).exists(),
    true,
  );
});

When(/^the user states "([^"]+)"$/, async function (this: LaserWorld, body: string) {
  this.lastMemoryId = await requireMemory(this).remember(body).user(requireUser(this)).send();
});

When(
  /^the user states "([^"]+)" in conversation "([^"]+)"$/,
  async function (this: LaserWorld, body: string, label: string) {
    this.lastMemoryId = await requireMemory(this)
      .remember(body)
      .scope(labeledConversation(this, label))
      .user(requireUser(this))
      .send();
  },
);

When(
  /^the user corrects it to "([^"]+)" in conversation "([^"]+)"$/,
  async function (this: LaserWorld, body: string, label: string) {
    const old = this.lastMemoryId;
    assert.ok(old !== undefined, "nothing was remembered to supersede");
    const id = await requireMemory(this)
      .supersede(old, body)
      .scope(labeledConversation(this, label))
      .user(requireUser(this))
      .send();
    this.lastMemoryId = id;
  },
);

Then(
  /^a folded recall for the user returns "([^"]+)" only$/,
  async function (this: LaserWorld, only: string) {
    const items = await requireMemory(this)
      .recall()
      .user(requireUser(this))
      .folded()
      .fetch();
    assert.deepEqual(texts(items), [only]);
  },
);

Then(
  /^a recall for the user returns "([^"]+)" only$/,
  async function (this: LaserWorld, only: string) {
    const items = await requireMemory(this)
      .recall()
      .user(requireUser(this))
      .fetch();
    assert.deepEqual(texts(items), [only]);
  },
);

Then(
  /^a folded semantic recall for "([^"]+)" returns (.+)$/,
  async function (this: LaserWorld, query: string, expected: string) {
    const items = await requireMemory(this)
      .recall()
      .user(requireUser(this))
      .semantic(query)
      .folded()
      .fetch();
    assert.deepEqual(texts(items), quoted(expected));
  },
);

Then(
  /^a folded keyword recall for "([^"]+)" returns "([^"]+)" only$/,
  async function (this: LaserWorld, query: string, only: string) {
    const items = await requireMemory(this)
      .recall()
      .user(requireUser(this))
      .keyword(query)
      .folded()
      .fetch();
    assert.deepEqual(texts(items), [only]);
  },
);

Given(
  /^agent "([^"]+)" answers workflow steps on the session topic$/,
  async function (this: LaserWorld, name: string) {
    await spawnAnswering(this, name, []);
  },
);

Given(
  /^agent "([^"]+)" advertising the skill "([^"]+)" answers workflow steps on the session topic$/,
  async function (this: LaserWorld, name: string, skill: string) {
    await spawnAnswering(this, name, [skill]);
  },
);

Given(
  /^agent "([^"]+)" fails every workflow step on the session topic$/,
  async function (this: LaserWorld, name: string) {
    const agent = Agent.builder()
      .id(AgentId.new(name))
      .listenOn(AgentTopic.Sessions)
      .respondOn(AgentTopic.Sessions)
      .retry(retryBackoff(1, 1))
      .handler({
        handle: () => Promise.reject(new Error(`${name} refuses every step`)),
      })
      .build()
      .spawn(this.requireLaser());
    await agent.ready();
    this.responders.push(agent);
  },
);

When(
  /^the workflow "([^"]+)" runs "([^"]+)", then "([^"]+)" after "([^"]+)", then "([^"]+)" after "([^"]+)" on "([^"]+)"$/,
  async function (
    this: LaserWorld,
    name: string,
    first: string,
    second: string,
    secondAfter: string,
    third: string,
    thirdAfter: string,
    agent: string,
  ) {
    const workflow = routedWorkflow(this, name);
    const target = routeTo(AgentId.new(agent));
    workflow.step(first, target, () => first);
    workflow.step(second, target, () => second).after(secondAfter);
    workflow.step(third, target, () => third).after(thirdAfter);
    await runWorkflow(this, workflow);
  },
);

When(
  /^the workflow "([^"]+)" runs the step "([^"]+)" on "([^"]+)" with a verifier that rejects every reply$/,
  async function (this: LaserWorld, name: string, step: string, agent: string) {
    const workflow = routedWorkflow(this, name);
    workflow
      .step(step, routeTo(AgentId.new(agent)), () => step)
      .verifyWith(() => false);
    await runWorkflow(this, workflow);
  },
);

When(
  /^the workflow "([^"]+)" runs "([^"]+)" and "([^"]+)" on "([^"]+)" and then "([^"]+)" on "([^"]+)", each with a compensation$/,
  async function (
    this: LaserWorld,
    name: string,
    first: string,
    second: string,
    agent: string,
    last: string,
    lastAgent: string,
  ) {
    const workflow = routedWorkflow(this, name);
    for (const [label, on] of [
      [first, agent],
      [second, agent],
      [last, lastAgent],
    ] as const) {
      workflow
        .step(label, routeTo(AgentId.new(on)), () => label)
        .compensateWith(() => `undo ${label}`);
    }
    await runWorkflow(this, workflow);
  },
);

When(
  /^the workflow "([^"]+)" runs the step "([^"]+)" on an agent with the skill "([^"]+)"$/,
  async function (this: LaserWorld, name: string, step: string, skill: string) {
    const workflow = routedWorkflow(this, name);
    workflow.step(step, routeToCapable(skill, { kind: "any" }), () => step);
    await runWorkflow(this, workflow);
  },
);

When(
  /^the workflow "([^"]+)" runs the step "([^"]+)" on "([^"]+)" with a deadline of (\d+) milliseconds$/,
  async function (
    this: LaserWorld,
    name: string,
    step: string,
    agent: string,
    deadlineMs: string,
  ) {
    const workflow = routedWorkflow(this, name);
    workflow
      .step(step, routeTo(AgentId.new(agent)), () => step)
      .deadline(Number(deadlineMs));
    await runWorkflow(this, workflow);
  },
);

When(
  /^the workflow "([^"]+)" runs the step "([^"]+)" on "([^"]+)" through the advertised inbox route$/,
  async function (this: LaserWorld, name: string, step: string, agent: string) {
    const workflow = this.requireLaser().workflow(name);
    workflow.step(step, routeTo(AgentId.new(agent)), () => step);
    await runWorkflow(this, workflow);
  },
);

When(
  /^the workflow "([^"]+)" runs the local steps "([^"]+)", "([^"]+)", "([^"]+)"$/,
  async function (
    this: LaserWorld,
    name: string,
    first: string,
    second: string,
    third: string,
  ) {
    await runWorkflow(
      this,
      localChain(this, name, [first, second, third], () => false),
    );
  },
);

When(
  /^the workflow "([^"]+)" runs the local steps "([^"]+)", "([^"]+)", "([^"]+)" and step "([^"]+)" fails once$/,
  async function (
    this: LaserWorld,
    name: string,
    first: string,
    second: string,
    third: string,
    failing: string,
  ) {
    let failed = false;
    const failsNow = (label: string): boolean => {
      if (label !== failing || failed) return false;
      failed = true;
      return true;
    };
    await runWorkflow(
      this,
      localChain(this, name, [first, second, third], failsNow),
    );
    assert.ok(this.workflowError !== undefined, "the first run did not fail");
  },
);

When(
  /^the workflow "([^"]+)" resumes the same run of local steps$/,
  async function (this: LaserWorld, _name: string) {
    const workflow = this.workflow;
    assert.ok(workflow !== undefined, "no workflow ran before the resume");
    // The engine keeps the run id of its last run, so running it again resumes that run.
    await runWorkflow(this, workflow);
  },
);

When(
  /^the workflow "([^"]+)" runs the local steps "([^"]+)" and "([^"]+)" with compensations and then "([^"]+)" which fails$/,
  async function (
    this: LaserWorld,
    name: string,
    first: string,
    second: string,
    last: string,
  ) {
    const workflow = this.requireLaser().workflow(name);
    for (const label of [first, second]) {
      workflow
        .stepLocal(label, () => `${label} done in process`)
        .compensateWith(() => {
          this.localUndone.push(`undo ${label}`);
          return "";
        });
    }
    workflow.stepLocal(last, () => {
      throw new Error(`${last} failed`);
    });
    await runWorkflow(this, workflow);
  },
);

Then(
  /^the workflow completed the steps (.+) in order$/,
  function (this: LaserWorld, labels: string) {
    assert.ok(
      this.workflowOutcome !== undefined,
      `the workflow failed: ${String(this.workflowError)}`,
    );
    assert.equal(this.workflowOutcome.status, "completed");
    assert.deepEqual(this.workflowOutcome.completedSteps, quoted(labels));
  },
);

Then(
  /^the workflow output (?:text )?of step "([^"]+)" is "([^"]+)"$/,
  function (this: LaserWorld, label: string, expected: string) {
    assert.equal(this.workflowOutcome?.text(label), expected);
  },
);

Then(
  /^the workflow fails at step "([^"]+)" with reason "([^"]+)"$/,
  function (this: LaserWorld, step: string, reason: string) {
    const error = this.workflowError;
    assert.ok(error instanceof StepFailedError, String(error));
    assert.deepEqual([error.step, error.reason], [step, reason]);
  },
);

Then(
  /^the failure names the target "([^"]+)" and a deadline of (\d+) milliseconds$/,
  function (this: LaserWorld, agent: string, deadlineMs: string) {
    const error = this.workflowError;
    assert.ok(error instanceof StepFailedError, String(error));
    assert.deepEqual(
      [error.target, error.deadlineMs],
      [`agent \`${agent}\``, Number(deadlineMs)],
    );
    assert.ok(
      error.message.endsWith(
        `target agent \`${agent}\`, deadline ${deadlineMs} ms`,
      ),
      error.message,
    );
  },
);

Then(
  /^agent "([^"]+)" received the compensations "([^"]+)" then "([^"]+)"$/,
  function (this: LaserWorld, agent: string, first: string, second: string) {
    assert.deepEqual(this.compensations.get(agent), [first, second]);
  },
);

Then(
  /^the workflow fails with the local step error "([^"]+)"$/,
  function (this: LaserWorld, message: string) {
    const error = this.workflowError;
    assert.ok(error instanceof Error, String(error));
    assert.equal(error.message, message);
  },
);

Then(
  /^the local compensations ran "([^"]+)" then "([^"]+)"$/,
  function (this: LaserWorld, first: string, second: string) {
    assert.deepEqual(this.localUndone, [first, second]);
  },
);

Then(
  /^the local step bodies ran (.+)$/,
  function (this: LaserWorld, labels: string) {
    assert.deepEqual(this.localRan, quoted(labels));
  },
);

// An agent that answers a step body "<body> done by <agent>" and records every
// compensation body "undo <label>" it receives.
async function spawnAnswering(
  world: LaserWorld,
  name: string,
  skills: readonly string[],
): Promise<void> {
  const received: string[] = [];
  world.compensations.set(name, received);
  const builder = Agent.builder()
    .id(AgentId.new(name))
    .listenOn(AgentTopic.Sessions)
    .respondOn(AgentTopic.Sessions)
    .handler({
      handle: (message, context) => {
        const body = decoder.decode(message.envelope?.body ?? message.payload);
        if (body.startsWith("undo ")) received.push(body);
        return context.respond(new TextEncoder().encode(`${body} done by ${name}`));
      },
    });
  const agent = (
    skills.length === 0
      ? builder
      : builder.capabilities(skills.map((skillId) => ({ skillId })))
  )
    .build()
    .spawn(world.requireLaser());
  await agent.ready();
  world.responders.push(agent);
}

function routedWorkflow(world: LaserWorld, name: string): Workflow {
  return world.requireLaser().workflow(name).inboxRoute(FIXED_SESSIONS);
}

// Local steps that each run after the previous one, recording the bodies they
// ran, a step `fails(label)` names throwing instead.
function localChain(
  world: LaserWorld,
  name: string,
  labels: readonly string[],
  fails: (label: string) => boolean,
): Workflow {
  const workflow = world.requireLaser().workflow(name);
  labels.forEach((label, index) => {
    const body = (_context: StepContext): string => {
      world.localRan.push(label);
      if (fails(label)) throw new Error(`${label} failed`);
      return `${label} done in process`;
    };
    const previous = labels[index - 1];
    const step = workflow.stepLocal(label, body);
    if (previous !== undefined) step.after(previous);
  });
  return workflow;
}

async function runWorkflow(world: LaserWorld, workflow: Workflow): Promise<void> {
  world.workflow = workflow;
  delete world.workflowOutcome;
  world.workflowError = undefined;
  try {
    world.workflowOutcome = await workflow.run({ signal: world.abort.signal });
  } catch (error) {
    world.workflowError = error;
  }
}

function currentScratch(world: LaserWorld): ScratchStream {
  const scratch = world.scratches.at(-1);
  if (scratch === undefined) throw new Error("scenario opened no scratch stream");
  return scratch;
}

function labeledConversation(world: LaserWorld, label: string): ConversationId {
  let conversation = world.conversationLabels.get(label);
  if (conversation === undefined) {
    conversation = ConversationId.new();
    world.conversationLabels.set(label, conversation);
  }
  return conversation;
}

// Every fact of a log memory scenario lives in memory "prefs" for the user "ana".
function requireMemory(world: LaserWorld): MemoryHandle {
  if (world.memory === undefined) {
    world.memory = world.requireLaser().memory("prefs");
    world.memoryUser = "ana";
  }
  return world.memory;
}

function requireUser(world: LaserWorld): string {
  if (world.memoryUser === undefined) throw new Error("scenario has no memory user");
  return world.memoryUser;
}

function errorText(world: LaserWorld): string {
  const error = world.error ?? world.workflowError;
  assert.ok(error instanceof Error, `the scenario raised no error: ${String(error)}`);
  return error.message;
}

function texts(items: readonly { readonly payload: Uint8Array }[]): readonly string[] {
  return items.map((item) => decoder.decode(item.payload));
}

// The quoted strings of a step list such as `"a", "b" then "c"`.
function quoted(list: string): readonly string[] {
  return [...list.matchAll(/"([^"]*)"/gu)].map((match) => match[1] ?? "");
}
