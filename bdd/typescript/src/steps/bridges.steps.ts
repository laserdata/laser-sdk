import assert from "node:assert/strict";
import { Given, Then, When } from "@cucumber/cucumber";
import {
  A2aBridge,
  Agent,
  AgentId,
  AgentTopic,
  CorrelationId,
  ConversationId,
  InvalidError,
  McpBridge,
  enterBridge,
} from "@laserdata/laser-sdk";
import { wire } from "@laserdata/laser-sdk/full";

import type { LaserWorld } from "../world.js";

const encoder = new TextEncoder();

When(
  /^bridge "([^"]+)" enters after hops "([^"]+)"$/,
  function (this: LaserWorld, bridge: string, hops: string) {
    this.bridgeHops = enterBridge(bridge, hops.split(","));
  },
);

Then(
  /^the bridge hops are "([^"]+)"$/,
  function (this: LaserWorld, hops: string) {
    assert.deepEqual(this.bridgeHops, hops.split(","));
  },
);

When(
  /^bridge "([^"]+)" enters the same route$/,
  function (this: LaserWorld, bridge: string) {
    try {
      enterBridge(bridge, this.bridgeHops);
      this.bridgeLoopRejected = false;
    } catch (error) {
      assert.ok(error instanceof InvalidError);
      this.bridgeLoopRejected = true;
    }
  },
);

Then("the bridge route is rejected as a loop", function (this: LaserWorld) {
  assert.equal(this.bridgeLoopRejected, true);
});

When("I submit and cancel an A2A task", async function (this: LaserWorld) {
  const bridge = new A2aBridge(
    this.requireLaser(),
    AgentId.new("a2a-gateway"),
    AgentTopic.Sessions,
    AgentTopic.Sessions,
  );
  const submitted = await bridge.submit({
    message: { role: "user", text: "cancel me" },
  });
  await bridge.cancel(submitted.id);
  const replayed = await bridge.task(submitted.id);
  if (replayed.status.state.kind !== "known") {
    throw new Error("A2A task returned an unrecognized state");
  }
  this.bridgeTaskState = replayed.status.state.name;
});

Then(
  /^the replayed A2A task state is "([^"]+)"$/,
  function (this: LaserWorld, state: string) {
    assert.equal(this.bridgeTaskState, state);
  },
);

When(
  "I publish an AG-UI count snapshot of 1 and replace it with 2",
  async function (this: LaserWorld) {
    const laser = this.requireLaser();
    const conversation = this.requireConversation();
    const source = AgentId.new("agui-gateway");
    await laser.publishStateSnapshot(source, conversation, { count: 1 });
    await laser.publishStateDelta(source, conversation, [
      { op: "replace", path: "/count", value: 2 },
    ]);
    for (let attempt = 0; attempt < 80; attempt += 1) {
      const state = await laser.reconstructState(conversation);
      if (isRecord(state) && state["count"] === 2) {
        this.reconstructedState = state;
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    assert.fail("state delta did not become visible");
  },
);

Then(
  /^the reconstructed AG-UI count is (\d+)$/,
  function (this: LaserWorld, count: string) {
    assert.deepEqual(this.reconstructedState, { count: Number(count) });
  },
);

When(
  /^I stream chat chunks "([^"]+)" and "([^"]+)"$/,
  async function (this: LaserWorld, first: string, second: string) {
    const laser = this.requireLaser();
    const conversation = this.requireConversation();
    const stream = laser
      .agdx(AgentTopic.Sessions, AgentId.new("assistant"), conversation)
      .stream(
        CorrelationId.parse(conversation.toString()),
        wire.OPERATION_CHAT,
      );
    await stream.write(encoder.encode(first));
    await stream.write(encoder.encode(second));
    await stream.finish("stop");
    this.aguiEventTypes = (
      await laser.aguiEvents(conversation, AgentTopic.Sessions)
    ).map((event) => event.type);
  },
);

Then("AG-UI renders the chat lifecycle in order", function (this: LaserWorld) {
  assert.deepEqual(this.aguiEventTypes, [
    "TEXT_MESSAGE_START",
    "TEXT_MESSAGE_CONTENT",
    "TEXT_MESSAGE_CONTENT",
    "TEXT_MESSAGE_END",
  ]);
});

function isRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

Given(
  /^responders "([^"]+)" and "([^"]+)" answer every command with their own name$/,
  async function (this: LaserWorld, first: string, second: string) {
    for (const name of [first, second]) {
      const answered: string[] = [];
      const responder = Agent.builder()
        .id(AgentId.new(name))
        .listenOn(AgentTopic.Sessions)
        .pollInterval(5)
        .handler({
          async handle(message, context): Promise<void> {
            const envelope = message.envelope;
            if (envelope?.kind !== wire.AgentKind.Command) return;
            answered.push(
              envelope.tool !== undefined
                ? "tool"
                : envelope.operation === "chat"
                  ? "task"
                  : "input",
            );
            await context.respondInput(
              AgentTopic.Sessions,
              encoder.encode(name),
            );
          },
        })
        .build()
        .spawn(this.requireLaser());
      await responder.ready();
      this.responders.push(responder);
      this.answered.set(name, answered);
    }
  },
);

When(
  /^I submit an A2A task to "([^"]+)"$/,
  async function (this: LaserWorld, target: string) {
    const bridge = new A2aBridge(
      this.requireLaser(),
      AgentId.new("a2a-gateway"),
      AgentTopic.Sessions,
      AgentTopic.Sessions,
    );
    const task = await bridge.submit(
      { message: { role: "user", text: "addressed" } },
      { target: AgentId.new(target) },
    );
    this.bridgeTask = task.id;
  },
);

When(
  /^I call the MCP tool "([^"]+)" on "([^"]+)"$/,
  async function (this: LaserWorld, tool: string, target: string) {
    const bridge = new McpBridge(
      this.requireLaser(),
      AgentId.new("mcp-gateway"),
      AgentTopic.Sessions,
      AgentTopic.Sessions,
      "tools",
    ).withTimeout(15_000);
    const result = await bridge.callTool(
      tool,
      { q: "addressed" },
      { target: AgentId.new(target) },
    );
    this.toolResult = result.content[0]?.text ?? "";
  },
);

When(
  /^I request input from "([^"]+)"$/,
  async function (this: LaserWorld, target: string) {
    const decision = await this.requireLaser()
      .agdx(
        AgentTopic.Sessions,
        AgentId.new("orchestrator"),
        ConversationId.new(),
      )
      .requestInput(AgentTopic.Sessions, encoder.encode("approve?"), 15_000, {
        target: AgentId.new(target),
      });
    this.inputDecision = new TextDecoder().decode(decision);
  },
);

Then(
  /^the A2A task completes with "([^"]+)"$/,
  async function (this: LaserWorld, text: string) {
    const bridge = new A2aBridge(
      this.requireLaser(),
      AgentId.new("a2a-gateway"),
      AgentTopic.Sessions,
      AgentTopic.Sessions,
    );
    const id = this.bridgeTask;
    assert.ok(id !== undefined, "a task was submitted");
    for (let attempt = 0; attempt < 600; attempt += 1) {
      const task = await bridge.task(id);
      const state = task.status.state;
      if (state.kind === "known" && state.name === "Completed") {
        assert.equal(task.artifacts[0]?.text, text);
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    throw new Error("the addressed A2A task did not complete");
  },
);

Then(
  /^the MCP tool result is "([^"]+)"$/,
  function (this: LaserWorld, text: string) {
    assert.equal(this.toolResult, text);
  },
);

Then(
  /^the input decision is "([^"]+)"$/,
  function (this: LaserWorld, text: string) {
    assert.equal(this.inputDecision, text);
  },
);

Then(
  /^responder "([^"]+)" answered exactly (".+")$/,
  function (this: LaserWorld, name: string, list: string) {
    const expected = list.split(", ").map((label) => label.replaceAll('"', ""));
    assert.deepEqual(this.answered.get(name), expected, `responder ${name}`);
  },
);
