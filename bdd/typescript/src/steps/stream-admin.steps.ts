import assert from "node:assert/strict";
import { Then, When } from "@cucumber/cucumber";
import type { Topic } from "@laserdata/laser-sdk";
import type { LaserWorld } from "../world.js";

function uniqueStream(world: LaserWorld): string {
  const stream = world.stream;
  assert.ok(stream !== undefined, "the scenario connects with a stream");
  return `${stream}-admin`;
}

function topic(world: LaserWorld, name: string): Topic {
  return world.requireLaser().stream(uniqueStream(world)).topic(name);
}

When(
  /^I create the unique stream with topic "([^"]+)" of (\d+) partitions$/,
  async function (this: LaserWorld, name: string, partitions: string) {
    await topic(this, name).ensure(Number(partitions));
  },
);

Then(
  "the stream listing includes the unique stream",
  async function (this: LaserWorld) {
    const streams = await this.requireLaser().streams();
    assert.ok(streams.some((stream) => stream.name === uniqueStream(this)));
  },
);

Then(
  "the stream listing omits the unique stream",
  async function (this: LaserWorld) {
    const streams = await this.requireLaser().streams();
    assert.ok(streams.every((stream) => stream.name !== uniqueStream(this)));
  },
);

Then(
  /^the unique stream exists with (\d+) topics?$/,
  async function (this: LaserWorld, topics: string) {
    const stream = this.requireLaser().stream(uniqueStream(this));
    assert.equal(await stream.exists(), true);
    assert.equal((await stream.get())?.topicsCount, Number(topics));
  },
);

Then(
  /^the unique stream lists topic "([^"]+)" with (\d+) partitions$/,
  async function (this: LaserWorld, name: string, partitions: string) {
    const topics = await this.requireLaser()
      .stream(uniqueStream(this))
      .topics();
    const info = topics.find((candidate) => candidate.name === name);
    assert.equal(info?.partitionsCount, Number(partitions));
  },
);

When("I delete the unique stream", async function (this: LaserWorld) {
  assert.equal(
    await this.requireLaser().stream(uniqueStream(this)).delete(),
    true,
  );
});

Then("the unique stream no longer exists", async function (this: LaserWorld) {
  const stream = this.requireLaser().stream(uniqueStream(this));
  assert.equal(await stream.exists(), false);
  assert.equal(await stream.get(), undefined);
});

When(
  /^I add (\d+) partitions to topic "([^"]+)"$/,
  async function (this: LaserWorld, count: string, name: string) {
    await topic(this, name).addPartitions(Number(count));
  },
);

Then(
  /^topic "([^"]+)" has (\d+) partitions$/,
  async function (this: LaserWorld, name: string, count: string) {
    const handle = topic(this, name);
    assert.equal((await handle.get())?.partitionsCount, Number(count));
    assert.equal((await handle.partitions()).length, Number(count));
  },
);

When(
  /^I delete topic "([^"]+)"$/,
  async function (this: LaserWorld, name: string) {
    assert.equal(await topic(this, name).delete(), true);
  },
);

Then(
  /^topic "([^"]+)" no longer exists$/,
  async function (this: LaserWorld, name: string) {
    const handle = topic(this, name);
    assert.equal(await handle.exists(), false);
    assert.equal(await handle.get(), undefined);
  },
);

Then(
  /^a second delete of topic "([^"]+)" reports it missing$/,
  async function (this: LaserWorld, name: string) {
    assert.equal(await topic(this, name).delete(), false);
  },
);

When(
  /^I create consumer group "([^"]+)" on topic "([^"]+)"$/,
  async function (this: LaserWorld, group: string, name: string) {
    await topic(this, name).ensureConsumerGroup(group);
  },
);

Then(
  /^topic "([^"]+)" lists consumer group "([^"]+)"$/,
  async function (this: LaserWorld, name: string, group: string) {
    const groups = await topic(this, name).consumerGroups();
    assert.ok(groups.some((summary) => summary.name === group));
  },
);

When(
  /^I delete consumer group "([^"]+)" on topic "([^"]+)"$/,
  async function (this: LaserWorld, group: string, name: string) {
    assert.equal(await topic(this, name).consumerGroup(group).delete(), true);
  },
);

Then(
  /^topic "([^"]+)" lists no consumer groups$/,
  async function (this: LaserWorld, name: string) {
    assert.deepEqual(await topic(this, name).consumerGroups(), []);
  },
);
