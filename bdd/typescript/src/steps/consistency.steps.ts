import assert from "node:assert/strict";
import { Then, When } from "@cucumber/cucumber";
import type { MutationPosition } from "@laserdata/laser-sdk";
import type { LaserWorld } from "../world.js";

const encoder = new TextEncoder();

function isPast(position: MutationPosition, other: MutationPosition): boolean {
  return position.topicGeneration === other.topicGeneration
    ? position.offset > other.offset
    : position.topicGeneration > other.topicGeneration;
}

When(
  /^I write "([^"]+)" to the managed key "([^"]+)" in namespace "([^"]+)"$/,
  async function (this: LaserWorld, value: string, key: string, namespace: string) {
    await this.requireLaser()
      .kv(namespace)
      .set(encoder.encode(key))
      .bytes(encoder.encode(value))
      .send();
  },
);

Then("the client has seen a key-value position", function (this: LaserWorld) {
  assert.ok(
    this.requireLaser().seenPositions().kv !== undefined,
    "the write reply carried the fold's position",
  );
});

When("I note the client's key-value position", function (this: LaserWorld) {
  this.notedPosition = this.requireLaser().seenPositions().kv;
});

Then("the client's key-value position moved past the noted one", function (this: LaserWorld) {
  const noted = this.notedPosition;
  const seen = this.requireLaser().seenPositions().kv;
  assert.ok(noted !== undefined && seen !== undefined);
  assert.ok(isPast(seen, noted), "the later write moved the position forward");
});

Then(
  /^the managed key "([^"]+)" in namespace "([^"]+)" reads "([^"]+)"$/,
  async function (this: LaserWorld, key: string, namespace: string, value: string) {
    const read = await this.requireLaser().kv(namespace).get(encoder.encode(key));
    assert.ok(read !== undefined, "the key is present");
    assert.equal(new TextDecoder().decode(read), value);
  },
);
