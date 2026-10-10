import assert from "node:assert/strict";
import { Given, Then, When } from "@cucumber/cucumber";
import { InvalidError, code } from "@laserdata/laser-sdk";
import type { LaserWorld } from "../world.js";

When("I read the negotiated capabilities", async function (this: LaserWorld) {
  this.capabilities = await this.requireLaser().capabilities();
});

Given(
  "a managed-query connection that does not advertise read-your-writes",
  async function (this: LaserWorld) {
    const laser = this.requireLaser();
    const capabilities = await laser.capabilities();
    this.laser = laser.withCapabilities({
      ...capabilities,
      managed: true,
      query: {
        ...capabilities.query,
        available: true,
        consistency: "eventual",
      },
    });
  },
);

Then("managed query is unavailable", function (this: LaserWorld) {
  assert.equal(this.capabilities?.query.available, false);
});

Then("managed key-value is unavailable", function (this: LaserWorld) {
  assert.equal(this.capabilities?.kv.available, false);
});

Then("forks are unavailable", function (this: LaserWorld) {
  assert.equal(this.capabilities?.forks, false);
});

Then("the coordination features are unavailable", function (this: LaserWorld) {
  assert.equal(this.capabilities?.kv.cas, false);
  assert.equal(this.capabilities?.query.consistency, "eventual");
});

When(
  /^I run a query against topic "([^"]+)"$/,
  async function (this: LaserWorld, topic: string) {
    await this.capture(() => this.requireLaser().query(topic).fetch());
  },
);

When(
  /^I run a read-your-writes query against topic "([^"]+)"$/,
  async function (this: LaserWorld, topic: string) {
    await this.capture(() =>
      this.requireLaser().query(topic).readYourWrites().fetch(),
    );
  },
);

When(
  /^I compare-and-swap key "([^"]+)" in namespace "([^"]+)" expecting it absent$/,
  async function (this: LaserWorld, key: string, namespace: string) {
    await this.capture(() =>
      this.requireLaser()
        .kv(namespace)
        .set(new TextEncoder().encode(key))
        .bytes(new Uint8Array([1]))
        .expectAbsent()
        .commit(),
    );
  },
);

When(
  /^I send a set of key "([^"]+)" in namespace "([^"]+)" expecting version (\d+) without commit$/,
  async function (
    this: LaserWorld,
    key: string,
    namespace: string,
    version: string,
  ) {
    await this.capture(() =>
      this.requireLaser()
        .kv(namespace)
        .set(new TextEncoder().encode(key))
        .bytes(new TextEncoder().encode("debug"))
        .expectVersion(BigInt(version))
        .send(),
    );
  },
);

Then("the call fails as invalid", function (this: LaserWorld) {
  assert.ok(this.error instanceof InvalidError);
});

Then(
  "the unified result code is invalid argument",
  function (this: LaserWorld) {
    assert.deepEqual(code(this.error), {
      kind: "known",
      name: "InvalidArgument",
    });
  },
);
