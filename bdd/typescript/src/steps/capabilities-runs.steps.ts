import assert from "node:assert/strict";
import { Given, Then, When } from "@cucumber/cucumber";
import { CommandExecutionError, ConversationId, FencedLeaseClient, InvalidError, ProjectionBuilder, code, isAmbiguousMutation, isRetryable, type Laser, type LaserError } from "@laserdata/laser-sdk";
import { wire } from "@laserdata/laser-sdk/full";
import type { LaserWorld } from "../world.js";

When("a prepared release receives managed result code {int}", async function (this: LaserWorld, number: number) {
  const result = wire.resultCodeFromCode(number);
  const encoded = result.kind === "known" ? wire.resultCodeWord(result) : new Map<string, unknown>([["unrecognized", number]]);
  const reply = wire.encodeNamed(new Map<string, unknown>([["code", encoded], ["message", "refused"]]));
  const client = new FencedLeaseClient({send: () => Promise.resolve(reply), reset: () => Promise.resolve()});
  const operation = client.prepareRelease({v: 1, namespace: "coord", key: new TextEncoder().encode("lease"), leaseToken: 1n, holderId: "worker"});
  try {
    await client.release(operation);
    throw new Error("the command must be refused");
  } catch (error) {
    assert.ok(error instanceof CommandExecutionError);
    this.error = error;
  }
});

When("a prepared release receives the unresolved result code {int}", async function (this: LaserWorld, number: number) {
  const result = wire.resultCodeFromCode(number);
  assert.equal(result.kind, "known");
  const reply = wire.encodeNamed(new Map<string, unknown>([["code", wire.resultCodeWord(result)], ["message", "refused"]]));
  const client = new FencedLeaseClient({send: () => Promise.resolve(reply), reset: () => Promise.resolve()});
  const operation = client.prepareRelease({v: 1, namespace: "coord", key: new TextEncoder().encode("lease"), leaseToken: 1n, holderId: "worker"});
  try {
    await client.release(operation);
    throw new Error("an unresolved attempt never succeeds");
  } catch (error) {
    assert.ok(isAmbiguousMutation(error), String(error));
    this.error = error as LaserError;
  }
});

Then("the release fails as an ambiguous mutation", function (this: LaserWorld) {
  assert.ok(isAmbiguousMutation(this.error), String(this.error));
});

Then("the command error keeps code {int} and retryability {word}", function (this: LaserWorld, number: number, retryable: string) {
  assert.ok(this.error instanceof CommandExecutionError);
  assert.deepEqual(code(this.error), wire.resultCodeFromCode(number));
  assert.equal(isRetryable(this.error), retryable === "true");
});

const MANAGED_SURFACES: Readonly<Record<string, (laser: Laser) => Promise<unknown>>> = {
  "projection registration": (laser) => laser.projections().register(new ProjectionBuilder("reading.v1").build()),
  materialization: (laser) => laser.topic("events").materialize().apply(),
  "schema registration": (laser) => laser.schemas().register({kind: "jsonSchema", schema: '{"type":"string"}'}).send(),
  "graph upsert": (laser) => laser.graph("kg").upsert([], []),
  "fork creation": (laser) => laser.fork("experiment").create().send(),
  "change feed": (laser) => laser.watch().records(),
  "lease acquisition": (laser) => laser.kv("coordination").lease(new TextEncoder().encode("lock"), "worker", 5000),
  "session read": (laser) => laser.sessions().get(ConversationId.new()),
  batch: (laser) => laser.executeBatch([]),
};

When(/^I call the managed (.+) surface$/, async function (this: LaserWorld, surface: string) {
  const call = MANAGED_SURFACES[surface];
  assert.ok(call !== undefined, `no managed surface named ${surface}`);
  await this.capture(() => call(this.requireLaser()));
});

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

Then("the call fails as invalid", function (this: LaserWorld) {
  assert.ok(this.error instanceof InvalidError);
});
