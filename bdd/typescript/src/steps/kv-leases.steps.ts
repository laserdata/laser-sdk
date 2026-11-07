import assert from "node:assert/strict";
import { Given, Then, When } from "@cucumber/cucumber";
import { TopicRetention, isLeaseLost, type Lease } from "@laserdata/laser-sdk";
import type { LaserWorld } from "../world.js";

const COORDINATION = "coordination";
const STATE = "state";
const TAKEOVER_DEADLINE_MS = 15_000;
const encoder = new TextEncoder();

// Every TypeScript client connects through a connection string, the client the
// fenced-lease verbs need, and the stream the namespaces are scoped to must exist.
Given("a fresh stream bootstrapped on a connection-string client", async function (this: LaserWorld) {
  await this.connect();
  await this.requireLaser().bootstrap(1, TopicRetention.expireAfter(86_400_000));
});

When(
  /^"([^"]+)" acquires the lease "([^"]+)" for (\d+) ms$/,
  async function (this: LaserWorld, holder: string, key: string, ttlMs: string) {
    const lease = await this.requireLaser().kv(COORDINATION).lease(encoder.encode(key), holder, Number(ttlMs));
    this.leases.set(holder, [key, lease]);
  },
);

// The earlier grant has to run out, so the step polls until the store grants
// the key rather than sleeping a fixed time.
When(
  /^"([^"]+)" acquires the lease "([^"]+)" once it is free$/,
  async function (this: LaserWorld, holder: string, key: string) {
    const deadline = Date.now() + TAKEOVER_DEADLINE_MS;
    for (;;) {
      try {
        const lease = await this.requireLaser().kv(COORDINATION).lease(encoder.encode(key), holder, 5000);
        this.leases.set(holder, [key, lease]);
        return;
      } catch (error) {
        if (Date.now() >= deadline) throw error;
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
    }
  },
);

When(/^"([^"]+)" renews the lease "([^"]+)"$/, async function (this: LaserWorld, holder: string, key: string) {
  const { token } = held(this, holder);
  this.renewedLease = await this.requireLaser().kv(COORDINATION).renewLease(encoder.encode(key), holder, token, 5000);
});

Then(/^the renewal of "([^"]+)" keeps its lease token$/, function (this: LaserWorld, holder: string) {
  assert.equal(this.renewedLease?.token, held(this, holder).token);
});

When(/^"([^"]+)" releases the lease "([^"]+)"$/, async function (this: LaserWorld, holder: string, key: string) {
  const { token } = held(this, holder);
  this.leaseReleased = await this.requireLaser().kv(COORDINATION).release(encoder.encode(key), holder, token);
});

Then("the release reports the lease was held", function (this: LaserWorld) {
  assert.equal(this.leaseReleased, true);
});

When(
  /^"([^"]+)" writes "([^"]+)" to "([^"]+)" fenced by its lease$/,
  async function (this: LaserWorld, holder: string, value: string, key: string) {
    const entry = this.leases.get(holder);
    assert.ok(entry !== undefined, `${holder} took no lease`);
    const [fenceKey, lease] = entry;
    const request = this.requireLaser()
      .kv(STATE)
      .casFenced(encoder.encode(key), COORDINATION, encoder.encode(fenceKey), lease.token)
      .bytes(encoder.encode(value));
    const conditioned =
      this.lastEntry === undefined ? request.expectAbsent() : request.expectVersion(this.lastEntry.version);
    await this.capture(() => conditioned.commit());
  },
);

Then("the fenced write fails as lease lost", function (this: LaserWorld) {
  assert.ok(isLeaseLost(this.error), String(this.error));
});

Then(
  /^a read of "([^"]+)" at the lease grant of "([^"]+)" returns "([^"]+)"$/,
  async function (this: LaserWorld, key: string, holder: string, value: string) {
    const entry = await this.requireLaser()
      .kv(STATE)
      .getEntryAtLeast(encoder.encode(key), held(this, holder).position);
    assert.ok(entry !== undefined, "the earlier holder's write is visible at the new grant");
    assert.deepEqual(entry.value, encoder.encode(value));
    this.lastEntry = entry;
  },
);

Then(/^reading "([^"]+)" returns "([^"]+)"$/, async function (this: LaserWorld, key: string, value: string) {
  assert.deepEqual(await this.requireLaser().kv(STATE).get(encoder.encode(key)), encoder.encode(value));
});

function held(world: LaserWorld, holder: string): Lease {
  const entry = world.leases.get(holder);
  assert.ok(entry !== undefined, `${holder} took no lease`);
  return entry[1];
}
