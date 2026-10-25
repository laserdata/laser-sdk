import assert from "node:assert/strict";
import { Then, When } from "@cucumber/cucumber";
import { PrincipalId } from "@laserdata/laser-sdk";
import type { LaserWorld } from "../world.js";

When("I list every connected client", async function (this: LaserWorld) {
  this.clients = await this.requireLaser().clientMetadata().all();
});

Then("the listing holds an authenticated connection", function (this: LaserWorld) {
  const user = this.clients.find((client) => client.userId !== undefined)?.userId;
  assert.ok(user !== undefined, "a listed connection is authenticated");
  this.clientUser = user;
});

When("I list every connected client of that user", async function (this: LaserWorld) {
  assert.ok(this.clientUser !== undefined, "a user was listed");
  this.clients = await this.requireLaser()
    .clientMetadata()
    .principal(PrincipalId.new(this.clientUser))
    .all();
});

Then("every listed connection belongs to that user", function (this: LaserWorld) {
  assert.ok(this.clients.length > 0, "the user's own connection is listed");
  assert.ok(
    this.clients.every((client) => client.userId === this.clientUser),
    "every listed connection carries the listed user",
  );
});
