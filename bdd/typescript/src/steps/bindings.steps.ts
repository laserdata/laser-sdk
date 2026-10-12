import assert from "node:assert/strict";
import { Given, Then, When } from "@cucumber/cucumber";
import { wire } from "@laserdata/laser-sdk/full";
import type { LaserWorld } from "../world.js";

Given(
  /^a binding from stream "([^"]+)" topic "([^"]+)"$/,
  function (this: LaserWorld, stream: string, topic: string) {
    this.bindingBuilder = new wire.ProjectionBindingBuilder().source(stream, topic);
  },
);

When("the binding lets the plane skip rejected records", function (this: LaserWorld) {
  assert.ok(this.bindingBuilder, "a binding builder");
  this.bindingBuilder = this.bindingBuilder.skipRejected();
});

When("I build the binding", function (this: LaserWorld) {
  assert.ok(this.bindingBuilder, "a binding builder");
  this.bindingWire = wire.encodeProjectionBinding(this.bindingBuilder.build());
});

Then("the binding's wire form omits skip_rejected", function (this: LaserWorld) {
  assert.equal(this.bindingWire?.has("skip_rejected"), false);
});

Then("the binding's wire form carries skip_rejected true", function (this: LaserWorld) {
  assert.equal(this.bindingWire?.get("skip_rejected"), true);
});
