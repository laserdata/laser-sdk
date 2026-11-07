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

When("the binding lets the backend skip rejected records", function (this: LaserWorld) {
  assert.ok(this.bindingBuilder, "a binding builder");
  this.bindingBuilder = this.bindingBuilder.invalidRecords("dead_letter");
});

When("I build the binding", function (this: LaserWorld) {
  assert.ok(this.bindingBuilder, "a binding builder");
  this.bindingWire = wire.encodeProjectionBinding(this.bindingBuilder.build());
});

Then("the binding's wire form omits invalid_records", function (this: LaserWorld) {
  assert.equal(this.bindingWire?.has("invalid_records"), false);
});

Then("the binding's wire form carries invalid_records dead_letter", function (this: LaserWorld) {
  assert.equal(this.bindingWire?.get("invalid_records"), "dead_letter");
});

When("the binding keeps references for oversized records", function (this: LaserWorld) {
  assert.ok(this.bindingBuilder, "a binding builder");
  this.bindingBuilder = this.bindingBuilder.oversize("reference");
});

Then("the binding's wire form carries oversize reference", function (this: LaserWorld) {
  assert.equal(this.bindingWire?.get("oversize"), "reference");
});
