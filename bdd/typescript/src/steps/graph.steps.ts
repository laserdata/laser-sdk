import assert from "node:assert/strict"
import { Given, Then, When } from "@cucumber/cucumber"
import { GraphEngine, type LaserWorld } from "../world.js"

Given("an empty graph", function (this: LaserWorld) {
  this.graph = new GraphEngine()
})

When(
  /^I observe "([^"]+)" (\w+) "([^"]+)"$/,
  function (this: LaserWorld, from: string, kind: string, to: string) {
    this.graph.observe(from, kind, to)
  }
)

When(
  /^I observe "([^"]+)" (\w+) "([^"]+)" valid from (\d+)$/,
  function (this: LaserWorld, from: string, kind: string, to: string, validFrom: string) {
    this.graph.observe(from, kind, to, Number(validFrom))
  }
)

When(
  /^I observe "([^"]+)" (\w+) "([^"]+)" from "([^"]+)"$/,
  function (this: LaserWorld, from: string, kind: string, to: string, source: string) {
    this.graph.observe(from, kind, to, undefined, source)
  }
)

Then(/^the graph holds (\d+) nodes$/, function (this: LaserWorld, count: string) {
  assert.equal(this.graph.nodes.size, Number(count))
})

Then(
  /^traversing from "([^"]+)" out "(\w+)" then "(\w+)" reaches "([^"]+)"$/,
  function (this: LaserWorld, start: string, first: string, second: string, target: string) {
    assert.ok(this.graph.traverse(start, "out", [first, second]).has(target))
  }
)

Then(
  /^traversing from "([^"]+)" out "(\w+)" reaches "([^"]+)"$/,
  function (this: LaserWorld, start: string, kind: string, target: string) {
    assert.ok(this.graph.traverse(start, "out", [kind]).has(target))
  }
)

Then(
  /^traversing from "([^"]+)" out "(\w+)" does not reach "([^"]+)"$/,
  function (this: LaserWorld, start: string, kind: string, target: string) {
    assert.equal(this.graph.traverse(start, "out", [kind]).has(target), false)
  }
)

Then(
  /^traversing from "([^"]+)" incoming "(\w+)" reaches "([^"]+)"$/,
  function (this: LaserWorld, start: string, kind: string, target: string) {
    assert.ok(this.graph.traverse(start, "incoming", [kind]).has(target))
  }
)

Then(
  /^traversing from "([^"]+)" out "(\w+)" as of (\d+) reaches "([^"]+)"$/,
  function (this: LaserWorld, start: string, kind: string, at: string, target: string) {
    assert.ok(this.graph.traverse(start, "out", [kind], Number(at)).has(target))
  }
)

Then(
  /^traversing from "([^"]+)" out "(\w+)" as of (\d+) does not reach "([^"]+)"$/,
  function (this: LaserWorld, start: string, kind: string, at: string, target: string) {
    assert.equal(this.graph.traverse(start, "out", [kind], Number(at)).has(target), false)
  }
)

Then(
  /^the source of node "([^"]+)" is "([^"]+)"$/,
  function (this: LaserWorld, node: string, source: string) {
    assert.equal(this.graph.nodes.get(node), source)
  }
)

Then(
  /^the source of edge "([^"]+)" (\w+) "([^"]+)" is "([^"]+)"$/,
  function (this: LaserWorld, from: string, kind: string, to: string, source: string) {
    assert.equal(
      this.graph.edges.find((edge) => edge.from === from && edge.kind === kind && edge.to === to)
        ?.source,
      source
    )
  }
)
