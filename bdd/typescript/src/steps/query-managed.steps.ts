import assert from "node:assert/strict";
import { Given, Then, When } from "@cucumber/cucumber";
import {
  ContentType,
  ProjectionBuilder,
  queryResultValueI64,
  queryResultValueText,
  type QueryResult,
  type Row,
} from "@laserdata/laser-sdk";
import type { LaserWorld } from "../world.js";

const CATCH_UP_MS = 120_000;

Given(
  /^(\d+) api-call records published and materialized into "([^"]+)"$/,
  async function (this: LaserWorld, countText: string, topic: string) {
    const handle = this.requireLaser().topic(topic);
    await handle.ensure(1);
    const projection = new ProjectionBuilder(topic)
      .contentType(ContentType.Json)
      .field("status")
      .field("latency_ms")
      .build();
    const materialization = await handle.materialize().typed(projection).apply();
    const records = Array.from({ length: Number(countText) }, (_, latency) => ({
      status: latency % 4 === 0 ? "500" : "200",
      latency_ms: latency,
    }));
    await handle.publishBatch().extendJson(records).send();
    const wait = await materialization.wait("caught_up", CATCH_UP_MS);
    assert.equal(wait.kind, "ready", JSON.stringify(wait.kind));
  },
);

When(
  /^the materialized "([^"]+)" is queried for latency_ms greater than (\d+) with its total$/,
  async function (this: LaserWorld, index: string, bound: string) {
    this.managedResult = await this.requireLaser()
      .query(index)
      .filterGt("latency_ms", { kind: "long", value: BigInt(bound) })
      .withTotal()
      .fetch();
  },
);

When(
  /^the materialized "([^"]+)" is queried ordered by latency_ms descending with limit (\d+)$/,
  async function (this: LaserWorld, index: string, limit: string) {
    this.managedResult = await this.requireLaser().query(index).orderDesc("latency_ms").limit(Number(limit)).fetch();
  },
);

When(
  /^the materialized "([^"]+)" is queried with limit (\d+) and its total$/,
  async function (this: LaserWorld, index: string, limit: string) {
    this.managedResult = await this.requireLaser().query(index).limit(Number(limit)).withTotal().fetch();
  },
);

When(/^the materialized "([^"]+)" is counted grouped by status$/, async function (this: LaserWorld, index: string) {
  this.managedResult = await this.requireLaser().query(index).count().groupBy(["status"]).fetch();
});

When(
  /^the materialized "([^"]+)" is walked in pages of (\d+) ordered by latency_ms$/,
  async function (this: LaserWorld, index: string, pageText: string) {
    const page = Number(pageText);
    const latencies: bigint[] = [];
    for (;;) {
      const result = await this.requireLaser()
        .query(index)
        .orderAsc("latency_ms")
        .limit(page)
        .offset(latencies.length)
        .fetch();
      latencies.push(...result.rows.map((row) => latency(result, row)));
      if (result.rows.length < page) break;
    }
    this.walkedLatencies = latencies;
  },
);

When(
  /^the materialized "([^"]+)" is read whole in pages of (\d+) ordered by latency_ms$/,
  async function (this: LaserWorld, index: string, pageText: string) {
    const walked = await this.requireLaser().query(index).orderAsc("latency_ms").limit(Number(pageText)).fetchAll();
    this.walkFrontierMoved = walked.frontierMoved;
    this.walkedLatencies = walked.rows.map((row) => {
      const value = row.valueI64("latency_ms");
      assert.ok(value !== undefined, "a numeric latency_ms");
      return value;
    });
  },
);

Then(/^the whole read reports its frontier unmoved$/, function (this: LaserWorld) {
  assert.equal(this.walkFrontierMoved, false);
});

Then(/^the materialized total is (\d+)$/, function (this: LaserWorld, total: string) {
  assert.equal(answered(this).page.total, BigInt(total));
});

Then(/^every materialized row has latency_ms greater than (\d+)$/, function (this: LaserWorld, bound: string) {
  const result = answered(this);
  assert.ok(result.rows.length > 0, "the page holds matching rows");
  for (const row of result.rows) assert.ok(latency(result, row) > BigInt(bound));
});

Then(/^the materialized latency_ms values are "([^"]+)" in order$/, function (this: LaserWorld, expected: string) {
  const result = answered(this);
  assert.deepEqual(
    result.rows.map((row) => latency(result, row).toString()),
    expected.split(", "),
  );
});

Then(/^the materialized page holds (\d+) rows$/, function (this: LaserWorld, rows: string) {
  assert.equal(answered(this).rows.length, Number(rows));
});

Then(
  /^the materialized group "([^"]+)" has count (\d+)$/,
  function (this: LaserWorld, status: string, count: string) {
    const result = answered(this);
    const row = result.rows.find((candidate) => queryResultValueText(result, candidate, "status") === status);
    assert.ok(row !== undefined, `a group row for ${status}`);
    assert.equal(queryResultValueI64(result, row, "count"), BigInt(count));
  },
);

Then(
  /^the walk returns every latency_ms from (\d+) to (\d+) once$/,
  function (this: LaserWorld, first: string, last: string) {
    const expected = Array.from({ length: Number(last) - Number(first) + 1 }, (_, step) => BigInt(Number(first) + step));
    assert.deepEqual(this.walkedLatencies, expected);
  },
);

function answered(world: LaserWorld): QueryResult {
  assert.ok(world.managedResult !== undefined, "a query ran");
  return world.managedResult;
}

function latency(result: QueryResult, row: Row): bigint {
  const value = queryResultValueI64(result, row, "latency_ms");
  assert.ok(value !== undefined, "a numeric latency_ms");
  return value;
}
