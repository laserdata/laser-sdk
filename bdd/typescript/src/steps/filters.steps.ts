import { randomUUID } from "node:crypto";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { Given, Then, When } from "@cucumber/cucumber";
import {
  CompiledFilter,
  ConsumerFilter,
  FilterExpr,
  UnsupportedError,
  type FilterHeader,
} from "@laserdata/laser-sdk";
import { wire } from "@laserdata/laser-sdk/full";
import type { LaserWorld } from "../world.js";

const TOPIC = "fleet_changes";
const READ_TIMEOUT_MS = 15_000;
const UTF8 = new TextEncoder();

interface RecordFixtures {
  readonly records: Readonly<
    Record<string, { readonly text?: string; readonly hex?: string }>
  >;
  readonly feed: readonly string[];
}

// The record payloads and the feed order, shared with the Rust and Python
// runners so every language evaluates byte-identical records.
const FIXTURES = JSON.parse(
  readFileSync(
    new URL("../../../scenarios/filter_records.json", import.meta.url),
    "utf8",
  ),
) as RecordFixtures;
const RECORDS: Readonly<Record<string, Uint8Array>> = Object.fromEntries(
  Object.entries(FIXTURES.records).map(([name, payload]) => [
    name,
    payload.text !== undefined
      ? UTF8.encode(payload.text)
      : Buffer.from(payload.hex ?? "", "hex"),
  ]),
);
const FEED = FIXTURES.feed;

function safeMode(): ConsumerFilter {
  const satellites = (): FilterExpr =>
    FilterExpr.pred("table", "eq", "satellites");
  return ConsumerFilter.json(
    FilterExpr.any([
      FilterExpr.all([
        satellites(),
        FilterExpr.pred("op", "eq", "u"),
        FilterExpr.pred("changed", "contains", "mode"),
        FilterExpr.pred("after.mode", "eq", "safe"),
      ]),
      FilterExpr.all([satellites(), FilterExpr.pred("op", "eq", "d")]),
      FilterExpr.all([
        FilterExpr.pred("event", "eq", "satellite.telemetry_changed"),
        FilterExpr.pred("fields.mode", "eq", "safe"),
      ]),
    ]),
  );
}

const FILTERS: Readonly<Record<string, () => ConsumerFilter>> = {
  "mode present": () => ConsumerFilter.json(FilterExpr.present("fields.mode")),
  "safe mode": safeMode,
  "safe mode values": () =>
    ConsumerFilter.json(
      FilterExpr.all([
        FilterExpr.pred("table", "eq", "satellites"),
        FilterExpr.pred("op", "eq", "u"),
        FilterExpr.pred("after.mode", "eq", "safe"),
      ]),
    ),
  "mode is not safe": () =>
    ConsumerFilter.json(
      FilterExpr.negate(FilterExpr.pred("after.mode", "eq", "safe")),
    ),
  "catalog number 9007199254740993": () =>
    ConsumerFilter.json(
      FilterExpr.pred("after.norad_id", "eq", 9007199254740993n),
    ),
  "contact after noon UTC": () =>
    ConsumerFilter.json(
      FilterExpr.predAs("contact_at", "gt", "2026-09-21T12:00:00Z", {
        kind: "timestamp",
        format: "rfc3339",
      }),
    ),
  "critical priority": () =>
    ConsumerFilter.headersOnly(FilterExpr.header("priority", "eq", "critical")),
};

function named<T>(catalogue: Readonly<Record<string, T>>, name: string): T {
  const value = catalogue[name];
  if (value === undefined) throw new Error(`no fixture named \`${name}\``);
  return value;
}

function evaluate(
  world: LaserWorld,
  payload: Uint8Array,
  headers: readonly FilterHeader[],
): void {
  if (world.filter === undefined) throw new Error("scenario has no filter");
  world.verdict = CompiledFilter.compile(world.filter).evaluate({
    payload,
    headers,
  });
}

Given(/^the "([^"]+)" filter$/, function (this: LaserWorld, name: string) {
  this.filter = named(FILTERS, name)();
});

When(
  /^it evaluates the "([^"]+)" record$/,
  function (this: LaserWorld, name: string) {
    evaluate(this, named(RECORDS, name), []);
  },
);

When(
  /^it evaluates the "([^"]+)" record with header "([^"]+)" set to "([^"]+)"$/,
  function (this: LaserWorld, name: string, key: string, value: string) {
    evaluate(this, named(RECORDS, name), [
      { key, value: { kind: "string", value } },
    ]);
  },
);

Then(
  /^the record is (selected|rejected|a fault)$/,
  function (this: LaserWorld, verdict: string) {
    assert.equal(this.verdict, verdict === "a fault" ? "fault" : verdict);
  },
);

Then(
  "its digest survives a round trip through its wire form",
  function (this: LaserWorld) {
    if (this.filter === undefined) throw new Error("scenario has no filter");
    const back = wire.decodeConsumerFilterJson(
      wire.consumerFilterJson(this.filter),
    );
    assert.deepEqual(
      ConsumerFilter.digest(back),
      ConsumerFilter.digest(this.filter),
    );
  },
);

Then(
  "a different fault policy gives a different digest",
  function (this: LaserWorld) {
    if (this.filter === undefined) throw new Error("scenario has no filter");
    const dropping = ConsumerFilter.json(this.filter.expr, "drop");
    assert.notDeepEqual(
      ConsumerFilter.digest(dropping),
      ConsumerFilter.digest(this.filter),
    );
  },
);

Given("a fresh fleet change feed", async function (this: LaserWorld) {
  await this.connect();
  const topic = this.requireLaser().topic(TOPIC);
  await topic.ensure(1);
  for (const name of FEED)
    await topic.send(named(RECORDS, name), { partition: 0 });
});

When(
  /^the anomaly desk reads the feed with the "([^"]+)" filter$/,
  async function (this: LaserWorld, name: string) {
    const group = this.requireLaser()
      .topic(TOPIC)
      .consumerGroup("anomaly-desk");
    await group.create({ filter: named(FILTERS, name)() });
    const reader = await group.reader().start({ kind: "first" }).build();
    const page = await reader.nextPage({ timeoutMs: READ_TIMEOUT_MS });
    await reader.ackPage(page);
    await reader.close();
    this.filtered = page.records.map((record) => record.message.payload);
  },
);

Then(
  /^it receives the "([^"]+)", "([^"]+)", and "([^"]+)" records$/,
  function (this: LaserWorld, first: string, second: string, third: string) {
    assert.deepEqual(
      this.filtered.map((payload) => Buffer.from(payload).toString("utf8")),
      [first, second, third].map((name) =>
        Buffer.from(named(RECORDS, name)).toString("utf8"),
      ),
    );
  },
);

When(
  "the anomaly desk lists its saved filters",
  async function (this: LaserWorld) {
    try {
      await this.requireLaser()
        .topic(TOPIC)
        .consumerGroup("anomaly-desk")
        .filter()
        .revisions();
      this.error = undefined;
    } catch (error) {
      if (!(error instanceof UnsupportedError)) throw error;
      this.error = error;
    }
  },
);

Then("the catalog is refused as unsupported", function (this: LaserWorld) {
  assert.ok(this.error instanceof UnsupportedError);
});

When(
  "the anomaly desk manages a saved policy by numeric group id",
  async function (this: LaserWorld) {
    const topic = this.requireLaser().topic(TOPIC);
    const group = topic.consumerGroup("managed-desk");
    const operationId = BigInt(`0x${randomUUID().replaceAll("-", "")}`);
    const first = await group.create({ filter: safeMode(), operationId });
    assert.deepEqual(
      await group.create({ filter: safeMode(), operationId }),
      first,
    );
    const binding = first.filter;
    assert.ok(binding !== undefined);
    const byId = topic.consumerGroupId(first.id);
    const consumer = await byId.consumer({
      commitPolicy: { kind: "disabled" },
    });
    const delivered: Uint8Array[] = [];
    try {
      for (let index = 0; index < 3; index += 1) {
        const record = await consumer.nextWithin(READ_TIMEOUT_MS);
        delivered.push(record.payload);
        await consumer.commit(record);
      }
    } finally {
      await consumer.shutdown();
    }
    this.filtered = delivered;
    await using reader = await byId.reader().start({ kind: "first" }).build();
    const page = await reader.nextPage({ timeoutMs: READ_TIMEOUT_MS });
    await group.filter().setRevisionEnabled(binding.revision, false);
    await reader.ackPage(page);
    await assert.rejects(reader.tryNextPage(), /revision_disabled/);
    await group.filter().setRevisionEnabled(binding.revision, true);
    assert.deepEqual(
      page.records.map((record) => record.message.payload),
      delivered,
    );
    await reader.close();
    await group.filter().release();
  },
);

When(
  "the anomaly desk reads every record through an unbound consumer group",
  async function (this: LaserWorld) {
    const group = this.requireLaser()
      .topic(TOPIC)
      .consumerGroup("unbound-consumers");
    const info = await group.create();
    const consumer = await this.requireLaser()
      .topic(TOPIC)
      .consumerGroupId(info.id)
      .consumer({ commitPolicy: { kind: "disabled" }, batchLength: 2 });
    const delivered: Uint8Array[] = [];
    try {
      for (const _ of FEED) {
        const record = await consumer.nextWithin(READ_TIMEOUT_MS);
        delivered.push(record.payload);
        await consumer.commit(record);
      }
    } finally {
      await consumer.shutdown();
    }
    this.filtered = delivered;
  },
);

When(
  "the anomaly desk reads every record through an unbound group reader",
  async function (this: LaserWorld) {
    const group = this.requireLaser()
      .topic(TOPIC)
      .consumerGroup("unbound-readers");
    await group.create();
    if (!(await this.requireLaser().capabilities()).filters.groupPolicyReads) {
      await assert.rejects(
        group.reader().count(2).maxExamined(2).build(),
        (error: unknown) => {
          assert.ok(error instanceof UnsupportedError);
          assert.equal(error.surface, "filters");
          assert.equal(error.feature, "group_policy_reads");
          this.error = error;
          return true;
        },
      );
      return;
    }
    await using reader = await group.reader().count(2).maxExamined(2).build();
    const delivered: Uint8Array[] = [];
    while (delivered.length < FEED.length) {
      const page = await reader.nextPage({ timeoutMs: READ_TIMEOUT_MS });
      assert.equal(page.policy.mode, "unfiltered");
      assert.equal(page.examined, page.records.length);
      assert.ok(page.records.length <= 2);
      assert.ok(page.records.every((record) => !record.evaluated));
      delivered.push(...page.records.map((record) => record.message.payload));
      await reader.ackPage(page);
    }
    this.filtered = delivered;
  },
);

Then(
  "the advanced reader follows the group-aware read capability",
  async function (this: LaserWorld) {
    if ((await this.requireLaser().capabilities()).filters.groupPolicyReads) {
      assert.deepEqual(
        this.filtered.map((record) => [...record]),
        FEED.map((name) => [...named(RECORDS, name)]),
      );
    } else {
      assert.ok(this.error instanceof UnsupportedError);
      assert.equal(this.error.surface, "filters");
      assert.equal(this.error.feature, "group_policy_reads");
    }
  },
);

Then("it receives every original feed record", function (this: LaserWorld) {
  assert.deepEqual(
    this.filtered.map((record) => [...record]),
    FEED.map((name) => [...named(RECORDS, name)]),
  );
});

When(
  "the anomaly desk scans a hundred non-matches before its first match",
  async function (this: LaserWorld) {
    const topic = this.requireLaser().topic("sparse_changes");
    await topic.ensure(1);
    const nonmatch = named(RECORDS, "ground station update");
    for (let index = 0; index < 100; index += 1)
      await topic.send(nonmatch, { partition: 0 });
    await topic.send(named(RECORDS, "mode update"), { partition: 0 });
    const group = topic.consumerGroup("bounded-readers");
    await group.create({ filter: safeMode() });
    await using reader = await group
      .reader()
      .start({ kind: "first" })
      .count(1)
      .maxExamined(100)
      .build();
    const [empty, more] = await reader.readRound();
    assert.equal(empty, undefined);
    assert.equal(more, true);
    assert.equal(reader.examinedInRound(), 100);
    const [matched] = await reader.readRound();
    assert.ok(matched !== undefined);
    assert.equal(reader.examinedInRound(), 1);
    assert.deepEqual(
      matched.records.map((record) => record.offset),
      [100n],
    );
    await reader.ackPage(matched);
    this.filtered = matched.records.map((record) => record.message.payload);
  },
);

Then(
  "its first match follows an empty scan of one hundred records",
  function (this: LaserWorld) {
    assert.deepEqual(
      this.filtered.map((record) => [...record]),
      [[...named(RECORDS, "mode update")]],
    );
  },
);

When(
  "the anomaly desk consumes a hundred non-matches before its first match",
  async function (this: LaserWorld) {
    const topic = this.requireLaser().topic("sparse_changes");
    await topic.ensure(1);
    for (let index = 0; index < 100; index += 1)
      await topic.send(named(RECORDS, "ground station update"), {
        partition: 0,
      });
    await topic.send(named(RECORDS, "mode update"), { partition: 0 });
    const group = topic.consumerGroup("sparse-consumers");
    await group.create({ filter: safeMode() });
    const consumer = await group.consumer({
      batchLength: 100,
      commitPolicy: { kind: "disabled" },
      startAt: { kind: "first" },
    });
    try {
      const record = await consumer.nextWithin(READ_TIMEOUT_MS);
      assert.equal(record.position.offset, 100n);
      assert.equal((await consumer.storedOffset(0))?.storedOffset, 99n);
      await consumer.commit(record);
      this.filtered = [record.payload];
    } finally {
      await consumer.shutdown();
    }
  },
);
