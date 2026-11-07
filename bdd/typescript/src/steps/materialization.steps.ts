import assert from "node:assert/strict";
import { Given, When, Then } from "@cucumber/cucumber";
import { ConversationId } from "@laserdata/laser-sdk";
import { wire } from "@laserdata/laser-sdk/full";
import type { LaserWorld } from "../world.js";

Given(
  /^a realized binding with captured head (\d+) and applied offset (\d+)$/,
  function (this: LaserWorld, head: string, applied: string) {
    this.materializationStatus = {
      history: { kind: "append" },
      source: { stream: "shop", topic: "orders" },
      index: "orders",
      realization: "realized",
      sourceIncarnation: 7n,
      partitions: [
        {
          partitionId: 0,
          headOffset: BigInt(head),
          appliedOffset: BigInt(applied),
          firstOffset: 0n,
        },
      ],
      holds: [],
      acceptedGaps: [],
      skippedRecords: 0n,
    };
    this.materializationTarget = this.materializationStatus;
  },
);
When(
  /^the binding advances to offset (\d+) while its source head grows to (\d+)$/,
  function (this: LaserWorld, applied: string, head: string) {
    assert.ok(this.materializationStatus);
    this.materializationStatus = {
      ...this.materializationStatus,
      partitions: [
        {
          partitionId: 0,
          headOffset: BigInt(head),
          appliedOffset: BigInt(applied),
          firstOffset: 0n,
        },
      ],
    };
  },
);
Then("the binding has reached the captured head", function (this: LaserWorld) {
  assert.ok(this.materializationStatus && this.materializationTarget);
  assert.equal(
    wire.bindingStatusCaughtUpTo(
      this.materializationStatus,
      this.materializationTarget,
    ),
    true,
  );
});
When("the binding has no known first offset", function (this: LaserWorld) {
  assert.ok(this.materializationStatus);
  this.materializationStatus = {
    ...this.materializationStatus,
    partitions: [{ partitionId: 0, headOffset: 5n, appliedOffset: 1n }],
  };
});
Then("the binding cannot claim complete history", function (this: LaserWorld) {
  assert.ok(this.materializationStatus);
  assert.equal(
    wire.bindingStatusHistoryComplete(this.materializationStatus, "block"),
    false,
  );
});
When(
  "another partition lost its records before the binding read them",
  function (this: LaserWorld) {
    assert.ok(this.materializationStatus);
    assert.equal(
      wire.bindingStatusHistoryComplete(this.materializationStatus, "block"),
      true,
    );
    this.materializationStatus = {
      ...this.materializationStatus,
      partitions: [
        ...this.materializationStatus.partitions,
        { partitionId: 1, firstOffset: 6n },
      ],
    };
  },
);
Given(
  /^a pending binding whose table creation failed with class "(\w+)"$/,
  function (this: LaserWorld, failure: string) {
    this.materializationStatus = {
      history: { kind: "append" },
      source: { stream: "shop", topic: "orders" },
      index: "orders",
      realization: "pending",
      failure: failure as wire.FailureClass,
      partitions: [],
      holds: [],
      acceptedGaps: [],
      skippedRecords: 0n,
    };
  },
);
When(
  /^its failure class becomes "(\w+)"$/,
  function (this: LaserWorld, failure: string) {
    assert.ok(this.materializationStatus);
    this.materializationStatus = {
      ...this.materializationStatus,
      failure: failure as wire.FailureClass,
    };
  },
);
Then("the binding has failed for good", function (this: LaserWorld) {
  assert.ok(this.materializationStatus);
  assert.equal(wire.bindingStatusIsFailed(this.materializationStatus), true);
});
Then("the binding has not failed for good", function (this: LaserWorld) {
  assert.ok(this.materializationStatus);
  assert.equal(wire.bindingStatusIsFailed(this.materializationStatus), false);
});
When(
  "I materialize whole records twice under one operation identity",
  async function (this: LaserWorld) {
    const topic = this.requireLaser().topic("whole_records");
    await topic.ensure(1);
    const request = topic
      .materialize()
      .operationId(ConversationId.new().asU128());
    await request.apply();
    this.materialization = await request.apply();
  },
);
When(
  "I publish two arbitrary records to the materialized topic",
  async function (this: LaserWorld) {
    for (const body of [
      new TextEncoder().encode("not-json"),
      new Uint8Array([0, 255, 1, 2]),
    ]) {
      await this.requireLaser()
        .topic("whole_records")
        .publish()
        .payload(body)
        .send();
    }
  },
);
Then(
  "materialization reaches the captured source head",
  async function (this: LaserWorld) {
    assert.ok(this.materialization);
    const result = await this.materialization.wait("caught_up", 20_000);
    assert.equal(result.kind, "ready");
  },
);
Then(
  "the materialized query returns both original bodies exactly once",
  async function (this: LaserWorld) {
    const result = await this.requireLaser()
      .query("whole_records")
      .withPayload()
      .fetch();
    assert.equal(result.rows.length, 2);
    const actual = result.rows
      .map((row) => {
        const value = wire.queryResultValue(
          result,
          row,
          "__laser_original_payload",
        );
        assert.equal(value?.kind, "binary");
        if (value?.kind !== "binary") throw new Error("missing body");
        return [...value.value].join(",");
      })
      .sort();
    assert.deepEqual(
      actual,
      [[...new TextEncoder().encode("not-json")].join(","), "0,255,1,2"].sort(),
    );
  },
);

When("I materialize keyed records", async function (this: LaserWorld) {
  await materializeKeyed(this, "delete");
});
When(
  "I materialize keyed records without a delete header",
  async function (this: LaserWorld) {
    await materializeKeyed(this);
  },
);
async function materializeKeyed(
  world: LaserWorld,
  deleteHeader?: string,
): Promise<void> {
  const topic = world.requireLaser().topic("keyed_records");
  await topic.ensure(1);
  const projection = new wire.ProjectionBuilder("keyed_records")
    .record()
    .keyed({ kind: "header", name: "key" }, deleteHeader)
    .build();
  world.materialization = await topic.materialize().typed(projection).apply();
}
When(
  /^I publish keyed value "([^"]+)"$/,
  async function (this: LaserWorld, value: string) {
    await this.requireLaser()
      .topic("keyed_records")
      .publish()
      .header("key", "one")
      .payload(new TextEncoder().encode(value))
      .send();
  },
);
When(
  /^I publish keyed value "([^"]+)" with a "([^"]+)" header$/,
  async function (this: LaserWorld, value: string, header: string) {
    await this.requireLaser()
      .topic("keyed_records")
      .publish()
      .header("key", "one")
      .header(header, "true")
      .payload(new TextEncoder().encode(value))
      .send();
  },
);
When("I delete the materialized key", async function (this: LaserWorld) {
  await this.requireLaser()
    .topic("keyed_records")
    .publish()
    .header("key", "one")
    .header("delete", "true")
    .payload(new Uint8Array([0]))
    .send();
});
Then(
  /^the keyed view contains only "([^"]+)"$/,
  async function (this: LaserWorld, value: string) {
    assert.ok(this.materialization);
    assert.equal(
      (await this.materialization.wait("caught_up", 20_000)).kind,
      "ready",
    );
    const result = await this.requireLaser()
      .query("keyed_records")
      .withPayload()
      .fetch();
    assert.equal(result.rows.length, 1);
    const body = wire.queryResultValue(
      result,
      result.rows[0]!,
      "__laser_original_payload",
    );
    assert.equal(body?.kind, "binary");
    if (body?.kind === "binary")
      assert.equal(new TextDecoder().decode(body.value), value);
  },
);
Then("the keyed view is empty", async function (this: LaserWorld) {
  assert.ok(this.materialization);
  assert.equal(
    (await this.materialization.wait("caught_up", 20_000)).kind,
    "ready",
  );
  assert.equal(
    (await this.requireLaser().query("keyed_records").fetch()).rows.length,
    0,
  );
});

Then(
  "operational SQL counts the two materialized records",
  async function (this: LaserWorld) {
    const result = await this.requireLaser()
      .query("whole_records")
      .rawSql("sqlite", "SELECT count(*) AS n FROM whole_records")
      .fetch();
    assert.equal(result.rows.length, 1);
    const first = result.rows[0];
    assert.ok(first);
    const count = wire.queryResultValue(result, first, "n");
    assert.ok(count);
    assert.equal(wire.typedValueDiagnosticText(count), "2");
  },
);
Then(
  "operational SQL refuses an internal relation and an unserved dialect",
  async function (this: LaserWorld) {
    await assert.rejects(
      this.requireLaser()
        .query("whole_records")
        .rawSql("sqlite", "SELECT * FROM _ld_watermarks")
        .fetch(),
    );
    await assert.rejects(
      this.requireLaser()
        .query("whole_records")
        .rawSql("my_sql", "SELECT * FROM whole_records")
        .fetch(),
    );
  },
);

const REMOTE_TOPIC = "remote_records";

Given("a declared SQL backend", async function (this: LaserWorld) {
  const capabilities = await this.requireLaser().capabilities();
  const descriptor = capabilities.backends.find(
    (candidate) =>
      ["postgres", "velodb"].includes(candidate.implementation.kind) &&
      candidate.mode === "operational",
  );
  assert.ok(descriptor, "the stack declares a Postgres or Doris backend");
  this.sqlBackend = {
    binding: {
      resourceId: descriptor.resourceId,
      generation: descriptor.observedBackendGeneration,
    },
    dialect:
      descriptor.implementation.kind === "postgres" ? "postgres" : "my_sql",
  };
});
When(
  "I materialize whole records on the declared SQL backend",
  async function (this: LaserWorld) {
    assert.ok(this.sqlBackend);
    const topic = this.requireLaser().topic(REMOTE_TOPIC);
    await topic.ensure(1);
    this.materialization = await topic
      .materialize()
      .backend(this.sqlBackend.binding)
      .apply();
  },
);
When(
  "I publish two arbitrary records to the remote materialized topic",
  async function (this: LaserWorld) {
    for (const body of [
      new TextEncoder().encode("not-json"),
      new Uint8Array([0, 255, 1, 2]),
    ]) {
      await this.requireLaser()
        .topic(REMOTE_TOPIC)
        .publish()
        .payload(body)
        .send();
    }
  },
);
Then(
  "the remote materialized query returns both original bodies exactly once",
  async function (this: LaserWorld) {
    const result = await this.requireLaser()
      .query(REMOTE_TOPIC)
      .withPayload()
      .fetch();
    assert.equal(result.rows.length, 2);
    const actual = result.rows
      .map((row) => {
        const value = wire.queryResultValue(
          result,
          row,
          "__laser_original_payload",
        );
        assert.equal(value?.kind, "binary");
        if (value?.kind !== "binary") throw new Error("missing body");
        return [...value.value].join(",");
      })
      .sort();
    assert.deepEqual(
      actual,
      [[...new TextEncoder().encode("not-json")].join(","), "0,255,1,2"].sort(),
    );
  },
);
Then(
  "operational SQL in the backend dialect counts the two remote records",
  async function (this: LaserWorld) {
    assert.ok(this.sqlBackend);
    const result = await this.requireLaser()
      .query(REMOTE_TOPIC)
      .rawSql(
        this.sqlBackend.dialect,
        `SELECT count(*) AS n FROM ${REMOTE_TOPIC}`,
      )
      .fetch();
    assert.equal(result.rows.length, 1);
    const first = result.rows[0];
    assert.ok(first);
    const count = wire.queryResultValue(result, first, "n");
    assert.ok(count);
    assert.equal(wire.typedValueDiagnosticText(count), "2");
  },
);
Then(
  "operational SQL refuses internal and catalog relations of the declared backend",
  async function (this: LaserWorld) {
    assert.ok(this.sqlBackend);
    for (const statement of [
      "SELECT * FROM _ld_watermarks",
      "SELECT * FROM ld_watermarks",
      "SELECT * FROM information_schema.tables",
      "SELECT * FROM whole_records",
    ]) {
      await assert.rejects(
        this.requireLaser()
          .query(REMOTE_TOPIC)
          .rawSql(this.sqlBackend.dialect, statement)
          .fetch(),
        statement,
      );
    }
    await assert.rejects(
      this.requireLaser()
        .query(REMOTE_TOPIC)
        .rawSql("sqlite", `SELECT count(*) AS n FROM ${REMOTE_TOPIC}`)
        .fetch(),
    );
  },
);
