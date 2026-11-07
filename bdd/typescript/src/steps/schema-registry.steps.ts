import assert from "node:assert/strict";
import { Then, When } from "@cucumber/cucumber";
import type { LaserWorld } from "../world.js";

When(
  /^the Avro writer schema "([^"]+)" is registered in the stream$/,
  async function (this: LaserWorld, name: string) {
    await register(this, requireStream(this), name);
  },
);

When(
  /^the Avro writer schema "([^"]+)" is registered in a second stream$/,
  async function (this: LaserWorld, name: string) {
    const stream = `${requireStream(this)}-second`;
    await this.requireLaser().stream(stream).ensure();
    await register(this, stream, name);
  },
);

Then("both registrations answer schema id 1", function (this: LaserWorld) {
  assert.deepEqual(
    this.schemaRegistrations.map(([, , id]) => id),
    [1, 1],
  );
});

Then(
  "each stream reads schema 1 as its own writer schema",
  async function (this: LaserWorld) {
    for (const [stream, name] of this.schemaRegistrations) {
      const info = await this.requireLaser().withDefaultStream(stream).schemas().get(1);
      assert.equal(info?.schema.name, name, stream);
    }
  },
);

Then(
  "each stream lists only its own writer schema",
  async function (this: LaserWorld) {
    for (const [stream, name] of this.schemaRegistrations) {
      const page = await this.requireLaser().withDefaultStream(stream).schemas().list().fetch();
      assert.deepEqual(
        page.items.map((info) => info.schema.name),
        [name],
        stream,
      );
    }
  },
);

function requireStream(world: LaserWorld): string {
  if (world.stream === undefined) throw new Error("scenario has no stream");
  return world.stream;
}

async function register(world: LaserWorld, stream: string, name: string): Promise<void> {
  const schema = JSON.stringify({
    type: "record",
    name,
    fields: [{ name: "id", type: "long" }],
  });
  const id = await world
    .requireLaser()
    .withDefaultStream(stream)
    .schemas()
    .register({ kind: "avro", schema })
    .name(name)
    .send();
  world.schemaRegistrations.push([stream, name, id]);
}
