import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { Given, Then, When } from "@cucumber/cucumber";
import { CodecError, CompiledSchema, InvalidError } from "@laserdata/laser-sdk";
import { wire } from "@laserdata/laser-sdk/full";
import type { LaserWorld } from "../world.js";

type CorpusEntry = ReadonlyMap<string, unknown>;

// The writer schema corpus every client and the managed backend reproduce.
const CORPUS = wire.parseCanonicalJson(
  readFileSync(
    new URL(
      "../../../../wire/fixtures/writer_schema_cases.json",
      import.meta.url,
    ),
    "utf8",
  ),
) as ReadonlyMap<string, readonly CorpusEntry[]>;

Given(
  /^a "([^"]+)" writer schema$/,
  function (this: LaserWorld, codec: string) {
    assert.ok(codec === "avro_long" || codec === "json_string");
    this.writerSchema = CompiledSchema.compile({
      id: 1,
      source:
        codec === "avro_long"
          ? { kind: "avro", schema: '"long"' }
          : { kind: "jsonSchema", schema: '{"type":"string"}' },
    });
  },
);

When(
  /^I decode schema payload "([^"]+)"$/,
  function (this: LaserWorld, payload: string) {
    assert.ok(this.writerSchema);
    const bytes = Uint8Array.from(JSON.parse(payload) as number[]);
    try {
      this.writerSchema.decode(bytes);
      this.writerPayloadRejected = false;
    } catch (error) {
      assert.ok(error instanceof CodecError);
      this.writerPayloadRejected = true;
    }
  },
);

Then(
  /^the schema payload is (accepted|rejected)$/,
  function (this: LaserWorld, outcome: string) {
    assert.equal(this.writerPayloadRejected, outcome === "rejected");
  },
);

When(
  "I decode every case of the shared writer schema corpus",
  function (this: LaserWorld) {
    const failures: string[] = [];
    for (const sample of CORPUS.get("cases") ?? []) {
      const id = Number(sample.get("schema"));
      const definition = (CORPUS.get("schemas") ?? []).find(
        (schema) => Number(schema.get("id")) === id,
      );
      assert.ok(definition !== undefined);
      const refused = sample.get("refused") === true;
      const expected = sample.get("expected");
      let compiled: CompiledSchema;
      try {
        compiled = CompiledSchema.compile({
          id,
          source: corpusSource(definition.get("source") as CorpusEntry),
        });
      } catch (error) {
        if (!refused || !(error instanceof InvalidError))
          failures.push(String(sample.get("name")));
        continue;
      }
      const hex = (sample.get("payload_hex") as string | undefined) ?? "";
      try {
        const value = compiled.decode(Uint8Array.from(Buffer.from(hex, "hex")));
        if (refused || !sameValue(value, expected))
          failures.push(String(sample.get("name")));
      } catch (error) {
        if (refused || expected !== undefined || !(error instanceof CodecError))
          failures.push(String(sample.get("name")));
      }
    }
    this.writerCorpusFailures = failures;
  },
);

Then(
  "every corpus case matches its expected value",
  function (this: LaserWorld) {
    assert.deepEqual(this.writerCorpusFailures, []);
  },
);

function corpusSource(source: CorpusEntry): wire.SchemaSource {
  const kind = source.get("kind");
  if (kind === "protobuf")
    return {
      kind: "protobuf",
      descriptorSet: Uint8Array.from(
        (source.get("descriptor_set") as readonly bigint[]).map(Number),
      ),
      messageType: source.get("message_type") as string,
    };
  return {
    kind: kind === "avro" ? "avro" : "jsonSchema",
    schema: source.get("schema") as string,
  };
}

// Numbers match by value, so a 32-bit `number` meets a corpus `bigint` and a
// 64-bit `bigint` stays exact.
function sameValue(actual: unknown, expected: unknown): boolean {
  if (typeof expected === "bigint")
    return typeof actual === "bigint"
      ? actual === expected
      : typeof actual === "number" &&
          Number.isSafeInteger(actual) &&
          BigInt(actual) === expected;
  if (Array.isArray(expected))
    return (
      Array.isArray(actual) &&
      actual.length === expected.length &&
      expected.every((item, index) => sameValue(actual[index], item))
    );
  if (expected instanceof Map) {
    if (actual === null || typeof actual !== "object" || Array.isArray(actual))
      return false;
    const keys = Object.keys(actual);
    return (
      keys.length === expected.size &&
      keys.every((key) =>
        sameValue((actual as Record<string, unknown>)[key], expected.get(key)),
      )
    );
  }
  return actual === expected;
}
