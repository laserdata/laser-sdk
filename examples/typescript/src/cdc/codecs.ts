import { readFile } from "node:fs/promises"
import { ConsumerFilter, FilterExpr, CompiledSchema, cborCodec, type Laser } from "@laserdata/laser-sdk"
import type { SchemaSource } from "@laserdata/laser-sdk/full"
import { phase } from "../common.js"

type Reading = { readonly satellite: string; readonly mode: string; readonly battery: number }

function reading(value: unknown): Reading {
  if (value === null || typeof value !== "object") throw new Error("expected a fleet reading")
  const item = value as Record<string, unknown>
  if (typeof item["satellite"] !== "string" || typeof item["mode"] !== "string" || typeof item["battery"] !== "number") {
    throw new Error("invalid fleet reading fields")
  }
  return { satellite: item["satellite"], mode: item["mode"], battery: item["battery"] }
}

export async function runCodecs(laser: Laser, stream: string, catalog: boolean): Promise<void> {
  phase("select typed records inside CBOR, Avro, and Protobuf payloads")
  const registered: number[] = []
  let failure: unknown
  try {
    for (const codec of ["cbor", "avro", "protobuf"] as const) {
      if (codec !== "cbor" && !catalog) continue
      let source: SchemaSource | undefined
      if (codec === "avro") source = { kind: "avro", schema: await readFile(new URL("../../../../shared/fleet-reading.avsc", import.meta.url), "utf8") }
      if (codec === "protobuf") source = { kind: "protobuf", descriptorSet: await readFile(new URL("../../../../shared/fleet-reading.desc", import.meta.url)), messageType: "fleet.Reading" }
      let id: number | undefined
      let compiled: CompiledSchema | undefined
      if (source !== undefined) {
        id = await laser.schemas().register(source).name(`fleet-${codec}`).send()
        registered.push(id)
        const deadline = Date.now() + 15_000
        while ((await laser.schemas().get(id)) === undefined) {
          if (Date.now() >= deadline) throw new Error("writer schema did not become visible")
          await new Promise((resolve) => setTimeout(resolve, 50))
        }
        compiled = CompiledSchema.compile({ id, source })
      }
      const cbor = cborCodec(reading)
      const topicName = `fleet_${codec}`
      const topic = laser.topic(topicName)
      await topic.ensure(1)
      const feed: readonly Reading[] = [
        { satellite: "sat-042", mode: "nominal", battery: 80 },
        { satellite: "sat-042", mode: "safe", battery: 20 },
        { satellite: "sat-042", mode: "nominal", battery: 60 }
      ]
      for (const value of feed) {
        const payload = compiled === undefined ? cbor.encode(value) : compiled.encode(value)
        const publish = topic.publish().rawBytes(payload, codec)
        if (id !== undefined) publish.schemaId(id)
        await publish.send()
      }
      const expr = FilterExpr.pred("mode", "eq", "safe")
      const schemaRefs = id === undefined ? [] : [id]
      const filter = codec === "cbor" ? ConsumerFilter.cbor(expr)
        : codec === "avro" ? ConsumerFilter.avro(expr, schemaRefs) : ConsumerFilter.protobuf(expr, schemaRefs)
      await using reader = await laser.filters().reader(stream, topicName).consumer(`safe-${codec}`)
        .inline(filter).start({ kind: "first" }).localGuard(true).build()
      const record = await reader.nextRecord({ timeoutMs: 15_000 })
      const selected = compiled === undefined ? cbor.decode(record.payload) : reading(compiled.decode(record.payload))
      console.log(`  ${codec}: ${selected.satellite} entered ${selected.mode} mode at ${String(selected.battery)}% battery, 1 of 3 records delivered`)
      await reader.ack(record)
    }
  } catch (error) { failure = error }
  for (const id of registered) {
    try { await laser.schemas().drop(id) } catch (error) { failure ??= error }
  }
  if (failure !== undefined) throw failure instanceof Error ? failure : new Error(String(failure))
}
