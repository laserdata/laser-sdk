// npm install @laserdata/laser-sdk tsx
// Run with: npx tsx main.mts
// Needs only LASER_CONNECTION_STRING. No server to start when it is set.
import { setTimeout as sleep } from "node:timers/promises"
import { step, workflow } from "@laserdata/laser-sdk"

const ingest = step("ingest", () => "frames from sat-7")
const analyze = step("analyze", async (frames: string) => {
  await sleep(1000)
  return `analyzed ${frames}`
})
const report = step("report", (analysis: string) => `report on ${analysis}`)

const pipeline = workflow("pipeline", async () => report(await analyze(await ingest())))

const run = await pipeline.run()
console.log(JSON.stringify({ status: run.status, steps: run.completedSteps, report: run.value }))
