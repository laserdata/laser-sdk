// npm install @laserdata/laser-sdk tsx
// Run with: npx tsx main.mts
// Needs only LASER_CONNECTION_STRING, which connectEnv() reads. No server to start when it is set.
// Check: echo "${LASER_CONNECTION_STRING:+configured}"
import { setTimeout as sleep } from "node:timers/promises"
import { Laser } from "@laserdata/laser-sdk"

const decode = (bytes?: Uint8Array) => new TextDecoder().decode(bytes)

await using root = await Laser.connectEnv()
await using scratch = await root.scratchStream("workflow")

const outcome = await scratch.laser
  .workflow("pipeline")
  .stepLocal("ingest", () => "frames from sat-7")
  .stepLocal("analyze", async ({ outputs }) => {
    await sleep(1000)
    return `analyzed ${decode(outputs.get("ingest"))}`
  })
  .after("ingest")
  .stepLocal("report", ({ outputs }) => `report on ${decode(outputs.get("analyze"))}`)
  .after("analyze")
  .run()

console.log(
  JSON.stringify({
    run_id: outcome.runId.toString(),
    status: outcome.status,
    completed_steps: outcome.completedSteps,
    report: outcome.text("report")
  })
)
