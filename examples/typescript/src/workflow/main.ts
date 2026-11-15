import { Laser } from "@laserdata/laser-sdk"

await using laser = await Laser.connectEnv()
await using scratch = await laser.scratchStream("workflow-demo")
const outcome = await scratch.laser
  .workflow("job-runner")
  .stepLocal("create", () => "job-42")
  .stepLocal(
    "process",
    ({ outputs }) => `processed ${new TextDecoder().decode(outputs.get("create"))}`
  )
  .after("create")
  .stepLocal("finish", () => "completed")
  .after("process")
  .run()
const report = {
  runId: outcome.runId.toString(),
  status: outcome.status,
  completedSteps: outcome.completedSteps,
  outputs: Object.fromEntries(outcome.completedSteps.map((label) => [label, outcome.text(label)]))
}
console.log(JSON.stringify(report, null, 2))
