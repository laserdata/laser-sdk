import { execFileSync } from "node:child_process"
import { readFileSync } from "node:fs"

const reports = [
  "laser-sdk.api.md",
  "laser-sdk-full.api.md",
  "laser-sdk-testing.api.md",
  "laser-sdk-opentelemetry.api.md"
].map((name) => new URL(`../etc/${name}`, import.meta.url))
const before = reports.map((report) => readFileSync(report, "utf8"))
execFileSync(process.execPath, ["./scripts/api-report.mjs"], { stdio: "inherit" })
const changed = reports.filter((report, index) => readFileSync(report, "utf8") !== before[index])
if (changed.length > 0) {
  console.error(
    `public API reports changed: ${changed.map((report) => report.pathname.split("/").at(-1)).join(", ")}`
  )
  console.error("run npm run api:report and review the result")
  process.exit(1)
}
