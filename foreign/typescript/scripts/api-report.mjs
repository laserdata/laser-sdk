import { execFileSync } from "node:child_process"
import { rmSync } from "node:fs"
import { fileURLToPath } from "node:url"

const executable = "./node_modules/@microsoft/api-extractor/bin/api-extractor"
const projectFolder = fileURLToPath(new URL("../", import.meta.url))
const configurations = [
  "api-extractor.json",
  "api-extractor.full.json",
  "api-extractor.testing.json",
  "api-extractor.opentelemetry.json"
]

rmSync(new URL("../dist", import.meta.url), { force: true, recursive: true })
execFileSync(
  process.execPath,
  ["./node_modules/@typescript/native/bin/tsc", "-p", "tsconfig.build.json"],
  { cwd: projectFolder, stdio: "inherit" }
)

for (const configuration of configurations) {
  execFileSync(process.execPath, [executable, "run", "--local", "--config", configuration], {
    cwd: projectFolder,
    stdio: "inherit"
  })
}
