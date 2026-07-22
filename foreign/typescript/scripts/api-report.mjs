import { execFileSync } from "node:child_process"

const executable = "./node_modules/@microsoft/api-extractor/bin/api-extractor"
const configurations = [
  "api-extractor.json",
  "api-extractor.full.json",
  "api-extractor.testing.json",
  "api-extractor.opentelemetry.json"
]

for (const configuration of configurations) {
  execFileSync(process.execPath, [executable, "run", "--local", "--config", configuration], {
    stdio: "inherit"
  })
}
