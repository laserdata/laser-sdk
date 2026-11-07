import { execFileSync } from "node:child_process"
import { readFileSync, rmSync } from "node:fs"
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

// The secondary entry points name types the main entry point exports. Each
// one their reports list must be a declaration of the main report, so no
// public signature uses a type no entry point exports.
const report = (name) => readFileSync(new URL(`../api/${name}`, import.meta.url), "utf8")
const exported = new Set(
  [
    ...report("laser-sdk.api.md").matchAll(
      /^export (?:declare )?(?:abstract )?(?:class|interface|type|const|function|enum|namespace) (\w+)/gm
    )
  ].map((match) => match[1])
)
const hidden = ["laser-sdk-testing.api.md", "laser-sdk-opentelemetry.api.md"].flatMap((name) =>
  [...report(name).matchAll(/\(ae-forgotten-export\) The symbol "(\w+)"/g)]
    .map((match) => match[1])
    .filter((symbol) => !exported.has(symbol))
    .map((symbol) => `${name}: ${symbol}`)
)
if (hidden.length > 0) {
  console.error(`types no entry point exports: ${hidden.join(", ")}`)
  process.exit(1)
}
