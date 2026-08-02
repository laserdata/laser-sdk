import { execFileSync } from "node:child_process"
import { readFileSync } from "node:fs"

const packageJson = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"))

const result = execFileSync("npm", ["pack", "--dry-run", "--json"], {
  encoding: "utf8"
})

const [entry] = JSON.parse(result)
if (!entry) {
  console.error("npm pack --dry-run --json returned no entry")
  process.exit(1)
}

const requiredFiles = [
  "dist/index.js",
  "dist/index.d.ts",
  "dist/full.js",
  "dist/full.d.ts",
  "dist/testing.js",
  "dist/testing.d.ts",
  "dist/opentelemetry.js",
  "dist/opentelemetry.d.ts",
  "dist/index.js.map",
  "dist/index.d.ts.map",
  "dist/full.js.map",
  "dist/full.d.ts.map",
  "dist/testing.js.map",
  "dist/testing.d.ts.map",
  "dist/opentelemetry.js.map",
  "dist/opentelemetry.d.ts.map",
  "README.md",
  "LICENSE",
  "NOTICE"
]
const packedPaths = new Set(entry.files.map((file) => file.path))

const missing = requiredFiles.filter((file) => !packedPaths.has(file))
if (missing.length > 0) {
  console.error(`pack manifest missing required files: ${missing.join(", ")}`)
  process.exit(1)
}

if (entry.name !== "@laserdata/laser-sdk") {
  console.error(`unexpected package name in pack manifest: ${entry.name}`)
  process.exit(1)
}

if (entry.version !== packageJson.version) {
  console.error(
    `pack manifest version ${entry.version} does not match package ${packageJson.version}`
  )
  process.exit(1)
}

const allowedTopLevel = new Set(["dist", "README.md", "LICENSE", "NOTICE", "package.json"])
const unexpected = entry.files
  .map((file) => file.path)
  .filter((file) => !allowedTopLevel.has(file.split("/")[0]))
if (unexpected.length > 0) {
  console.error(`pack manifest contains unexpected files: ${unexpected.join(", ")}`)
  process.exit(1)
}

const requiredExports = [".", "./full", "./testing", "./opentelemetry"]
const missingExports = requiredExports.filter((name) => packageJson.exports[name] === undefined)
if (missingExports.length > 0) {
  console.error(`package exports missing: ${missingExports.join(", ")}`)
  process.exit(1)
}

if (packageJson.sideEffects !== false || packageJson.type !== "module") {
  console.error("package must be side-effect-free ESM")
  process.exit(1)
}

if (entry.size > 2 * 1024 * 1024) {
  console.error(`packed tarball exceeds 2 MiB: ${entry.size} bytes`)
  process.exit(1)
}

if (Object.keys(packageJson.dependencies ?? {}).length > 12) {
  console.error("published dependency count exceeds the budget of 12")
  process.exit(1)
}

console.log(`pack:check ok, ${entry.files.length} files, ${entry.size} bytes`)
