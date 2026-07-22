import { execFileSync } from "node:child_process"
import { readFileSync } from "node:fs"
import path from "node:path"

const [tarball, tag] = process.argv.slice(2)
if (tarball === undefined || tag === undefined) {
  console.error("usage: validate-release.mjs <tarball> <ts-vTAG>")
  process.exit(2)
}

const prefix = "ts-v"
if (!tag.startsWith(prefix)) {
  console.error(`release tag must start with ${prefix}`)
  process.exit(1)
}

const version = tag.slice(prefix.length)
const semver = /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(alpha|beta|rc)\.(0|[1-9]\d*))?$/
if (!semver.test(version)) {
  console.error(`release tag has an unsupported semantic version: ${version}`)
  process.exit(1)
}

const packageJson = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"))
const packageLock = JSON.parse(
  readFileSync(new URL("../package-lock.json", import.meta.url), "utf8")
)
const packedJson = execFileSync("tar", ["-xOf", path.resolve(tarball), "package/package.json"], {
  encoding: "utf8"
})
const packed = JSON.parse(packedJson)

const versions = [
  packageJson.version,
  packageLock.version,
  packageLock.packages?.[""]?.version,
  packed.version
]
if (versions.some((candidate) => candidate !== version)) {
  console.error(`tag, package, lockfile, and tarball versions must match ${version}`)
  process.exit(1)
}

if (packed.name !== packageJson.name || packed.repository?.url !== packageJson.repository?.url) {
  console.error("tarball identity does not match package metadata")
  process.exit(1)
}

try {
  const published = execFileSync(
    "npm",
    ["view", `${packageJson.name}@${version}`, "version", "--json"],
    {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"]
    }
  ).trim()
  if (published.length > 0 && published !== "null") {
    console.error(`${packageJson.name}@${version} is already published`)
    process.exit(1)
  }
} catch (error) {
  const status =
    typeof error === "object" && error !== null && "status" in error ? error.status : undefined
  const stderr =
    typeof error === "object" && error !== null && "stderr" in error ? String(error.stderr) : ""
  if (status !== 1 || !stderr.includes("E404")) throw error
}

console.log(`release validation ok, ${packageJson.name}@${version}`)
