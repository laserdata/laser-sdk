import { readFileSync } from "node:fs"

const lock = JSON.parse(readFileSync(new URL("../package-lock.json", import.meta.url), "utf8"))
const allowed = new Set([
  "Apache-2.0",
  "Apache-2.0 AND MIT",
  "BSD-2-Clause",
  "BSD-3-Clause",
  "BlueOak-1.0.0",
  "ISC",
  "MIT"
])

const rejected = Object.entries(lock.packages)
  .filter(([name]) => name !== "")
  .flatMap(([name, metadata]) => {
    const license = metadata.license
    return typeof license === "string" && allowed.has(license)
      ? []
      : [`${name}: ${license ?? "missing"}`]
  })

if (rejected.length > 0) {
  console.error(`dependencies with unapproved licenses:\n${rejected.join("\n")}`)
  process.exit(1)
}

console.log(`license:check ok, ${Object.keys(lock.packages).length - 1} installed packages`)
