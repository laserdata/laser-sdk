// Supply-chain cooldown: a locked dependency version must have been on the
// registry for at least MINIMUM_RELEASE_AGE_DAYS before CI accepts it, so a
// freshly published (possibly compromised) release cannot enter the lockfile
// until the ecosystem has had time to catch it. With a baseline lockfile
// argument (the merge base's package-lock.json) only versions the change
// introduces are checked, so existing pins are not retroactively blocked.
import { existsSync, readFileSync } from "node:fs"

const MINIMUM_RELEASE_AGE_DAYS = Number(process.env.MINIMUM_RELEASE_AGE_DAYS ?? "7")
const REGISTRY = "https://registry.npmjs.org"
const CONCURRENCY = 16

function lockedVersions(lockPath) {
  const lock = JSON.parse(readFileSync(lockPath, "utf8"))
  const locked = new Map()
  for (const [path, metadata] of Object.entries(lock.packages)) {
    if (path === "" || metadata.link === true) continue
    if (typeof metadata.resolved === "string" && !metadata.resolved.startsWith(REGISTRY)) continue
    const name =
      metadata.name ?? path.slice(path.lastIndexOf("node_modules/") + "node_modules/".length)
    locked.set(`${name}@${metadata.version}`, { name, version: metadata.version })
  }
  return locked
}

const locked = lockedVersions(new URL("../package-lock.json", import.meta.url))

const baselinePath = process.argv[2]
if (baselinePath !== undefined && existsSync(baselinePath)) {
  for (const key of lockedVersions(baselinePath).keys()) locked.delete(key)
}

const cutoff = Date.now() - MINIMUM_RELEASE_AGE_DAYS * 24 * 60 * 60 * 1000
const entries = [...locked.values()]
const tooYoung = []
const failures = []

async function check({ name, version }) {
  const response = await fetch(`${REGISTRY}/${name}`)
  if (!response.ok) {
    failures.push(`${name}: registry returned ${String(response.status)}`)
    return
  }
  const document = await response.json()
  const published = document.time?.[version]
  if (published === undefined) {
    failures.push(`${name}@${version}: no publish time in registry metadata`)
    return
  }
  if (Date.parse(published) > cutoff) {
    tooYoung.push(`${name}@${version}: published ${published}`)
  }
}

let next = 0
await Promise.all(
  Array.from({ length: CONCURRENCY }, async () => {
    while (next < entries.length) {
      const entry = entries[next]
      next += 1
      await check(entry)
    }
  })
)

if (failures.length > 0) {
  console.error(`release-age:check could not verify:\n${failures.join("\n")}`)
  process.exit(1)
}

if (tooYoung.length > 0) {
  console.error(
    `dependency versions younger than ${String(MINIMUM_RELEASE_AGE_DAYS)} days:\n${tooYoung.join("\n")}`
  )
  process.exit(1)
}

console.log(
  `release-age:check ok, ${String(entries.length)} checked versions are at least ${String(MINIMUM_RELEASE_AGE_DAYS)} days old`
)
