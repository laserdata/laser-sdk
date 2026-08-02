import type { Laser } from "@laserdata/laser-sdk"
import { decodeUtf8, managedGate, phase, runExample, utf8 } from "../common.js"

export const EXAMPLE = "kv"
const NAMESPACE = "profiles"
const KEY = "user:42"
const TTL_MICROS = 86_400_000_000n // 86,400s
const FORK = "experiment-1"

function planOf(value: Uint8Array | undefined): string {
  if (value === undefined) return "no plan"
  const parsed: unknown = JSON.parse(decodeUtf8(value))
  return typeof parsed === "object" && parsed !== null && "plan" in parsed
    ? String((parsed as { plan: unknown }).plan)
    : "no plan"
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const capabilities = await laser.capabilities()
  if (!managedGate(capabilities, "kv", EXAMPLE)) return
  const kv = laser.kv(NAMESPACE)

  phase("set and get keyed state")
  await kv.set(utf8(KEY)).json({ plan: "pro" }).ttl(TTL_MICROS).send()
  console.log(`  ${KEY} is on ${planOf(await kv.get(utf8(KEY)))}`)

  if (managedGate(capabilities, "kvCas", EXAMPLE)) {
    phase("compare-and-swap: the write lands only if nobody moved first")
    const entry = await kv.getEntry(utf8(KEY))
    if (entry === undefined) throw new Error(`${KEY} vanished`)
    await kv.set(utf8(KEY)).json({ plan: "enterprise" }).expectVersion(entry.version).commit()
    const upgraded = planOf(await kv.get(utf8(KEY)))
    console.log(
      `  version ${String(entry.version)} accepted the upgrade, ${KEY} is now on ${upgraded}`
    )
  }

  if (managedGate(capabilities, "forks", EXAMPLE)) {
    phase("fork: a branch of the same state, promoted or thrown away")
    const fork = laser.fork(FORK)
    await fork.squash()
    await fork.create().severed().tables([NAMESPACE]).send()
    await fork.putRow(NAMESPACE, 0, 0n).field("plan", "enterprise-preview").send()
    const applied = await fork.promote()
    console.log(`  fork \`${FORK}\` promoted, ${String(applied)} row(s) applied`)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
