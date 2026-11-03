import { KvExecutionError, type Laser } from "@laserdata/laser-sdk"
import {
  PARTITIONS,
  decodeUtf8,
  ensureView,
  indexFor,
  managedGate,
  phase,
  runExample,
  utf8
} from "../common.js"

export const EXAMPLE = "kv"
const NAMESPACE = "config"
const KEY = "service:auth"
const TTL_MS = 86_400_000 // 86,400s
const FORK = "experiment-1"
const LEASE_KEY = "lease:service:auth"
const HOLDER = "worker-a"
const LEASE_TTL_MS = 30_000 // 30s

function levelOf(value: Uint8Array | undefined): string {
  if (value === undefined) return "no level"
  const parsed: unknown = JSON.parse(decodeUtf8(value))
  return typeof parsed === "object" && parsed !== null && "log_level" in parsed
    ? String((parsed as { log_level: unknown }).log_level)
    : "no level"
}

function isLeaseLost(error: unknown): boolean {
  return (
    error instanceof KvExecutionError &&
    typeof error.detail === "object" &&
    error.detail !== null &&
    "kind" in error.detail &&
    error.detail.kind === "leaseLost"
  )
}

export async function run(laser: Laser, _signal: AbortSignal): Promise<void> {
  const capabilities = await laser.capabilities()
  if (!managedGate(capabilities, "kv", EXAMPLE)) return
  const kv = laser.kv(NAMESPACE)

  phase("set and get keyed state")
  await kv.set(utf8(KEY)).json({ log_level: "info" }).ttl(TTL_MS).send()
  console.log(`  ${KEY} logs at ${levelOf(await kv.get(utf8(KEY)))}`)

  if (managedGate(capabilities, "kvCas", EXAMPLE)) {
    phase("compare-and-swap: the write lands only if nobody moved first")
    const entry = await kv.getEntry(utf8(KEY))
    if (entry === undefined) throw new Error(`${KEY} vanished`)
    await kv.set(utf8(KEY)).json({ log_level: "debug" }).expectVersion(entry.version).commit()
    const raised = levelOf(await kv.get(utf8(KEY)))
    console.log(
      `  version ${String(entry.version)} accepted the change, ${KEY} now logs at ${raised}`
    )
  }

  if (managedGate(capabilities, "kvFencedLeases", EXAMPLE)) {
    phase("lease and fenced write: at most one effective writer")
    const lease = await kv.lease(utf8(LEASE_KEY), HOLDER, LEASE_TTL_MS)
    console.log(`  ${HOLDER} holds ${LEASE_KEY} at fence ${String(lease.token)}`)
    // Barriered read: the answering fold has applied at least the grant, so a
    // holder that just took over never plans against its predecessor's state.
    const held = await kv.getEntryAtLeast(utf8(KEY), lease.position)
    if (held === undefined) throw new Error(`${KEY} vanished`)
    const fenced = await kv
      .casFenced(utf8(KEY), NAMESPACE, utf8(LEASE_KEY), lease.token)
      .json({ log_level: "trace" })
      .ttl(TTL_MS)
      .expectVersion(held.version)
      .commit()
    console.log(
      `  barriered read saw ${levelOf(held.value)}, the fenced write landed as version ${String(fenced)}`
    )
    const renewed = await kv.renewLease(utf8(LEASE_KEY), HOLDER, lease.token, LEASE_TTL_MS)
    await kv.release(utf8(LEASE_KEY), HOLDER, renewed.token)
    console.log(`  lease renewed at the same fence ${String(renewed.token)}, then released`)
    // The gate holds without waiting for a successor: a released fence is
    // already dead, so a zombie holder cannot commit through it.
    try {
      await kv
        .casFenced(utf8(KEY), NAMESPACE, utf8(LEASE_KEY), lease.token)
        .json({ log_level: "zombie" })
        .expectVersion(fenced)
        .commit()
      throw new Error("a released fence was accepted")
    } catch (error) {
      if (!isLeaseLost(error)) throw error
      console.log("  after release the same fence is refused: lease-lost")
    }
  }

  if (managedGate(capabilities, "forks", EXAMPLE)) {
    phase("fork: a branch of the same state, promoted or thrown away")
    const table = indexFor(NAMESPACE)
    await laser.topic(NAMESPACE).ensure(PARTITIONS)
    await ensureView(laser, NAMESPACE, table, ["log_level"])
    const fork = laser.fork(FORK)
    await fork.squash()
    await fork.create().severed().tables([table]).send()
    await fork.putRow(table, 0, 0n).field("log_level", "trace-preview").send()
    const applied = await fork.promote()
    console.log(`  fork \`${FORK}\` promoted, ${String(applied)} row(s) applied`)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) await runExample(EXAMPLE, run)
