"""kv (State primitive): fast keyed state, with an undo button.

A key-value store living next to your log: point reads, compare-and-set, TTLs.
Forks give you git-like copies of your data. Branch it, try something, then
promote it or throw it away.

What it shows:
  - set a keyed JSON value with a TTL, then read it back typed
  - upgrade it under compare-and-swap, so the write lands only if the version
    still matches
  - hold a revocable lease, read behind its barrier, and write under its fence,
    so a released fence is refused
  - create a severed fork, write one speculative row, then promote it

State is a managed feature: it needs Laser Stack or LaserData Cloud and skips
on Apache Iggy without a managed backend.

Run it:
    LASER_CONNECTION_STRING=user:pwd@your-host python3 kv.py

Docs: https://docs.laserdata.cloud/laser-sdk/state
Full scenario: concierge.py (compare-and-swap ledgers and a speculative fork under load)
"""

from __future__ import annotations

import asyncio
import json

import _common
import laser_sdk as ls

EXAMPLE = "kv"
NAMESPACE = "profiles"
KEY = "user:42"
TTL_SECS = 86_400
FORK_ID = "experiment-1"
LEASE_KEY = "lease:user:42"
HOLDER = "worker-a"
LEASE_TTL_SECS = 30


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    caps = await laser.capabilities()
    if not _common.managed_gate(caps.kv, "state (kv)", EXAMPLE):
        return

    _common.phase("set and get keyed state")
    store = laser.kv(NAMESPACE)
    await store.set(KEY).json({"plan": "pro"}).ttl(TTL_SECS).send()
    profile = await store.get_typed(KEY)
    print(f"  {KEY} is on {profile['plan']}")

    if caps.kv_cas:
        _common.phase("compare-and-swap: the write lands only if nobody moved first")
        entry = await store.get_entry(KEY)
        await store.set(KEY).json({"plan": "enterprise"}).expect_version(entry.version).commit()
        upgraded = await store.get_typed(KEY)
        print(f"  version {entry.version} accepted the upgrade, {KEY} is now on {upgraded['plan']}")

    if caps.kv_fenced_leases:
        _common.phase("lease and fenced write: at most one effective writer")
        lease = await store.lease(LEASE_KEY, HOLDER, LEASE_TTL_SECS)
        print(f"  {HOLDER} holds {LEASE_KEY} at fence {lease.token}")
        # Barriered read: the answering fold has applied at least the grant, so a
        # holder that just took over never plans against its predecessor's state.
        held = await store.get_entry_at_least(KEY, lease.position)
        fenced = await store.cas_fenced(
            KEY,
            NAMESPACE,
            LEASE_KEY,
            lease.token,
            json.dumps({"plan": "enterprise-plus"}).encode(),
            expect_version=held.version,
            ttl_secs=TTL_SECS,
        )
        seen = json.loads(held.value)["plan"]
        print(f"  barriered read saw {seen}, the fenced write landed as version {fenced}")
        renewed = await store.renew_lease(LEASE_KEY, HOLDER, lease.token, LEASE_TTL_SECS)
        await store.release(LEASE_KEY, HOLDER, renewed.token)
        print(f"  lease renewed at the same fence {renewed.token}, then released")
        # The gate holds without waiting for a successor: a released fence is
        # already dead, so a zombie holder cannot commit through it.
        try:
            await store.cas_fenced(
                KEY,
                NAMESPACE,
                LEASE_KEY,
                lease.token,
                json.dumps({"plan": "zombie"}).encode(),
                expect_version=fenced,
            )
        except ls.LaserError as error:
            if not error.lease_lost:
                raise
            print("  after release the same fence is refused: lease-lost")
        else:
            raise RuntimeError("a released fence was accepted")

    if caps.forks:
        _common.phase("fork: a branch of the same state, promoted or thrown away")
        table = _common.index_for(NAMESPACE)
        await laser.topic(NAMESPACE).ensure(_common.PARTITIONS)
        await _common.start_projector(laser, NAMESPACE, ["plan"], index=table)
        fork = laser.fork(FORK_ID)
        await fork.squash()
        await fork.create(severed=True, tables=[table])
        await fork.put_row(table, 0, 0).field("plan", "enterprise-preview").send()
        applied = await fork.promote()
        print(f"  fork '{FORK_ID}' promoted, {applied} row(s) applied")


if __name__ == "__main__":
    asyncio.run(main())
