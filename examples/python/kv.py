"""kv (State primitive): fast keyed state, with an undo button.

A key-value store living next to your log: point reads, compare-and-set, TTLs.
Forks give you git-like copies of your data. Branch it, try something, then
promote it or throw it away.

What it shows:
  - set a keyed JSON value with a TTL, then read it back typed
  - upgrade it under compare-and-swap, so the write lands only if the version
    still matches
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

import _common

EXAMPLE = "kv"
NAMESPACE = "profiles"
KEY = "user:42"
TTL_SECS = 86_400
FORK_ID = "experiment-1"


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

    if caps.forks:
        _common.phase("fork: a branch of the same state, promoted or thrown away")
        fork = laser.fork(FORK_ID)
        await fork.squash()
        await fork.create(severed=True, tables=[NAMESPACE])
        await fork.put_row(NAMESPACE, 0, 0).field("plan", "enterprise-preview").send()
        applied = await fork.promote()
        print(f"  fork '{FORK_ID}' promoted, {applied} row(s) applied")


if __name__ == "__main__":
    asyncio.run(main())
