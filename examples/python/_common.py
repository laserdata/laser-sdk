"""Shared helpers for the Python examples.

Connection resolution mirrors the Rust examples so the same environment points
either language at a local server or a LaserData Cloud deployment with no code
change. See ``../README.md`` for the full variable list.
"""

from __future__ import annotations

import asyncio
import os
import time

import laser_sdk as ls

LOCAL_CONNECTION_STRING = "iggy:iggy@127.0.0.1:8090"
DEFAULT_PORT = 8090
DEFAULT_STREAM = "laser"
PARTITIONS = 4

# How long to wait for the managed plane to materialize a freshly registered
# index, and how often to poll while waiting. Mirrors the Rust example crate.
PROJECTOR_TIMEOUT = 60.0
PROJECTION_POLL = 0.15


def phase(title: str) -> None:
    """Print the shared phase heading used by all language examples."""
    rule = "─" * (len(title) + 3)
    print(f"\n\033[1;36m▸ {title}\033[0m\n\033[36m{rule}\033[0m")


def _env(name: str) -> str:
    return os.environ.get(name, "").strip()


def stream_for(example: str) -> str:
    """The data stream an example uses: ``LASER_STREAM`` if set, otherwise a
    per-example ``laser-<example>`` stream so several examples share one local
    server without colliding on the well-known agent topics."""
    return _env("LASER_STREAM") or f"{DEFAULT_STREAM}-{example}"


def env_int(name: str, default: int) -> int:
    raw = _env(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_bool(name: str, default: bool) -> bool:
    raw = _env(name).lower()
    return raw in ("1", "true", "yes", "on") if raw else default


def messages(default: int) -> int:
    return max(1, env_int("LASER_MESSAGES", default))


def batch(default: int) -> int:
    return max(1, env_int("LASER_BATCH", default))


def _ensure_default_port(connection_string: str) -> str:
    scheme, separator, remainder = connection_string.partition("://")
    if not separator:
        return connection_string
    cut = len(remainder)
    for index, char in enumerate(remainder):
        if char in "/?":
            cut = index
            break
    authority, path_and_query = remainder[:cut], remainder[cut:]
    head, at, tail = authority.rpartition("@")
    user_info, host_and_port = (f"{head}@", tail) if at else ("", authority)
    if host_and_port.startswith("["):
        closing = host_and_port.find("]")
        if closing >= 0 and host_and_port[closing + 1 :].startswith(":"):
            return connection_string
    elif ":" in host_and_port:
        return connection_string
    return f"{scheme}://{user_info}{host_and_port}:{DEFAULT_PORT}{path_and_query}"


def _normalize_connection_string(connection_string: str) -> str:
    if "://" not in connection_string:
        connection_string = f"iggy+tcp://{connection_string}"
    return _ensure_default_port(connection_string)


def _resolve_credentials() -> str:
    token = _env("LASER_TOKEN")
    if token:
        return f"{token}@"
    username = _env("LASER_USERNAME")
    password = _env("LASER_PASSWORD")
    if username and password:
        return f"{username}:{password}@"
    raise SystemExit(
        "LaserData Cloud needs credentials: set LASER_TOKEN, or LASER_USERNAME + LASER_PASSWORD"
    )


def _resolve_connection_string() -> str:
    provided = _env("LASER_CONNECTION_STRING")
    if provided:
        return _normalize_connection_string(provided)
    server = _env("LASER_SERVER")
    if not server:
        return LOCAL_CONNECTION_STRING
    return _normalize_connection_string(f"iggy+tcp://{_resolve_credentials()}{server}")


async def connect(example: str) -> ls.Laser:
    """Connect over the resolved target, pinned to the example's stream."""
    return await ls.Laser.connect(_resolve_connection_string(), stream=stream_for(example))


def managed_gate(available: bool, feature: str, example: str) -> bool:
    """Guard a managed-only phase. Returns ``True`` when the feature is available.
    Otherwise prints how to point the example at LaserData Cloud and returns
    ``False`` so the example stays green on raw Apache Iggy."""
    if available:
        return True
    print(
        f"\n  {feature} is a LaserData Cloud feature and the connected server is raw "
        f"Apache Iggy, so this phase is skipped.\n"
        f"  Point the example at a deployment to run it live:\n"
        f"    LASER_CONNECTION_STRING=user:pwd@your-host python {example}.py\n"
    )
    return False


class Rng:
    """A tiny deterministic xorshift64* generator, so an example replays the
    identical sequence on every run without a third-party dependency. Mirrors
    the `Rng` the Rust examples use, masking to 64 bits at each step."""

    _MASK = (1 << 64) - 1
    _MUL = 0x2545F4914F6CDD1D

    def __init__(self, seed: int) -> None:
        self.state = (seed | 1) & self._MASK

    def next_u64(self) -> int:
        x = self.state
        x ^= (x << 13) & self._MASK
        x ^= x >> 7
        x ^= (x << 17) & self._MASK
        self.state = x
        return (x * self._MUL) & self._MASK

    def below(self, bound: int) -> int:
        return self.next_u64() % max(1, bound)

    def pick(self, choices):
        return choices[self.below(len(choices))]


async def start_projector(laser, topic, fields, *, content_type="json") -> None:
    """Register `topic`'s index over `fields` on the managed plane and wait until
    `query(topic)` returns rows, the Python analog of the Rust example crate's
    projector. Index-only so each record's own `inline_payload` decides inlining.
    Query is a managed feature, so call this only behind a managed gate: against
    raw Apache Iggy the registration itself raises `UnsupportedError`."""
    projection_id = f"{topic}.v1"
    await laser.register_projection(
        {
            "id": projection_id,
            "name": topic,
            "version": 1,
            "content_type": content_type,
            "extraction": {
                "fields": [{"name": field, "pointer": f"/{field}"} for field in fields],
                "inline_payload": False,
            },
            "inline_payload_default": False,
        }
    )
    await laser.apply_binding(
        {
            "source": {"stream": laser.default_stream, "topic": topic},
            "allowed_projections": [projection_id],
            "default_projection": projection_id,
            # Opt into the change feed so a reader can await the view's advance
            # (laser.watch()) instead of re-querying blind.
            "notify": True,
            "targets": [
                {
                    "backend": "embedded",
                    "table": topic,
                    "role": "read_write",
                    "delivery": "effectively_once",
                    "required": True,
                }
            ],
        }
    )
    # The registration is applied asynchronously: a query errors until the table
    # exists, then returns an empty page. Wait for that transition so records
    # published next flow into a live projector.
    deadline = time.monotonic() + PROJECTOR_TIMEOUT
    while True:
        try:
            await laser.query(topic).fetch()
            return
        except ls.LaserError:
            if time.monotonic() >= deadline:
                raise
            await asyncio.sleep(PROJECTION_POLL)


async def wait_for_projection(laser, topic, expected) -> int:
    """Wait until the projector has materialized at least `expected` rows in
    `topic`, tolerant of a not-yet-created index. Returns the final total.

    Await-then-query where the deployment publishes the change feed (LaserData
    Cloud with a notifying binding): each tick drains the feed and re-runs the
    count only when the plane reported the view advanced. Elsewhere every tick
    queries, the plain bounded poll."""
    deadline = time.monotonic() + PROJECTOR_TIMEOUT
    feed = laser.watch(index=topic) if (await laser.capabilities()).watch else None
    last = -1
    while True:
        advanced = last < 0 or feed is None or bool(await feed.poll())
        if advanced:
            try:
                total = (await laser.query(topic).with_total().fetch()).total
            except ls.LaserError:
                total = 0
            if total != last:
                print(f"  projector materialized {total}/{expected} rows")
                last = total
            if total >= expected:
                return total
        if time.monotonic() >= deadline:
            raise ls.InvalidError(
                f"projector indexed only {max(last, 0)}/{expected} rows in "
                f"'{topic}' before the deadline"
            )
        await asyncio.sleep(PROJECTION_POLL)
