"""concierge (agentic): an AI support desk operating a live incident, end to end.

The full-AGDX showcase, the Python peer of the Rust `concierge`. One realistic
story, each platform feature doing the job it exists for:

  1. WORLD       a ticket firehose bulk-ingests into a queryable index (the
                 desk's world model), every record carrying the message_type
                 and ts convention fields.
  2. MEMORY      past resolution notes are remembered semantically, the desk
                 recalls the closest ones when the incident arrives.
  3. THE DESK    four agents on the agent topics: triage queries the index as a
                 tool and fans one diagnostic angle per specialist call under a
                 deadline, the specialist answers each angle from recalled
                 memory plus the LLM, the resolver applies remediation credits
                 effectively once (KV-deduplicated, large ones behind a durable
                 approval), and the approver stands in for the human at the gate.
  4. SPECULATION the diagnosis proposes bulk-resolving the matching backlog. The
                 desk tries it in a copy-on-write fork, compares the forked
                 backlog against the trunk, and logs the verdict. Set
                 LASER_APPLY_PLAN=1 to act on it (promote when it clears the
                 criticals, squash when it does not).
  5. AUDIT       the whole incident is one conversation on the log, rebuilt by
                 folding it back together at the end.

The LLM seam is `default_llm()`: a deterministic mock by default, real Claude
when ANTHROPIC_API_KEY is set. Scale the world with the shared volume knobs:

    python concierge.py
    LASER_MESSAGES=200000 LASER_BATCH=1000 python concierge.py

Ticket ingest streams onto the log anywhere. The query index, semantic memory,
key-value credits, and forks are LaserData Cloud features: on raw Apache Iggy the
desk phase prints how to point at a deployment and skips, so the run stays green.
Point it at a deployment to run the whole desk live:

    LASER_CONNECTION_STRING=iggy://user:pwd@your-host python concierge.py
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.error
import urllib.request

import _common
import laser_sdk as ls

EXAMPLE = "concierge"

TICKETS_TOPIC = "support_tickets"
MEMORY_TOPIC = "concierge_memory"
MEMORY_PROJECTION = "concierge_memory.v1"
TRIAGE_FORK = "bulk-resolve-plan"

EMBEDDING_DIMS = 64
TOP_K = 3
TOOL_TIMEOUT = 30.0
DESK_TIMEOUT = 90.0
APPROVAL_TIMEOUT = 30.0
# Dedup keys self-expire, long enough to outlive a redelivery.
DEDUP_TTL = 3600.0
# Credits at or above this hold for a durable approval first.
APPROVAL_CENTS = 100

CUSTOMERS = ["acme", "globex", "initech", "umbrella", "stark"]
COMPONENTS = ["checkout", "billing", "search", "auth", "uploads"]
SEVERITIES = ["low", "medium", "high", "critical"]

# What the desk has learned resolving past incidents, recalled semantically when
# a similar one arrives.
RESOLUTION_NOTES = [
    "checkout latency spikes are usually database connection pool exhaustion",
    "billing double-charges trace back to retries without an idempotency key",
    "search returning stale results means the nightly index rebuild failed",
    "auth token errors after a deploy come from the rotated signing key",
    "upload failures over 10 MB are the proxy body-size limit, not the bucket",
    "critical checkout pages resolve fastest by failing over the read replica",
]

INCIDENT = "checkout is slow for several customers"

# The diagnostic angles triage fans out, one specialist call each.
ANGLES = ["most likely root cause", "fastest mitigation", "blast radius to check"]

# (idempotency key, customer, credit cents) the diagnosis remediates with. The
# list is sent twice to prove the resolver is effectively once: the redelivery
# must not double-credit anyone.
CREDITS = [("cr-1", "acme", 150), ("cr-2", "globex", 50), ("cr-3", "initech", 80)]
CREDIT_TOTALS = {"acme": 150, "globex": 50, "initech": 80}


def phase(title: str) -> None:
    print(f"\n=== {title} ===")


# The deterministic bag-of-words embedder: hash tokens into buckets and
# L2-normalize. Swap a real model in behind the same `embed` seam. Async because
# the SDK awaits the embedder, so a real model can do a network call here.
async def embed(text: str) -> list[float]:
    vector = [0.0] * EMBEDDING_DIMS
    token = ""
    for char in text.lower():
        if char.isalnum():
            token += char
            continue
        if token:
            vector[_fnv1a(token) % EMBEDDING_DIMS] += 1.0
            token = ""
    if token:
        vector[_fnv1a(token) % EMBEDDING_DIMS] += 1.0
    norm = sum(component * component for component in vector) ** 0.5
    if norm > 0.0:
        vector = [component / norm for component in vector]
    return vector


def _fnv1a(text: str) -> int:
    # 32-bit FNV-1a, enough to spread tokens across embedding buckets.
    mask = (1 << 32) - 1
    hash_value = 0x811C9DC5
    for byte in text.encode():
        hash_value = ((hash_value ^ byte) * 0x01000193) & mask
    return hash_value


# The LLM seam. The mock is deterministic so the example replays free in CI; a
# real model answers when a key is present, the same handler code either way.
class MockLlm:
    async def complete(self, prompt: str) -> str:
        return f"[mock-llm] {prompt}"


class AnthropicLlm:
    ENDPOINT = "https://api.anthropic.com/v1/messages"

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.model = _common._env("ANTHROPIC_MODEL") or "claude-sonnet-4-6"

    async def complete(self, prompt: str) -> str:
        body = json.dumps(
            {
                "model": self.model,
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}],
            }
        ).encode()
        request = urllib.request.Request(
            self.ENDPOINT,
            data=body,
            headers={
                "content-type": "application/json",
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
            },
        )

        def call() -> str:
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    payload = json.loads(response.read())
                return "".join(block.get("text", "") for block in payload.get("content", []))
            except (urllib.error.URLError, ValueError) as error:
                return f"[anthropic error] {error}"

        # The blocking HTTP call goes on a thread so it never stalls the loop.
        return await asyncio.to_thread(call)


def default_llm():
    api_key = _common._env("ANTHROPIC_API_KEY")
    return AnthropicLlm(api_key) if api_key else MockLlm()


def _scalar(result) -> str:
    # First aggregate value of a single-aggregate result, as text.
    return result.rows[0].headers.get("count", "0") if result.rows else "0"


async def _read_u64(store, key: str) -> int:
    value = await store.get(key)
    if value is None:
        return 0
    try:
        return int(bytes(value).decode())
    except ValueError:
        return 0


def make_triage(llm, index: str):
    # The orchestrator. Reads the live backlog off the index (the index as a
    # tool), fans one diagnostic angle per specialist call under a deadline, and
    # synthesizes the findings into a diagnosis with the LLM. The diagnosis and
    # findings ride back on the conversation, durable on the log.
    async def triage(ctx, message):
        # The resolver shares the Commands topic. Credits are its traffic, free
        # text is ours. Never fail on foreign messages.
        try:
            payload = message.json()
        except ls.CodecError:
            payload = None
        if isinstance(payload, dict) and "customer" in payload and "cents" in payload:
            return
        incident = bytes(message.payload).decode("utf-8", "replace")

        # Tool 1: the materialized index. The desk reads the live blast radius
        # the same way an on-call would.
        blast = await (
            ctx.laser()
            .query(index)
            .filter_eq("severity", "critical")
            .filter_eq("status", "open")
            .filter_eq("component", "checkout")
            .count()
            .fetch()
        )
        open_criticals = _scalar(blast)
        print(f"  triage: {open_criticals} open critical checkout tickets")

        # Tool 2: the specialist, one deadline-bounded call per angle, each on
        # its own correlation conversation so the replies never cross.
        deadline_micros = int(time.time() * 1_000_000) + int(TOOL_TIMEOUT * 1_000_000)

        async def ask(angle: str):
            correlation = ls.Provenance(
                conversation_id=ls.new_conversation_id(),
                agent="triage",
                deadline_micros=deadline_micros,
            )
            reply = await ctx.request(
                ls.Topics.TOOL_CALLS,
                ls.Topics.TOOL_RESULTS,
                f"{angle} for: {incident}".encode(),
                correlation,
                timeout_secs=TOOL_TIMEOUT,
            )
            return bytes(reply.payload).decode("utf-8", "replace")

        results = await asyncio.gather(*(ask(angle) for angle in ANGLES), return_exceptions=True)
        findings = [result for result in results if isinstance(result, str)]
        print(f"  triage: gathered {len(findings)} findings")

        prompt = (
            f"Diagnose this incident and recommend one mitigation.\nIncident: {incident}\n"
            f"Open critical checkout tickets: {open_criticals}\nFindings:\n" + "\n".join(findings)
        )
        diagnosis = await llm.complete(prompt)
        await ctx.respond(json.dumps({"diagnosis": diagnosis, "findings": findings}).encode())

    return triage


def make_specialist(llm):
    # The tool agent. Answers one diagnostic angle from what the desk remembers
    # (semantic recall over past resolutions) plus the LLM. The query-backed
    # memory borrows the connection, so it is built per call from ctx.laser().
    async def specialist(ctx, message):
        query = bytes(message.payload).decode("utf-8", "replace")
        memory = ctx.laser().query_memory(embed, MEMORY_TOPIC)
        recalled = await memory.recall(semantic=query, limit=TOP_K)
        remembered = [item.text for item in recalled]
        print(f"  specialist: recalled {len(remembered)} past resolutions")
        answer = await llm.complete(
            f"Answer briefly: {query}\nWhat past incidents taught us:\n" + "\n".join(remembered)
        )
        await ctx.respond(answer.encode())

    return specialist


def make_resolver(credits_namespace: str):
    # Applies a remediation credit to a customer's balance in KV. The effect is a
    # read-modify-write, which is exactly why the dedup gate in front of it
    # matters. Credits at or above the threshold hold for a durable approval.
    async def resolver(ctx, message):
        # Triage shares the Commands topic. Free text is its traffic.
        try:
            credit = message.json()
        except ls.CodecError:
            return
        if not (isinstance(credit, dict) and "customer" in credit and "cents" in credit):
            return
        key = message.idempotency_key or "?"
        if credit["cents"] >= APPROVAL_CENTS:
            print(f"  resolver: large credit {key}, requesting approval")
            if not await _approved(ctx, credit):
                print(f"  resolver: credit {key} declined")
                return
        store = ctx.laser().kv(credits_namespace)
        balance = await _read_u64(store, credit["customer"]) + credit["cents"]
        await store.set(credit["customer"]).payload(str(balance)).send()
        print(f"  resolver: applied {key}, {credit['customer']} balance now {balance}")

    return resolver


# Stands in for the approval UI. In production a person clicks a button, here an
# agent keeps the run deterministic. The approval is durable: it rides the log
# like everything else and survives a restart.
async def approver(ctx, message):
    print("  approver: approved a held credit")
    await ctx.respond(b"approved")


# Hold a large credit for approval: ask on the human-input topic and block on the
# decision. Returns whether to apply it.
async def _approved(ctx, credit) -> bool:
    request = ls.Provenance(conversation_id=ls.new_conversation_id(), agent="resolver")
    decision = await ctx.request(
        ls.Topics.HUMAN_INPUT,
        ls.Topics.RESPONSES,
        f"approve a {credit['cents']} cent credit to {credit['customer']}?".encode(),
        request,
        timeout_secs=APPROVAL_TIMEOUT,
    )
    return bytes(decision.payload) == b"approved"


def make_kv_deduplicator(laser, namespace: str, ttl: float):
    # A durable, self-expiring dedup backend over the managed KV store, plugged
    # into spawn_agent via dedup=. The runtime calls it before the handler and
    # skips the handler when it returns False. The default is an in-memory
    # window, KV is the durable drop-in that survives a restart.
    store = laser.kv(namespace)

    async def observe(key: str) -> bool:
        try:
            if await store.get(key) is not None:
                print(f"  dedup: duplicate {key}, skipping")
                return False
            await store.set(key).payload(b"1").ttl(ttl).send()
        except ls.LaserError:
            # Fail open: on a store error, process rather than silently drop.
            return True
        return True

    return observe


async def ingest_tickets(laser, total: int, chunk: int) -> None:
    # Publish `total` tickets in `chunk`-sized batches. Every field rides as an
    # indexed header and the JSON body is inlined, so LaserData Cloud materializes
    # a fully queryable ticket table while the log keeps the raw bytes.
    rng = _common.Rng(7)
    ts = 1_900_000_000_000_000
    published = 0
    while published < total:
        size = min(chunk, total - published)
        request = laser.publish_batch(TICKETS_TOPIC).inline_payload()
        for index in range(size):
            ts += rng.below(60_000_000)
            request = request.add_json(
                {
                    "ticket_id": f"t-{published + index:08}",
                    "message_type": "ticket_opened",
                    "customer": rng.pick(CUSTOMERS),
                    "component": rng.pick(COMPONENTS),
                    "severity": rng.pick(SEVERITIES),
                    "status": "open" if rng.below(100) < 80 else "resolved",
                    "ts": ts,
                }
            )
        await request.send()
        published += size
    print(f"  ingested {total} tickets in batches of {chunk}")


async def backlog_snapshot(laser) -> None:
    # The questions an on-call asks first, straight off the materialized index.
    by_severity = await (
        laser.query(TICKETS_TOPIC)
        .filter_eq("status", "open")
        .count()
        .group_by(["severity"])
        .fetch()
    )
    for row in by_severity.rows:
        print(f"  open {row.headers.get('severity', '?')} tickets: {row.headers.get('count', '?')}")


async def seed_memory(laser) -> None:
    # Register the memory projection (vector field for semantic recall) and
    # remember every past resolution note, embedded.
    await laser.ensure_topic(MEMORY_TOPIC, _common.PARTITIONS)
    await laser.register_projection(
        {
            "id": MEMORY_PROJECTION,
            "name": "concierge_memory",
            "version": 1,
            "content_type": "json",
            "extraction": {
                "fields": [
                    {"name": "memory_id", "pointer": "/memory_id"},
                    {"name": "conversation_id", "pointer": "/conversation_id"},
                    {"name": "agent_id", "pointer": "/agent_id"},
                ],
                "vector_field": "/embedding",
                "inline_payload": True,
            },
            "inline_payload_default": True,
        }
    )
    await laser.apply_binding(
        {
            "source": {"stream": laser.stream, "topic": MEMORY_TOPIC},
            "allowed_projections": [MEMORY_PROJECTION],
            "default_projection": MEMORY_PROJECTION,
            "targets": [
                {
                    "backend": "embedded",
                    "table": MEMORY_TOPIC,
                    "role": "read_write",
                    "delivery": "effectively_once",
                    "required": True,
                }
            ],
        }
    )
    memory = laser.query_memory(embed, MEMORY_TOPIC)
    conversation = ls.new_conversation_id()
    for note in RESOLUTION_NOTES:
        await memory.remember(note, conversation=conversation)
    await _common.wait_for_projection(laser, MEMORY_TOPIC, len(RESOLUTION_NOTES))


async def remember_resolution(laser, diagnosis: str) -> None:
    # Close the memory loop: what this incident taught the desk becomes a note
    # the next incident recalls.
    memory = laser.query_memory(embed, MEMORY_TOPIC)
    await memory.remember(f"checkout slowdowns: {diagnosis}", conversation=ls.new_conversation_id())
    print("  remembered the resolution for the next incident")


async def send_credits(laser, conversation: str, credits) -> None:
    for key, customer, cents in credits:
        provenance = ls.Provenance(conversation_id=conversation, idempotency_key=key)
        await laser.send_agent(
            ls.Topics.COMMANDS,
            json.dumps({"customer": customer, "cents": cents}).encode(),
            provenance,
        )


async def coordination_demo(laser) -> None:
    # Three coordination primitives on one connection: optimistic concurrency
    # (compare-and-swap), read-your-writes consistency, and the unified result
    # space that classifies any outcome. Where a backend does not serve one, the
    # error's classifier flags say why and we log it rather than failing, the
    # exact branch a real client uses to adapt, never a silent fallback.
    ledger = laser.kv("concierge_ledger")
    account = "acct:demo"

    try:
        version = await ledger.set(account).payload(b"0").expect_absent().commit()
        print(f"  seeded the credit ledger at version {version} (compare-and-swap)")
    except ls.LaserError as error:
        if getattr(error, "version_conflict", False):
            print("  ledger already seeded by a concurrent writer")
        else:
            print("  compare-and-swap not served here, skipping the demo")
            return

    # A read-modify-CAS loop: the race-safe way two agents apply credits to the
    # same balance. On a version conflict, re-read and retry, anything else is a
    # real error. Bounded retries, and exhausting them is a failure we surface.
    applied = False
    for attempt in range(5):
        entry = await ledger.get_entry(account)
        if entry is None:
            raise ls.InvalidError("ledger entry vanished after it was seeded")
        balance = int(bytes(entry.value).decode())
        try:
            version = await (
                ledger.set(account)
                .payload(str(balance + 25))
                .expect_version(entry.version)
                .commit()
            )
            print(f"  applied a credit via compare-and-swap, balance {balance + 25}")
            applied = True
            break
        except ls.LaserError as error:
            if getattr(error, "version_conflict", False):
                print(f"  lost the compare-and-swap race on attempt {attempt}, retrying")
                continue
            raise
    if not applied:
        raise ls.InvalidError("credit not applied after 5 compare-and-swap attempts")

    # A read-your-writes query: read at a level that waits for the projector to
    # catch up instead of racing it. A stale outcome is retryable and distinct
    # from an unsupported level, and the unified result space tells them apart.
    try:
        result = await laser.query(TICKETS_TOPIC).read_your_writes().limit(1).fetch()
        print(f"  read-your-writes query served fresh ({len(result.rows)} row)")
    except ls.LaserError as error:
        if getattr(error, "stale", False):
            print("  projector still catching up (stale): a real client retries")
        else:
            print("  read-your-writes not served here")


async def speculative_bulk_resolve(laser) -> None:
    # What-if remediation without touching the trunk: fork the read model, mark
    # the open critical checkout tickets resolved in the overlay, compare the
    # backlogs, then log the verdict. The fork stays open by default so it shows
    # up in LaserData Cloud. Set LASER_APPLY_PLAN=1 to act on the verdict.
    criticals = await (
        laser.query(TICKETS_TOPIC)
        .filter_eq("severity", "critical")
        .filter_eq("status", "open")
        .filter_eq("component", "checkout")
        .limit(10)
        .fetch()
    )
    if not criticals.rows:
        print("  no open critical checkout tickets to plan against")
        return

    fork = laser.fork(TRIAGE_FORK)
    # A previous run may have left the fork open. Clear it for a fresh overlay.
    try:
        await fork.squash()
    except ls.LaserError:
        pass
    await fork.create()
    for row in criticals.rows:
        if row.partition is None or row.offset is None:
            continue
        await (
            fork.put_row(TICKETS_TOPIC, row.partition, row.offset)
            .field("status", "resolved")
            .send()
        )

    forked_open = await (
        laser.query(TICKETS_TOPIC)
        .fork(TRIAGE_FORK)
        .filter_eq("severity", "critical")
        .filter_eq("status", "open")
        .filter_eq("component", "checkout")
        .count()
        .fetch()
    )
    cleared = _scalar(forked_open) == "0"

    if not _env_bool("LASER_APPLY_PLAN"):
        print(
            f"  plan staged in fork '{TRIAGE_FORK}' (clears the backlog: {cleared}), left open so "
            f"LaserData Cloud shows it. Apply the verdict with LASER_APPLY_PLAN=1"
        )
        return
    if cleared:
        applied = await fork.promote()
        print(f"  plan clears the backlog, promoted {applied} row(s) to the trunk")
    else:
        await fork.squash()
        print(f"  plan does not clear the backlog, squashed '{TRIAGE_FORK}', trunk unchanged")


async def recover_incident(laser) -> dict:
    # Rebuild the incident by folding its steps off the Responses log. This is the
    # recovery and audit path: state lives in the stream, so any agent can
    # reconstruct it with no side database.
    recovered = {"diagnosis": "", "findings": []}
    for message in await laser.reader(ls.Topics.RESPONSES).poll():
        try:
            step = message.json()
        except ls.CodecError:
            continue
        if isinstance(step, dict) and "findings" in step:
            if step.get("diagnosis"):
                recovered["diagnosis"] = step["diagnosis"]
            recovered["findings"].extend(step.get("findings", []))
    return recovered


def _env_bool(name: str) -> bool:
    return _common._env(name).lower() in ("1", "true", "yes", "on")


async def main() -> None:
    laser = await _common.connect(EXAMPLE)
    phase("warming up")
    await laser.bootstrap(_common.PARTITIONS)
    await laser.ensure_topic(TICKETS_TOPIC, _common.PARTITIONS)

    total = _common.messages(2_000)
    chunk = _common.batch(200)
    phase("ingesting the ticket firehose (the desk's world model)")
    await ingest_tickets(laser, total, chunk)

    caps = await laser.capabilities()
    if not _common.managed_gate(caps.query, "the agentic concierge desk", EXAMPLE):
        return

    phase("registering the index and waiting for the projector")
    await _common.start_projector(
        laser,
        TICKETS_TOPIC,
        ["ticket_id", "message_type", "customer", "component", "severity", "status", "ts"],
    )
    await _common.wait_for_projection(laser, TICKETS_TOPIC, total)
    await backlog_snapshot(laser)

    phase("seeding semantic memory with past resolutions")
    await seed_memory(laser)

    phase("spawning the desk: triage, specialist, resolver, approver")
    llm = default_llm()
    # Run-scoped namespaces so reruns never read each other's state.
    run = ls.new_conversation_id()
    dedup_namespace = f"concierge-dedup-{run}"
    credits_namespace = f"concierge-credits-{run}"
    triage = laser.spawn_agent(
        "triage",
        ls.Topics.COMMANDS,
        make_triage(llm, TICKETS_TOPIC),
        respond_on=ls.Topics.RESPONSES,
        poll_interval_ms=10,
    )
    specialist = laser.spawn_agent(
        "specialist",
        ls.Topics.TOOL_CALLS,
        make_specialist(llm),
        respond_on=ls.Topics.TOOL_RESULTS,
        poll_interval_ms=10,
    )
    approver_agent = laser.spawn_agent(
        "approver",
        ls.Topics.HUMAN_INPUT,
        approver,
        respond_on=ls.Topics.RESPONSES,
        poll_interval_ms=10,
    )
    resolver = laser.spawn_agent(
        "resolver",
        ls.Topics.COMMANDS,
        make_resolver(credits_namespace),
        poll_interval_ms=10,
        dedup=make_kv_deduplicator(laser, dedup_namespace, DEDUP_TTL),
    )
    agents = [triage, specialist, approver_agent, resolver]
    for agent in agents:
        await agent.ready()

    try:
        phase("triaging the incident through the desk")
        incident = ls.new_conversation_id()
        print(f"  incident on conversation {incident}: {INCIDENT}")
        reply = await laser.request(
            ls.Topics.COMMANDS,
            ls.Topics.RESPONSES,
            INCIDENT.encode(),
            ls.Provenance(conversation_id=incident),
            timeout_secs=DESK_TIMEOUT,
        )
        diagnosed = reply.json()
        print(f"  diagnosis: {diagnosed['diagnosis']}")

        phase("executing remediation credits effectively once")
        # Send the credit list twice. The KV deduplicator keyed on each credit's
        # idempotency key makes the redelivery a no-op, so the totals stay exact.
        await send_credits(laser, incident, CREDITS)
        await send_credits(laser, incident, CREDITS)
        deadline = time.monotonic() + 45.0
        store = laser.kv(credits_namespace)
        while time.monotonic() < deadline:
            # A list comprehension, not a generator: each `await` is evaluated
            # here, where `all` then sees plain bools (an async generator is not
            # iterable by `all`).
            applied = [
                await _read_u64(store, customer) >= total_cents
                for customer, total_cents in CREDIT_TOTALS.items()
            ]
            if all(applied):
                break
            await asyncio.sleep(0.25)
        for customer, expected in CREDIT_TOTALS.items():
            actual = await _read_u64(store, customer)
            if actual != expected:
                raise ls.InvalidError(
                    f"credits were not effectively once: {customer}={actual}, want {expected}"
                )
        print(f"  credits applied exactly once despite the redelivery ({credits_namespace})")

        phase("optimistic concurrency, read-your-writes, and the unified result space")
        await coordination_demo(laser)

        if caps.forks:
            phase("speculating a bulk-resolve plan in a fork")
            await speculative_bulk_resolve(laser)
        else:
            print("  read-model forks unavailable here, skipping the speculative plan")

        phase("remembering this resolution for the next incident")
        await remember_resolution(laser, diagnosed["diagnosis"])

        phase("rebuilding the incident from the log alone (the audit trail)")
        recovered = await recover_incident(laser)
        print(
            f"  recovered from the log: {len(recovered['findings'])} findings, "
            f"diagnosis intact: {bool(recovered['diagnosis'])}"
        )
    finally:
        for agent in agents:
            await agent.shutdown()
    phase("done")


if __name__ == "__main__":
    asyncio.run(main())
