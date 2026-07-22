import time
from pathlib import Path

import laser_sdk as ls
import pytest
from pytest_bdd import given, parsers, scenarios, then, when

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "governance.feature"))


class BlockNeedle:
    """The reference policy of the governance scenarios: block any payload
    starting with the configured needle, allow everything else."""

    def __init__(self, needle):
        self.needle = needle.encode()

    async def decide(self, action):
        if action.payload.startswith(self.needle):
            return ls.ActionDecision.block("blocked by policy")
        return ls.ActionDecision.allow()


@given(parsers.parse('the laser is governed by a policy that blocks "{needle}" in "{mode}" mode'))
def govern_the_laser(world, needle, mode):
    world.governed = world.laser.with_governor(BlockNeedle(needle), mode=mode)


@when(parsers.parse('I send a governed agent command "{payload}"'))
def send_governed(world, payload):
    provenance = ls.Provenance(conversation_id=world.conversation)
    world.capture(
        lambda: world.governed.send_agent(ls.Topics.COMMANDS, payload.encode(), provenance)
    )


@when(parsers.parse('I publish a governed business record "{payload}"'))
def publish_governed(world, payload):
    world.run(lambda: world.governed.topic("business.audit").ensure(partitions=1))
    provenance = ls.Provenance(conversation_id=world.conversation)
    world.capture(
        lambda: (
            world.governed.topic("business.audit")
            .publish()
            .provenance(provenance)
            .payload(payload.encode())
            .send()
        )
    )


@then("the send is rejected by policy")
def rejected_by_policy(world):
    assert isinstance(world.error, ls.PolicyBlockedError)
    assert not world.error.retryable


@then(parsers.parse('the audit topic records a "{decision}" decision with outcome "{outcome}"'))
def audit_records(world, decision, outcome):
    # Evidence lands asynchronously with the send, so poll briefly.
    deadline = time.monotonic() + 10
    while True:
        messages = world.run(
            lambda: world.laser.assemble_context(world.conversation, topics=[ls.Topics.AUDIT])
        )
        for message in messages:
            envelope = message.envelope
            if not envelope or envelope.get("operation") != "policy_decision":
                continue
            evidence = ls.PolicyEvidence.decode(bytes(message.agdx_body))
            if evidence.decision == decision and evidence.outcome == outcome:
                assert len(evidence.receipt_digest) == 64
                return
        if time.monotonic() > deadline:
            pytest.fail(f"no `{decision}` decision with outcome `{outcome}` on the audit topic")
        time.sleep(0.2)
