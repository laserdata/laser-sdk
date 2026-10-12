from pathlib import Path

import laser_sdk as ls
from pytest_bdd import given, parsers, scenarios, then, when

# The Background steps live in conftest.py (shared across features).
SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "capabilities.feature"))
scenarios(str(SCENARIOS / "capabilities_injected.feature"))


@when(parsers.parse("a prepared release receives managed result code {code:d}"))
def prepared_release_error(world, code):
    words = {
        5: "conflict",
        18: "target_unavailable",
        10: "forbidden",
        2: "not_found",
        7: "version_skew",
        19: "pending",
        20: "plane_unavailable",
    }
    if code in words:
        word = words[code].encode()
        encoded_code = bytes([0x60 + len(word)]) + word
    else:
        encoded_code = b"\xa1\x6cunrecognized\x18" + bytes([code])
    reply = b"\xa2\x64code" + encoded_code + b"\x67message\x67refused"

    class CommandReply:
        async def send(self, command, frame):
            return reply

        async def reset(self):
            pass

    client = ls.FencedLeaseClient(CommandReply())
    operation = client.prepare_release(
        {"v": 1, "namespace": "coord", "key": b"lease", "lease_token": 1, "holder_id": "worker"}
    )
    try:
        world.run(lambda: client.release(operation))
    except ls.CommandError as error:
        assert not error.ambiguous_mutation
        world.error = error
    else:
        raise AssertionError("the command must be refused")


@then(parsers.parse("the command error keeps code {code:d} and retryability {retryable}"))
def command_error_classifiers(world, code, retryable):
    names = {
        5: "Conflict",
        18: "TargetUnavailable",
        10: "Forbidden",
        2: "NotFound",
        7: "VersionSkew",
        19: "Pending",
        20: "PlaneUnavailable",
    }
    assert world.error.code == names.get(code, f"Unrecognized({code})")
    assert world.error.retryable is (retryable == "true")


@given("a managed-query connection that does not advertise read-your-writes")
def inject_eventual_query(world):
    world.laser = world.run(
        lambda: world.laser.with_capabilities(
            managed=True,
            query=True,
            query_consistency="eventual",
        )
    )


@when("I read the negotiated capabilities")
def read_capabilities(world):
    world.caps = world.run(lambda: world.laser.capabilities())


@then("managed query is unavailable")
def query_unavailable(world):
    assert world.caps.query.available is False


@then("managed key-value is unavailable")
def kv_unavailable(world):
    assert world.caps.kv.available is False


@then("forks are unavailable")
def forks_unavailable(world):
    assert world.caps.forks is False


@then("the coordination features are unavailable")
def coordination_unavailable(world):
    assert world.caps.kv.cas is False
    # Consistency is one ordered level. Raw Apache Iggy serves only the weakest.
    assert world.caps.query.consistency == "eventual"


@when(parsers.parse('I run a query against topic "{topic}"'))
def run_query(world, topic):
    world.capture(lambda: world.laser.query(topic).fetch())


@when(parsers.parse('I run a read-your-writes query against topic "{topic}"'))
def run_ryw_query(world, topic):
    world.capture(lambda: world.laser.query(topic).read_your_writes().fetch())


@when(
    parsers.parse('I compare-and-swap key "{key}" in namespace "{namespace}" expecting it absent')
)
def compare_and_swap(world, key, namespace):
    world.capture(lambda: world.laser.kv(namespace).set(key).bytes(b"x").expect_absent().commit())


@when(
    parsers.parse(
        'I send a set of key "{key}" in namespace "{namespace}" expecting version {version:d}'
        " without commit"
    )
)
def send_set_with_precondition(world, key, namespace, version):
    world.capture(
        lambda: world.laser.kv(namespace).set(key).bytes(b"debug").expect_version(version).send()
    )


@then("the call fails as invalid")
def fails_invalid(world):
    assert isinstance(world.error, ls.InvalidError)


@then("the unified result code is invalid argument")
def unified_code_invalid_argument(world):
    assert world.error is not None
    assert world.error.code == "InvalidArgument"


@then("the call fails as unsupported")
def fails_unsupported(world):
    assert isinstance(world.error, ls.UnsupportedError)


@then("the unified result code is unsupported")
def unified_code_unsupported(world):
    assert world.error is not None
    assert world.error.code == "Unsupported"
