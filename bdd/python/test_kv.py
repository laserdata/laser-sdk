from pathlib import Path

from pytest_bdd import given, parsers, scenarios, then, when
from reference import KvEngine

SCENARIOS = Path(__file__).parent.parent / "scenarios"
scenarios(str(SCENARIOS / "kv_cas.feature"))

# A fixed logical clock for the deterministic-expiry scenarios. Steps that care
# about expiry pass an absolute micros value relative to this base.
NOW = 1_000


@given("an empty KV store")
def open_store(bench):
    bench.kv_engine = KvEngine()


@given(parsers.parse('key "{key}" holds "{value}"'))
def seed_key(bench, key, value):
    bench.kv_engine.set(key.encode(), value.encode(), None, NOW)


@given(parsers.parse('key "{key}" holds "{value}" expiring at {expiry:d}'))
def seed_key_expiring(bench, key, value, expiry):
    bench.kv_engine.set(key.encode(), value.encode(), expiry, NOW)


@when(parsers.parse('I create "{key}" with "{value}" if absent'))
def cas_absent(bench, key, value):
    bench.last_cas = bench.kv_engine.cas(key.encode(), value.encode(), "absent", None, NOW)


@when(parsers.parse('I create "{key}" with "{value}" if absent at {now:d}'))
def cas_absent_at(bench, key, value, now):
    bench.last_cas = bench.kv_engine.cas(key.encode(), value.encode(), "absent", None, now)


@when(parsers.parse('I swap "{key}" to "{value}" expecting version {version:d}'))
def cas_match(bench, key, value, version):
    bench.last_cas = bench.kv_engine.cas(key.encode(), value.encode(), version, None, NOW)


@when(parsers.parse('I swap "{key}" to "{value}" expecting version {version:d} at {now:d}'))
def cas_match_at(bench, key, value, version, now):
    bench.last_cas = bench.kv_engine.cas(key.encode(), value.encode(), version, None, now)


@then(parsers.parse("the swap commits version {version:d}"))
def then_commits(bench, version):
    assert not bench.last_cas.is_conflict, "expected a commit, got a conflict"
    assert bench.last_cas.committed == version


@then("the swap conflicts because the key is absent")
def then_conflict_absent(bench):
    assert bench.last_cas.is_conflict
    assert bench.last_cas.conflict_current is None


@then(parsers.parse("the swap conflicts with current version {current:d}"))
def then_conflict_current(bench, current):
    assert bench.last_cas.is_conflict
    assert bench.last_cas.conflict_current == current
