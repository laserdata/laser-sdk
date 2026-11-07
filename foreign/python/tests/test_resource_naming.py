import laser_sdk as ls
import pytest

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_given_a_default_stream_when_querying_an_index_then_should_send_the_scoped_index(
    laser,
):
    stream = laser.default_stream
    query = laser.query("readings").fork("trial").into_query()
    assert query["target"] == {"kind": "operational", "index": f"stream:{stream}/readings"}
    assert query["fork"] == f"stream:{stream}/trial"


@pytest.mark.asyncio
async def test_given_an_operational_target_dict_when_querying_then_should_scope_its_index(laser):
    stream = laser.default_stream
    query = laser.query_target({"kind": "operational", "index": "readings"}).into_query()
    assert query["target"] == {"kind": "operational", "index": f"stream:{stream}/readings"}


@pytest.mark.asyncio
async def test_given_an_already_scoped_name_when_querying_then_should_send_it_unchanged(laser):
    stream = laser.default_stream
    query = laser.query(f"stream:{stream}/readings").fork(f"stream:{stream}/trial").into_query()
    assert query["target"] == {"kind": "operational", "index": f"stream:{stream}/readings"}
    assert query["fork"] == f"stream:{stream}/trial"


@pytest.mark.asyncio
async def test_given_another_streams_name_when_querying_then_should_stay_in_the_handle_stream(
    laser,
):
    stream = laser.default_stream
    query = laser.query("stream:other/readings").fork("stream:other/trial").into_query()
    assert query["target"] == {
        "kind": "operational",
        "index": f"stream:{stream}/stream:other/readings",
    }
    assert query["fork"] == f"stream:{stream}/stream:other/trial"
    assert laser.resource_name("stream:other/sessions") == f"stream:{stream}/stream:other/sessions"
    assert laser.with_resource_naming("bare").resource_name("stream:other/x") == "stream:other/x"
    assert laser.with_default_stream("other").resource_name("stream:other/x") == "stream:other/x"


@pytest.mark.asyncio
async def test_given_a_slashed_default_stream_when_a_managed_call_runs_then_should_raise_invalid(
    laser,
):
    managed = laser.with_capabilities(managed=True, kv=True)
    slashed = managed.with_default_stream("tenants/acme")
    with pytest.raises(ls.InvalidError, match="contains `/`"):
        await slashed.kv("profiles").get(b"user-1")


@pytest.mark.asyncio
async def test_given_bare_naming_when_querying_then_should_send_names_as_written(laser):
    bare = laser.with_resource_naming("bare")
    query = bare.query("readings").fork("trial").into_query()
    assert query["target"] == {"kind": "operational", "index": "readings"}
    assert query["fork"] == "trial"
    target = bare.query_target({"kind": "operational", "index": "readings"}).into_query()
    assert target["target"] == {"kind": "operational", "index": "readings"}


@pytest.mark.asyncio
async def test_given_a_scoped_name_when_reading_handle_names_then_should_report_the_local_name(
    laser,
):
    stream = laser.default_stream
    kv = laser.kv(f"stream:{stream}/sessions")
    assert kv.namespace == "sessions"
    assert kv.resource_namespace == f"stream:{stream}/sessions"
    fork = laser.fork(f"stream:{stream}/trial")
    assert fork.id == "trial"
    assert fork.resource_id == f"stream:{stream}/trial"
    plain = laser.kv("sessions")
    assert plain.namespace == "sessions"
    assert plain.resource_namespace == f"stream:{stream}/sessions"


@pytest.mark.asyncio
async def test_given_a_put_row_with_a_nul_field_when_sent_then_should_refuse_it_before_the_wire(
    laser,
):
    fork = laser.fork("trial")
    for put in (
        fork.put_row("readings", 0, 1).field("status", "o\x00k"),
        fork.put_row("readings", 0, 1).metadata("ro\x00le", "user"),
        fork.put_row("readings", 0, 1).projection("reading\x00", 1),
    ):
        with pytest.raises(ls.InvalidError, match="NUL"):
            await put.send()
